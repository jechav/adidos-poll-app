"""User-facing v1 routes (I-003).

`GET /v1/polls` (I-011) is real: it reads poll metadata from PostgreSQL
(`fetch_polls_page`), overlays live vote counts via I-009's
`compute_poll_results_batch` in a single Redis round trip, and falls back
to I-010's `vote_counts` table (`fetch_vote_counts_fallback`) when Redis
is unavailable — never a 503 for that reason alone. `POST /v1/vote`
(I-005) sequences poll/answer validation, the rate limiter (I-007), the
uniqueness reservation (I-006), and the queue hand-off (I-008 drains it)
— it does not reimplement any of those, only composes them. `GET
/v1/user/votes` (I-012) is also real: it deliberately bypasses I-009's
Redis aggregation path (this is a low-volume, per-user lookup, not the
hot aggregate path) and reads the caller's own votes directly from the
single Postgres shard `user.user_id` hashes to.
"""

from typing import Literal
from uuid import uuid4

from fastapi import APIRouter, Depends, Query, Request, status
from redis.asyncio.cluster import RedisCluster
from redis.exceptions import RedisClusterException, RedisError

from src.api.dependencies.auth import get_current_user
from src.cache.redis_client import redis_dependency
from src.db.queries.polls import fetch_polls_page, fetch_vote_counts_fallback
from src.db.queries.user_votes import get_user_votes
from src.schemas.polls import Pagination as PollPagination
from src.schemas.polls import PollListData, PollSummary, _format_ts
from src.schemas.responses import now_iso, success_envelope
from src.schemas.results import AggregatedResult
from src.schemas.user_context import UserContext
from src.schemas.user_votes import Pagination, UserVoteEntry, UserVotesData
from src.schemas.votes import VoteRequest
from src.services.bot_detection import record_vote_attempt
from src.services.polls import load_poll_and_answer, validate_vote_target
from src.services.rate_limit import check_rate_limit
from src.services.result_aggregator import compute_poll_results_batch
from src.services.uniqueness import check_and_reserve_uniqueness, release_reservation
from src.services.vote_queue import VotePayload, VoteQueueUnavailableError, enqueue_vote

router = APIRouter(prefix="/v1", tags=["user"])


@router.get("/polls")
async def list_polls(
    request: Request,
    state: Literal["active", "closed", "archived"] = "active",
    limit: int = Query(default=20, le=100),
    offset: int = Query(default=0, ge=0),
    user: UserContext = Depends(get_current_user),
    redis: RedisCluster = Depends(redis_dependency),
):
    """List polls with live results (I-011, spec stories #1, #5, #8).

    Poll metadata comes from PostgreSQL (`fetch_polls_page`); vote counts
    come from I-009's Redis path when it's healthy, or I-010's
    `vote_counts` fallback when it isn't — `meta.stale` tells the client
    which one served this response.
    """
    polls, total = await fetch_polls_page(state=state, limit=limit, offset=offset)
    poll_ids = [p.poll_id for p in polls]

    try:
        results_by_poll = await compute_poll_results_batch(poll_ids, redis)
        stale = False
    except (RedisError, RedisClusterException):
        # A connection error, timeout, or a fully-unreachable cluster
        # (RedisClusterException at node-discovery time isn't itself a
        # RedisError subclass) must never surface as a 503 here — I-010's
        # vote_counts fallback exists for exactly this case.
        results_by_poll = await fetch_vote_counts_fallback(poll_ids)
        stale = True

    empty_result = AggregatedResult(poll_id=uuid4(), total_votes=0, answers=[])
    summaries = [
        PollSummary(
            poll_id=poll.poll_id,
            question=poll.question,
            state=poll.state,
            answers=results_by_poll.get(
                poll.poll_id, empty_result
            ).answers,
            total_votes=results_by_poll.get(poll.poll_id, empty_result).total_votes,
            created_at=_format_ts(poll.created_at),
            activated_at=_format_ts(poll.activated_at),
        )
        for poll in polls
    ]
    data = PollListData(
        polls=summaries,
        pagination=PollPagination(limit=limit, offset=offset, total=total),
    )
    envelope = success_envelope(data.model_dump(mode="json"), request)
    envelope["meta"]["stale"] = stale
    return envelope


@router.post("/vote", status_code=status.HTTP_202_ACCEPTED)
async def cast_vote(
    request: Request,
    vote: VoteRequest,
    user: UserContext = Depends(get_current_user),
    redis: RedisCluster = Depends(redis_dependency),
):
    poll, answer = await load_poll_and_answer(vote.poll_id, vote.answer_id)
    validate_vote_target(poll, answer, vote.poll_id, vote.answer_id)

    client_ip = request.client.host if request.client else "unknown"

    # I-014's bot-detection counters see every attempt that reaches this
    # handler, not just ones that pass I-007's rate limiter — a burst
    # that gets individually rate-limited is itself part of the pattern
    # this issue exists to catch. Best-effort: never raises, never adds
    # a blocking read to this hot path.
    await record_vote_attempt(redis, ip=client_ip, user_id=user.user_id)

    await check_rate_limit(user_id=user.user_id, ip=client_ip)

    # I-006's Layer 1 reservation runs immediately before the enqueue, per
    # I-005's spec: the queue must never receive a vote that will be
    # rejected as a duplicate.
    await check_and_reserve_uniqueness(redis, user.user_id, vote.poll_id)

    payload = VotePayload(
        vote_id=uuid4(),
        user_id=user.user_id,
        poll_id=vote.poll_id,
        answer_id=vote.answer_id,
        requested_at=now_iso(),
    )
    try:
        await enqueue_vote(redis, payload)
    except VoteQueueUnavailableError:
        # The reservation above already succeeded; see
        # release_reservation()'s docstring for why this is worth
        # attempting and what trade-off it accepts.
        await release_reservation(redis, user.user_id, vote.poll_id)
        raise

    return success_envelope(
        {"status": "queued", "poll_id": str(vote.poll_id), "answer_id": str(vote.answer_id)},
        request,
    )


@router.get("/user/votes")
async def list_user_votes(
    request: Request,
    user: UserContext = Depends(get_current_user),
    limit: int = Query(default=20, ge=1, le=100),
    offset: int = Query(default=0, ge=0),
):
    """The caller's own voting history (I-012).

    `user.user_id` — resolved exclusively from the validated token
    (I-004) — is the *only* source of identity here; there is no
    `user_id` path/query parameter for it to be spoofed through, and no
    admin variant exists (per spec story #38, admins never see raw
    per-user vote logs). Closed/archived polls the user voted on are
    included, not filtered out — only voting itself is blocked on them,
    elsewhere (I-005/I-006).
    """
    rows, total = await get_user_votes(user.user_id, limit, offset)
    votes = [
        UserVoteEntry(
            poll_id=row.poll_id,
            question=row.question,
            poll_state=row.poll_state,
            answer_id=row.answer_id,
            answer_text=row.answer_text,
            voted_at=row.voted_at.strftime("%Y-%m-%dT%H:%M:%SZ"),
        )
        for row in rows
    ]
    data = UserVotesData(
        votes=votes, pagination=Pagination(limit=limit, offset=offset, total=total)
    )
    return success_envelope(data.model_dump(mode="json"), request)
