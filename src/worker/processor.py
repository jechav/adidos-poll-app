"""Batch write + cache update + shard-down failover (I-008).

`process_batch` is the point where "queued" becomes "durable": it writes
a shard's slice of a batch to Postgres, then increments the Redis result
cache for exactly the rows that committed. The increment always happens
*after* the DB commit, never before — the cache must never show a vote
that isn't durably recorded yet.

If the shard is unreachable, the batch is requeued onto `queue:votes`
rather than dropped or retried in a hot loop against a dead connection —
per SPECIFICATION.md's failover decision and user story #34.

I-018: successful writes log `vote_written` only for sampled votes
(`is_sampled`, deterministic by `vote_id`) — the worker-side leg of the
same lifecycle the API's `vote_accepted` and the worker's own
`vote_dequeued` already logged for that vote. Every rejection or failure
(a row the DB's UNIQUE constraint bounced, or a shard outage forcing a
requeue) is logged at 100%, independent of sampling, per decision #15.
"""

import structlog
from psycopg import errors as pg_errors

from src.logging.sampling import is_sampled
from src.worker.db import (
    ShardConnectionPool,
    ShardUnavailableError,
    batch_insert_votes,
    insert_votes_individually,
)
from src.worker.models import VotePayload
from src.worker.queue_consumer import QUEUE_KEY

logger = structlog.get_logger("poll_app.worker")


def _vote_context(vote: VotePayload):
    """The `request_id`/`vote_id` pair every worker-side log line for one
    vote needs bound, so `grep request_id=...` finds this line alongside
    the API's `vote_accepted` and the worker's own `vote_dequeued` for the
    same vote.
    """
    return structlog.contextvars.bound_contextvars(
        request_id=vote.request_id, vote_id=str(vote.vote_id)
    )


async def requeue(redis, votes: list[VotePayload], *, queue_key: str = QUEUE_KEY) -> None:
    for vote in votes:
        await redis.lpush(queue_key, vote.model_dump_json())
        with _vote_context(vote):
            logger.warning("vote_requeued", outcome="shard_unavailable")
    if votes:
        logger.error("shard_unavailable_requeued", count=len(votes))


async def process_batch(
    shard_id: int,
    votes: list[VotePayload],
    pool: ShardConnectionPool,
    redis,
) -> list[VotePayload]:
    """Write `votes` (already routed to `shard_id`) and update the cache.

    Returns the votes that were actually written (for tests/observability);
    callers don't need the return value in production use.
    """
    if not votes:
        return []

    try:
        conn = await pool.get_connection(shard_id)
    except ShardUnavailableError:
        await requeue(redis, votes)
        return []

    try:
        successful = await batch_insert_votes(conn, votes)
    except pg_errors.UniqueViolation:
        try:
            successful = await insert_votes_individually(conn, votes, shard_id=shard_id)
        except ShardUnavailableError:
            await pool.invalidate(shard_id)
            await requeue(redis, votes)
            return []
        _log_constraint_violations(shard_id, votes, successful)
    except ShardUnavailableError:
        await pool.invalidate(shard_id)
        await requeue(redis, votes)
        return []

    for vote in successful:
        await redis.incr(f"cache:poll:{vote.poll_id}:answer:{vote.answer_id}")
        if is_sampled(vote.vote_id):
            with _vote_context(vote):
                logger.info("vote_written", shard=shard_id, outcome="success")

    return successful


def _log_constraint_violations(
    shard_id: int, attempted: list[VotePayload], successful: list[VotePayload]
) -> None:
    """Layer 2's durable backstop (I-006) bounced a row: Layer 1's Redis
    reservation missed a duplicate that made it all the way to the
    per-row insert. Logged at 100%, independent of sampling — a
    constraint violation is a rejection, not a routine success.
    """
    successful_ids = {vote.vote_id for vote in successful}
    for vote in attempted:
        if vote.vote_id in successful_ids:
            continue
        with _vote_context(vote):
            logger.warning("vote_write_rejected", shard=shard_id, outcome="constraint_violation")
