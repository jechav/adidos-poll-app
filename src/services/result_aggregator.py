"""Result aggregation from Redis vote counters (I-009).

Turns the live `cache:poll:{poll_id}:answer:{answer_id}` counters I-008's
vote processor `INCR`s after each durable vote write into the numbers a
client actually wants: total votes and a percentage breakdown per answer.
This module owns computation only — it is read-only with respect to the
counters (written exclusively by I-008) and does not implement the
PostgreSQL fallback (I-010) or the HTTP endpoint that serves results
(I-011); both build on top of this module instead of reimplementing its
rounding rule.

Redis errors (connection failure, timeout) are deliberately not caught
here — they propagate to the caller. I-011 is responsible for catching
them and falling back to I-010's `vote_counts` table; this module's job
is correct computation when Redis is healthy, not resilience when it
isn't.
"""

from __future__ import annotations

from uuid import UUID

from src.cache.answer_cache import get_cached_answers
from src.cache.redis_client import mget_pipelined
from src.schemas.results import AggregatedResult, AnswerResult

# Matches `vote_counts.percentage`'s `DECIMAL(5,2)` column (I-001) so the
# Redis path (this module) and the PostgreSQL fallback (I-010) never
# visibly disagree when a client fails over between them.
PERCENTAGE_DECIMAL_PLACES = 2


def _percentage(count: int, total_votes: int) -> float:
    """Deterministic, independently-rounded percentage for one answer.

    `total_votes == 0` always returns `0.0` (never `NaN` or a
    `ZeroDivisionError`) so a brand-new active poll with no votes yet
    renders cleanly.

    Each answer's percentage is rounded independently to
    `PERCENTAGE_DECIMAL_PLACES`, so the two answers' percentages may sum
    to something other than exactly 100.00 (e.g. 99.98-100.02) rather
    than being corrected to sum exactly — this is intentional (matches
    the `VoteCount` invariant in DOMAIN_MODEL.md: "Percentages sum to
    100% (or <=100% if rounding)") and I-010 must use this exact same
    rule so the two paths never disagree.
    """
    if total_votes == 0:
        return 0.0
    return round((count / total_votes) * 100, PERCENTAGE_DECIMAL_PLACES)


def _counter_key(poll_id: UUID, answer_id) -> str:
    return f"cache:poll:{poll_id}:answer:{answer_id}"


def _build_result(
    poll_id: UUID, answers: list[dict], counts: list[int]
) -> AggregatedResult:
    total_votes = sum(counts)
    answer_results = [
        AnswerResult(
            answer_id=answer["answer_id"],
            text=answer["text"],
            vote_count=count,
            percentage=_percentage(count, total_votes),
        )
        for answer, count in zip(answers, counts)
    ]
    return AggregatedResult(poll_id=poll_id, total_votes=total_votes, answers=answer_results)


async def compute_poll_results(poll_id: UUID, redis) -> AggregatedResult:
    """Total votes and per-answer count + percentage for a single poll.

    One Redis round trip beyond the (usually cache-hit) answers lookup:
    both of the poll's counter keys are read via a single pipelined
    `MGET`-equivalent (`mget_pipelined`), not two sequential `GET`s.
    Missing/expired counter keys come back as `None` from Redis and are
    coerced to `0` before summing — identical treatment to a counter key
    that exists and is `0`.
    """
    answers = await get_cached_answers(poll_id, redis)
    keys = [_counter_key(poll_id, answer["answer_id"]) for answer in answers]

    raw_counts = await mget_pipelined(redis, keys)
    counts = [int(c) if c is not None else 0 for c in raw_counts]

    return _build_result(poll_id, answers, counts)


async def compute_poll_results_batch(
    poll_ids: list[UUID], redis
) -> dict[UUID, AggregatedResult]:
    """`compute_poll_results` for N polls, with only one `MGET` round
    trip for *all* of their vote counters combined.

    GET /v1/polls (I-011) renders a list of polls, not one; calling
    `compute_poll_results` per poll would mean N separate counter round
    trips for N polls in the response. Here every poll's counter keys are
    flattened into a single flat list up front, fetched in one
    `mget_pipelined` call, then regrouped back by poll_id — so this
    function's counter-reading cost is O(1) round trips regardless of how
    many polls are requested.

    Answer-metadata lookups (`get_cached_answers`) are not folded into
    that same round trip: they're a separate, near-always-cache-hit GET
    per poll (1h TTL, populated lazily), not the hot counter path this
    function's single-round-trip guarantee is about.
    """
    if not poll_ids:
        return {}

    answers_by_poll = {
        poll_id: await get_cached_answers(poll_id, redis) for poll_id in poll_ids
    }

    all_keys: list[str] = []
    key_ranges: dict[UUID, tuple[int, int]] = {}
    for poll_id in poll_ids:
        start = len(all_keys)
        all_keys.extend(
            _counter_key(poll_id, answer["answer_id"])
            for answer in answers_by_poll[poll_id]
        )
        key_ranges[poll_id] = (start, len(all_keys))

    raw_counts = await mget_pipelined(redis, all_keys)

    results: dict[UUID, AggregatedResult] = {}
    for poll_id in poll_ids:
        start, end = key_ranges[poll_id]
        counts = [int(c) if c is not None else 0 for c in raw_counts[start:end]]
        results[poll_id] = _build_result(poll_id, answers_by_poll[poll_id], counts)

    return results
