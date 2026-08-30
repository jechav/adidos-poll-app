"""Hourly reconciliation job (I-017), implementing user story #30: compare
I-009's live Redis vote counters against I-010's materialized/raw DB
counts and publish the worst per-poll drift as
`poll_reconciliation_drift_max_ratio`.

Run as a Kubernetes CronJob (hourly schedule — see
`docs/architecture/metrics.md`), invoking `main()` below. This job's sole
output is a metric; it has no user-facing behavior of its own, which is
why it lives here rather than in I-009 or I-010.

`worst_drift` (the single largest per-poll drift ratio observed in the
run) is what gets published, since one alert threshold compares against
one number, not a per-poll vector — per-poll detail only goes to the
structured log line (I-018), which an operator can grep if the alert
fires.
"""

from __future__ import annotations

import asyncio
import logging
from uuid import UUID

from src.cache.redis_client import get_redis
from src.db.queries.reconciliation import (
    ReconciliationPoll,
    get_active_and_recently_closed_polls,
    get_materialized_vote_counts,
)
from src.metrics.registry import set_reconciliation_drift
from src.services.result_aggregator import compute_poll_results

logger = logging.getLogger("poll_app.reconciliation")

# Per user story #30: "alert if it exceeds 1%". This job only decides
# whether a per-poll drift is worth a structured log line — the actual
# PromQL alert on the published gauge is I-019's job, not this one's.
DRIFT_ALERT_THRESHOLD = 0.01


def compute_drift(redis_total: int, db_total: int) -> float | None:
    """One poll's drift ratio, or `None` if there's nothing to compare
    against (`db_total == 0` — a brand-new poll with no durable votes
    yet must not be reported as 100% drift or raise `ZeroDivisionError`).
    """
    if db_total == 0:
        return None
    return abs(redis_total - db_total) / db_total


async def get_redis_vote_counts(poll_id: UUID, redis) -> dict:
    """`{answer_id: vote_count}` from I-009's live Redis counters, reusing
    `compute_poll_results` rather than re-reading the raw counter keys —
    this job and the results endpoint must never disagree about what a
    "live count" is.
    """
    result = await compute_poll_results(poll_id, redis)
    return {answer.answer_id: answer.vote_count for answer in result.answers}


async def _reconcile_one_poll(
    poll: ReconciliationPoll, redis, *, num_shards: int | None = None
) -> float | None:
    redis_counts = await get_redis_vote_counts(poll.poll_id, redis)
    db_counts = await get_materialized_vote_counts(poll.poll_id, num_shards=num_shards)
    redis_total = sum(redis_counts.values())
    db_total = sum(db_counts.values())

    drift = compute_drift(redis_total, db_total)
    if drift is None:
        return None

    if drift > DRIFT_ALERT_THRESHOLD:
        logger.warning(
            "reconciliation_drift_detected",
            extra={
                "poll_id": str(poll.poll_id),
                "redis_total": redis_total,
                "db_total": db_total,
                "drift": drift,
            },
        )
    return drift


async def run_reconciliation(*, num_shards: int | None = None) -> float:
    """One full run: check every active/recently-closed poll, publish the
    worst drift observed (`0.0` if there were no polls, or every poll had
    a zero DB total) as the gauge value, and return it (mainly for
    tests/observability — callers in production don't need it).

    `num_shards` is threaded through to the raw-count fallback
    (`get_materialized_vote_counts`) — see that function's docstring for
    why local dev/tests need to override it.
    """
    redis = await get_redis()
    polls = await get_active_and_recently_closed_polls()

    worst_drift = 0.0
    for poll in polls:
        drift = await _reconcile_one_poll(poll, redis, num_shards=num_shards)
        if drift is not None:
            worst_drift = max(worst_drift, drift)

    set_reconciliation_drift(worst_drift)
    return worst_drift


async def main() -> None:
    logging.basicConfig(level=logging.INFO)
    await run_reconciliation()


if __name__ == "__main__":
    asyncio.run(main())
