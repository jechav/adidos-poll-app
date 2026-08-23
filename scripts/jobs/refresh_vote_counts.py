"""Cross-shard `vote_counts` refresh job (I-010).

Recomputes the materialized `vote_counts` table every 5 minutes (deployed
as a Kubernetes CronJob, see `deploy/cronjobs/refresh-vote-counts.yaml`),
so GET /v1/polls (I-011) has a durable fallback when I-009's Redis path is
unavailable.

`votes` is sharded by `user_id` (I-008), so no single shard has a poll's
full vote count — this job fans out to every shard in parallel, sums the
partial counts, and broadcasts the merged, percentage-annotated rows back
to every shard (`vote_counts` itself is a replicated reference table, not
sharded, per I-001's schema).

Percentage rounding reuses I-009's `_percentage` rather than
reimplementing it: the two paths (Redis, this fallback) must never
visibly disagree when a client fails over between them (see
`src/services/result_aggregator.py`'s docstring on this exact concern).

A shard being unreachable — on either the read or the broadcast-write
side — degrades this run's accuracy for that shard's users but does not
fail the whole job; the next 5-minute run self-heals once the shard
recovers. This matches I-008's `ShardConnectionPool`, which is reused
here rather than introducing a second shard-connection abstraction.
"""

from __future__ import annotations

import asyncio
import logging
from collections import defaultdict
from uuid import UUID

from src.services.result_aggregator import _percentage
from src.worker.db import ShardConnectionPool, ShardUnavailableError

logger = logging.getLogger(__name__)

# Zero-vote answers must still appear in `vote_counts`: starting from the
# replicated `answers` table and LEFT JOINing `votes` (rather than
# `GROUP BY` on `votes` alone) means a freshly-activated poll with no
# votes yet on this shard still produces a `vote_count = 0` row.
SHARD_COUNT_QUERY = """
    SELECT
      a.poll_id,
      a.answer_id,
      COUNT(v.vote_id) AS vote_count
    FROM answers a
    LEFT JOIN votes v ON v.answer_id = a.answer_id
    GROUP BY a.poll_id, a.answer_id
"""

UPSERT_VOTE_COUNTS_SQL = """
    INSERT INTO vote_counts (poll_id, answer_id, count, percentage, last_updated_at)
    VALUES (%s, %s, %s, %s, now())
    ON CONFLICT (poll_id, answer_id)
    DO UPDATE SET
      count = EXCLUDED.count,
      percentage = EXCLUDED.percentage,
      last_updated_at = EXCLUDED.last_updated_at
"""

ShardRow = tuple[UUID, UUID, int]


async def fetch_shard_counts(conn) -> list[ShardRow]:
    """This shard's partial `(poll_id, answer_id) -> vote_count` rows."""
    async with conn.cursor() as cur:
        await cur.execute(SHARD_COUNT_QUERY)
        rows = await cur.fetchall()
    return [(row[0], row[1], row[2]) for row in rows]


def merge_shard_results(
    per_shard_results: list[list[ShardRow] | Exception],
) -> dict[tuple[UUID, UUID], int]:
    """Sum partial per-shard counts into one global count per answer.

    A shard that raised (surfaced by `asyncio.gather(..., return_exceptions=True)`
    as an `Exception` in this list rather than a row list) is logged and
    skipped — partial results from the other shards are still merged, so
    one bad shard doesn't blank out the whole job run. `per_shard_results`
    is assumed to be in shard-id order (0..N-1), matching how
    `refresh_vote_counts` builds the `gather` call from `range(num_shards)`
    — `asyncio.gather` preserves input order, so this holds.
    """
    merged: dict[tuple[UUID, UUID], int] = defaultdict(int)
    for shard_id, shard_result in enumerate(per_shard_results):
        if isinstance(shard_result, Exception):
            logger.warning(
                "shard unreachable during vote_counts refresh",
                extra={"shard_id": shard_id},
                exc_info=shard_result,
            )
            continue
        for poll_id, answer_id, vote_count in shard_result:
            merged[(poll_id, answer_id)] += vote_count
    return dict(merged)


def compute_percentages(
    merged: dict[tuple[UUID, UUID], int],
) -> dict[tuple[UUID, UUID], tuple[int, float]]:
    """Attach each answer's percentage of its own poll's total votes.

    Uses I-009's `_percentage` unchanged (independent per-answer rounding,
    `0.0` at zero votes, no sum-to-100 correction) so this fallback path
    and the Redis path never visibly disagree.
    """
    totals_by_poll: dict[UUID, int] = defaultdict(int)
    for (poll_id, _answer_id), count in merged.items():
        totals_by_poll[poll_id] += count

    return {
        (poll_id, answer_id): (count, _percentage(count, totals_by_poll[poll_id]))
        for (poll_id, answer_id), count in merged.items()
    }


async def upsert_vote_counts(
    pool: ShardConnectionPool,
    num_shards: int,
    rows: dict[tuple[UUID, UUID], tuple[int, float]],
) -> None:
    """Broadcast the merged rows to every shard.

    `vote_counts` is replicated (every shard carries the full table), so
    the same upserts are written to all `num_shards` connections, not
    just one. A shard failing at write time — whether unreachable at
    connect time, or the `INSERT`/`commit` itself failing on a connection
    that later drops mid-write — is logged and skipped without aborting
    the other shards' writes: same availability-over-consistency posture
    as the read side.
    """
    if not rows:
        return

    async def write_to_shard(shard_id: int) -> None:
        try:
            conn = await pool.get_connection(shard_id)
            async with conn.cursor() as cur:
                for (poll_id, answer_id), (count, percentage) in rows.items():
                    await cur.execute(
                        UPSERT_VOTE_COUNTS_SQL,
                        (str(poll_id), str(answer_id), count, percentage),
                    )
            await conn.commit()
        except ShardUnavailableError:
            logger.warning(
                "shard unreachable during vote_counts broadcast write",
                extra={"shard_id": shard_id},
            )
        except Exception:
            # Anything past the connect step (e.g. the connection dropping
            # mid-`INSERT`) must not abort the other shards' writes either
            # — only `ShardUnavailableError` gets its own branch above
            # because it's the expected, already-classified failure mode.
            logger.warning(
                "shard write failed during vote_counts broadcast",
                extra={"shard_id": shard_id},
                exc_info=True,
            )

    await asyncio.gather(*(write_to_shard(shard_id) for shard_id in range(num_shards)))


async def refresh_vote_counts(pool: ShardConnectionPool, num_shards: int) -> None:
    """One full refresh cycle: fan out reads, merge, percentage, broadcast."""

    async def read_shard(shard_id: int) -> list[ShardRow]:
        conn = await pool.get_connection(shard_id)
        return await fetch_shard_counts(conn)

    per_shard_results = await asyncio.gather(
        *(read_shard(shard_id) for shard_id in range(num_shards)),
        return_exceptions=True,
    )
    merged = merge_shard_results(per_shard_results)
    rows = compute_percentages(merged)
    await upsert_vote_counts(pool, num_shards, rows)


async def main() -> None:
    from src.config import settings

    logging.basicConfig(level=logging.INFO)
    pool = ShardConnectionPool()
    await refresh_vote_counts(pool, settings.num_shards)


if __name__ == "__main__":
    asyncio.run(main())
