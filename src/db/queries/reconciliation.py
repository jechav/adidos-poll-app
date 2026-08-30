"""Reconciliation queries (I-017): the set of polls the hourly job checks,
and the "authoritative" DB side of the drift comparison — I-010's
materialized `vote_counts`, falling back to a live `COUNT(*) FROM votes`
fan-out when that view is stale.

Mirrors `src/db/queries/polls.py`'s single-shared-connection pattern for
`polls`/`vote_counts` (both replicated to every shard, per I-001) — no
shard routing needed for those reads, unlike the raw `votes` fallback
below, which fans out across every shard the same way
`scripts/jobs/refresh_vote_counts.py` does.
"""

from __future__ import annotations

import asyncio
from collections import defaultdict
from dataclasses import dataclass
from datetime import timedelta
from uuid import UUID

import psycopg

from src.config import settings
from src.worker.db import ShardConnectionPool, ShardUnavailableError

# I-010's refresh job runs every 5 minutes (docs/architecture/metrics.md);
# a `vote_counts` row older than this means the last refresh cycle didn't
# land, so this job falls back to a live count instead of trusting a
# stale materialized number.
MATERIALIZED_VIEW_STALENESS_WINDOW = timedelta(minutes=5)

# Story #30 reconciles active polls plus ones that *just* closed — a poll
# closed hours ago was already reconciled on prior hourly runs and its
# counts have stopped changing, so re-checking it forever is wasted work.
RECENTLY_CLOSED_WINDOW = timedelta(hours=1)

_conn: psycopg.AsyncConnection | None = None
_conn_lock = asyncio.Lock()


@dataclass(frozen=True)
class ReconciliationPoll:
    poll_id: UUID


ACTIVE_AND_RECENTLY_CLOSED_QUERY = """
    SELECT poll_id FROM polls
    WHERE state = 'active'
       OR (state = 'closed' AND closed_at >= now() - %s::interval)
"""

# `is_fresh` is computed server-side (`now()` on the DB host) rather than
# compared against a Python-side timestamp, so this never has to reconcile
# the app server's clock/timezone against Postgres's `TIMESTAMP` (no time
# zone) column.
VOTE_COUNTS_QUERY = """
    SELECT answer_id, count, (last_updated_at >= now() - %s::interval) AS is_fresh
    FROM vote_counts
    WHERE poll_id = %s
"""

RAW_VOTE_COUNT_QUERY = """
    SELECT answer_id, COUNT(*) FROM votes WHERE poll_id = %s GROUP BY answer_id
"""


async def _get_connection() -> psycopg.AsyncConnection:
    """Assumes the caller already holds `_conn_lock`."""
    global _conn
    if _conn is None or _conn.closed:
        _conn = await psycopg.AsyncConnection.connect(
            settings.database_url, connect_timeout=2
        )
    return _conn


async def get_active_and_recently_closed_polls() -> list[ReconciliationPoll]:
    async with _conn_lock:
        conn = await _get_connection()
        async with conn.cursor() as cur:
            await cur.execute(
                ACTIVE_AND_RECENTLY_CLOSED_QUERY,
                (f"{RECENTLY_CLOSED_WINDOW.total_seconds()} seconds",),
            )
            rows = await cur.fetchall()
    return [ReconciliationPoll(poll_id=row[0]) for row in rows]


async def get_materialized_vote_counts(
    poll_id: UUID, *, num_shards: int | None = None
) -> dict[UUID, int]:
    """`{answer_id: count}` for `poll_id`, from I-010's `vote_counts` when
    every row for that poll was refreshed within the staleness window,
    otherwise a live fan-out `COUNT(*) FROM votes` across every shard.

    A poll with no `vote_counts` rows at all (never refreshed yet) is
    treated the same as "stale" — the raw fallback is correct either way.

    `num_shards` defaults to `settings.num_shards` (production: one
    physical shard per count) but can be overridden — local dev/tests
    stand in one Postgres instance for every shard (per
    `src/worker/db.py`'s `default_shard_dsn` fallback), so fanning out to
    the real shard count against that single instance would sum the same
    rows `num_shards` times over.
    """
    async with _conn_lock:
        conn = await _get_connection()
        async with conn.cursor() as cur:
            await cur.execute(
                VOTE_COUNTS_QUERY,
                (f"{MATERIALIZED_VIEW_STALENESS_WINDOW.total_seconds()} seconds", str(poll_id)),
            )
            rows = await cur.fetchall()

    if rows and all(is_fresh for _answer_id, _count, is_fresh in rows):
        return {answer_id: count for answer_id, count, _is_fresh in rows}

    return await _raw_vote_counts_fallback(poll_id, num_shards=num_shards)


async def _raw_vote_counts_fallback(
    poll_id: UUID, *, num_shards: int | None = None
) -> dict[UUID, int]:
    """Fan out `COUNT(*) FROM votes WHERE poll_id = ...` to every shard
    and sum: `votes` is sharded by `user_id` (I-008), so no single shard
    holds a poll's full count. Mirrors
    `scripts/jobs/refresh_vote_counts.py`'s fan-out/merge shape; a shard
    that's unreachable is skipped (logged nowhere here — a permanently
    undercounted total simply shows up as drift, which is exactly what
    this job exists to surface).
    """
    pool = ShardConnectionPool()
    merged: dict[UUID, int] = defaultdict(int)

    async def read_shard(shard_id: int) -> list[tuple[UUID, int]]:
        try:
            conn = await pool.get_connection(shard_id)
        except ShardUnavailableError:
            return []
        async with conn.cursor() as cur:
            await cur.execute(RAW_VOTE_COUNT_QUERY, (str(poll_id),))
            return await cur.fetchall()

    shard_count = num_shards if num_shards is not None else settings.num_shards
    per_shard = await asyncio.gather(
        *(read_shard(shard_id) for shard_id in range(shard_count))
    )
    for rows in per_shard:
        for answer_id, count in rows:
            merged[answer_id] += count
    return dict(merged)
