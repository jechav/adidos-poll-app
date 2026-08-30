"""Answer metadata cache (I-009).

A poll always has exactly two answers, and answers are immutable once a
poll is active (DOMAIN_MODEL.md `Answer` invariants) — so unlike the vote
counters this module's caller reads, staleness here is not a correctness
concern, and metadata can be cached aggressively (1h TTL) rather than
invalidated on write.

`result_aggregator` needs this to know *which* two `answer_id`s belong to
a poll before it can build the `cache:poll:{poll_id}:answer:{answer_id}`
counter keys to `MGET` (see docs/architecture/redis-keys.md).

Mirrors `src/services/polls.py`'s single-shared-connection pattern: answers
are replicated to every shard (I-001), so a lookup is served from one
shared connection — no shard routing needed here, unlike votes themselves.
"""

from __future__ import annotations

import asyncio
import json
from uuid import UUID

import psycopg

from src.config import settings
from src.metrics.registry import CACHE_LABEL_ANSWERS, record_cache_hit, record_cache_miss

ANSWERS_CACHE_TTL_SECONDS = 3600

_conn: psycopg.AsyncConnection | None = None
_conn_lock = asyncio.Lock()


def answers_cache_key(poll_id: UUID) -> str:
    return f"cache:poll:{poll_id}:answers"


async def get_cached_answers(poll_id: UUID, redis) -> list[dict]:
    """Return `[{"answer_id", "text", "order"}, ...]` for `poll_id`,
    ordered by `order`.

    Read-through: a cache hit deserializes the cached JSON directly; a
    miss loads from PostgreSQL and populates the cache with a 1h TTL
    before returning. Callers (`result_aggregator`) don't need to know
    which path was taken.
    """
    key = answers_cache_key(poll_id)
    cached = await redis.get(key)
    if cached is not None:
        record_cache_hit(CACHE_LABEL_ANSWERS)
        return json.loads(cached)

    record_cache_miss(CACHE_LABEL_ANSWERS)
    answers = await _load_answers_from_db(poll_id)
    await redis.set(key, json.dumps(answers), ex=ANSWERS_CACHE_TTL_SECONDS)
    return answers


async def _load_answers_from_db(poll_id: UUID) -> list[dict]:
    """Assumes the caller does not hold `_conn_lock`."""
    async with _conn_lock:
        conn = await _get_connection()
        async with conn.cursor() as cur:
            await cur.execute(
                'SELECT answer_id, answer_text, "order" FROM answers '
                'WHERE poll_id = %s ORDER BY "order"',
                (poll_id,),
            )
            rows = await cur.fetchall()

    return [
        {"answer_id": str(row[0]), "text": row[1], "order": row[2]} for row in rows
    ]


async def _get_connection() -> psycopg.AsyncConnection:
    """Assumes the caller already holds `_conn_lock`."""
    global _conn
    if _conn is None or _conn.closed:
        _conn = await psycopg.AsyncConnection.connect(
            settings.database_url, connect_timeout=2
        )
    return _conn
