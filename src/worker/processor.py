"""Batch write + cache update + shard-down failover (I-008).

`process_batch` is the point where "queued" becomes "durable": it writes
a shard's slice of a batch to Postgres, then increments the Redis result
cache for exactly the rows that committed. The increment always happens
*after* the DB commit, never before — the cache must never show a vote
that isn't durably recorded yet.

If the shard is unreachable, the batch is requeued onto `queue:votes`
rather than dropped or retried in a hot loop against a dead connection —
per SPECIFICATION.md's failover decision and user story #34.
"""

import logging

from psycopg import errors as pg_errors

from src.worker.db import (
    ShardConnectionPool,
    ShardUnavailableError,
    batch_insert_votes,
    insert_votes_individually,
)
from src.worker.models import VotePayload
from src.worker.queue_consumer import QUEUE_KEY

logger = logging.getLogger(__name__)


async def requeue(redis, votes: list[VotePayload], *, queue_key: str = QUEUE_KEY) -> None:
    for vote in votes:
        await redis.lpush(queue_key, vote.model_dump_json())
    if votes:
        logger.error("shard_unavailable_requeued", extra={"count": len(votes)})


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
    except ShardUnavailableError:
        await pool.invalidate(shard_id)
        await requeue(redis, votes)
        return []

    for vote in successful:
        await redis.incr(f"cache:poll:{vote.poll_id}:answer:{vote.answer_id}")

    return successful
