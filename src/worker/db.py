"""Per-shard PostgreSQL connections and vote inserts (I-008).

Coordinates with I-006's uniqueness backstop: the `UNIQUE(user_id,
poll_id)` constraint from I-001 is the correctness guarantee, and this
module's job is to make sure one duplicate row in a batch doesn't roll
back the other 499 good ones (batch insert, and on a unique violation,
fall back to inserting one row at a time and skipping the duplicate —
logged, not retried, no error surfaced, since the client already got a
202 at queue time).
"""

import logging
import os
import time

import psycopg
from psycopg import errors as pg_errors

from src.config import settings
from src.metrics.registry import record_constraint_violation
from src.worker.models import VotePayload

logger = logging.getLogger(__name__)

INSERT_VOTE_SQL = """
    INSERT INTO votes (vote_id, user_id, poll_id, answer_id)
    VALUES (%s, %s, %s, %s)
"""


class ShardUnavailableError(Exception):
    """Raised when a shard's DB can't be reached (down, timed out, refused)."""


def default_shard_dsn(shard_id: int) -> str:
    """Resolve shard N's DSN.

    Per-shard override via `POLL_APP_SHARD_{N}_DSN` (production: each
    shard is a distinct host, per docs/setup/sharding.md); falls back to
    `settings.database_url`, which is enough for local dev where a single
    Postgres instance stands in for every shard.
    """
    override = os.environ.get(f"POLL_APP_SHARD_{shard_id}_DSN")
    return override or settings.database_url


class ShardConnectionPool:
    """Lazily-connected, per-shard async connections with backoff on failure.

    A shard that's down must not be hot-looped: consecutive connection
    failures push `next_retry_at` further out (capped), so a persistently
    unreachable shard is only probed occasionally rather than on every
    batch. Other shards' pools/entries are entirely independent, so one
    shard's outage never blocks another's.
    """

    def __init__(
        self,
        dsn_resolver=default_shard_dsn,
        *,
        initial_backoff_s: float = 1.0,
        max_backoff_s: float = 30.0,
        connect_timeout_s: float = 2,
        clock=time.monotonic,
    ):
        self._dsn_resolver = dsn_resolver
        self._connect_timeout_s = connect_timeout_s
        self._initial_backoff_s = initial_backoff_s
        self._max_backoff_s = max_backoff_s
        self._clock = clock
        self._connections: dict[int, psycopg.AsyncConnection] = {}
        self._next_retry_at: dict[int, float] = {}
        self._backoff_s: dict[int, float] = {}

    async def get_connection(self, shard_id: int) -> psycopg.AsyncConnection:
        conn = self._connections.get(shard_id)
        if conn is not None and not conn.closed:
            return conn

        now = self._clock()
        next_retry_at = self._next_retry_at.get(shard_id)
        if next_retry_at is not None and now < next_retry_at:
            raise ShardUnavailableError(
                f"shard {shard_id} unavailable; backing off until {next_retry_at:.1f}"
            )

        try:
            conn = await psycopg.AsyncConnection.connect(
                self._dsn_resolver(shard_id), connect_timeout=self._connect_timeout_s
            )
        except psycopg.OperationalError as exc:
            backoff = self._backoff_s.get(shard_id, self._initial_backoff_s)
            self._next_retry_at[shard_id] = now + backoff
            self._backoff_s[shard_id] = min(backoff * 2, self._max_backoff_s)
            raise ShardUnavailableError(f"shard {shard_id} unreachable") from exc

        self._connections[shard_id] = conn
        self._next_retry_at.pop(shard_id, None)
        self._backoff_s.pop(shard_id, None)
        return conn

    async def invalidate(self, shard_id: int) -> None:
        conn = self._connections.pop(shard_id, None)
        if conn is not None:
            try:
                await conn.close()
            except Exception:
                pass


async def batch_insert_votes(
    conn: psycopg.AsyncConnection, votes: list[VotePayload]
) -> list[VotePayload]:
    """Insert the whole batch in one transaction. All-or-nothing: raises
    `UniqueViolation` (caller falls back to `insert_votes_individually`)
    or `ShardUnavailableError` if the connection drops mid-batch.
    """
    try:
        async with conn.cursor() as cur:
            for vote in votes:
                await cur.execute(
                    INSERT_VOTE_SQL,
                    (str(vote.vote_id), vote.user_id, str(vote.poll_id), str(vote.answer_id)),
                )
        await conn.commit()
    except pg_errors.UniqueViolation:
        await conn.rollback()
        raise
    except psycopg.OperationalError as exc:
        raise ShardUnavailableError("shard connection lost mid-batch") from exc
    return list(votes)


async def insert_votes_individually(
    conn: psycopg.AsyncConnection, votes: list[VotePayload], *, shard_id: int
) -> list[VotePayload]:
    """Per-row fallback after a batch-level UniqueViolation: one duplicate
    must not sink the whole batch. Duplicates are logged and skipped —
    per I-006, no retry, no client-facing error (the client already got
    a 202 when the vote was queued).

    Each skipped duplicate also increments I-017's
    `poll_db_constraint_violations_total{shard=...}` — a rate anomaly
    here (many violations/min on one shard) can mean the Redis `SET NX`
    uniqueness layer (I-006) is failing to catch duplicates before they
    reach the queue, per the metric's design in docs/issues/I-017-monitoring.md.
    """
    successful: list[VotePayload] = []
    for vote in votes:
        try:
            async with conn.cursor() as cur:
                await cur.execute(
                    INSERT_VOTE_SQL,
                    (str(vote.vote_id), vote.user_id, str(vote.poll_id), str(vote.answer_id)),
                )
            await conn.commit()
            successful.append(vote)
        except pg_errors.UniqueViolation:
            await conn.rollback()
            record_constraint_violation(shard_id)
            logger.warning(
                "duplicate_vote_at_db_layer",
                extra={
                    "vote_id": str(vote.vote_id),
                    "user_id": vote.user_id,
                    "poll_id": str(vote.poll_id),
                },
            )
            continue
        except psycopg.OperationalError as exc:
            await conn.rollback()
            raise ShardUnavailableError("shard connection lost mid-batch") from exc
    return successful
