"""Unit tests for I-008's `ShardConnectionPool`: connection caching and
the exponential-backoff-on-failure behavior that keeps a down shard from
being hot-looped. All against a monkeypatched `psycopg.AsyncConnection.connect`
— no real Postgres needed (see tests/integration/test_vote_processor_pipeline.py
for the real-DB round-trip tests).
"""

import uuid

import psycopg
import pytest
from prometheus_client import generate_latest
from prometheus_client.parser import text_string_to_metric_families
from psycopg import errors as pg_errors

from src.metrics.registry import REGISTRY
from src.worker.db import (
    ShardConnectionPool,
    ShardUnavailableError,
    default_shard_dsn,
    insert_votes_individually,
)
from src.worker.models import VotePayload


def _violations_count(shard: str) -> float:
    text = generate_latest(REGISTRY).decode("utf-8")
    for family in text_string_to_metric_families(text):
        for s in family.samples:
            if s.name == "poll_db_constraint_violations_total" and s.labels.get("shard") == shard:
                return s.value
    return 0.0


class FakeClock:
    def __init__(self, start: float = 0.0):
        self.now = start

    def __call__(self) -> float:
        return self.now

    def advance(self, dt: float) -> None:
        self.now += dt


class FakeConn:
    def __init__(self):
        self.closed = False

    async def close(self):
        self.closed = True


@pytest.mark.asyncio
async def test_get_connection_caches_across_calls(monkeypatch):
    fake_conn = FakeConn()

    async def fake_connect(dsn, connect_timeout=2):
        return fake_conn

    monkeypatch.setattr(psycopg.AsyncConnection, "connect", fake_connect)

    pool = ShardConnectionPool(lambda shard_id: "dsn")
    first = await pool.get_connection(0)
    second = await pool.get_connection(0)

    assert first is fake_conn
    assert second is fake_conn


@pytest.mark.asyncio
async def test_get_connection_wraps_operational_error(monkeypatch):
    async def fake_connect(dsn, connect_timeout=2):
        raise psycopg.OperationalError("connection refused")

    monkeypatch.setattr(psycopg.AsyncConnection, "connect", fake_connect)

    pool = ShardConnectionPool(lambda shard_id: "dsn")
    with pytest.raises(ShardUnavailableError):
        await pool.get_connection(0)


@pytest.mark.asyncio
async def test_get_connection_backs_off_instead_of_reconnecting_immediately(monkeypatch):
    attempts = 0

    async def fake_connect(dsn, connect_timeout=2):
        nonlocal attempts
        attempts += 1
        raise psycopg.OperationalError("connection refused")

    monkeypatch.setattr(psycopg.AsyncConnection, "connect", fake_connect)

    clock = FakeClock()
    pool = ShardConnectionPool(lambda shard_id: "dsn", initial_backoff_s=1.0, clock=clock)

    with pytest.raises(ShardUnavailableError):
        await pool.get_connection(0)
    assert attempts == 1

    # No time has passed: a second call must NOT hit the DB again — this
    # is the "no hot loop against a dead connection" requirement.
    with pytest.raises(ShardUnavailableError):
        await pool.get_connection(0)
    assert attempts == 1

    clock.advance(1.01)
    with pytest.raises(ShardUnavailableError):
        await pool.get_connection(0)
    assert attempts == 2


@pytest.mark.asyncio
async def test_backoff_doubles_up_to_a_cap(monkeypatch):
    async def fake_connect(dsn, connect_timeout=2):
        raise psycopg.OperationalError("connection refused")

    monkeypatch.setattr(psycopg.AsyncConnection, "connect", fake_connect)

    clock = FakeClock()
    pool = ShardConnectionPool(
        lambda shard_id: "dsn", initial_backoff_s=1.0, max_backoff_s=3.0, clock=clock
    )

    for expected_backoff in (1.0, 2.0, 3.0, 3.0):
        with pytest.raises(ShardUnavailableError):
            await pool.get_connection(0)
        clock.advance(expected_backoff + 0.01)


@pytest.mark.asyncio
async def test_backoff_resets_after_a_successful_reconnect(monkeypatch):
    should_fail = True
    fake_conn = FakeConn()

    async def fake_connect(dsn, connect_timeout=2):
        if should_fail:
            raise psycopg.OperationalError("connection refused")
        return fake_conn

    monkeypatch.setattr(psycopg.AsyncConnection, "connect", fake_connect)

    clock = FakeClock()
    pool = ShardConnectionPool(lambda shard_id: "dsn", initial_backoff_s=1.0, clock=clock)

    with pytest.raises(ShardUnavailableError):
        await pool.get_connection(0)

    clock.advance(1.01)
    should_fail = False
    conn = await pool.get_connection(0)

    assert conn is fake_conn
    assert 0 not in pool._next_retry_at
    assert 0 not in pool._backoff_s


@pytest.mark.asyncio
async def test_other_shards_are_not_affected_by_one_shards_backoff(monkeypatch):
    async def fake_connect(dsn, connect_timeout=2):
        if dsn == "dsn-down":
            raise psycopg.OperationalError("connection refused")
        return FakeConn()

    monkeypatch.setattr(psycopg.AsyncConnection, "connect", fake_connect)

    pool = ShardConnectionPool(lambda shard_id: "dsn-down" if shard_id == 0 else "dsn-up")

    with pytest.raises(ShardUnavailableError):
        await pool.get_connection(0)

    # Shard 1 must connect fine even though shard 0 just failed.
    conn = await pool.get_connection(1)
    assert conn is not None


@pytest.mark.asyncio
async def test_invalidate_closes_and_forgets_the_connection(monkeypatch):
    fake_conn = FakeConn()

    async def fake_connect(dsn, connect_timeout=2):
        return fake_conn

    monkeypatch.setattr(psycopg.AsyncConnection, "connect", fake_connect)

    pool = ShardConnectionPool(lambda shard_id: "dsn")
    await pool.get_connection(0)

    await pool.invalidate(0)

    assert fake_conn.closed is True
    assert 0 not in pool._connections


def test_default_shard_dsn_falls_back_to_settings_database_url(monkeypatch):
    monkeypatch.delenv("POLL_APP_SHARD_0_DSN", raising=False)
    from src.config import settings

    assert default_shard_dsn(0) == settings.database_url


def test_default_shard_dsn_prefers_per_shard_override(monkeypatch):
    monkeypatch.setenv("POLL_APP_SHARD_3_DSN", "postgresql://shard-3.internal/poll_app")
    assert default_shard_dsn(3) == "postgresql://shard-3.internal/poll_app"


def _vote(user_id: str = "user-1") -> VotePayload:
    return VotePayload(
        vote_id=uuid.uuid4(), user_id=user_id, poll_id=uuid.uuid4(), answer_id=uuid.uuid4()
    )


class FakeCursor:
    def __init__(self, conn):
        self._conn = conn

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    async def execute(self, sql, params):
        user_id = params[1]
        if user_id in self._conn.duplicate_user_ids:
            raise pg_errors.UniqueViolation("duplicate key value")
        self._conn.committed_user_ids.append(user_id)


class FakeInsertConn:
    """Fake connection exercising `insert_votes_individually`'s real
    per-row commit/rollback control flow (unlike test_worker_processor.py's
    fakes, which monkeypatch this function out entirely) — a chosen subset
    of `user_id`s raise `UniqueViolation` on `execute`, mirroring a real
    unique-constraint rejection.
    """

    def __init__(self, duplicate_user_ids: set[str]):
        self.duplicate_user_ids = duplicate_user_ids
        self.committed_user_ids: list[str] = []
        self.rollback_count = 0

    def cursor(self):
        return FakeCursor(self)

    async def commit(self):
        pass

    async def rollback(self):
        self.rollback_count += 1


@pytest.mark.asyncio
async def test_insert_votes_individually_skips_duplicates_and_commits_the_rest():
    votes = [_vote("user-1"), _vote("user-2"), _vote("user-3")]
    conn = FakeInsertConn(duplicate_user_ids={"user-2"})

    successful = await insert_votes_individually(conn, votes, shard_id=0)

    assert {v.user_id for v in successful} == {"user-1", "user-3"}
    assert conn.rollback_count == 1


@pytest.mark.asyncio
async def test_insert_votes_individually_increments_the_constraint_violation_counter():
    votes = [_vote("user-1"), _vote("user-2")]
    conn = FakeInsertConn(duplicate_user_ids={"user-2"})
    before = _violations_count("4")

    await insert_votes_individually(conn, votes, shard_id=4)

    assert _violations_count("4") == before + 1


@pytest.mark.asyncio
async def test_insert_votes_individually_does_not_increment_the_counter_when_nothing_duplicates():
    votes = [_vote("user-1"), _vote("user-2")]
    conn = FakeInsertConn(duplicate_user_ids=set())
    before = _violations_count("5")

    await insert_votes_individually(conn, votes, shard_id=5)

    assert _violations_count("5") == before
