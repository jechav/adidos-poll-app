"""Unit tests for I-008's `ShardConnectionPool`: connection caching and
the exponential-backoff-on-failure behavior that keeps a down shard from
being hot-looped. All against a monkeypatched `psycopg.AsyncConnection.connect`
— no real Postgres needed (see tests/integration/test_vote_processor_pipeline.py
for the real-DB round-trip tests).
"""

import psycopg
import pytest

from src.worker.db import ShardConnectionPool, ShardUnavailableError, default_shard_dsn


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
