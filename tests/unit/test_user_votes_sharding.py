"""Unit tests for I-012's shard routing: `get_user_votes` must route to
exactly the shard `shard_for_user` computes for the caller's `user_id`
(I-008's function, reused not reimplemented) and never fan out across
shards.

The pool and connection are faked so these exercise only the routing
decision and query wiring, independent of a live Postgres instance
(tests/integration/test_user_votes.py covers the same paths against real
infra).
"""

import pytest

import src.db.queries.user_votes as user_votes_module
from src.config import settings
from src.db.queries.user_votes import get_user_votes
from src.worker.sharding import shard_for_user


class FakeCursor:
    def __init__(self, rows, total):
        self._rows = rows
        self._total = total
        self.executed: list[tuple] = []

    async def execute(self, query, params):
        self.executed.append((query, params))

    async def fetchall(self):
        return self._rows

    async def fetchone(self):
        return (self._total,)

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False


class FakeConnection:
    def __init__(self, rows, total):
        self._cursor = FakeCursor(rows, total)

    def cursor(self):
        return self._cursor


class FakePool:
    """Records which shard_id each call asked for, so tests can assert
    routing without a real per-shard connection pool.
    """

    def __init__(self, rows=(), total=0):
        self.requested_shards: list[int] = []
        self._rows = rows
        self._total = total
        self.connections: dict[int, FakeConnection] = {}

    async def get_connection(self, shard_id: int) -> FakeConnection:
        self.requested_shards.append(shard_id)
        conn = self.connections.setdefault(
            shard_id, FakeConnection(self._rows, self._total)
        )
        return conn


@pytest.fixture(autouse=True)
def fake_pool(monkeypatch):
    pool = FakePool()
    monkeypatch.setattr(user_votes_module, "_pool", pool)
    return pool


@pytest.mark.asyncio
async def test_get_user_votes_routes_to_the_users_shard(fake_pool):
    user_id = "user-42"
    expected_shard = shard_for_user(user_id, settings.num_shards)

    await get_user_votes(user_id, limit=20, offset=0)

    assert fake_pool.requested_shards == [expected_shard]


@pytest.mark.asyncio
async def test_get_user_votes_for_users_on_different_shards_hit_different_pools_calls(
    fake_pool,
):
    # Pick two user_ids that are known to hash to different shards so the
    # routing decision is actually exercised, not just "some shard".
    candidates = [f"user-{i}" for i in range(50)]
    shard_of = {u: shard_for_user(u, settings.num_shards) for u in candidates}
    by_shard: dict[int, str] = {}
    for u, s in shard_of.items():
        by_shard.setdefault(s, u)
    assert len(by_shard) >= 2, "expected at least two distinct shards among candidates"

    shard_ids = list(by_shard)
    user_a, user_b = by_shard[shard_ids[0]], by_shard[shard_ids[1]]

    await get_user_votes(user_a, limit=20, offset=0)
    await get_user_votes(user_b, limit=20, offset=0)

    assert fake_pool.requested_shards == [shard_of[user_a], shard_of[user_b]]
    assert fake_pool.requested_shards[0] != fake_pool.requested_shards[1]


@pytest.mark.asyncio
async def test_get_user_votes_passes_limit_and_offset_to_the_query(fake_pool):
    await get_user_votes("user-1", limit=5, offset=10)

    shard = fake_pool.requested_shards[0]
    conn = fake_pool.connections[shard]
    select_query, select_params = conn._cursor.executed[0]
    assert select_params == ("user-1", 5, 10)

    count_query, count_params = conn._cursor.executed[1]
    assert count_params == ("user-1",)


@pytest.mark.asyncio
async def test_get_user_votes_returns_rows_and_total(monkeypatch):
    rows = [("poll-1", "Q?", "active", "answer-1", "yes", "2026-01-01T00:00:00Z")]
    pool = FakePool(rows=rows, total=7)
    monkeypatch.setattr(user_votes_module, "_pool", pool)

    result_rows, total = await get_user_votes("user-1", limit=20, offset=0)

    assert result_rows == rows
    assert total == 7
