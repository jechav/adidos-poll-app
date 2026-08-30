"""Unit tests for I-008's `process_batch`: DB write -> cache update
ordering, unique-violation fallback, and shard-down requeue — all against
fakes, no real Redis/Postgres needed.
"""

import uuid

import pytest
from psycopg import errors as pg_errors

import src.worker.processor as processor_module
from src.worker.db import ShardUnavailableError
from src.worker.models import VotePayload
from src.worker.processor import process_batch, requeue
from src.worker.queue_consumer import QUEUE_KEY


class FakeConnection:
    def __init__(self, shard_id: int):
        self.shard_id = shard_id


class FakePool:
    def __init__(self, *, connect_fails: bool = False):
        self.connect_fails = connect_fails
        self.invalidated: list[int] = []

    async def get_connection(self, shard_id: int):
        if self.connect_fails:
            raise ShardUnavailableError(f"shard {shard_id} down")
        return FakeConnection(shard_id)

    async def invalidate(self, shard_id: int) -> None:
        self.invalidated.append(shard_id)


class FakeRedis:
    def __init__(self):
        self.lists: dict[str, list[str]] = {}
        self.counters: dict[str, int] = {}
        self.trace: list[tuple] = []

    async def lpush(self, key: str, value: str) -> None:
        self.lists.setdefault(key, []).insert(0, value)
        self.trace.append(("lpush", key))

    async def incr(self, key: str) -> int:
        self.counters[key] = self.counters.get(key, 0) + 1
        self.trace.append(("incr", key))
        return self.counters[key]


def _vote(user_id: str = "user-1") -> VotePayload:
    return VotePayload(
        vote_id=uuid.uuid4(), user_id=user_id, poll_id=uuid.uuid4(), answer_id=uuid.uuid4()
    )


@pytest.fixture
def redis():
    return FakeRedis()


@pytest.mark.asyncio
async def test_process_batch_empty_votes_is_a_noop(redis):
    result = await process_batch(0, [], FakePool(), redis)
    assert result == []
    assert redis.trace == []


@pytest.mark.asyncio
async def test_process_batch_increments_cache_only_after_db_commit(monkeypatch, redis):
    votes = [_vote("user-1"), _vote("user-2")]
    trace: list[str] = []

    async def fake_batch_insert(conn, votes):
        trace.append("db_commit")
        return list(votes)

    monkeypatch.setattr(processor_module, "batch_insert_votes", fake_batch_insert)

    orig_incr = redis.incr

    async def tracking_incr(key):
        trace.append("cache_incr")
        return await orig_incr(key)

    redis.incr = tracking_incr

    successful = await process_batch(0, votes, FakePool(), redis)

    assert successful == votes
    assert trace == ["db_commit", "cache_incr", "cache_incr"]
    for vote in votes:
        assert redis.counters[f"cache:poll:{vote.poll_id}:answer:{vote.answer_id}"] == 1


@pytest.mark.asyncio
async def test_process_batch_falls_back_to_per_row_insert_on_unique_violation(monkeypatch, redis):
    votes = [_vote("user-1"), _vote("user-2"), _vote("user-3")]

    async def fake_batch_insert(conn, votes):
        raise pg_errors.UniqueViolation("duplicate")

    async def fake_insert_individually(conn, votes, *, shard_id):
        # Simulate votes[1] being a duplicate that gets skipped.
        return [v for v in votes if v is not votes[1]]

    monkeypatch.setattr(processor_module, "batch_insert_votes", fake_batch_insert)
    monkeypatch.setattr(processor_module, "insert_votes_individually", fake_insert_individually)

    successful = await process_batch(0, votes, FakePool(), redis)

    assert successful == [votes[0], votes[2]]
    # Only the two committed rows should have bumped the cache — the
    # skipped duplicate must not increment a counter for a vote that was
    # never durably written.
    assert redis.counters == {
        f"cache:poll:{votes[0].poll_id}:answer:{votes[0].answer_id}": 1,
        f"cache:poll:{votes[2].poll_id}:answer:{votes[2].answer_id}": 1,
    }


@pytest.mark.asyncio
async def test_process_batch_requeues_without_touching_cache_when_shard_connection_fails(redis):
    votes = [_vote("user-1"), _vote("user-2")]
    pool = FakePool(connect_fails=True)

    successful = await process_batch(0, votes, pool, redis)

    assert successful == []
    assert redis.counters == {}
    requeued = redis.lists[QUEUE_KEY]
    requeued_vote_ids = {VotePayload.model_validate_json(item).vote_id for item in requeued}
    assert requeued_vote_ids == {v.vote_id for v in votes}
    # get_connection itself failed (pool never handed back a connection),
    # so there's nothing to invalidate.
    assert pool.invalidated == []


@pytest.mark.asyncio
async def test_process_batch_requeues_and_invalidates_connection_lost_mid_batch(
    monkeypatch, redis
):
    votes = [_vote("user-1"), _vote("user-2")]
    pool = FakePool()

    async def fake_batch_insert(conn, votes):
        raise ShardUnavailableError("connection dropped mid-batch")

    monkeypatch.setattr(processor_module, "batch_insert_votes", fake_batch_insert)

    successful = await process_batch(3, votes, pool, redis)

    assert successful == []
    assert redis.counters == {}
    assert pool.invalidated == [3]
    requeued = redis.lists[QUEUE_KEY]
    assert len(requeued) == 2


@pytest.mark.asyncio
async def test_other_shards_are_unaffected_while_one_shard_is_down(monkeypatch, redis):
    healthy_votes = [_vote("user-a")]
    down_votes = [_vote("user-b")]

    class SelectivePool:
        def __init__(self):
            self.invalidated = []

        async def get_connection(self, shard_id):
            if shard_id == 99:
                raise ShardUnavailableError("shard 99 down")
            return FakeConnection(shard_id)

        async def invalidate(self, shard_id):
            self.invalidated.append(shard_id)

    async def fake_batch_insert(conn, votes):
        return list(votes)

    monkeypatch.setattr(processor_module, "batch_insert_votes", fake_batch_insert)

    pool = SelectivePool()
    healthy_result = await process_batch(1, healthy_votes, pool, redis)
    down_result = await process_batch(99, down_votes, pool, redis)

    assert healthy_result == healthy_votes
    assert down_result == []
    assert redis.counters == {
        f"cache:poll:{healthy_votes[0].poll_id}:answer:{healthy_votes[0].answer_id}": 1
    }
    assert len(redis.lists[QUEUE_KEY]) == 1


@pytest.mark.asyncio
async def test_requeue_pushes_json_payloads_for_every_vote(redis):
    votes = [_vote("user-1"), _vote("user-2"), _vote("user-3")]

    await requeue(redis, votes)

    requeued = redis.lists[QUEUE_KEY]
    assert len(requeued) == 3
    parsed_ids = {VotePayload.model_validate_json(item).vote_id for item in requeued}
    assert parsed_ids == {v.vote_id for v in votes}
