"""Unit tests for I-008's `dequeue_batch` — batching/timeout behavior
against a fake Redis list, no real Redis needed.
"""

import time
import uuid

import pytest

from src.worker.models import VotePayload
from src.worker.queue_consumer import QUEUE_KEY, dequeue_batch


class FakeRedis:
    """In-memory stand-in for the subset of redis-py's async API this
    worker uses: LPUSH prepends, BRPOP pops from the tail (FIFO overall,
    matching real Redis list semantics), never actually blocks.
    """

    def __init__(self):
        self.lists: dict[str, list[str]] = {}
        self.counters: dict[str, int] = {}
        self.brpop_calls = 0

    async def lpush(self, key: str, value: str) -> None:
        self.lists.setdefault(key, []).insert(0, value)

    async def brpop(self, key: str, timeout: int = 1):
        self.brpop_calls += 1
        lst = self.lists.get(key)
        if lst:
            return key, lst.pop()
        return None

    async def incr(self, key: str) -> int:
        self.counters[key] = self.counters.get(key, 0) + 1
        return self.counters[key]


def _vote_json(user_id: str = "user-1") -> str:
    return VotePayload(
        vote_id=uuid.uuid4(), user_id=user_id, poll_id=uuid.uuid4(), answer_id=uuid.uuid4()
    ).model_dump_json()


@pytest.fixture
def redis():
    return FakeRedis()


@pytest.mark.asyncio
async def test_dequeue_batch_parses_vote_payloads(redis):
    await redis.lpush(QUEUE_KEY, _vote_json("user-1"))

    batch = await dequeue_batch(redis, min_size=1, max_size=1, timeout_s=1)

    assert len(batch) == 1
    assert isinstance(batch[0], VotePayload)
    assert batch[0].user_id == "user-1"


@pytest.mark.asyncio
async def test_dequeue_batch_preserves_fifo_order(redis):
    for i in range(3):
        await redis.lpush(QUEUE_KEY, _vote_json(f"user-{i}"))

    batch = await dequeue_batch(redis, min_size=1, max_size=3, timeout_s=1)

    assert [v.user_id for v in batch] == ["user-0", "user-1", "user-2"]


@pytest.mark.asyncio
async def test_dequeue_batch_stops_at_max_size_without_waiting_out_the_deadline(redis):
    for i in range(5):
        await redis.lpush(QUEUE_KEY, _vote_json(f"user-{i}"))

    start = time.monotonic()
    batch = await dequeue_batch(redis, min_size=1, max_size=2, timeout_s=5)
    elapsed = time.monotonic() - start

    assert len(batch) == 2
    assert elapsed < 1, "should return as soon as max_size is reached, not wait for the deadline"


@pytest.mark.asyncio
async def test_dequeue_batch_returns_early_once_min_size_reached_and_queue_drains(redis):
    for i in range(2):
        await redis.lpush(QUEUE_KEY, _vote_json(f"user-{i}"))

    start = time.monotonic()
    # Queue only has 2 items; min_size=2 lets the batch return as soon as
    # a subsequent empty read happens, without waiting out the full
    # (generous) timeout.
    batch = await dequeue_batch(redis, min_size=2, max_size=10, timeout_s=5, poll_timeout_s=1)
    elapsed = time.monotonic() - start

    assert len(batch) == 2
    assert elapsed < 1


@pytest.mark.asyncio
async def test_dequeue_batch_waits_out_full_deadline_when_below_min_size(redis):
    start = time.monotonic()
    batch = await dequeue_batch(redis, min_size=5, max_size=10, timeout_s=0.1, poll_timeout_s=1)
    elapsed = time.monotonic() - start

    assert batch == []
    assert elapsed >= 0.09


@pytest.mark.asyncio
async def test_dequeue_batch_does_not_busy_poll_forever_on_empty_queue(redis):
    # A real BRPOP with poll_timeout_s=1 blocks server-side for up to 1s
    # per call, so an idle worker never spins; this fake returns
    # immediately instead of blocking, so the only thing guaranteeing
    # termination here is the `timeout_s` deadline check — assert the
    # call actually returns promptly rather than hanging forever
    # (regression guard against an infinite `while True`).
    start = time.monotonic()
    batch = await dequeue_batch(redis, min_size=1, max_size=10, timeout_s=0.1, poll_timeout_s=1)
    elapsed = time.monotonic() - start

    assert batch == []
    assert elapsed < 2, "dequeue_batch did not honor its timeout_s deadline"
