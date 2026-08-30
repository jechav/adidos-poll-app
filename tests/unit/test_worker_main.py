"""Unit tests for I-008's `run_worker` drain loop: cross-shard requeue and
"no double-processing" under multiple concurrently-running worker
instances, all against fakes.
"""

import asyncio
import uuid

import pytest

import src.worker.processor as processor_module
from src.worker.main import run_worker
from src.worker.models import VotePayload
from src.worker.queue_consumer import QUEUE_KEY
from src.worker.sharding import shard_for_user


class FakeRedis:
    """Same in-memory list semantics as the queue_consumer fake: LPUSH
    prepends, BRPOP pops the tail. A single shared instance is handed to
    multiple concurrently-running `run_worker` coroutines below, mirroring
    how real worker pods share one `queue:votes` list in Redis.
    """

    def __init__(self):
        self.lists: dict[str, list[str]] = {}
        self.counters: dict[str, int] = {}

    async def lpush(self, key: str, value: str) -> None:
        self.lists.setdefault(key, []).insert(0, value)

    async def brpop(self, key: str, timeout: int = 1):
        lst = self.lists.get(key)
        if lst:
            return key, lst.pop()
        return None

    async def incr(self, key: str) -> int:
        self.counters[key] = self.counters.get(key, 0) + 1
        return self.counters[key]

    async def llen(self, key: str) -> int:
        return len(self.lists.get(key, []))


class RecordingPool:
    """Fake shard connection pool: `get_connection` always "succeeds" —
    what matters for these tests is what `process_batch` does with the
    votes it's handed, not real DB I/O.
    """

    async def get_connection(self, shard_id: int):
        return shard_id

    async def invalidate(self, shard_id: int) -> None:
        pass


def _vote(user_id: str) -> VotePayload:
    return VotePayload(
        vote_id=uuid.uuid4(), user_id=user_id, poll_id=uuid.uuid4(), answer_id=uuid.uuid4()
    )


@pytest.fixture
def redis():
    return FakeRedis()


@pytest.fixture(autouse=True)
def fake_db_writes(monkeypatch):
    """Replace the real Postgres write with an in-memory record of
    (shard_id, vote_id) so tests can assert on routing/dedup without a
    database.
    """
    written: list[tuple[int, uuid.UUID]] = []

    async def fake_batch_insert(conn, votes):
        for v in votes:
            written.append((conn, v.vote_id))
        return list(votes)

    monkeypatch.setattr(processor_module, "batch_insert_votes", fake_batch_insert)
    return written


@pytest.mark.asyncio
async def test_run_worker_requeues_votes_that_belong_to_other_shards(redis, fake_db_writes):
    num_shards = 4
    user_id = "user-not-shard-0"
    while shard_for_user(user_id, num_shards) == 0:
        user_id += "x"
    vote = _vote(user_id)
    await redis.lpush(QUEUE_KEY, vote.model_dump_json())

    await run_worker(
        0, num_shards, redis=redis, pool=RecordingPool(), iterations=1, min_batch_size=1,
        max_batch_size=10, batch_timeout_s=0.2,
    )

    assert fake_db_writes == []
    requeued = redis.lists[QUEUE_KEY]
    assert len(requeued) == 1
    assert VotePayload.model_validate_json(requeued[0]).vote_id == vote.vote_id


@pytest.mark.asyncio
async def test_run_worker_processes_votes_that_belong_to_its_own_shard(redis, fake_db_writes):
    num_shards = 4
    user_id = "user-for-shard-2"
    target_shard = 2
    while shard_for_user(user_id, num_shards) != target_shard:
        user_id += "x"
    vote = _vote(user_id)
    await redis.lpush(QUEUE_KEY, vote.model_dump_json())

    await run_worker(
        target_shard, num_shards, redis=redis, pool=RecordingPool(), iterations=1,
        min_batch_size=1, max_batch_size=10, batch_timeout_s=0.2,
    )

    assert fake_db_writes == [(target_shard, vote.vote_id)]
    assert redis.lists.get(QUEUE_KEY, []) == []
    assert redis.counters[f"cache:poll:{vote.poll_id}:answer:{vote.answer_id}"] == 1


@pytest.mark.asyncio
async def test_concurrent_workers_never_double_process_the_same_vote(redis, fake_db_writes):
    """Two worker instances (could be two pods for different shards, or
    two replicas of the same shard) drain the same shared queue
    concurrently. Because BRPOP pops each list entry exactly once, no
    vote should ever be written twice, regardless of how many workers are
    running.
    """
    num_shards = 4
    votes = [_vote(f"user-{i}") for i in range(40)]
    for vote in votes:
        await redis.lpush(QUEUE_KEY, vote.model_dump_json())

    pool = RecordingPool()
    await asyncio.gather(
        *(
            run_worker(
                shard_id, num_shards, redis=redis, pool=pool, iterations=20,
                min_batch_size=1, max_batch_size=10, batch_timeout_s=0.2,
            )
            for shard_id in range(num_shards)
        )
    )

    written_vote_ids = [vote_id for _, vote_id in fake_db_writes]
    assert len(written_vote_ids) == len(set(written_vote_ids)), (
        "a vote was written more than once across concurrently-running workers"
    )
    # Every vote should eventually have been drained by its owning shard
    # (queue is empty and nothing is left orphaned in the fake DB writes).
    assert set(written_vote_ids) == {v.vote_id for v in votes}
    assert redis.lists.get(QUEUE_KEY, []) == []


@pytest.mark.asyncio
async def test_run_worker_skips_processing_when_batch_is_empty(redis, fake_db_writes):
    # No votes queued at all: run_worker should just cycle without error.
    await run_worker(
        0, 4, redis=redis, pool=RecordingPool(), iterations=1,
        min_batch_size=1, max_batch_size=10, batch_timeout_s=0.1,
    )
    assert fake_db_writes == []
