"""Unit tests for I-018's worker-side logging: `dequeue_batch` re-binding
`request_id`/`vote_id` on dequeue and logging `vote_dequeued` (sampled),
and `process_batch` logging `vote_written` (sampled) on success and
`vote_write_rejected` (100%, never sampled) for per-row duplicates caught
by the unique-violation fallback.
"""

import json
import uuid

import pytest
from psycopg import errors as pg_errors

import src.worker.processor as processor_module
from src.logging.config import configure_logging
from src.worker.models import VotePayload
from src.worker.processor import process_batch
from src.worker.queue_consumer import QUEUE_KEY, dequeue_batch

# uuid.UUID(int=N * 1000) is always sampled (is_sampled checks % 1000 == 0);
# int=N * 1000 + 1 never is.
SAMPLED_VOTE_ID = uuid.UUID(int=7000)
UNSAMPLED_VOTE_ID = uuid.UUID(int=7001)


class FakeRedis:
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


class FakeConnection:
    pass


class FakePool:
    async def get_connection(self, shard_id: int):
        return FakeConnection()

    async def invalidate(self, shard_id: int) -> None:
        pass


def _vote(vote_id: uuid.UUID, *, request_id: str = "req-worker-test") -> VotePayload:
    return VotePayload(
        vote_id=vote_id,
        user_id="user-1",
        poll_id=uuid.uuid4(),
        answer_id=uuid.uuid4(),
        request_id=request_id,
    )


def _json_lines(capsys):
    out = capsys.readouterr().out.strip()
    return [json.loads(line) for line in out.splitlines() if line.strip()]


@pytest.fixture
def redis():
    return FakeRedis()


@pytest.fixture(autouse=True)
def _configure(capsys):
    configure_logging()
    capsys.readouterr()


@pytest.mark.asyncio
async def test_dequeue_batch_logs_vote_dequeued_with_correlation_ids_for_sampled_vote(
    redis, capsys
):
    vote = _vote(SAMPLED_VOTE_ID, request_id="req-sampled")
    await redis.lpush(QUEUE_KEY, vote.model_dump_json())

    await dequeue_batch(redis, min_size=1, max_size=1, timeout_s=1)

    lines = _json_lines(capsys)
    dequeued = [line for line in lines if line["event"] == "vote_dequeued"]
    assert len(dequeued) == 1
    assert dequeued[0]["request_id"] == "req-sampled"
    assert dequeued[0]["vote_id"] == str(SAMPLED_VOTE_ID)


@pytest.mark.asyncio
async def test_dequeue_batch_does_not_log_vote_dequeued_for_unsampled_vote(redis, capsys):
    vote = _vote(UNSAMPLED_VOTE_ID, request_id="req-unsampled")
    await redis.lpush(QUEUE_KEY, vote.model_dump_json())

    await dequeue_batch(redis, min_size=1, max_size=1, timeout_s=1)

    lines = _json_lines(capsys)
    assert [line for line in lines if line["event"] == "vote_dequeued"] == []


@pytest.mark.asyncio
async def test_process_batch_logs_vote_written_for_sampled_successful_vote(
    monkeypatch, redis, capsys
):
    vote = _vote(SAMPLED_VOTE_ID, request_id="req-sampled")

    async def fake_batch_insert(conn, votes):
        return list(votes)

    monkeypatch.setattr(processor_module, "batch_insert_votes", fake_batch_insert)

    await process_batch(3, [vote], FakePool(), redis)

    lines = _json_lines(capsys)
    written = [line for line in lines if line["event"] == "vote_written"]
    assert len(written) == 1
    assert written[0]["request_id"] == "req-sampled"
    assert written[0]["vote_id"] == str(SAMPLED_VOTE_ID)
    assert written[0]["shard"] == 3
    assert written[0]["outcome"] == "success"


@pytest.mark.asyncio
async def test_process_batch_does_not_log_vote_written_for_unsampled_successful_vote(
    monkeypatch, redis, capsys
):
    vote = _vote(UNSAMPLED_VOTE_ID, request_id="req-unsampled")

    async def fake_batch_insert(conn, votes):
        return list(votes)

    monkeypatch.setattr(processor_module, "batch_insert_votes", fake_batch_insert)

    await process_batch(3, [vote], FakePool(), redis)

    lines = _json_lines(capsys)
    assert [line for line in lines if line["event"] == "vote_written"] == []


@pytest.mark.asyncio
async def test_process_batch_logs_vote_write_rejected_at_100_percent_even_when_unsampled(
    monkeypatch, redis, capsys
):
    """A row rejected by the DB's UNIQUE constraint (Layer 2 duplicate
    backstop, I-006) is an outcome, not a routine success — it must be
    logged regardless of `is_sampled`.
    """
    rejected_vote = _vote(UNSAMPLED_VOTE_ID, request_id="req-unsampled-rejected")
    votes = [rejected_vote]

    async def fake_batch_insert(conn, votes):
        raise pg_errors.UniqueViolation("duplicate")

    async def fake_insert_individually(conn, votes, *, shard_id):
        return []  # every row in this batch was a duplicate

    monkeypatch.setattr(processor_module, "batch_insert_votes", fake_batch_insert)
    monkeypatch.setattr(processor_module, "insert_votes_individually", fake_insert_individually)

    await process_batch(1, votes, FakePool(), redis)

    lines = _json_lines(capsys)
    rejected = [line for line in lines if line["event"] == "vote_write_rejected"]
    assert len(rejected) == 1
    assert rejected[0]["request_id"] == "req-unsampled-rejected"
    assert rejected[0]["vote_id"] == str(UNSAMPLED_VOTE_ID)
    assert rejected[0]["outcome"] == "constraint_violation"
    assert rejected[0]["level"] == "warning"
