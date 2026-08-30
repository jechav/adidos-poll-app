"""Unit tests for I-005's `queue:votes` enqueue step.

Uses a small in-memory fake Redis (same style as I-007's
tests/unit/test_rate_limit.py) implementing just the `lpush` surface this
module touches.
"""

import uuid

import pytest
from prometheus_client.parser import text_string_to_metric_families
from redis.exceptions import ConnectionError as RedisConnectionError

from src.metrics.registry import REGISTRY
from src.services.vote_queue import QUEUE_KEY, VotePayload, VoteQueueUnavailableError, enqueue_vote


def _queue_depth_gauge_value(queue: str) -> float | None:
    from prometheus_client import generate_latest

    text = generate_latest(REGISTRY).decode("utf-8")
    for family in text_string_to_metric_families(text):
        for s in family.samples:
            if s.name == "poll_queue_depth" and s.labels.get("queue") == queue:
                return s.value
    return None


class FakeRedis:
    def __init__(self):
        self.pushed: list[tuple[str, str]] = []
        self._raise: Exception | None = None

    async def lpush(self, key: str, value: str) -> int:
        if self._raise is not None:
            raise self._raise
        self.pushed.append((key, value))
        return len(self.pushed)

    def fail_next_with(self, exc: Exception) -> None:
        self._raise = exc


@pytest.fixture
def fake_redis():
    return FakeRedis()


def _payload() -> VotePayload:
    return VotePayload(
        vote_id=uuid.uuid4(),
        user_id="user-1",
        poll_id=uuid.uuid4(),
        answer_id=uuid.uuid4(),
        requested_at="2026-08-22T12:00:00Z",
    )


@pytest.mark.asyncio
async def test_enqueue_vote_pushes_to_queue_votes_key(fake_redis):
    payload = _payload()

    await enqueue_vote(fake_redis, payload)

    assert len(fake_redis.pushed) == 1
    key, raw = fake_redis.pushed[0]
    assert key == QUEUE_KEY == "queue:votes"


@pytest.mark.asyncio
async def test_enqueue_vote_payload_contains_required_fields(fake_redis):
    payload = _payload()

    await enqueue_vote(fake_redis, payload)

    _, raw = fake_redis.pushed[0]
    parsed = VotePayload.model_validate_json(raw)
    assert parsed.vote_id == payload.vote_id
    assert parsed.user_id == payload.user_id
    assert parsed.poll_id == payload.poll_id
    assert parsed.answer_id == payload.answer_id
    assert parsed.requested_at == payload.requested_at


@pytest.mark.asyncio
async def test_enqueue_vote_raises_service_unavailable_on_redis_error(fake_redis):
    fake_redis.fail_next_with(RedisConnectionError("connection refused"))

    with pytest.raises(VoteQueueUnavailableError):
        await enqueue_vote(fake_redis, _payload())


@pytest.mark.asyncio
async def test_enqueue_vote_updates_the_queue_depth_gauge(fake_redis):
    await enqueue_vote(fake_redis, _payload())
    assert _queue_depth_gauge_value("votes") == 1

    await enqueue_vote(fake_redis, _payload())
    assert _queue_depth_gauge_value("votes") == 2


@pytest.mark.asyncio
async def test_enqueue_vote_does_not_update_the_gauge_when_lpush_fails(fake_redis):
    fake_redis.fail_next_with(RedisConnectionError("connection refused"))
    before = _queue_depth_gauge_value("votes")

    with pytest.raises(VoteQueueUnavailableError):
        await enqueue_vote(fake_redis, _payload())

    assert _queue_depth_gauge_value("votes") == before
