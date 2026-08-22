"""Unit tests for I-006's dual-layer uniqueness enforcement.

Layer 1 (Redis SET NX) is tested against a small in-memory fake that
implements the same `set(key, value, nx=True)` contract redis-py's
cluster client exposes — no real Redis required. Layer 2 (the DB
UNIQUE-violation backstop) is tested against a fake insert function that
raises a stand-in "unique violation" exception, since the module accepts
the violation type as a parameter precisely so it isn't coupled to
psycopg here (see tests/integration for the real-Postgres version).
"""

import asyncio
import logging

import pytest
from redis.exceptions import ConnectionError as RedisConnectionError

from src.services.uniqueness import (
    DuplicateVoteError,
    UniquenessCheckUnavailableError,
    check_and_reserve_uniqueness,
    insert_vote_or_log_duplicate,
    release_reservation,
    vote_reservation_key,
)


class FakeRedis:
    """Minimal fake honoring SET NX semantics (atomic within a single
    asyncio event loop, since no `await` occurs between the existence
    check and the write)."""

    def __init__(self):
        self.store: dict[str, str] = {}

    async def set(self, key: str, value: str, nx: bool = False):
        if nx and key in self.store:
            return None
        self.store[key] = value
        return True

    async def delete(self, key: str) -> int:
        return 1 if self.store.pop(key, None) is not None else 0


class BrokenRedis:
    async def set(self, key: str, value: str, nx: bool = False):
        raise RedisConnectionError("connection refused")

    async def delete(self, key: str):
        raise RedisConnectionError("connection refused")


def test_vote_reservation_key_format():
    assert (
        vote_reservation_key("user-123", "poll-abc")
        == "vote:user:user-123:poll:poll-abc"
    )


@pytest.mark.asyncio
async def test_first_reservation_succeeds():
    redis = FakeRedis()
    await check_and_reserve_uniqueness(redis, "user-1", "poll-1")
    assert redis.store["vote:user:user-1:poll:poll-1"] == "1"


@pytest.mark.asyncio
async def test_second_reservation_for_same_pair_raises_duplicate():
    redis = FakeRedis()
    await check_and_reserve_uniqueness(redis, "user-1", "poll-1")

    with pytest.raises(DuplicateVoteError) as exc_info:
        await check_and_reserve_uniqueness(redis, "user-1", "poll-1")

    assert exc_info.value.user_id == "user-1"
    assert exc_info.value.poll_id == "poll-1"


@pytest.mark.asyncio
async def test_release_reservation_clears_the_key():
    redis = FakeRedis()
    await check_and_reserve_uniqueness(redis, "user-1", "poll-1")

    await release_reservation(redis, "user-1", "poll-1")

    assert "vote:user:user-1:poll:poll-1" not in redis.store
    # The reservation is free again.
    await check_and_reserve_uniqueness(redis, "user-1", "poll-1")


@pytest.mark.asyncio
async def test_release_reservation_swallows_redis_errors():
    redis = BrokenRedis()

    # Must not raise: this is a best-effort cleanup, not a correctness
    # requirement (see the docstring's accepted trade-off).
    await release_reservation(redis, "user-1", "poll-1")


@pytest.mark.asyncio
async def test_different_poll_same_user_does_not_collide():
    redis = FakeRedis()
    await check_and_reserve_uniqueness(redis, "user-1", "poll-1")
    # Different poll_id -> different key -> should not raise.
    await check_and_reserve_uniqueness(redis, "user-1", "poll-2")


@pytest.mark.asyncio
async def test_different_user_same_poll_does_not_collide():
    redis = FakeRedis()
    await check_and_reserve_uniqueness(redis, "user-1", "poll-1")
    await check_and_reserve_uniqueness(redis, "user-2", "poll-1")


@pytest.mark.asyncio
async def test_redis_failure_raises_unavailable_not_duplicate():
    redis = BrokenRedis()
    with pytest.raises(UniquenessCheckUnavailableError) as exc_info:
        await check_and_reserve_uniqueness(redis, "user-1", "poll-1")
    assert exc_info.value.user_id == "user-1"
    assert isinstance(exc_info.value.__cause__, RedisConnectionError)


@pytest.mark.asyncio
async def test_concurrent_identical_requests_exactly_one_wins():
    redis = FakeRedis()
    n = 50

    async def attempt():
        try:
            await check_and_reserve_uniqueness(redis, "user-race", "poll-race")
            return "reserved"
        except DuplicateVoteError:
            return "rejected"

    results = await asyncio.gather(*(attempt() for _ in range(n)))

    assert results.count("reserved") == 1
    assert results.count("rejected") == n - 1


class UniqueViolation(Exception):
    """Stand-in for psycopg.errors.UniqueViolation."""


@pytest.mark.asyncio
async def test_insert_vote_succeeds_when_no_conflict():
    inserted_rows = []

    async def insert_fn(vote):
        inserted_rows.append(vote)

    vote = {"vote_id": "v1", "user_id": "user-1", "poll_id": "poll-1"}
    result = await insert_vote_or_log_duplicate(
        insert_fn, vote, unique_violation=UniqueViolation
    )

    assert result is True
    assert inserted_rows == [vote]


@pytest.mark.asyncio
async def test_insert_vote_returns_false_and_logs_on_conflict(caplog):
    async def insert_fn(vote):
        raise UniqueViolation("duplicate key value violates unique constraint")

    vote = {"vote_id": "v2", "user_id": "user-1", "poll_id": "poll-1"}

    with caplog.at_level(logging.WARNING, logger="poll_app.uniqueness"):
        result = await insert_vote_or_log_duplicate(
            insert_fn, vote, unique_violation=UniqueViolation
        )

    assert result is False
    assert any(
        record.message == "duplicate_vote_at_db_layer" for record in caplog.records
    )


@pytest.mark.asyncio
async def test_insert_vote_does_not_raise_on_conflict():
    """The caller (I-008's worker) must never see an exception here —
    the client already received its 202 at queue time."""

    async def insert_fn(vote):
        raise UniqueViolation()

    vote = {"vote_id": "v3", "user_id": "user-1", "poll_id": "poll-1"}
    # Should not raise.
    await insert_vote_or_log_duplicate(insert_fn, vote, unique_violation=UniqueViolation)


@pytest.mark.asyncio
async def test_insert_vote_propagates_other_exceptions():
    class SomeOtherDbError(Exception):
        pass

    async def insert_fn(vote):
        raise SomeOtherDbError("connection lost")

    vote = {"vote_id": "v4", "user_id": "user-1", "poll_id": "poll-1"}
    with pytest.raises(SomeOtherDbError):
        await insert_vote_or_log_duplicate(
            insert_fn, vote, unique_violation=UniqueViolation
        )


@pytest.mark.asyncio
async def test_second_insert_for_same_user_poll_is_skipped_not_double_counted():
    """End-to-end-ish simulation of the acceptance criteria's failure
    mode: two votes for the same (user_id, poll_id) reach Layer 2 (as if
    Layer 1's key had been cleared/bypassed). Exactly one insert lands,
    the second is skipped silently, no error propagates."""

    store: dict[tuple[str, str], dict] = {}

    async def insert_fn(vote):
        key = (vote["user_id"], vote["poll_id"])
        if key in store:
            raise UniqueViolation()
        store[key] = vote

    vote_a = {"vote_id": "v-a", "user_id": "user-9", "poll_id": "poll-9"}
    vote_b = {"vote_id": "v-b", "user_id": "user-9", "poll_id": "poll-9"}

    first = await insert_vote_or_log_duplicate(
        insert_fn, vote_a, unique_violation=UniqueViolation
    )
    second = await insert_vote_or_log_duplicate(
        insert_fn, vote_b, unique_violation=UniqueViolation
    )

    assert first is True
    assert second is False
    assert len(store) == 1
