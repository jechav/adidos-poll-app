"""Unit tests for I-007's sliding-window rate limiter.

Uses a small in-memory fake Redis client (no fakeredis dependency in
requirements.txt, no live cluster required) that implements just the
`incr`/`expire`/`ping` surface `src/services/rate_limit.py` calls. Because
the fake's `incr` does its read-modify-write without any `await` in
between, and asyncio tasks only yield at `await` points, concurrent
`asyncio.gather()`'d calls against it exercise the same "no interleaved
read-modify-write" atomicity a real Redis `INCR` guarantees.
"""

import asyncio

import pytest
import pytest_asyncio
from redis.exceptions import RedisError

import src.services.rate_limit as rate_limit_module
from src.services.rate_limit import (
    RateLimitExceededError,
    RateLimitServiceUnavailableError,
    _check_limit,
    check_rate_limit,
)


class FakeRedis:
    """Minimal async fake standing in for the parts of RedisCluster the
    rate limiter touches: INCR, EXPIRE, and a way to simulate TTL expiry.
    """

    def __init__(self):
        self._counters: dict[str, int] = {}
        self.expire_calls: list[tuple[str, int]] = []
        self._raise_on_incr: Exception | None = None

    async def incr(self, key: str) -> int:
        if self._raise_on_incr is not None:
            raise self._raise_on_incr
        self._counters[key] = self._counters.get(key, 0) + 1
        return self._counters[key]

    async def expire(self, key: str, ttl: int) -> None:
        self.expire_calls.append((key, ttl))

    async def ping(self) -> bool:
        return True

    def expire_window(self, key: str) -> None:
        """Simulate the key's TTL elapsing (what real Redis does on its own)."""
        self._counters.pop(key, None)

    def fail_next_incr_with(self, exc: Exception) -> None:
        self._raise_on_incr = exc


@pytest_asyncio.fixture
async def fake_redis(monkeypatch):
    fake = FakeRedis()

    async def _get_redis():
        return fake

    monkeypatch.setattr(rate_limit_module, "get_redis", _get_redis)
    return fake


@pytest.mark.asyncio
async def test_check_limit_allows_requests_within_limit(fake_redis):
    for _ in range(5):
        assert await _check_limit("k", 5) is True


@pytest.mark.asyncio
async def test_check_limit_blocks_once_over_limit(fake_redis):
    for _ in range(5):
        await _check_limit("k", 5)
    assert await _check_limit("k", 5) is False


@pytest.mark.asyncio
async def test_check_limit_sets_ttl_only_on_first_increment(fake_redis):
    for _ in range(4):
        await _check_limit("k", 5)
    assert fake_redis.expire_calls == [("k", 61)]


@pytest.mark.asyncio
async def test_check_limit_window_resets_after_ttl_expiry(fake_redis):
    for _ in range(5):
        await _check_limit("k", 5)
    assert await _check_limit("k", 5) is False

    # Simulate the 61s TTL elapsing (Redis auto-deletes the key).
    fake_redis.expire_window("k")

    assert await _check_limit("k", 5) is True
    assert fake_redis.expire_calls == [("k", 61), ("k", 61)]


@pytest.mark.asyncio
async def test_check_limit_raises_service_unavailable_on_redis_error(fake_redis):
    fake_redis.fail_next_incr_with(RedisError("connection refused"))
    with pytest.raises(RateLimitServiceUnavailableError):
        await _check_limit("k", 5)


@pytest.mark.asyncio
async def test_check_limit_concurrent_requests_enforce_limit_atomically(fake_redis):
    results = await asyncio.gather(*[_check_limit("k", 5) for _ in range(20)])
    assert sum(1 for allowed in results if allowed) == 5
    assert sum(1 for allowed in results if not allowed) == 15


class TestCheckRateLimit:
    @pytest.mark.asyncio
    async def test_fifth_vote_succeeds_sixth_is_rejected(self, fake_redis):
        for _ in range(5):
            await check_rate_limit(user_id="user-1", ip="1.2.3.4")

        with pytest.raises(RateLimitExceededError) as exc_info:
            await check_rate_limit(user_id="user-1", ip="1.2.3.4")
        assert exc_info.value.retry_after == 60
        assert "User" in exc_info.value.message

    @pytest.mark.asyncio
    async def test_window_expiry_allows_voting_again(self, fake_redis):
        for _ in range(5):
            await check_rate_limit(user_id="user-1", ip="1.2.3.4")
        with pytest.raises(RateLimitExceededError):
            await check_rate_limit(user_id="user-1", ip="1.2.3.4")

        fake_redis.expire_window("rate_limit:user-1")

        # Should not raise.
        await check_rate_limit(user_id="user-1", ip="1.2.3.4")

    @pytest.mark.asyncio
    async def test_per_user_limit_is_independent_across_users(self, fake_redis):
        for _ in range(5):
            await check_rate_limit(user_id="user-1", ip="9.9.9.9")

        # A different user sharing nothing with user-1 is unaffected.
        await check_rate_limit(user_id="user-2", ip="8.8.8.8")

    @pytest.mark.asyncio
    async def test_two_ips_at_the_limit_are_independent_a_51st_from_either_fails(
        self, fake_redis
    ):
        # 50/IP/min (PROJECT_STATUS.md, and the Solution section's limit=50
        # sample). Two IPs each hitting their own 50-vote quota (100 votes
        # total) both succeed -- proving the per-IP counter is independent
        # per key rather than a single limit shared across IPs, which is
        # what would make the 100th (rather than the 51st) vote the first
        # to fail.
        for i in range(50):
            await check_rate_limit(user_id=f"user-a-{i}", ip="1.1.1.1")
        for i in range(50):
            await check_rate_limit(user_id=f"user-b-{i}", ip="2.2.2.2")

        with pytest.raises(RateLimitExceededError) as exc_info:
            await check_rate_limit(user_id="user-a-51", ip="1.1.1.1")
        assert "IP" in exc_info.value.message

        with pytest.raises(RateLimitExceededError):
            await check_rate_limit(user_id="user-b-51", ip="2.2.2.2")

    @pytest.mark.asyncio
    async def test_user_limit_checked_before_ip_limit(self, fake_redis):
        # Exhaust the user's own quota without touching the IP counter
        # for the 6th (over-limit) attempt.
        for _ in range(5):
            await check_rate_limit(user_id="user-1", ip="5.5.5.5")

        with pytest.raises(RateLimitExceededError) as exc_info:
            await check_rate_limit(user_id="user-1", ip="5.5.5.5")
        assert "User" in exc_info.value.message

        # The IP counter should have only been incremented 5 times (once
        # per successful call) -- the 6th, rejected-on-user-limit call
        # never reached the IP check.
        assert fake_redis._counters["rate_limit_ip:5.5.5.5"] == 5

    @pytest.mark.asyncio
    async def test_redis_unavailable_raises_service_unavailable_not_bypass(
        self, fake_redis
    ):
        fake_redis.fail_next_incr_with(RedisError("cluster down"))
        with pytest.raises(RateLimitServiceUnavailableError):
            await check_rate_limit(user_id="user-1", ip="1.2.3.4")
