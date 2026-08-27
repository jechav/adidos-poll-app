"""Unit tests for I-015's 3-strike duplicate block: strike counting and
threshold crossing, and `enforce_not_blocked`'s short-circuit behavior —
against a small in-memory fake Redis (no live cluster required),
mirroring tests/unit/test_rate_limit.py.
"""

import pytest

import src.services.anomalies as anomalies_module
from src.services.duplicate_block import (
    STRIKE_THRESHOLD,
    _block_key,
    _strike_key,
    enforce_not_blocked,
    record_duplicate_attempt,
)
from src.services.rate_limit import RateLimitExceededError


class FakeRedis:
    def __init__(self):
        self.strings: dict[str, str] = {}
        self.expire_calls: list[tuple[str, int]] = []
        self.set_calls: list[tuple[str, str, int | None]] = []

    async def incr(self, key: str) -> int:
        self.strings[key] = str(int(self.strings.get(key, 0)) + 1)
        return int(self.strings[key])

    async def expire(self, key: str, ttl: int) -> None:
        self.expire_calls.append((key, ttl))

    async def exists(self, key: str) -> int:
        return int(key in self.strings)

    async def set(self, key: str, value: str, ex: int | None = None, nx: bool = False):
        self.strings[key] = value
        self.set_calls.append((key, value, ex))
        return True


@pytest.fixture
def fake_redis():
    return FakeRedis()


@pytest.fixture
def captured_anomalies(monkeypatch):
    created: list = []

    async def fake_create(**kwargs):
        row = anomalies_module.AnomalyRow(
            alert_id=kwargs["alert_id"],
            alert_type=kwargs["alert_type"],
            severity=kwargs["severity"],
            user_id=kwargs.get("user_id"),
            ip_address=kwargs.get("ip_address"),
            poll_id=kwargs.get("poll_id"),
            description=kwargs["description"],
            created_at=kwargs["created_at"],
            acknowledged_at=None,
            action_taken=kwargs.get("action_taken"),
        )
        created.append(row)
        return row

    monkeypatch.setattr(anomalies_module, "create", fake_create)
    return created


class TestEnforceNotBlocked:
    @pytest.mark.asyncio
    async def test_allows_when_no_block_key_set(self, fake_redis):
        await enforce_not_blocked("user-1", "1.2.3.4", fake_redis)  # must not raise

    @pytest.mark.asyncio
    async def test_raises_rate_limit_exceeded_when_blocked(self, fake_redis):
        fake_redis.strings[_block_key("user-1", "1.2.3.4")] = "1"
        with pytest.raises(RateLimitExceededError):
            await enforce_not_blocked("user-1", "1.2.3.4", fake_redis)

    @pytest.mark.asyncio
    async def test_block_is_scoped_to_the_exact_user_ip_pair(self, fake_redis):
        fake_redis.strings[_block_key("user-1", "1.2.3.4")] = "1"
        await enforce_not_blocked("user-1", "9.9.9.9", fake_redis)  # different ip, not raised
        await enforce_not_blocked("user-2", "1.2.3.4", fake_redis)  # different user, not raised


class TestRecordDuplicateAttempt:
    @pytest.mark.asyncio
    async def test_first_and_second_strikes_do_not_block_or_write_anomaly(
        self, fake_redis, captured_anomalies
    ):
        poll_id = "poll-a"
        await record_duplicate_attempt("user-1", "1.2.3.4", poll_id, fake_redis)
        await record_duplicate_attempt("user-1", "1.2.3.4", poll_id, fake_redis)

        assert fake_redis.strings[_strike_key("user-1", "1.2.3.4", poll_id)] == "2"
        assert _block_key("user-1", "1.2.3.4") not in fake_redis.strings
        assert captured_anomalies == []

    @pytest.mark.asyncio
    async def test_third_strike_sets_block_and_writes_one_critical_anomaly(
        self, fake_redis, captured_anomalies
    ):
        poll_id = "poll-a"
        for _ in range(STRIKE_THRESHOLD):
            await record_duplicate_attempt("user-1", "1.2.3.4", poll_id, fake_redis)

        assert fake_redis.strings[_block_key("user-1", "1.2.3.4")] == "1"
        assert len(captured_anomalies) == 1
        anomaly = captured_anomalies[0]
        assert anomaly.alert_type == "duplicate_attempts_blocked"
        assert anomaly.severity == "critical"
        assert anomaly.user_id == "user-1"
        assert anomaly.ip_address == "1.2.3.4"

    @pytest.mark.asyncio
    async def test_ttl_set_only_on_first_strike(self, fake_redis, captured_anomalies):
        poll_id = "poll-a"
        for _ in range(3):
            await record_duplicate_attempt("user-1", "1.2.3.4", poll_id, fake_redis)

        strike_key = _strike_key("user-1", "1.2.3.4", poll_id)
        assert fake_redis.expire_calls == [(strike_key, 3600)]

    @pytest.mark.asyncio
    async def test_strikes_scoped_per_poll_do_not_cross_contaminate(
        self, fake_redis, captured_anomalies
    ):
        await record_duplicate_attempt("user-1", "1.2.3.4", "poll-a", fake_redis)
        await record_duplicate_attempt("user-1", "1.2.3.4", "poll-a", fake_redis)
        # A duplicate on a *different* poll doesn't add to poll-a's strikes.
        await record_duplicate_attempt("user-1", "1.2.3.4", "poll-b", fake_redis)

        assert fake_redis.strings[_strike_key("user-1", "1.2.3.4", "poll-a")] == "2"
        assert fake_redis.strings[_strike_key("user-1", "1.2.3.4", "poll-b")] == "1"
        assert _block_key("user-1", "1.2.3.4") not in fake_redis.strings
        assert captured_anomalies == []
