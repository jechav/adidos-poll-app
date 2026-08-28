"""Unit tests for I-014: the inline counter writer
(`src/services/bot_detection.py`) and the periodic evaluator's rules
(`src/jobs/bot_detection.py`), against a small in-memory fake Redis —
mirroring tests/unit/test_rate_limit.py's no-live-cluster-required
pattern.
"""

import fnmatch
import time

import pytest

import src.jobs.bot_detection as evaluator
import src.services.anomalies as anomalies_module
from src.services.bot_detection import (
    global_bucket_key,
    ip_burst_key,
    ip_users_key,
    record_vote_attempt,
)


class FakePipeline:
    def __init__(self, redis):
        self._redis = redis
        self._ops: list[tuple] = []

    def zadd(self, key, mapping):
        self._ops.append(("zadd", key, mapping))
        return self

    def sadd(self, key, *members):
        self._ops.append(("sadd", key, *members))
        return self

    def expire(self, key, ttl):
        self._ops.append(("expire", key, ttl))
        return self

    def incr(self, key):
        self._ops.append(("incr", key))
        return self

    async def execute(self):
        results = []
        for op in self._ops:
            name, *args = op
            method = getattr(self._redis, name)
            results.append(await method(*args))
        self._ops = []
        return results


class FakeRedis:
    """Implements just the ZADD/ZCARD/ZREMRANGEBYSCORE, SADD/SCARD,
    INCR/GET/SET/EXISTS/DELETE/EXPIRE, SCAN_ITER, and pipeline surface
    I-014's counter writer and evaluator touch.
    """

    def __init__(self):
        self.zsets: dict[str, dict[str, float]] = {}
        self.sets: dict[str, set] = {}
        self.strings: dict[str, str] = {}
        self.expire_calls: list[tuple[str, int]] = []

    def pipeline(self, transaction=False):
        return FakePipeline(self)

    async def zadd(self, key, mapping):
        self.zsets.setdefault(key, {}).update(mapping)

    async def zremrangebyscore(self, key, min_, max_):
        z = self.zsets.get(key, {})
        for member in [m for m, s in z.items() if min_ <= s <= max_]:
            del z[member]

    async def zcard(self, key):
        return len(self.zsets.get(key, {}))

    async def sadd(self, key, *members):
        self.sets.setdefault(key, set()).update(members)

    async def scard(self, key):
        return len(self.sets.get(key, set()))

    async def incr(self, key):
        self.strings[key] = str(int(self.strings.get(key, 0)) + 1)
        return int(self.strings[key])

    async def get(self, key):
        return self.strings.get(key)

    async def set(self, key, value, ex=None, nx=False):
        if nx and key in self.strings:
            return None
        self.strings[key] = value
        return True

    async def exists(self, key):
        return int(key in self.strings or key in self.sets or key in self.zsets)

    async def delete(self, key):
        self.strings.pop(key, None)
        self.sets.pop(key, None)
        self.zsets.pop(key, None)

    async def expire(self, key, ttl):
        self.expire_calls.append((key, ttl))

    async def scan_iter(self, match=None):
        keys = set(self.zsets) | set(self.sets) | set(self.strings)
        for k in keys:
            if match is None or fnmatch.fnmatch(k, match):
                yield k


@pytest.fixture
def fake_redis():
    return FakeRedis()


@pytest.fixture
def captured_anomalies(monkeypatch):
    created: list = []
    notified: list = []

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

    async def fake_notify(alert):
        notified.append(alert)

    monkeypatch.setattr(anomalies_module, "create", fake_create)
    monkeypatch.setattr(evaluator, "notify_admins", fake_notify)
    return created, notified


# --- record_vote_attempt (inline counter writer) -----------------------


class TestRecordVoteAttempt:
    @pytest.mark.asyncio
    async def test_writes_zadd_sadd_incr_in_one_pipeline(self, fake_redis):
        await record_vote_attempt(fake_redis, ip="1.2.3.4", user_id="user-1")

        assert len(fake_redis.zsets[ip_burst_key("1.2.3.4")]) == 1
        assert fake_redis.sets[ip_users_key("1.2.3.4")] == {"user-1"}
        bucket = int(time.time())
        assert fake_redis.strings[global_bucket_key(bucket)] == "1"

    @pytest.mark.asyncio
    async def test_two_attempts_from_same_ip_do_not_overwrite_each_other(self, fake_redis):
        await record_vote_attempt(fake_redis, ip="1.2.3.4", user_id="user-1")
        await record_vote_attempt(fake_redis, ip="1.2.3.4", user_id="user-2")

        assert len(fake_redis.zsets[ip_burst_key("1.2.3.4")]) == 2
        assert fake_redis.sets[ip_users_key("1.2.3.4")] == {"user-1", "user-2"}

    @pytest.mark.asyncio
    async def test_never_raises_when_redis_lacks_pipeline_support(self):
        class BrokenRedis:
            pass

        # Must swallow the AttributeError, not propagate it -- a
        # bot-detection hiccup must never fail a vote request.
        await record_vote_attempt(BrokenRedis(), ip="1.2.3.4", user_id="user-1")


# --- evaluate_single_ip_bursts ------------------------------------------


def _seed_burst(fake_redis, ip, count, *, at=None):
    now = at if at is not None else time.time()
    fake_redis.zsets[ip_burst_key(ip)] = {f"m{i}": now for i in range(count)}


class TestSingleIpBurst:
    @pytest.mark.asyncio
    async def test_100_attempts_produces_exactly_one_warning(
        self, fake_redis, captured_anomalies
    ):
        created, notified = captured_anomalies
        _seed_burst(fake_redis, "9.9.9.9", 100)

        await evaluator.evaluate_single_ip_bursts(fake_redis)

        assert len(created) == 1
        assert created[0].severity == "warning"
        assert created[0].alert_type == "bot_pattern_detected"
        assert created[0].ip_address == "9.9.9.9"
        assert notified == []  # never notified for a warning

    @pytest.mark.asyncio
    async def test_under_threshold_raises_nothing(self, fake_redis, captured_anomalies):
        created, _ = captured_anomalies
        _seed_burst(fake_redis, "9.9.9.9", 99)

        await evaluator.evaluate_single_ip_bursts(fake_redis)

        assert created == []

    @pytest.mark.asyncio
    async def test_cooldown_suppresses_repeat_warning_within_window(
        self, fake_redis, captured_anomalies
    ):
        created, _ = captured_anomalies
        _seed_burst(fake_redis, "9.9.9.9", 100)

        await evaluator.evaluate_single_ip_bursts(fake_redis)
        await evaluator.evaluate_single_ip_bursts(fake_redis)

        assert len(created) == 1

    @pytest.mark.asyncio
    async def test_expired_entries_are_trimmed_before_counting(
        self, fake_redis, captured_anomalies
    ):
        created, _ = captured_anomalies
        old = time.time() - 20  # older than the 10s window
        _seed_burst(fake_redis, "9.9.9.9", 100, at=old)

        await evaluator.evaluate_single_ip_bursts(fake_redis)

        assert created == []
        assert len(fake_redis.zsets[ip_burst_key("9.9.9.9")]) == 0

    @pytest.mark.asyncio
    async def test_condition_sustained_3_ticks_escalates_to_critical(
        self, fake_redis, captured_anomalies
    ):
        created, notified = captured_anomalies

        for _ in range(3):
            _seed_burst(fake_redis, "9.9.9.9", 100)
            await evaluator.evaluate_single_ip_bursts(fake_redis)

        severities = [row.severity for row in created]
        assert severities.count("warning") == 1  # suppressed by its own cooldown after tick 1
        assert severities.count("critical") == 1
        assert len(notified) == 1  # notify_admins called exactly once, for the critical row

    @pytest.mark.asyncio
    async def test_condition_clearing_resets_the_streak(
        self, fake_redis, captured_anomalies
    ):
        created, _ = captured_anomalies

        _seed_burst(fake_redis, "9.9.9.9", 100)
        await evaluator.evaluate_single_ip_bursts(fake_redis)

        fake_redis.zsets[ip_burst_key("9.9.9.9")] = {}  # condition clears
        await evaluator.evaluate_single_ip_bursts(fake_redis)

        _seed_burst(fake_redis, "9.9.9.9", 100)
        await evaluator.evaluate_single_ip_bursts(fake_redis)
        _seed_burst(fake_redis, "9.9.9.9", 100)
        await evaluator.evaluate_single_ip_bursts(fake_redis)

        # Streak was reset by the clearing tick, so 2 more consecutive
        # hits (ticks 3 and 4 overall) is not yet 3-in-a-row -- no
        # critical escalation should have fired.
        assert "critical" not in [row.severity for row in created]


# --- evaluate_global_spike ------------------------------------------------


class TestGlobalSpike:
    @pytest.mark.asyncio
    async def test_100k_in_current_bucket_produces_critical_and_notifies(
        self, fake_redis, captured_anomalies
    ):
        created, notified = captured_anomalies
        bucket = int(time.time())
        fake_redis.strings[global_bucket_key(bucket)] = "100000"

        await evaluator.evaluate_global_spike(fake_redis)

        assert len(created) == 1
        assert created[0].severity == "critical"
        assert created[0].ip_address is not None  # satisfies the anomalies CHECK constraint
        assert len(notified) == 1

    @pytest.mark.asyncio
    async def test_under_threshold_raises_nothing(self, fake_redis, captured_anomalies):
        created, _ = captured_anomalies
        bucket = int(time.time())
        fake_redis.strings[global_bucket_key(bucket)] = "99999"

        await evaluator.evaluate_global_spike(fake_redis)

        assert created == []

    @pytest.mark.asyncio
    async def test_cooldown_suppresses_repeat_alert(self, fake_redis, captured_anomalies):
        created, _ = captured_anomalies
        bucket = int(time.time())
        fake_redis.strings[global_bucket_key(bucket)] = "200000"

        await evaluator.evaluate_global_spike(fake_redis)
        await evaluator.evaluate_global_spike(fake_redis)

        assert len(created) == 1


# --- evaluate_low_and_slow -------------------------------------------------


def _mark_active(fake_redis, ip):
    """`recent_active_ips` discovers IPs via their `botcheck:ip:*` key —
    real `record_vote_attempt` calls always write both that key and the
    matching `ip_users` entry together in one pipeline, so a low-and-slow
    fixture needs a (small, under-threshold) burst entry too, or the
    evaluator would never consider the IP at all.
    """
    fake_redis.zsets.setdefault(ip_burst_key(ip), {})["seed"] = time.time()


class TestLowAndSlow:
    @pytest.mark.asyncio
    async def test_20_distinct_users_from_one_ip_produces_warning(
        self, fake_redis, captured_anomalies
    ):
        created, notified = captured_anomalies
        _mark_active(fake_redis, "5.5.5.5")
        fake_redis.sets[ip_users_key("5.5.5.5")] = {f"user-{i}" for i in range(20)}

        await evaluator.evaluate_low_and_slow(fake_redis)

        assert len(created) == 1
        assert created[0].severity == "warning"
        assert notified == []

    @pytest.mark.asyncio
    async def test_under_threshold_raises_nothing(self, fake_redis, captured_anomalies):
        created, _ = captured_anomalies
        _mark_active(fake_redis, "5.5.5.5")
        fake_redis.sets[ip_users_key("5.5.5.5")] = {f"user-{i}" for i in range(19)}

        await evaluator.evaluate_low_and_slow(fake_redis)

        assert created == []

    @pytest.mark.asyncio
    async def test_cooldown_suppresses_repeat_alert(self, fake_redis, captured_anomalies):
        created, _ = captured_anomalies
        _mark_active(fake_redis, "5.5.5.5")
        fake_redis.sets[ip_users_key("5.5.5.5")] = {f"user-{i}" for i in range(20)}

        await evaluator.evaluate_low_and_slow(fake_redis)
        await evaluator.evaluate_low_and_slow(fake_redis)

        assert len(created) == 1
