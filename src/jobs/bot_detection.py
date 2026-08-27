"""Periodic bot-pattern evaluator (I-014).

Reads the `botcheck:*` counters `src.services.bot_detection.record_vote_attempt`
writes inline during vote acceptance (I-005), checks them against a fixed
rule table every `EVALUATOR_INTERVAL_SECONDS`, and writes `anomalies`
rows (I-001) when a rule fires — exactly the `bot_pattern_detected`
`alert_type`; `rate_limit_exceeded` (I-007) and `duplicate_attempts_blocked`
(I-015) are written elsewhere.

Runs as a standalone async loop in its own process/pod (see
`deploy/deployments/bot-detection-evaluator.yaml`), never inside an API
request handler and never colocated with I-008's vote processor workers
— evaluation work must not compete with request-serving or DB-write
capacity during the exact bursts it's trying to detect.
"""

from __future__ import annotations

import asyncio
import logging
import time
from datetime import datetime, timezone
from uuid import UUID, uuid4

from redis.exceptions import RedisError

from src.services import anomalies as anomalies_repo
from src.services.bot_detection import (
    IP_BURST_WINDOW_SECONDS,
    global_bucket_key,
    ip_burst_key,
    ip_users_key,
)
from src.services.notifications import notify_admins

logger = logging.getLogger("poll_app.bot_detection.evaluator")

EVALUATOR_INTERVAL_SECONDS = 5

# --- Rule thresholds and cooldowns (fixed constants — see I-014's
# "Out of Scope": no runtime-tunable thresholds in this issue). ---------

SINGLE_IP_BURST_THRESHOLD = 100
SINGLE_IP_BURST_COOLDOWN_SECONDS = 60

# "Still true on 3 consecutive evaluator ticks" — tracked via a small
# Redis counter (`botcheck:ip_burst_streak:{ip}`) incremented each tick
# the burst condition holds and reset otherwise; its TTL is a little
# more than one tick interval so a skipped/slow tick doesn't leave a
# stale streak lingering forever.
SINGLE_IP_SUSTAINED_TICKS = 3
SINGLE_IP_SUSTAINED_COOLDOWN_SECONDS = 300

GLOBAL_SPIKE_THRESHOLD = 100_000
GLOBAL_SPIKE_COOLDOWN_SECONDS = 30
# `anomalies` requires `user_id IS NOT NULL OR ip_address IS NOT NULL`
# (I-001); a global, service-wide spike has neither a specific user nor
# IP, so this fixed sentinel satisfies the constraint while making clear
# in admin/Adidos-facing views that the alert isn't about one address.
GLOBAL_SPIKE_IP_SENTINEL = "global"

LOW_AND_SLOW_THRESHOLD = 20
LOW_AND_SLOW_WINDOW_SECONDS = 300
LOW_AND_SLOW_COOLDOWN_SECONDS = 600


def _cooldown_key(rule: str, target: str) -> str:
    return f"botcheck:cooldown:{rule}:{target}"


def _streak_key(ip: str) -> str:
    return f"botcheck:ip_burst_streak:{ip}"


async def recent_active_ips(redis) -> set[str]:
    """IPs with a non-expired `botcheck:ip:*` key — i.e. touched within
    the last `IP_BURST_WINDOW_SECONDS`-ish window. A `SCAN` against this
    small, short-lived key set is acceptable here: it only runs out of
    band, once per evaluator tick, never on the vote-acceptance hot path.
    """
    ips: set[str] = set()
    prefix = "botcheck:ip:"
    async for key in redis.scan_iter(match=f"{prefix}*"):
        ips.add(key[len(prefix):])
    return ips


async def raise_anomaly(
    *,
    severity: str,
    alert_type: str,
    description: str,
    ip_address: str | None = None,
    user_id: str | None = None,
    poll_id: UUID | None = None,
) -> None:
    alert = await anomalies_repo.create(
        alert_id=uuid4(),
        alert_type=alert_type,
        severity=severity,
        description=description,
        created_at=datetime.now(timezone.utc),
        user_id=user_id,
        ip_address=ip_address,
        poll_id=poll_id,
    )
    if severity == "critical":
        await notify_admins(alert)


async def evaluate_single_ip_bursts(redis) -> None:
    """Single-IP burst (>=100 attempts/10s -> warning) and its escalation
    to critical once the condition has held for
    `SINGLE_IP_SUSTAINED_TICKS` consecutive ticks (~30s at a 10s
    evaluator interval).
    """
    now = time.time()
    for ip in await recent_active_ips(redis):
        key = ip_burst_key(ip)
        await redis.zremrangebyscore(key, 0, now - IP_BURST_WINDOW_SECONDS)
        count = await redis.zcard(key)
        streak_key = _streak_key(ip)

        if count < SINGLE_IP_BURST_THRESHOLD:
            await redis.delete(streak_key)
            continue

        streak = await redis.incr(streak_key)
        await redis.expire(streak_key, EVALUATOR_INTERVAL_SECONDS * 3)

        warning_cooldown = _cooldown_key("single_ip_burst", ip)
        if not await redis.exists(warning_cooldown):
            await raise_anomaly(
                ip_address=ip,
                alert_type="bot_pattern_detected",
                severity="warning",
                description=(
                    f"{count} vote attempts from {ip} in the last "
                    f"{IP_BURST_WINDOW_SECONDS} seconds"
                ),
            )
            await redis.set(warning_cooldown, "1", ex=SINGLE_IP_BURST_COOLDOWN_SECONDS)

        if streak >= SINGLE_IP_SUSTAINED_TICKS:
            critical_cooldown = _cooldown_key("single_ip_sustained_burst", ip)
            if not await redis.exists(critical_cooldown):
                await raise_anomaly(
                    ip_address=ip,
                    alert_type="bot_pattern_detected",
                    severity="critical",
                    description=(
                        f"Single-IP burst from {ip} has persisted for "
                        f"{streak} consecutive evaluator ticks"
                    ),
                )
                await redis.set(
                    critical_cooldown, "1", ex=SINGLE_IP_SUSTAINED_COOLDOWN_SECONDS
                )


async def evaluate_global_spike(redis) -> None:
    """Global traffic spike (>=100,000 accepted attempts in any trailing
    1-second bucket -> critical, story #20).
    """
    now_bucket = int(time.time())
    # "any bucket in the trailing few seconds": the current bucket plus
    # the previous one, since a bucket can still be accumulating when
    # this tick runs mid-second.
    for bucket in (now_bucket, now_bucket - 1):
        raw = await redis.get(global_bucket_key(bucket))
        count = int(raw) if raw else 0
        if count < GLOBAL_SPIKE_THRESHOLD:
            continue

        cooldown_key = _cooldown_key("global_spike", "global")
        if await redis.exists(cooldown_key):
            continue

        await raise_anomaly(
            ip_address=GLOBAL_SPIKE_IP_SENTINEL,
            alert_type="bot_pattern_detected",
            severity="critical",
            description=f"{count} accepted vote attempts service-wide in a single second",
        )
        await redis.set(cooldown_key, "1", ex=GLOBAL_SPIKE_COOLDOWN_SECONDS)


async def evaluate_low_and_slow(redis) -> None:
    """Distributed low-and-slow (>=20 distinct user_ids from one IP
    within 5 minutes -> warning) — catches an attack no individual user
    or IP ever trips I-007's per-request quota for.
    """
    for ip in await recent_active_ips(redis):
        count = await redis.scard(ip_users_key(ip))
        if count < LOW_AND_SLOW_THRESHOLD:
            continue

        cooldown_key = _cooldown_key("low_and_slow", ip)
        if await redis.exists(cooldown_key):
            continue

        await raise_anomaly(
            ip_address=ip,
            alert_type="bot_pattern_detected",
            severity="warning",
            description=(
                f"{count} distinct users voted from {ip} in the last "
                f"{LOW_AND_SLOW_WINDOW_SECONDS} seconds"
            ),
        )
        await redis.set(cooldown_key, "1", ex=LOW_AND_SLOW_COOLDOWN_SECONDS)


async def run_bot_detection_loop(redis) -> None:
    """The evaluator's main loop: run every rule, sleep, repeat, forever.

    A single Redis failure degrades one tick (the rules simply don't run
    that cycle) rather than crashing the loop — the next tick retries
    normally. Any other unexpected error is logged the same way so one
    bad tick can never turn into a crash loop for the whole process.
    """
    while True:
        try:
            await evaluate_single_ip_bursts(redis)
            await evaluate_global_spike(redis)
            await evaluate_low_and_slow(redis)
        except RedisError:
            logger.warning("bot_detection_tick_skipped_redis_unavailable", exc_info=True)
        except Exception:
            logger.exception("bot_detection_tick_failed")
        await asyncio.sleep(EVALUATOR_INTERVAL_SECONDS)


async def main() -> None:
    from src.cache.redis_client import get_redis

    logging.basicConfig(level=logging.INFO)
    redis = await get_redis()
    await run_bot_detection_loop(redis)


if __name__ == "__main__":
    asyncio.run(main())
