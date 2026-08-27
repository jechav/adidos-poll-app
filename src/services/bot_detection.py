"""Inline bot-detection counter updates, called from I-005's vote
acceptance path (I-014).

These `botcheck:*` keys are deliberately separate from I-007's
`rate_limit:*`/`rate_limit_ip:*` counters (see docs/architecture/redis-keys.md):
reusing those would mean a pattern that stays under the per-request quota
(e.g. 40 distinct users from one IP, each individually fine) would never
accumulate anywhere I-014's periodic evaluator (`src/jobs/bot_detection.py`)
can see it.

`record_vote_attempt` is the only function this module exposes to the
hot path. It is written to fail silently on any Redis error — a
bot-detection counter miss must never fail, delay, or retry a vote
request; the evaluator simply sees a slightly incomplete picture for
that one attempt, which is an acceptable trade-off for never adding
latency or a new failure mode to `POST /v1/vote`.
"""

from __future__ import annotations

import logging
import time
from uuid import uuid4

logger = logging.getLogger("poll_app.bot_detection")

# See docs/architecture/redis-keys.md for the full contract these three
# keys extend.
IP_BURST_WINDOW_SECONDS = 10
IP_USERS_TTL_SECONDS = 300
GLOBAL_BUCKET_TTL_SECONDS = 5


def ip_burst_key(ip: str) -> str:
    return f"botcheck:ip:{ip}"


def ip_users_key(ip: str) -> str:
    return f"botcheck:ip_users:{ip}"


def global_bucket_key(bucket: int) -> str:
    return f"botcheck:global:{bucket}"


async def record_vote_attempt(redis, *, ip: str, user_id: str) -> None:
    """Update all three inline counters for one vote attempt in a single
    pipelined round trip: `ZADD` the IP's rolling-window timestamp,
    `SADD` the user into that IP's distinct-user set (refreshing its
    TTL), and `INCR` the current-second global bucket (refreshing its
    TTL). No blocking reads — every command here is a write.

    Best-effort: any failure (Redis unreachable, wrong client type in a
    test double, etc.) is logged and swallowed, never raised, so a
    bot-detection hiccup can never fail or slow down a vote request.
    """
    try:
        now = time.time()
        bucket = int(now)
        # A `ZADD` member must be unique per attempt, not just per
        # timestamp — two attempts landing in the same wall-clock
        # instant (readily possible during exactly the bursts this
        # module exists to detect) must not overwrite one another's
        # score-only entry.
        member = f"{now}:{uuid4()}"

        pipe = redis.pipeline(transaction=False)
        pipe.zadd(ip_burst_key(ip), {member: now})
        pipe.sadd(ip_users_key(ip), user_id)
        pipe.expire(ip_users_key(ip), IP_USERS_TTL_SECONDS)
        pipe.incr(global_bucket_key(bucket))
        pipe.expire(global_bucket_key(bucket), GLOBAL_BUCKET_TTL_SECONDS)
        await pipe.execute()
    except Exception:
        logger.warning("bot_detection_counter_update_failed", exc_info=True)
