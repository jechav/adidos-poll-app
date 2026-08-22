"""Redis contribution to GET /health (I-002).

A single node failure should not flip the whole service unhealthy (spec
decision #7: availability over consistency) — a `PING` failure is reported
as "degraded", not "down", since the cluster keeps serving unaffected
hash slots. The ping is bounded: a node that's reachable but not
responding (e.g. a network partition, or misconfigured cluster-announce
addressing) must count as "degraded" too, not hang the health check.
"""

import asyncio

from src.cache.redis_client import get_redis

PING_TIMEOUT_SECONDS = 2


async def redis_health() -> dict:
    redis = await get_redis()
    try:
        await asyncio.wait_for(redis.ping(), timeout=PING_TIMEOUT_SECONDS)
        return {"redis": "ok"}
    except Exception:
        return {"redis": "degraded"}
