"""Redis contribution to GET /health (I-002).

A single node failure should not flip the whole service unhealthy (spec
decision #7: availability over consistency) — a `PING` failure is reported
as "degraded", not "down", since the cluster keeps serving unaffected
hash slots.
"""

from src.cache.redis_client import get_redis


async def redis_health() -> dict:
    redis = await get_redis()
    try:
        await redis.ping()
        return {"redis": "ok"}
    except Exception:
        return {"redis": "degraded"}
