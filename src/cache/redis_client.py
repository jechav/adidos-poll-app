"""Shared async Redis Cluster client (I-002).

A single pooled `RedisCluster` connection is created lazily on first use and
reused across the process — callers should depend on `redis_dependency`
(a FastAPI dependency) rather than constructing their own client, so the
whole app shares one pool instead of reconnecting per request.

redis-py's asyncio cluster client resolves hash slots via `CLUSTER SLOTS`
and automatically retries against the new topology on `MOVED`/`ASK`
responses, so callers don't need to handle cluster redirects themselves.
"""

from redis.asyncio.cluster import RedisCluster

from src.config import settings

_redis: RedisCluster | None = None


async def get_redis() -> RedisCluster:
    global _redis
    if _redis is None:
        _redis = RedisCluster(
            host=settings.redis_startup_host,
            port=settings.redis_startup_port,
            decode_responses=True,
            max_connections=100,
        )
    return _redis


async def close_redis() -> None:
    """Close the pooled client. Call from the app's shutdown/lifespan hook."""
    global _redis
    if _redis is not None:
        await _redis.aclose()
        _redis = None


async def redis_dependency() -> RedisCluster:
    """FastAPI dependency: `Depends(redis_dependency)` in route handlers."""
    return await get_redis()
