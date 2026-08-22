"""Shared async Redis Cluster client (I-002).

A single pooled `RedisCluster` connection is created lazily on first use and
reused across the process — callers should depend on `redis_dependency`
(a FastAPI dependency) rather than constructing their own client, so the
whole app shares one pool instead of reconnecting per request.

redis-py's asyncio cluster client resolves hash slots via `CLUSTER SLOTS`
and automatically retries against the new topology on `MOVED`/`ASK`
responses, so callers don't need to handle cluster redirects themselves.

Both timeouts below are set explicitly rather than left at redis-py's
default (no timeout — block forever): a slot redirect to an address this
process can't route to (see I-025) must surface as a fast
`redis.exceptions.TimeoutError` (a `RedisError`, so every existing
`except RedisError`/`except Exception` fallback in auth.py, rate_limit.py,
uniqueness.py, and vote_queue.py already handles it correctly) rather than
hang the request indefinitely.
"""

from redis.asyncio.cluster import RedisCluster

from src.config import settings

_redis: RedisCluster | None = None

_SOCKET_CONNECT_TIMEOUT_SECONDS = 3
_SOCKET_TIMEOUT_SECONDS = 3


async def get_redis() -> RedisCluster:
    global _redis
    if _redis is None:
        _redis = RedisCluster(
            host=settings.redis_startup_host,
            port=settings.redis_startup_port,
            decode_responses=True,
            max_connections=100,
            socket_connect_timeout=_SOCKET_CONNECT_TIMEOUT_SECONDS,
            socket_timeout=_SOCKET_TIMEOUT_SECONDS,
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


async def mget_pipelined(redis: RedisCluster, keys: list[str]) -> list[str | None]:
    """Fetch multiple keys in a single pipelined round trip (I-009).

    Plain `MGET` isn't safe to use directly against a Redis Cluster: keys
    for different polls/answers routinely land in different hash slots,
    and Cluster's `MGET` requires every key to share one slot (otherwise
    it raises `CROSSSLOT`). A pipeline of individual `GET`s sidesteps
    that — redis-py's cluster pipeline groups the buffered commands by
    the node that owns each key's slot and sends each node's batch in one
    round trip, so this stays a small, bounded number of round trips
    (one per node touched) rather than one round trip per key.

    Returns results in the same order as `keys`; a missing key comes back
    as `None`, matching `MGET` semantics.
    """
    if not keys:
        return []
    pipe = redis.pipeline(transaction=False)
    for key in keys:
        pipe.get(key)
    return await pipe.execute()
