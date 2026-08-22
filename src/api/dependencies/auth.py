"""Token extraction and identity resolution for route handlers (I-004).

Implements the trust-and-cache strategy from SPECIFICATION.md Implementation
Decision #1 (loose coupling): this service never calls back to Adidos
synchronously to validate a token. Instead:

1. The token is hashed (SHA-256) and looked up in Redis under
   ``auth:token:{token_hash}`` (the cache provisioned in I-002).
2. **Cache hit** — the cached `{user_id, role}` is reused directly; the
   token is never re-decoded.
3. **Cache miss** — the token is trusted on first use ("trust-on-first-use"):
   it is decoded locally (no network call), the result is written back to
   the cache with a 10 minute TTL (top of the 5-10 min window the spec
   allows), and the resolved identity is returned.

Known, accepted risk — revocation lag: if Adidos revokes a token, this
service will not notice until the cached entry expires (up to the 10
minute TTL below), per spec Implementation Decision #17 (Token Refresh
Lifecycle). A user whose token was revoked mid-window can still be treated
as authenticated until the cache entry ages out. This is a deliberate
trade-off of the loose-coupling architecture (spec decision #1), not a
bug: closing that window would require either a synchronous Adidos call
per request (rejected — it puts a network hop on the hot path of a
100K-votes/sec burst) or Adidos-pushed revocation events (out of scope,
deferred to a future version per the issue's "Out of Scope" section). If
the window proves unacceptable in practice, the mitigation is lowering
the TTL toward the 5 minute floor, not re-introducing tight coupling.

Availability note: Redis itself is treated the same way I-002's health
check treats a degraded cluster (spec decision #7, availability over
consistency) — if the cache is unreachable, reads/writes to it are
treated as a miss/no-op rather than surfaced as a 401 or 500. Auth must
keep working even if the cache layer is having a bad day; the token is
still decoded and trusted exactly as it would be on a normal cache miss.

Token format: Adidos hasn't specified a wire format for the token this
service receives at the API boundary (it's opaque to us — see spec
decision #1). `decode_adidos_token` therefore implements the convention
already established by I-003's routing tests and stub: the token content
*is* the `user_id`, and an `admin:` prefix denotes the admin role. This
keeps the real Adidos-integration detail (whatever `decode_adidos_token`
ultimately needs to become, e.g. a signed/opaque token format) as a
follow-up when that format is specified, without changing the dependency
surface routes already depend on.
"""

import hashlib
import logging

from fastapi import Depends, Header, HTTPException
from redis.asyncio.cluster import RedisCluster

from src.cache.redis_client import redis_dependency
from src.schemas.user_context import UserContext

logger = logging.getLogger("poll_app.api")

# Upper bound of the spec's 5-10 minute cache TTL window.
_CACHE_TTL_SECONDS = 600


class InvalidTokenError(Exception):
    """Raised when a bearer token cannot be decoded into a user identity."""


def _unauthorized(message: str = "Missing or malformed Authorization header") -> HTTPException:
    return HTTPException(
        status_code=401,
        detail={"code": "UNAUTHORIZED", "message": message},
    )


def extract_bearer_token(authorization: str | None = Header(default=None)) -> str:
    if not authorization or not authorization.startswith("Bearer "):
        raise _unauthorized()
    token = authorization.removeprefix("Bearer ").strip()
    if not token:
        raise _unauthorized()
    return token


def hash_token(token: str) -> str:
    """SHA-256 hash used as the cache key. The raw token is never stored
    in Redis or logged — only this hash appears in the cache key."""
    return hashlib.sha256(token.encode()).hexdigest()


def decode_adidos_token(token: str) -> dict:
    """Decode an Adidos-issued token into its claims, with no network call.

    See the module docstring's "Token format" note: this implements the
    `admin:`-prefix convention already established by I-003, pending a
    real Adidos token format spec.
    """
    if not token or not token.strip():
        raise InvalidTokenError("empty token")
    if token.startswith("admin:"):
        user_id = token.removeprefix("admin:")
        if not user_id:
            raise InvalidTokenError("empty user_id in admin token")
        return {"user_id": user_id, "role": "admin"}
    return {"user_id": token, "role": "user"}


async def get_current_user(
    token: str = Depends(extract_bearer_token),
    redis: RedisCluster = Depends(redis_dependency),
) -> UserContext:
    token_hash = hash_token(token)
    cache_key = f"auth:token:{token_hash}"

    cached: dict = {}
    try:
        cached = await redis.hgetall(cache_key)
    except Exception:
        # Cache unavailable: fail open to trust-on-first-use rather than
        # rejecting the request or blocking on Adidos (spec decision #1).
        logger.warning("auth_cache_read_failed", extra={"cache_key": cache_key})

    if cached:
        return UserContext(user_id=cached["user_id"], role=cached["role"])

    # Cache miss (or cache unreachable): trust-on-first-use.
    try:
        claims = decode_adidos_token(token)
    except InvalidTokenError:
        raise _unauthorized("Invalid or expired token")

    user_ctx = UserContext(user_id=claims["user_id"], role=claims.get("role", "user"))

    try:
        await redis.hset(cache_key, mapping={"user_id": user_ctx.user_id, "role": user_ctx.role})
        await redis.expire(cache_key, _CACHE_TTL_SECONDS)
    except Exception:
        # Best-effort caching: population failure shouldn't fail the request,
        # it just means the next request pays the decode cost again too.
        logger.warning("auth_cache_write_failed", extra={"cache_key": cache_key})

    return user_ctx


async def require_admin(user: UserContext = Depends(get_current_user)) -> UserContext:
    if user.role != "admin":
        raise HTTPException(
            status_code=403,
            detail={"code": "FORBIDDEN", "message": "User is not admin"},
        )
    return user
