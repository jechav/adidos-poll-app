"""Sliding-window vote rate limiting (I-007).

Two independent counters are checked before a vote would reach I-006's
uniqueness reservation or I-005's `queue:votes` enqueue step:

- per-user: `rate_limit:{user_id}`, 5 votes/minute
- per-IP:   `rate_limit_ip:{ip}`, 50 votes/minute

Both use a fixed-window counter with a 61s TTL (`INCR` + `EXPIRE` on the
first hit of a window) rather than a sorted-set sliding log. A single
`INCR` is one atomic Redis round trip and does double duty as both the
write and the read of the current count; `EXPIRE` only runs when the
window is new (`count == 1`), so the TTL is set once per window rather
than refreshed on every vote (refreshing it would turn a fixed window
into an unbounded one).

This is a fixed window, not a true sliding one: a user who votes 5 times
at 0:59 and 5 more at 1:01 sends 10 votes in ~2 seconds without ever
exceeding the per-window limit — up to a 2x burst is possible at the
window boundary. See docs/issues/I-007-rate-limiting.md for why that
trade-off is accepted at these limits (5/min and 50/min) rather than
paying for a `ZADD`/`ZREMRANGEBYSCORE`/`ZCARD` sliding log on every vote.

Key format and TTL policy are documented in docs/architecture/redis-keys.md
(the canonical, shared Redis keyspace doc).
"""

import logging

from redis.exceptions import RedisError

from src.cache.redis_client import get_redis

logger = logging.getLogger("poll_app.rate_limit")

USER_VOTE_LIMIT_PER_MINUTE = 5
IP_VOTE_LIMIT_PER_MINUTE = 50
WINDOW_TTL_SECONDS = 61
RETRY_AFTER_SECONDS = 60


class RateLimitExceededError(Exception):
    """Raised when a per-user or per-IP vote rate limit is exceeded.

    Carries `retry_after` (seconds) so the API layer can set the
    `Retry-After` header alongside the 429 `RATE_LIMIT_EXCEEDED` envelope
    (spec decision #10: this is a retryable error, clients should back
    off, not retry immediately).
    """

    def __init__(self, message: str, retry_after: int = RETRY_AFTER_SECONDS):
        self.message = message
        self.retry_after = retry_after
        super().__init__(message)


class RateLimitServiceUnavailableError(Exception):
    """Raised when Redis is unreachable during a rate limit check.

    Deliberately a distinct error from "allowed" — a rate limit check
    that can't reach Redis must fail closed (503), never silently
    fall through as if the vote were within limits.
    """

    def __init__(self, message: str = "Rate limit check is temporarily unavailable"):
        self.message = message
        super().__init__(message)


def _user_key(user_id: str) -> str:
    return f"rate_limit:{user_id}"


def _ip_key(ip: str) -> str:
    return f"rate_limit_ip:{ip}"


async def _check_limit(key: str, limit: int) -> bool:
    """Increment `key` and report whether it's still within `limit`.

    Returns True if the request represented by this increment is allowed
    (count <= limit), False if it pushed the window over the limit.
    """
    redis = await get_redis()
    try:
        count = await redis.incr(key)
        if count == 1:
            await redis.expire(key, WINDOW_TTL_SECONDS)
    except RedisError as exc:
        logger.error("rate_limit_redis_unavailable key=%s", key, exc_info=exc)
        raise RateLimitServiceUnavailableError() from exc
    return count <= limit


async def check_rate_limit(user_id: str, ip: str) -> None:
    """Raise if `user_id` or `ip` has exceeded its vote rate this window.

    Intended to be called as the first check in I-005's `POST /v1/vote`
    handler, before I-006's uniqueness reservation and before the vote is
    enqueued — a rate-limited request should never touch either.

    Per-user is checked first: it's the tighter, more specific limit, and
    rejecting on it means one abusive user never charges the shared
    per-IP counter, which would otherwise risk falsely throttling other,
    legitimate users behind the same IP (e.g. office/mobile-carrier NAT).

    Raises:
        RateLimitExceededError: the user or IP has exceeded its quota
            for the current window.
        RateLimitServiceUnavailableError: Redis could not be reached to
            perform the check.
    """
    user_allowed = await _check_limit(_user_key(user_id), USER_VOTE_LIMIT_PER_MINUTE)
    if not user_allowed:
        raise RateLimitExceededError("User rate limit exceeded")

    ip_allowed = await _check_limit(_ip_key(ip), IP_VOTE_LIMIT_PER_MINUTE)
    if not ip_allowed:
        raise RateLimitExceededError("IP rate limit exceeded")
