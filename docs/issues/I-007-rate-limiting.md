# I-007: Rate Limiting (Per-User, Per-IP)

**Status**: Ready for Implementation  
**Epic**: Voting Infrastructure  
**Priority**: P0 (Blocker)  
**Estimated Effort**: 3 days

---

## Problem Statement

Individual bot accounts and botnet swarms can attempt to inflate vote counts or exhaust API quota by voting far more frequently than a real user would. The poll app needs to throttle both:
- A single user_id voting too fast (compromised token, scripted account)
- A single IP address voting too fast (botnet, shared script hitting many fake accounts from one source)

This check must run **before** a vote is queued (I-005) so that throttled requests never reach `queue:votes` or the uniqueness reservation, and it must be cheap enough to run on every single vote request without adding meaningful latency to the P99 < 200ms target.

This issue is scoped strictly to the sliding **1-minute rate window** (spec decision #5, first half). It is explicitly not the 3-strike duplicate-block mechanism — that's a related but separate anomaly response, tracked in I-015, and is out of scope here.

---

## Solution

Implement Redis-backed rate limiting with two independent counters checked on every vote request:
- **Per-user**: 5 votes/minute, key `rate_limit:{user_id}`
- **Per-IP**: 50 votes/minute, key `rate_limit_ip:{ip}`

Both keys use TTL 61 seconds (per spec decision #5) so they self-expire without any cleanup job. If either counter is exceeded, the request is rejected with `429 Too Many Requests` before rate-limit checking would even reach I-006's uniqueness check or I-005's enqueue step.

---

## User Stories

(from SPECIFICATION.md)

21. As the system, I want to reject votes from users exceeding 5 votes per minute, so that I throttle individual bot accounts
22. As the system, I want to reject votes from IPs exceeding 50 votes per minute, so that I throttle botnet swarms

---

## Implementation Decisions

### Sliding Window Approach: Fixed-Window-with-TTL

**Decision**: Use a fixed-window counter with a 61-second TTL (Redis `INCR` + `EXPIRE`), not a sorted-set sliding log.

```python
async def _check_limit(key: str, limit: int) -> bool:
    """Returns True if the request is allowed, False if the limit is exceeded."""
    count = await redis.incr(key)
    if count == 1:
        await redis.expire(key, 61)
    return count <= limit

async def check_rate_limit(user_id: str, ip: str) -> None:
    user_ok = await _check_limit(f"rate_limit:{user_id}", limit=5)
    if not user_ok:
        raise RateLimitExceededError("User rate limit exceeded", retry_after=60)

    ip_ok = await _check_limit(f"rate_limit_ip:{ip}", limit=50)
    if not ip_ok:
        raise RateLimitExceededError("IP rate limit exceeded", retry_after=60)
```

**Trade-off, stated honestly**: A fixed window is not a true sliding window. A user who votes 5 times at 0:59 and 5 more times at 1:01 sends 10 votes in a 2-second span while never exceeding the per-window limit — up to a 2x burst is possible at the window boundary. A sorted-set-based sliding log (`ZADD` with per-request timestamps, `ZREMRANGEBYSCORE` to evict entries older than 60s, `ZCARD` to count) would be exact, but costs 2-3 Redis operations per check instead of 1-2, and requires storing one entry per vote attempt instead of a single integer, per user, per window.

Given the actual limits here — 5/min per user is a tight bound where a 2x boundary burst (worst case 10 votes/min) is still far below any threshold that matters for bot defense — the extra precision of a sliding log isn't worth doubling Redis ops on every single vote request at 1M+ vote scale. The per-IP limit (50/min) has the same trade-off at a larger scale, same reasoning. If boundary bursts prove to be a real attack vector in practice (observable via I-014's bot detection), this can be revisited without changing the calling contract in I-005.

### Check Order

Per-user check runs before per-IP check. A single abusive user is cheaper to reject early (one key lookup) than to fall through to the shared per-IP counter, and rejecting on the tighter, more specific limit first gives a clearer error to legitimate users who happen to share an IP (e.g., office NAT, mobile carrier NAT) with someone else who's being throttled.

### Response on Rate Limit Exceeded

```json
{
  "success": false,
  "error": {
    "code": "RATE_LIMIT_EXCEEDED",
    "message": "User rate limit exceeded",
    "details": { "retry_after_seconds": 60 }
  },
  "meta": {
    "timestamp": "2026-08-10T11:00:00Z",
    "request_id": "req-abc123"
  }
}
```
HTTP status 429, `Retry-After: 60` header set alongside the standard error envelope from I-003. Per spec decision #10, this is a retryable error — clients should back off exponentially, not retry immediately.

### Relationship to I-005 and I-006

`check_rate_limit()` is called by I-005's `POST /v1/vote` handler as the *first* check after poll/answer validation, before I-006's uniqueness reservation. This ordering means a user who is already rate-limited never consumes a uniqueness `SET NX` call or reaches the queue — rate limiting is the cheapest and earliest gate in the request pipeline.

### Out of Scope: 3-Strike Duplicate Block

The `blocked:{user_id}:{ip}` key and its 1-hour lockout after 3 duplicate-vote attempts (spec decision #5, second half; domain model's `VoteRateLimitQuota.duplicate_block_threshold`) is a **separate mechanism**, tracked in **I-015: Duplicate Detection (3-Strike Block)**. It is related — both live in the "reject before queue" path and both write to Redis-backed keys — but it responds to a different signal (repeated *duplicate* attempts, not raw request volume) and is not implemented as part of this issue. I-015 will likely call into this issue's Redis client utilities but owns its own key namespace and logic.

---

## Acceptance Criteria

- [ ] A user's 5th vote within 1 minute succeeds; the 6th returns 429 `RATE_LIMIT_EXCEEDED`
- [ ] After the window expires (61s TTL), the same user can vote again
- [ ] Two different IPs each voting 25 times (50 total) both succeed; a 26th vote from either IP alone returns 429
- [ ] `Retry-After` header set to 60 on all 429 responses from this check
- [ ] Rate limit check runs and rejects before any call to I-006's uniqueness check or I-005's enqueue step (verified: rejected request never touches `queue:votes` or the uniqueness key)
- [ ] Redis unavailability during a rate limit check returns 503, not a silent bypass
- [ ] Keys `rate_limit:{user_id}` and `rate_limit_ip:{ip}` expire automatically (TTL 61s), no cleanup job required

---

## Testing Strategy

- **Unit Tests**: `_check_limit` against fakeredis/real Redis — 5 calls succeed, 6th fails, TTL set on first call only
- **Integration Tests**: Through `POST /v1/vote` — 5 successful votes from one user, 6th returns 429; two IPs each at 25 succeed, 26th from either fails
- **Boundary Tests**: Explicitly test and document the known fixed-window burst behavior (votes at window edge) so it's a known, tested trade-off rather than an undiscovered gap
- **Load Tests**: Confirm rate-limit checks add negligible latency (<5ms) under 1000 req/sec, feeds into I-023
- **Not Tested Here**: Sliding window algorithm internals (per spec's testing philosophy — behavior only), 3-strike block logic (I-015's responsibility)
- **Prior Art**: Mirror I-001's Redis-backed constraint tests structurally

---

## Related Issues

- I-002: Redis Cluster Setup (hosts `rate_limit:*` and `rate_limit_ip:*` keyspaces)
- I-003: API Framework & Routing (429 error envelope and `Retry-After` conventions)
- I-005: Vote Acceptance & Queueing (calls this check as the first gate in the vote pipeline)
- I-006: Uniqueness Enforcement (runs immediately after this check; distinct concern)
- I-009: Result Aggregation (unaffected — reads vote counts, not rate limit state)
- I-015: Duplicate Detection (3-Strike Block) — related but out-of-scope mechanism, see above

---

## Implementation Checklist

- [ ] Implement `_check_limit()` and `check_rate_limit()` in `src/services/rate_limit.py`
- [ ] Define `RateLimitExceededError` and wire to `429 RATE_LIMIT_EXCEEDED` + `Retry-After` header in I-003's exception handlers
- [ ] Wire `check_rate_limit(user_id, ip)` into I-005's `POST /v1/vote` handler, first check in the pipeline
- [ ] Add Redis connection failure handling → `503 SERVICE_UNAVAILABLE`
- [ ] Write unit tests for window/TTL behavior and boundary-burst trade-off
- [ ] Write integration tests for per-user and per-IP limits through the live endpoint
- [ ] Document key format and TTL policy in `docs/architecture/redis-keys.md` (shared doc with I-006)

---

**Acceptance**: Per-user and per-IP limits enforced and tested, boundary behavior documented, PR reviewed and merged.
