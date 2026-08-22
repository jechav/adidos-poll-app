# I-004: Authentication Middleware

**Status**: Done
**Epic**: Voting Infrastructure
**Priority**: P0 (Blocker)
**Estimated Effort**: 2 days
**Depends On**: I-003 (API Framework & Routing), I-002 (Redis Cluster)

---

## Problem Statement

Every user-facing and admin route needs to know who's calling: a `user_id` for voting/history endpoints, and a role check (`admin`) for poll management endpoints. The service does not own user identity — Adidos does — so this issue must:

- Extract and identify the caller from the Adidos token attached to each request
- Avoid a real-time call back to Adidos on every request (per spec decision #1: loose coupling, cost optimization)
- Distinguish `admin` from regular users for admin-only routes (I-013)
- Produce 401/403 responses that match the error code table already established in I-003
- Do this cheaply enough to sit in front of a 1M-vote, bursty workload without becoming the bottleneck

This issue implements the FastAPI dependency layer other route handlers plug into — it does not implement any of the actual vote/poll/admin business logic (those are I-005, I-011, I-012, I-013).

---

## Solution

Implement Adidos token validation as a pair of FastAPI dependencies:
1. **`get_current_user`** — extracts the token, resolves `user_id` + `role`, usable on any authenticated route
2. **`require_admin`** — builds on `get_current_user`, rejects with 403 if `role != "admin"`, usable on admin-only routes

Both are backed by the Redis token cache provisioned in I-002 (`auth:token:{token_hash}`), following the **trust-and-cache** strategy from spec Implementation Decision #1: the token is treated as valid on a cache miss (loose coupling — no synchronous call back to Adidos), and the validation result is cached for 5-10 minutes.

---

## User Stories

1. As a client, I want to call any authenticated endpoint with `Authorization: Bearer <token>`, so that the service knows who I am without a separate login step
2. As a client, I want my token validated on every request, so that I get a clear 401 if it's missing or malformed
3. As an admin, I want admin-only routes to reject non-admin tokens with 403, so that poll management stays restricted
4. As a developer, I want a reusable `Depends(get_current_user)` dependency, so that I don't duplicate token-parsing logic across route handlers
5. As an operator, I want token validation cached in Redis (5-10 min TTL), so that we're not adding a network round-trip to Adidos on every one of a million votes
6. As a privacy/security reviewer, I want the revocation-lag trade-off documented explicitly, so that it's a known, accepted risk rather than a surprise in an incident review

---

## Implementation Decisions

### Token Extraction

- **Header**: `Authorization: Bearer <token>` — standard bearer scheme, matching Adidos platform convention
- **Missing/malformed header**: immediate `401 UNAUTHORIZED`, no cache lookup, no processing
- Token is never logged in full; only a hash (see below) may appear in cache keys or debug logs

```python
# src/api/dependencies/auth.py
from fastapi import Header, HTTPException

def extract_bearer_token(authorization: str | None = Header(default=None)) -> str:
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail={
            "code": "UNAUTHORIZED",
            "message": "Missing or malformed Authorization header",
        })
    return authorization.removeprefix("Bearer ").strip()
```

### Trust-and-Cache Strategy (Spec Decision #1)

- **Decision**: The token itself is opaque to this service — we do not verify a signature or call Adidos synchronously on every request. Instead:
  1. Hash the token (`sha256`) to use as the cache key: `auth:token:{token_hash}`
  2. **Cache hit**: use the cached `{user_id, role}` — fast path, no further work
  3. **Cache miss**: treat the token as valid ("trust on first use"), extract `user_id`/`role` from the token payload itself (Adidos-issued, self-describing token), and populate the cache with a 5-10 minute TTL for subsequent requests
- **Rationale**: this is the loose-coupling trade-off from spec decision #1 — validating synchronously against Adidos on every vote would add a network hop to the hot path of a service that needs to absorb 100K votes/sec bursts. The cost of being wrong (accepting a token Adidos has since revoked) is bounded by the cache TTL and is judged acceptable against the cost of a synchronous dependency on every request.
- **This is a deliberate trade-off, not a bug.** It is documented here, in SPECIFICATION.md Implementation Decision #1, and in the Known Risks section, precisely so it isn't rediscovered as a surprise later.

```python
# src/api/dependencies/auth.py (continued)
import hashlib

async def get_current_user(
    token: str = Depends(extract_bearer_token),
    redis: RedisCluster = Depends(redis_dependency),
) -> UserContext:
    token_hash = hashlib.sha256(token.encode()).hexdigest()
    cache_key = f"auth:token:{token_hash}"

    cached = await redis.hgetall(cache_key)
    if cached:
        return UserContext(user_id=cached["user_id"], role=cached["role"])

    # Cache miss: trust-on-first-use. Decode Adidos-issued token payload directly.
    try:
        claims = decode_adidos_token(token)  # no network call; self-describing token
    except InvalidTokenError:
        raise HTTPException(status_code=401, detail={
            "code": "UNAUTHORIZED",
            "message": "Invalid or expired token",
        })

    user_ctx = UserContext(user_id=claims["user_id"], role=claims.get("role", "user"))
    await redis.hset(cache_key, mapping={"user_id": user_ctx.user_id, "role": user_ctx.role})
    await redis.expire(cache_key, 600)  # 10 min TTL (upper bound of 5-10 min window)
    return user_ctx
```

### Revocation Lag (Known, Accepted Risk)

- **Decision**: If Adidos revokes a token, this service will not notice until the cached entry expires — up to the full TTL window (5-10 minutes), per spec Implementation Decision #17 (Token Refresh Lifecycle).
- **Impact**: A user whose token was revoked mid-window can still cast votes against this service until the cache entry ages out. Adidos is the source of truth for session validity; this service does not attempt to shorten that window with proactive polling or webhooks (out of scope, and would reintroduce the tight coupling decision #1 explicitly avoided).
- **Cross-reference**: This matches SPECIFICATION.md's Known Risks framing — it is called out there as an accepted trade-off of the loose-coupling architecture, not tracked as an open bug.
- **Mitigation available if this becomes a problem in practice**: lower the TTL (floor of 5 min) to shrink the window, or move to an Adidos-pushed revocation event in a future version — both are explicitly deferred, not implemented here.

### `require_admin` Dependency

```python
# src/api/dependencies/auth.py (continued)
async def require_admin(user: UserContext = Depends(get_current_user)) -> UserContext:
    if user.role != "admin":
        raise HTTPException(status_code=403, detail={
            "code": "FORBIDDEN",
            "message": "User is not admin",
        })
    return user
```

- Builds on `get_current_user` rather than duplicating token extraction — admin routes get both authentication and authorization from a single dependency
- Used by I-013's admin routes: `POST /v1/admin/polls`, `PUT /v1/admin/polls/:id/state`, `GET /v1/admin/anomalies`

### Usage on Routes

```python
# example usage in a future route handler (I-005, I-011, I-012, I-013)
@router.get("/v1/user/votes")
async def get_user_votes(user: UserContext = Depends(get_current_user)):
    ...

@router.post("/v1/admin/polls")
async def create_poll(admin: UserContext = Depends(require_admin)):
    ...
```

### Error Responses

Matches the error code table established in I-003 exactly — no new codes introduced here.

| Code | HTTP Status | Trigger |
|------|-------------|---------|
| `UNAUTHORIZED` | 401 | Missing/malformed `Authorization` header, or token fails to decode |
| `FORBIDDEN` | 403 | Valid token, but `role != "admin"` on an admin-only route |

Both use the standard error envelope from I-003:

```json
{
  "success": false,
  "error": {
    "code": "UNAUTHORIZED",
    "message": "Missing or malformed Authorization header"
  },
  "meta": {
    "timestamp": "2026-08-10T11:00:00Z",
    "request_id": "req-abc123"
  }
}
```

---

## Acceptance Criteria

- [x] `Depends(get_current_user)` extracts and validates token, returns `UserContext(user_id, role)`
- [x] `Depends(require_admin)` rejects non-admin users with 403, using the standard error envelope
- [x] Missing or malformed `Authorization` header returns 401 without any Redis lookup
- [x] Valid token on cache hit resolves from Redis (`auth:token:{token_hash}`) without decoding the token again
- [x] Valid token on cache miss is trusted (fail-open), decoded once, and cached with 5-10 min TTL
- [x] Token itself is never stored in Redis or logs — only its SHA-256 hash appears in the cache key
- [x] Revocation lag behavior is documented in this issue and cross-referenced against SPECIFICATION.md Known Risks
- [x] Both dependencies are usable on any route via standard `Depends()` injection, no per-route boilerplate
- [x] Error responses match I-003's error code table exactly (`UNAUTHORIZED` / 401, `FORBIDDEN` / 403)

---

## Testing Strategy

- **Unit Tests**: Token extraction (missing header, malformed header, valid header); cache hit path; cache miss path (trust-on-first-use + cache population); `require_admin` accept/reject logic
- **Integration Tests**: Full request through `Depends(get_current_user)` and `Depends(require_admin)` against a live Redis cluster (from I-002); verify TTL is set on cache population; verify cached entry is reused on second request without re-decoding
- **Prior Art**: Existing Adidos API integration tests for token handling (mirror structure per project convention)

---

## Out of Scope

- Synchronous, real-time validation against Adidos on every request (explicitly rejected — see Trust-and-Cache Strategy)
- Token issuance or refresh (owned entirely by Adidos, per spec Implementation Decision #17)
- Proactive revocation notification (webhook or push-based invalidation) — deferred to a future version if revocation lag proves unacceptable in practice
- Fine-grained permission scopes beyond `user` / `admin` (not part of the current domain model)

---

## Related Issues

- I-003: API Framework & Routing (this middleware plugs into its middleware stack and error envelope)
- I-002: Redis Cluster Setup (provides the `auth:token:{token_hash}` cache this issue depends on)
- I-013: Admin Poll Management Endpoints (uses `require_admin` on all its routes)
- I-005: Vote Acceptance & Queueing (uses `get_current_user` to resolve `user_id` for the vote)
- I-011: GET /v1/polls Endpoint (uses `get_current_user` to resolve `user_answered` state)
- I-012: GET /v1/user/votes Endpoint (uses `get_current_user` to scope results to the caller)

---

## Implementation Checklist

- [x] Create `src/api/dependencies/auth.py` (`extract_bearer_token`, `get_current_user`, `require_admin`)
- [x] Create `src/schemas/user_context.py` (`UserContext` Pydantic/dataclass model: `user_id`, `role`)
- [x] Wire `auth:token:{token_hash}` reads/writes against the Redis client from `src/cache/redis_client.py` (I-002)
- [x] Add token decode helper (`decode_adidos_token`) matching the Adidos token format
- [x] Add 401/403 handling consistent with I-003's exception handlers
- [x] Document revocation-lag trade-off inline (this file) and confirm it matches SPECIFICATION.md wording
- [x] Test locally against the docker-compose Redis cluster from I-002

---

**Acceptance**: Both dependencies implemented and unit/integration tested, error responses match I-003's error table, revocation-lag trade-off documented, PR reviewed and merged.
