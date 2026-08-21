# I-015: Duplicate Detection (3-Strike Block)

**Status**: Ready for Implementation  
**Epic**: Admin & Security  
**Priority**: P0 (Blocker)  
**Estimated Effort**: 2 days

---

## Problem Statement

I-006's uniqueness layer already rejects a duplicate vote attempt with 409 — but a rejection alone doesn't discourage a client from retrying the same duplicate over and over, and repeated duplicate attempts from the same source are themselves a bot signal (spec decision #5, decision #13; domain model's `RateLimitEntry`). The service needs to:
- Count duplicate vote attempts per `(user_id, ip)` pair, scoped to the poll being repeatedly duplicated
- After the 3rd duplicate attempt, block that `(user_id, ip)` combo from voting on *any* poll for 1 hour
- Make that block cheap to check on every subsequent request, without re-running the full uniqueness check
- Surface the block as a recorded anomaly, so it feeds admin visibility (I-013) and Adidos reporting (I-016)

This is spec Implementation Decision #5 ("After 3 failed duplicates on same poll: Block user+IP combo for 1 hour") and #13, and it is a narrow, mechanical rule — a counter and a threshold — deliberately distinct from I-014's broader pattern inference (see Related Issues for the contrast).

---

## Solution

Add a duplicate-attempt counter and a block flag, both in Redis (I-002), and wire two check points into the existing vote-acceptance flow (I-005):

1. **Block check** (`enforce_not_blocked`) — runs alongside/before I-007's rate limiter, as the cheapest possible early-exit for a known-bad `(user_id, ip)` pair. If `blocked:{user_id}:{ip}` exists, return 429 immediately — no poll lookup, no uniqueness check.
2. **Strike recording** (`record_duplicate_attempt`) — runs when I-006's uniqueness check reports a duplicate (the same code path that would return 409). Increments a per-`(user_id, ip, poll_id)` counter; on the 3rd strike, sets the block key and writes an `anomalies` row.

Both use the exact key names already reserved in I-002's key namespace table (`blocked:{user_id}:{ip}`, TTL 3600) plus one new key this issue introduces for the strike counter itself.

---

## User Stories

(from SPECIFICATION.md)

6. As a user, I want to be blocked from voting twice on the same poll, so that my opinion counts only once (enforced without confusion)
23. As the system, I want to detect duplicate vote attempts (same user, same poll, repeated) and block after 3 attempts for 1 hour, so that I deter systematic attacks

---

## Implementation Decisions

### Redis Keys

| Key Pattern | Type | TTL | Notes |
|---|---|---|---|
| `blocked:{user_id}:{ip}` | String (flag) | 3600s | Already reserved in I-002's key namespace table, written by this issue, read by I-005 |
| `dup_attempts:{user_id}:{ip}:{poll_id}` | String (`INCR`) | 3600s (set on first increment) | **New key introduced by this issue** — must be appended to `docs/architecture/redis-keys.md` |

The strike counter is scoped per-poll (`...{poll_id}`), matching the domain model's exact wording ("3 failed duplicate attempts *on the same poll*"), but the resulting block applies to the `(user_id, ip)` pair across *all* polls, matching I-002's key (`blocked:{user_id}:{ip}` has no `poll_id` component). A user who repeatedly duplicates on Poll A is blocked from voting on Poll B too, for the block duration.

The counter's TTL matches the block's TTL (3600s) so both reset together: if a user makes one duplicate attempt and then stops for over an hour, the strike count naturally forgets that attempt rather than accumulating indefinitely toward a stale threshold.

### Core Functions
```python
# src/services/duplicate_block.py
STRIKE_THRESHOLD = 3
BLOCK_TTL_SECONDS = 3600

def _block_key(user_id: str, ip: str) -> str:
    return f"blocked:{user_id}:{ip}"

def _strike_key(user_id: str, ip: str, poll_id: UUID) -> str:
    return f"dup_attempts:{user_id}:{ip}:{poll_id}"

async def enforce_not_blocked(user_id: str, ip: str, redis: RedisCluster) -> None:
    if await redis.exists(_block_key(user_id, ip)):
        raise RateLimitExceededError("Blocked after repeated duplicate vote attempts")

async def record_duplicate_attempt(user_id: str, ip: str, poll_id: UUID, redis: RedisCluster) -> None:
    key = _strike_key(user_id, ip, poll_id)
    strikes = await redis.incr(key)
    if strikes == 1:
        await redis.expire(key, BLOCK_TTL_SECONDS)
    if strikes >= STRIKE_THRESHOLD:
        await redis.set(_block_key(user_id, ip), "1", ex=BLOCK_TTL_SECONDS)
        await anomalies_repo.create(
            alert_id=uuid4(),
            user_id=user_id,
            ip_address=ip,
            alert_type="duplicate_attempts_blocked",
            poll_id=poll_id,
            severity="critical",
            description=(
                f"User {user_id} blocked for 1h after {strikes} duplicate vote "
                f"attempts on poll {poll_id} from {ip}"
            ),
            created_at=utcnow(),
            action_taken=f"blocked:{user_id}:{ip} set for {BLOCK_TTL_SECONDS}s",
        )
```
Severity is `critical` — per the domain model's `AnomalyAlert` business rules ("Rate limit exceeded → severity = warning; 3 duplicates → severity = critical"), a completed 3-strike block is treated as a confirmed attack signal, not a mere warning.

`enforce_not_blocked` reuses the existing `RATE_LIMIT_EXCEEDED` error code (429) from I-003 — no new error code is introduced, since a block and a rate-limit rejection present identically to the client (retry later).

### Integration Into I-005's Vote Handler

This issue does not own `POST /v1/vote` (I-005 does), but requires two specific insertion points into that handler's existing sequence:

```python
await enforce_not_blocked(user_id, ip, redis)        # I-015 — must run before I-006's uniqueness check
await check_rate_limit(user_id=user_id, ip=ip)        # I-007
try:
    await check_and_reserve_uniqueness(user_id, body.poll_id)  # I-006
except DuplicateVoteError:
    await record_duplicate_attempt(user_id, ip, body.poll_id, redis)  # I-015
    raise
```
`enforce_not_blocked` runs first — before poll/answer lookup and before I-007's rate limiter — because a blocked `(user_id, ip)` pair should be rejected as cheaply as possible; there's no reason to spend a DB-backed poll lookup or a rate-limit check on a request this issue already knows will fail. `record_duplicate_attempt` is only called from the 409 path, so a legitimate first-time vote never touches the strike counter.

### Self-Limiting Anomaly Writes

Because `enforce_not_blocked` short-circuits every request once the block key is set, `record_duplicate_attempt` only ever reaches its 3rd-strike branch once per block cycle — attempts 4, 5, 6... during the block window never reach I-006's uniqueness check at all, so no duplicate `anomalies` rows are written for the same block. A fresh set of 3 strikes only becomes possible again after the block (and its matching counter) expires.

---

## Acceptance Criteria

- [ ] 1st and 2nd duplicate vote attempts on the same poll return 409 `DUPLICATE_VOTE`, no block yet
- [ ] 3rd duplicate vote attempt on the same poll returns 409 `DUPLICATE_VOTE` **and** sets `blocked:{user_id}:{ip}` with TTL 3600
- [ ] 3rd duplicate vote attempt writes exactly one `anomalies` row with `alert_type = 'duplicate_attempts_blocked'`, `severity = 'critical'`
- [ ] Any vote request (even for a different poll) from a blocked `(user_id, ip)` pair returns 429 `RATE_LIMIT_EXCEEDED` immediately, without a poll lookup or uniqueness check
- [ ] A blocked pair's request during the block window does **not** write a second `anomalies` row
- [ ] After the block's TTL expires, a vote request from the same pair proceeds through normal validation again
- [ ] Duplicate-attempt strikes are scoped per `(user_id, ip, poll_id)` — duplicating on poll A does not count toward duplicating on poll B
- [ ] The block itself (`blocked:{user_id}:{ip}`) applies across all polls, not just the poll that triggered it

---

## Testing Strategy

- **Unit Tests**: `record_duplicate_attempt` strike counting and threshold crossing (mock Redis, assert `INCR`/`EXPIRE`/`SET` calls at each strike count); `enforce_not_blocked` short-circuit behavior
- **Integration Tests**: Full request/response cycle — same user+IP duplicates a poll 3 times → 3rd returns 409 and blocks; 4th request (same or different poll) → 429 without reaching uniqueness check; verify exactly one `anomalies` row written; simulate TTL expiry (test Redis with short override TTL) → block lifts, normal flow resumes
- **Contract Tests**: Verify the block check is a dependency/call this handler makes rather than reimplemented rate-limit logic (mirrors I-005's contract-test pattern for I-006/I-007)
- **Prior Art**: Mirror I-005's error-mapping test structure and I-002's Redis-key test conventions

---

## Out of Scope

- Broader pattern detection beyond this one deterministic rule (distributed attacks, coordinated low-volume duplication across many pairs) — see I-014
- The per-request rate-limit thresholds themselves (5/min user, 50/min IP) — see I-007
- Reporting blocked pairs to Adidos — see I-016
- Admin-initiated early unblock (manual override before the 3600s TTL expires) — not required for launch; could be a future addition to I-013's admin routes
- Any notification/paging on a 3rd-strike block — critical anomalies of type `bot_pattern_detected` trigger `notify_admins()` per I-014; this issue writes the row but does not add a second notification path for `duplicate_attempts_blocked`

---

## Related Issues

- I-001: Database Schema (`anomalies` table this issue writes to)
- I-002: Redis Cluster Setup (`blocked:{user_id}:{ip}` key already reserved here; this issue adds `dup_attempts:{user_id}:{ip}:{poll_id}`)
- I-005: Vote Acceptance & Queueing (this issue's two functions are called from that handler)
- I-006: Uniqueness Enforcement (the 409 path this issue hooks into to count strikes)
- I-007: Rate Limiting (the block check runs alongside this issue's existing per-request checks)
- I-013: Admin Poll Management Endpoints (surfaces the `duplicate_attempts_blocked` rows this issue writes)
- I-014: Bot Detection & Alerting (broader, heuristic sibling — see the comparison table in that issue)
- I-016: Anomaly Reporting to Adidos (batches and forwards the rows this issue writes)

---

## Implementation Checklist

- [ ] Create `src/services/duplicate_block.py` (`enforce_not_blocked`, `record_duplicate_attempt`)
- [ ] Wire `enforce_not_blocked` and `record_duplicate_attempt` into `POST /v1/vote` in `src/api/routes/user.py` (coordinate with I-005 owner)
- [ ] Add `dup_attempts:{user_id}:{ip}:{poll_id}` key to `docs/architecture/redis-keys.md` (extends I-002's table)
- [ ] Add `RateLimitExceededError` reuse (or import from I-007's error module if already defined) for the block-check rejection path
- [ ] Write unit tests for strike counting and threshold crossing
- [ ] Write integration tests covering all rows in Acceptance Criteria
- [ ] Verify no duplicate `anomalies` rows are written during an active block window

---

**Acceptance**: All acceptance criteria met, integration tests pass, PR reviewed and merged.
