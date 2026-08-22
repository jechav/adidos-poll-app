# I-006: Uniqueness Enforcement (Redis + DB)

**Status**: Done  
**Epic**: Voting Infrastructure  
**Priority**: P0 (Blocker)  
**Estimated Effort**: 3 days

---

## Problem Statement

The poll app guarantees a user can vote on a poll **at most once** (spec: "one vote per user per question, enforced at the database level"). Because votes are accepted synchronously but written to PostgreSQL asynchronously (I-005, I-008), a single check at write time isn't enough — two requests from the same user for the same poll can race through the API before either reaches the database. The system needs:
- A fast, atomic check that rejects an obvious duplicate before it's even queued
- A guarantee that survives if that fast check is ever bypassed, stale, or wrong (Redis restart, race condition, cache flush)
- No silent double-counting and no silent vote loss when both layers disagree

---

## Solution

Implement **dual-layer uniqueness enforcement**, exactly as decided in SPECIFICATION.md decision #4:

1. **Layer 1 — Redis `SET NX` (fast path, pre-queue)**: An atomic `SET vote:user:{user_id}:poll:{poll_id} 1 NX` reserves the (user, poll) pair before the vote is ever pushed to `queue:votes`. If the key already exists, the vote is rejected immediately with 409 — no queueing, no DB roundtrip.
2. **Layer 2 — PostgreSQL `UNIQUE(user_id, poll_id)` (correctness backstop)**: The `votes` table's unique constraint, already defined in I-001, is the source of truth. If Redis is down, restarted, flushed, or simply raced (two workers processing near-simultaneous writes), the DB constraint is what actually prevents a duplicate row from ever existing.

Layer 1 is a defense that makes duplicates rare and fast to reject. Layer 2 is a guarantee that makes duplicates impossible, full stop — even if Layer 1 fails entirely. Neither layer is "the" uniqueness system; they exist together because Redis is fast-but-not-durable and PostgreSQL is durable-but-too-slow-to-check-on-every-request.

---

## User Stories

(from SPECIFICATION.md)

6. As a user, I want to be blocked from voting twice on the same poll, so that my opinion counts only once (enforced without confusion)
25. As the system, I want to verify that a vote is only cast once per user per question, enforced at the database level, so that I guarantee data integrity even if the async queue fails

---

## Implementation Decisions

### Layer 1: Redis SET NX (Pre-Queue Reservation)

**Key**: `vote:user:{user_id}:poll:{poll_id}` → value `1`, no TTL (must persist for the life of the poll; polls are never hard-deleted per spec's immutability principle, so the key can live indefinitely or be cleaned up alongside archival/anonymization tooling)

```python
async def check_and_reserve_uniqueness(user_id: str, poll_id: UUID) -> None:
    key = f"vote:user:{user_id}:poll:{poll_id}"
    reserved = await redis.set(key, "1", nx=True)
    if not reserved:
        raise DuplicateVoteError("User already voted on this poll")
```

This call is made by I-005's handler *after* rate limiting but *immediately before* enqueueing — it is the last gate before a vote enters `queue:votes`. Because `SET NX` is atomic at the Redis level, two concurrent requests for the same (user, poll) cannot both succeed even if they arrive within microseconds of each other; Redis serializes the command and exactly one caller gets `reserved = True`.

**Decision**: If the `SET NX` fails (key already exists), the vote is rejected with `409 DUPLICATE_VOTE` immediately — strict, no queueing, no "let the DB sort it out." This matches spec decision #4's directive and keeps the fast path fast: rejected votes never touch the queue or a worker.

### Layer 2: PostgreSQL UNIQUE Constraint (Backstop)

Uses the constraint already defined in I-001:
```sql
-- from I-001's votes table
UNIQUE(user_id, poll_id)
```

The vote processor (I-008) inserts votes it dequeues. If an insert violates this constraint, the worker:
- Logs the violation (user_id, poll_id, vote_id, timestamp) for audit/anomaly purposes
- Skips the row — **no retry, no error surfaced to the client**, since the client already received 202 for the original accepted request

```python
try:
    await db.execute(INSERT_VOTE_SQL, vote.dict())
except UniqueViolationError:
    logger.warning("duplicate_vote_at_db_layer", vote_id=vote.vote_id,
                    user_id=vote.user_id, poll_id=vote.poll_id)
    # No retry: the DB is correct, this row must not exist twice.
    # No error to client: 202 was already returned by I-005 at queue time.
    continue
```

This case should be rare — it only fires when Layer 1 didn't catch the duplicate (Redis was down, key was evicted, or a bug let two reservations through). When it does fire, it's not a bug in the worker; it's the system doing exactly what it's designed to do.

### Why Two Layers, Not One

| | Redis `SET NX` | PostgreSQL `UNIQUE` |
|---|---|---|
| **Speed** | Sub-millisecond, checked synchronously in the request path | Only checked during async batch insert, seconds after the request |
| **Durability** | Not guaranteed — cluster failover, restart, or eviction can lose keys | Guaranteed — ACID constraint, survives everything |
| **Role** | Fast-path defense: stops duplicates from ever reaching the queue, keeps `queue:votes` clean | Correctness guarantee: the actual invariant the system promises ("one vote per user per poll") |
| **Failure mode if this layer alone existed** | A Redis outage/restart would silently let duplicates through with no fallback | Every vote request would need a synchronous cross-shard DB query, defeating the async/202 model entirely |

Relying on Redis alone would make "one vote per user" only as reliable as Redis's persistence, which is explicitly *not* guaranteed to survive node failures without app-level backstops. Relying on the DB alone would mean serializing every vote request behind a synchronous, cross-shard database round-trip — incompatible with the "accept in <200ms, absorb bursts" performance target from I-005 and the spec's async queue strategy. Together, Redis gives speed and the DB gives the actual guarantee.

### Consistency Window

Between Layer 1 reserving a key and Layer 2 durably committing the row (via I-008's batch write, every 1-2 seconds), there is a small window where the Redis key says "voted" but the DB row doesn't exist yet. This is acceptable: it matches the system's overall "availability over consistency" trade-off (spec decision, Key Trade-offs), and no other component makes a decision during this window that would break under it — the DB constraint alone (not the Redis key) is what's ever relied on for query-time correctness (result aggregation reads from `vote_counts`/Redis cache, not from the uniqueness key).

---

## Acceptance Criteria

- [x] `check_and_reserve_uniqueness(user_id, poll_id)` performs an atomic Redis `SET NX` and raises `DuplicateVoteError` if the key already existed
- [x] Two concurrent requests for the same (user_id, poll_id) result in exactly one reservation succeeding, verified under concurrent load
- [x] A duplicate vote request is rejected with 409 (`DuplicateVoteError` → `409 DUPLICATE_VOTE`, verified via the exception-handler wiring); `queue:votes` doesn't exist yet since I-005 isn't built — `check_and_reserve_uniqueness` never touches it, so a rejected duplicate never reaches a queue regardless
- [x] PostgreSQL `UNIQUE(user_id, poll_id)` constraint (from I-001) is exercised via `insert_vote_or_log_duplicate`, the insert-path helper I-008's vote processor will call
- [x] A simulated DB-layer duplicate (two votes inserted for the same user+poll with no Redis reservation in between) results in one row in `votes`, one skipped insert, one logged warning, and zero errors surfaced to any client
- [x] Redis outage during the reservation check causes `UniquenessCheckUnavailableError` → `503 SERVICE_UNAVAILABLE` (not a silent bypass) — the fast path fails safe, it does not fail open
- [x] Uniqueness key format documented and matches I-002's namespace: `vote:user:{user_id}:poll:{poll_id}` (already present in `docs/architecture/redis-keys.md` from I-002; `vote_reservation_key()` is tested against it)

---

## Testing Strategy

- **Unit Tests**: `check_and_reserve_uniqueness` against a real (or fakeredis) Redis instance — first call succeeds, second call for same key raises `DuplicateVoteError`
- **Integration Tests**: End-to-end through `POST /v1/vote` (I-005) — vote, then vote again, confirm 409 and confirm the DB never receives a second row
- **Concurrency Tests**: Fire N concurrent identical vote requests, assert exactly 1 succeeds and N-1 return 409
- **Failure-Mode Tests**: Manually delete the Redis reservation key after a vote is queued but before I-008 processes it, queue a second vote for the same (user, poll), confirm the DB constraint catches it and the worker logs-and-skips without raising
- **Not Tested Here**: Redis cluster failover mechanics (that's I-002's concern), the vote processor's batching/shard-routing logic (that's I-008's concern)
- **Prior Art**: Mirror I-001's constraint-violation integration tests

---

## Related Issues

- I-001: Database Schema (source of the `UNIQUE(user_id, poll_id)` constraint that is Layer 2)
- I-002: Redis Cluster Setup (hosts the `vote:user:{user_id}:poll:{poll_id}` keyspace, Layer 1)
- I-003: API Framework & Routing (error envelope / 409 response shape used here)
- I-005: Vote Acceptance & Queueing (calls this check immediately before enqueueing)
- I-007: Rate Limiting (runs before this check in I-005's handler; distinct concern, not reimplemented here)
- I-008: Vote Processor Workers (performs the Layer 2 insert and handles constraint violations)
- I-009: Result Aggregation (reads from `vote_counts`/cache, unaffected by this issue's consistency window)

---

## Implementation Checklist

- [x] Implement `check_and_reserve_uniqueness()` in `src/services/uniqueness.py`
- [x] Define `DuplicateVoteError` and wire it to `409 DUPLICATE_VOTE` in I-003's exception handlers
- [x] Add Redis connection failure handling → `503 SERVICE_UNAVAILABLE` (fail safe, not fail open) via `UniquenessCheckUnavailableError`
- [x] Add `insert_vote_or_log_duplicate()`, the insert-path helper I-008's vote processor will call, which catches a unique-violation, logs, and continues without raising
- [x] Add structured logging for DB-layer duplicate catches (feeds I-016 anomaly reporting) — `logger.warning("duplicate_vote_at_db_layer", ...)`
- [x] Write concurrency test: N parallel identical votes → exactly 1 success
- [x] Write failure-mode test: DB constraint still catches a duplicate reaching Layer 2 with no Redis reservation
- [x] Document key format and TTL policy in `docs/architecture/redis-keys.md` (already present from I-002)

---

**Acceptance**: Both layers verified independently and together (`tests/unit/test_uniqueness.py`,
`tests/integration/test_uniqueness_db.py`, `tests/integration/test_uniqueness_error_handlers.py`).
Built as a standalone module (`src/services/uniqueness.py`) since I-005/I-008 aren't implemented yet —
not wired into a live vote-acceptance endpoint. Exception handlers for `DuplicateVoteError` /
`UniquenessCheckUnavailableError` are registered now so I-005 can simply raise them once it exists.
