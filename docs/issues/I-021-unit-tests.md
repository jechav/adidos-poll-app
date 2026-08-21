# I-021: Unit Tests (Vote Acceptance, Rate Limiting)

**Status**: Ready for Implementation
**Epic**: Observability & Launch
**Priority**: P0 (Blocker)
**Estimated Effort**: 5 days

---

## Problem Statement

The voting engine has no automated test suite yet. Before launch, the two highest-risk behaviors — vote acceptance/uniqueness and rate limiting — need fast, deterministic unit-level coverage that runs on every commit. Without this:
- Regressions in duplicate-vote rejection or rate-limit enforcement could ship unnoticed
- Contributors have no safety net for refactoring the vote path or the rate-limit windows
- There's no fast feedback loop (< 1 min) to catch bugs before they reach integration/load testing

This issue covers **Test Module 1 (Vote Acceptance Tests)** and **Test Module 2 (Rate Limit Tests)** from SPECIFICATION.md's Testing Decisions section. It depends on I-005 (Vote Acceptance & Queueing), I-006 (Uniqueness Enforcement), and I-007 (Rate Limiting) being implemented, since these tests exercise that code directly.

---

## Solution

Build a `pytest` + `pytest-asyncio` unit test suite that exercises the vote-acceptance and rate-limiting code paths in isolation, using:
- **`fakeredis`** (async client) in place of a real Redis instance — no network I/O, no external dependencies
- A lightweight test database (SQLite in-memory or a throwaway PostgreSQL test schema) standing in for a single shard
- **`httpx.AsyncClient`** against the FastAPI app (via ASGI transport, no running server) to drive requests through the real route handlers

Per SPECIFICATION.md's testing philosophy: tests assert on **external behavior** (HTTP status codes, response bodies, whether a vote is counted) — never on internal mechanics (Redis key names like `user:{user_id}:poll:{poll_id}`, queue list structure, sliding-window bucket internals). This means the suite should keep passing if Redis is swapped for another cache/queue technology, as long as the observable contract (202/409/429/400 responses, vote counts) is preserved.

---

## User Stories

1. As a developer, I want a vote on an active poll to return 202 Accepted and be reflected in the vote count, so that I know the happy path works
2. As a developer, I want a second vote from the same user on the same poll to return 409 Conflict, so that I know uniqueness enforcement works
3. As a developer, I want a vote on a closed poll to return 400 Bad Request, so that I know poll-state checks work
4. As a developer, I want a vote with an invalid/missing token to return 401 Unauthorized, so that I know auth is wired into the vote path
5. As a developer, I want a vote with a missing `answer_id` to return 400 Bad Request, so that I know request validation works
6. As a developer, I want a user's 5th vote within a minute to succeed, so that I know the rate limit boundary is inclusive and correct
7. As a developer, I want a user's 6th vote within a minute to return 429 Too Many Requests, so that I know per-user throttling works
8. As a developer, I want the rate limit window to reset after 60 seconds so the next vote succeeds, so that I know the sliding window expires correctly
9. As a developer, I want two different IPs to each cast 25 votes and both succeed, so that I know per-IP limits are isolated per IP
10. As a developer, I want the 26th vote from either IP to return 429, so that I know the per-IP ceiling (50/min) is enforced correctly
11. As a developer, I want a user blocked after 3 duplicate-vote attempts on the same poll to receive 429 for the following hour, so that I know the 3-strike lockout works
12. As a CI pipeline, I want this whole suite to run in under 1 minute using mocked Redis and a throwaway test DB, so that it can run on every commit without slowing down development

---

## Implementation Decisions

### Test Doubles, Not Real Infrastructure
- **Redis**: `fakeredis.aioredis.FakeRedis` (or `fakeredis` async equivalent), injected via FastAPI dependency override — no real Redis connection, no Docker required for this suite
- **Database**: SQLite in-memory (via SQLAlchemy) or an ephemeral PostgreSQL schema created/dropped per test module — a single shard is sufficient; multi-shard behavior belongs to I-022 (Integration Tests)
- **HTTP layer**: `httpx.AsyncClient(app=app, base_url="http://test")` — exercises real FastAPI routing/middleware/dependency injection without binding a socket

### Directory Structure
```
tests/
  unit/
    conftest.py                 # fakeredis + test DB fixtures, app override
    test_vote_acceptance.py     # Test Module 1
    test_rate_limiting.py       # Test Module 2
```

### Fixture Pattern (conftest.py)
```python
import pytest
import pytest_asyncio
from fakeredis.aioredis import FakeRedis
from httpx import AsyncClient, ASGITransport
from src.api.app import app
from src.dependencies.redis import get_redis
from src.dependencies.db import get_db

@pytest_asyncio.fixture
async def fake_redis():
    redis = FakeRedis(decode_responses=True)
    yield redis
    await redis.flushall()

@pytest_asyncio.fixture
async def test_db():
    # in-memory SQLite session, tables created fresh per test
    ...

@pytest_asyncio.fixture
async def client(fake_redis, test_db):
    app.dependency_overrides[get_redis] = lambda: fake_redis
    app.dependency_overrides[get_db] = lambda: test_db
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        yield ac
    app.dependency_overrides.clear()
```

### Module 1: Vote Acceptance Tests (test_vote_acceptance.py)
Concrete test cases (from SPECIFICATION.md Test Module 1, "Good Tests"):
- `test_vote_on_active_poll_returns_202_and_is_counted` — POST /v1/vote on an active poll returns 202, and a subsequent result read shows the vote counted
- `test_duplicate_vote_returns_409` — same user votes twice on the same poll; second request returns 409 with `DUPLICATE_VOTE` error code
- `test_vote_on_closed_poll_returns_400` — voting on a poll in `closed` state returns 400 with `POLL_CLOSED`
- `test_vote_with_invalid_token_returns_401` — missing or malformed Adidos token returns 401 `UNAUTHORIZED`
- `test_vote_with_missing_answer_id_returns_400` — request body missing `answer_id` returns 400 `INVALID_REQUEST`

Explicitly **not tested here** (per spec): Redis `SET NX` key naming/implementation, queue list structure — those are implementation details, not observable behavior.

### Module 2: Rate Limit Tests (test_rate_limiting.py)
Concrete test cases (from SPECIFICATION.md Test Module 2, "Good Tests"):
- `test_fifth_vote_within_minute_succeeds` — same user casts 5 votes (on 5 distinct polls, to avoid tripping uniqueness) within 60s; all 5 succeed
- `test_sixth_vote_within_minute_returns_429` — 6th vote from the same user within the window returns 429 `RATE_LIMIT_EXCEEDED`
- `test_rate_limit_resets_after_window` — after simulating 60s elapsed (via fakeredis TTL/clock or a frozen-time helper), the next vote from a previously-throttled user succeeds
- `test_two_ips_each_voting_25_times_both_succeed` — two distinct IPs, 25 votes each, all succeed (under the 50/IP/min ceiling)
- `test_26th_vote_from_either_ip_returns_429` — the 26th vote from either IP returns 429
- `test_user_blocked_for_1_hour_after_3_duplicate_attempts` — same user attempts to vote on the same already-voted poll 3 times; the 3rd triggers a 1-hour block, and a subsequent vote attempt from that user+IP combo returns 429 during the block window

Explicitly **not tested here** (per spec): sliding-window algorithm internals (bucket math, timestamp precision).

### Time Control
Rate-limit window tests need deterministic time control rather than real 60-second sleeps. Use `freezegun` or a small injectable clock abstraction (`src/utils/clock.py`) so tests can advance time without slowing down the suite.

---

## Acceptance Criteria

- [ ] All 5 Vote Acceptance test cases (Module 1) implemented and passing
- [ ] All 6 Rate Limit test cases (Module 2) implemented and passing
- [ ] Suite uses `fakeredis`, not a real Redis instance
- [ ] Suite uses an in-memory/ephemeral test DB, not a shared dev database
- [ ] No test asserts on Redis key names, queue internals, or sliding-window bucket structure — only on HTTP status codes, response bodies, and vote counts
- [ ] Full suite runs in under 1 minute locally and in CI
- [ ] Suite runs automatically on every commit (CI hook configured)
- [ ] Tests are independent and order-agnostic (no shared mutable state between tests)
- [ ] Time-dependent tests (rate limit window reset) use simulated/frozen time, not real sleeps

---

## Testing Strategy

- **Unit Tests**: This issue *is* the unit test suite — no meta-testing beyond verifying the suite itself runs green in CI
- **Coverage Target**: 100% of the "Good Tests" bullets enumerated in SPECIFICATION.md Test Modules 1 and 2
- **Isolation Check**: Deliberately swap `fakeredis` for a different in-memory dict-backed stub in one local run to confirm no test depends on Redis-specific behavior beyond the documented contract
- **Prior Art**: Existing Adidos API unit test conventions (mirror structure, naming, fixture patterns)

---

## Out of Scope

- Bot detection / anomaly tests (Test Module 4) — covered by I-014/I-015/I-016 implementation issues, not this one
- Multi-shard distribution behavior — covered by I-022 (Integration Tests, Test Module 5)
- Real Redis or real PostgreSQL — covered by I-022 (Integration Tests)
- Load/concurrency behavior — covered by I-023 (Load Tests)

---

## Related Issues

- I-005: Vote Acceptance & Queueing (implementation under test)
- I-006: Uniqueness Enforcement (implementation under test)
- I-007: Rate Limiting (implementation under test)
- I-003-api-framework.md (FastAPI app + route handlers this suite drives via `httpx.AsyncClient`)
- I-022-integration-tests.md (next layer up — real Postgres/Redis, end-to-end flows)
- I-023-load-tests.md (high-volume/concurrency testing, out of scope here)
- I-024-launch-checklist.md (this suite's green status is a launch gate input)

---

## Implementation Checklist

- [ ] Add `pytest`, `pytest-asyncio`, `fakeredis`, `httpx`, `freezegun` to dev dependencies
- [ ] Create `tests/unit/conftest.py` (fakeredis fixture, test DB fixture, app dependency overrides)
- [ ] Create `tests/unit/test_vote_acceptance.py` (5 test cases from Module 1)
- [ ] Create `tests/unit/test_rate_limiting.py` (6 test cases from Module 2)
- [ ] Create `src/utils/clock.py` (injectable clock abstraction, if not already present from I-007)
- [ ] Add `pytest.ini` / `pyproject.toml` test configuration (async mode, test discovery paths)
- [ ] Wire suite into CI (run on every push/PR, fail build on red)
- [ ] Add `make test-unit` (or equivalent) local dev shortcut
- [ ] Document how to run the suite locally in `docs/setup/testing.md`

---

**Acceptance**: All 11 test cases pass, suite runs in < 1 min, wired into CI, PR reviewed and merged.
