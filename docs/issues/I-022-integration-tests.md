# I-022: Integration Tests (End-to-End Workflows)

**Status**: Ready for Implementation
**Epic**: Observability & Launch
**Priority**: P0 (Blocker)
**Estimated Effort**: 5 days

---

## Problem Statement

Unit tests (I-021) verify vote acceptance and rate limiting against **mocked** Redis and a throwaway DB — they don't catch bugs that only appear when real infrastructure is involved: actual queue-to-worker handoff, real PostgreSQL uniqueness constraints across shards, real Redis cluster behavior, or the full admin poll lifecycle enforced end-to-end through the API. Without integration coverage:
- A bug in the worker's DB write path (e.g. a shard-routing miscalculation) could ship undetected
- Shard distribution fairness (the core defense against hot-shard bottlenecks on viral polls) is unverified
- Admin poll state transitions (draft → active → closed → archived) could regress and allow illegal backward transitions
- The full vote → queue → worker → DB → cache → results-visible pipeline has no automated check

This issue covers **Test Module 3 (Result Aggregation)**, **Test Module 5 (Shard Distribution)**, **Test Module 6 (Admin Poll Management)**, and **Test Module 7 (Integration Tests / end-to-end workflows)** from SPECIFICATION.md's Testing Decisions section. It depends on I-008 (Vote Processor Workers), I-009 (Result Aggregation), I-011 (GET /v1/polls), I-012 (GET /v1/user/votes), and I-013 (Admin Endpoints) — this suite exercises the real, wired-together system those issues produce.

---

## Solution

Build a `pytest` + `pytest-asyncio` integration test suite that runs against **real, dockerized infrastructure** — not mocks. Use **`testcontainers-python`** to spin up ephemeral PostgreSQL (at least 2 shards, to exercise cross-shard behavior) and a Redis instance for the duration of the test run, plus the real FastAPI app and a real (or in-process, but non-mocked) vote processor worker.

**This is the key distinction from I-021's unit tests**: I-021 mocks Redis (`fakeredis`) and uses an in-memory/single-shard test DB to get sub-minute, hermetic, per-commit feedback on the vote-acceptance and rate-limit *decision logic*. I-022 trades speed for realism — it verifies that the actual Redis protocol, actual PostgreSQL constraint enforcement, actual shard routing, and actual worker dequeue/write loop behave correctly together. Both suites run on every commit, but I-022 is allowed a larger budget (target < 5 min vs. I-021's < 1 min) because container startup and real I/O cost more than in-memory fakes.

Per SPECIFICATION.md's testing philosophy, these tests still assert only on **external, observable behavior** (HTTP responses, result counts and percentages, shard vote-count fairness, poll state transitions) — never on Redis key layouts, queue internals, or the shard-hashing algorithm itself.

---

## User Stories

1. As a developer, I want after 10 votes (6 for answer A, 4 for answer B) that GET /v1/polls returns 60%/40%, so that I know result aggregation math is correct end-to-end
2. As a developer, I want poll results to include `total_votes` and per-answer counts, so that I know the response shape matches the contract
3. As a developer, I want closed poll results to remain queryable, so that historical results stay accessible
4. As a developer, I want results to be visible within 5 seconds of a vote being submitted, so that I know the async queue → worker → cache pipeline meets its latency budget
5. As a developer, I want results still computable (from the materialized view) when Redis is down, so that I know the fallback path works against a real database
6. As a developer, I want 1000 votes from random users to land within ±10% across shards (no shard > 55% of total), so that I know shard distribution is fair under real hashing
7. As a developer, I want a simulated viral poll (100K votes from distinct users) to produce consistent results across all shards, so that I know a hot poll doesn't create a bottleneck or data inconsistency
8. As a developer, I want votes for a downed shard to queue and get processed after the shard recovers, so that I know shard failover doesn't lose votes
9. As an admin, I want to create a poll with a question and 2 answers and have it land in `draft` state, so that I know poll creation works end-to-end
10. As an admin, I want activating a poll to make it votable, so that I know the draft → active transition is enforced
11. As an admin, I want closing an active poll to immediately reject new votes with 400, so that I know the active → closed transition takes effect atomically
12. As an admin, I want archiving a closed poll to remove it from the active list while keeping it in history, so that I know archival preserves data but changes visibility
13. As a non-admin user, I want poll-creation attempts to return 403, so that I know authorization is enforced on admin routes
14. As an admin, I want an attempt to move a poll backward (e.g. closed → active) to return 400, so that I know state transitions are strictly one-way
15. As two distinct users, I want to both vote on the same poll and see each other's impact on results, so that I know the system is consistent across concurrent users
16. As a developer, I want a full poll-of-1M-votes-in-10-seconds scenario (scaled appropriately for CI) to show P99 < 200ms with no data loss, so that I know the system holds up under realistic concurrent load, not just single requests
17. As a developer, I want the service to stay up and eventually consistent when a Redis cluster node is dropped mid-test, so that I know Redis HA doesn't cause outages
18. As a developer, I want the reconciliation job to detect and alert on a deliberately introduced Redis/DB drift > 1%, so that I know drift detection actually works against real data
19. As a developer, I want closed polls to appear in a user's voting history but reject new votes, so that I know the read path and write path agree on poll state

---

## Implementation Decisions

### Real Infrastructure via Testcontainers
```python
# tests/integration/conftest.py
import pytest_asyncio
from testcontainers.postgres import PostgresContainer
from testcontainers.redis import RedisContainer

@pytest_asyncio.fixture(scope="session")
def postgres_shards():
    # spin up N=2 shard containers for CI (spec assumes 8+ in prod;
    # 2 is sufficient to exercise cross-shard routing and distribution math)
    containers = [PostgresContainer("postgres:16") for _ in range(2)]
    for c in containers:
        c.start()
    yield containers
    for c in containers:
        c.stop()

@pytest_asyncio.fixture(scope="session")
def redis_container():
    with RedisContainer("redis:7") as redis:
        yield redis
```

Unlike I-021, **no dependency overrides swap in fakes** — the FastAPI app, the real Redis client, and a real vote processor worker process (run in-process as an asyncio task, or as a subprocess for closer production fidelity) all point at the containers above.

### Directory Structure
```
tests/
  integration/
    conftest.py                    # testcontainers fixtures (Postgres shards, Redis)
    test_result_aggregation.py     # Test Module 3
    test_shard_distribution.py     # Test Module 5
    test_admin_poll_management.py  # Test Module 6
    test_end_to_end_workflows.py   # Test Module 7
```

### Module 3: Result Aggregation Tests (test_result_aggregation.py)
- `test_60_40_split_after_10_votes` — 6 votes answer A, 4 votes answer B → GET /v1/polls shows 60% / 40%
- `test_results_include_total_and_per_answer_counts`
- `test_closed_poll_results_still_queryable`
- `test_results_visible_within_5_seconds_of_vote` — poll, then poll GET /v1/polls until count reflects the vote or 5s elapses, whichever first
- `test_results_fall_back_to_materialized_view_when_redis_down` — stop the Redis container mid-test, confirm results still computable (slower, but accurate) from PostgreSQL

Not tested here (per spec): Redis key structure, cache expiration timing internals.

### Module 5: Shard Distribution Tests (test_shard_distribution.py)
- `test_1000_votes_distributed_within_10_percent_across_shards` — no single shard exceeds 55% of total votes
- `test_viral_poll_100k_votes_consistent_across_shards` — scaled for CI runtime as needed, but must exercise multiple shards under a single hot poll
- `test_shard_failover_queues_and_recovers` — pause one shard container, submit votes for users hashing to it, resume the container, confirm votes are eventually written

Not tested here: the user_id hashing algorithm's internals (only its distribution outcome).

### Module 6: Admin Poll Management Tests (test_admin_poll_management.py)
- `test_admin_creates_poll_lands_in_draft`
- `test_admin_activates_poll_enables_voting`
- `test_admin_closes_poll_rejects_new_votes` — returns 400
- `test_admin_archives_poll_removed_from_active_list_visible_in_history`
- `test_non_admin_cannot_create_poll_returns_403`
- `test_poll_cannot_transition_backward_returns_400` — e.g. closed → active

Not tested here: admin authentication flow itself (owned by Adidos; see I-013's auth dependency for what's already covered by I-004).

### Module 7: Integration / End-to-End Workflow Tests (test_end_to_end_workflows.py)
- `test_two_users_vote_and_see_each_others_impact_on_results`
- `test_high_volume_burst_p99_under_200ms_no_data_loss` — scaled-down but representative burst (e.g. 10K votes in 10s in CI) to keep runtime under the 5-minute budget; the full 1M/10s and 100K/sec scenarios belong to I-023's dedicated load tests
- `test_redis_node_loss_keeps_service_up_eventually_consistent` — requires a multi-node Redis test topology or a stubbed single-node "kill" to simulate cluster degradation
- `test_reconciliation_job_detects_drift_over_1_percent` — manually desync a vote_counts row from actual vote rows, run the reconciliation job, assert an alert/anomaly is recorded
- `test_closed_poll_visible_in_history_but_not_votable`

Not tested here: infrastructure-level failures like disk-full or network partitions (explicitly out of scope per spec).

---

## Acceptance Criteria

- [ ] All test cases from Modules 3, 5, 6, and 7 (19 user stories above) implemented and passing
- [ ] Suite runs against real dockerized PostgreSQL (2+ shards) and Redis via testcontainers — no mocks
- [ ] Distinction from I-021 is documented in the suite's README: unit tests mock infrastructure for speed, integration tests use real infrastructure for correctness
- [ ] Shard distribution test enforces the ±10% / no-shard->55% fairness bound from spec
- [ ] Admin state-machine tests cover all four states and reject all backward transitions
- [ ] End-to-end vote flow test verifies data visible at every stage: API accepted → queued → worker processed → DB row exists → cache updated → GET /v1/polls reflects it
- [ ] Suite runs on every commit in CI
- [ ] Full suite completes in under 5 minutes in CI
- [ ] Tests clean up containers after each run (no leaked Docker resources)
- [ ] No test asserts on Redis key names, queue structure, or hashing algorithm internals — only on observable outcomes

---

## Testing Strategy

- **Integration Tests**: This issue *is* the integration test suite
- **Infrastructure**: `testcontainers-python` for ephemeral, isolated Postgres + Redis per CI run
- **Scale-down for CI**: Scenarios described at production scale (100K votes, 1M votes/10s) are scaled down proportionally for the < 5 min CI budget; full-scale versions of these scenarios are owned by I-023 (Load Tests)
- **Prior Art**: Existing Adidos integration test conventions (mirror structure for consistency)

---

## Out of Scope

- Full-scale load/stress scenarios (1000+ votes/sec sustained, 50K-100K votes/sec bursts) — see I-023
- Bot detection and anomaly-reporting tests (Test Module 4) — owned by I-014/I-015/I-016 implementation issues
- Infrastructure failure modes beyond Redis node loss / shard pause (disk full, network partition) — explicitly out of scope per spec

---

## Related Issues

- I-008: Vote Processor Workers (worker process exercised by this suite)
- I-009: Result Aggregation (Redis) (logic exercised by Module 3 tests)
- I-011: GET /v1/polls Endpoint (exercised by Modules 3, 6, 7)
- I-012: GET /v1/user/votes Endpoint (exercised by Module 7)
- I-013-admin-endpoints.md (admin routes exercised by Module 6)
- I-021-unit-tests.md (fast, mocked-infrastructure unit suite this issue builds on top of)
- I-023-load-tests.md (full-scale load/burst scenarios that are scaled down here for CI speed)
- I-020-runbooks.md (shard-down / Redis-node-loss scenarios here should match documented incident runbook behavior)
- I-024-launch-checklist.md (this suite's green status is a launch gate input)

---

## Implementation Checklist

- [ ] Add `testcontainers`, `pytest`, `pytest-asyncio` to dev dependencies
- [ ] Create `tests/integration/conftest.py` (Postgres shard containers, Redis container, app wiring against real containers)
- [ ] Create `tests/integration/test_result_aggregation.py` (Module 3, 5 test cases)
- [ ] Create `tests/integration/test_shard_distribution.py` (Module 5, 3 test cases)
- [ ] Create `tests/integration/test_admin_poll_management.py` (Module 6, 6 test cases)
- [ ] Create `tests/integration/test_end_to_end_workflows.py` (Module 7, 5 test cases)
- [ ] Add helper to run the vote processor worker in-process for tests (or as a managed subprocess)
- [ ] Wire suite into CI as a separate job/step from unit tests (allow parallel execution, longer timeout)
- [ ] Add `make test-integration` local dev shortcut
- [ ] Document Docker prerequisites and how to run the suite locally in `docs/setup/testing.md`

---

**Acceptance**: All test cases across Modules 3, 5, 6, and 7 pass against real containerized infrastructure, suite runs in < 5 min in CI, PR reviewed and merged.
