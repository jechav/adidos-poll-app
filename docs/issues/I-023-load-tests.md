# I-023: Load Tests (5x Peak: 100K votes/sec)

**Status**: Ready for Implementation
**Epic**: Observability & Launch
**Priority**: P0 (Blocker)
**Estimated Effort**: 5 days

---

## Problem Statement

Unit tests (I-021) and integration tests (I-022) verify correctness at small, deterministic scale. Neither proves the system survives the scenario the entire architecture was designed around: **an influencer-driven burst that sends tens of thousands of votes per second at a single poll with no warning**. Without dedicated load testing:
- Auto-scaling behavior (API replicas, vote processor pods) is unverified under real burst conditions
- P99 latency targets (< 200ms) are unvalidated at anything beyond toy scale
- There's no evidence the queue absorbs a 0 → 10K votes/sec spike without falling over
- The single highest-named risk in PROJECT_STATUS.md's risk register ("load test doesn't catch hot-poll bottleneck") remains unmitigated until this issue ships

This issue covers **Test Module 8 (Load/Stress Tests)** from SPECIFICATION.md's Testing Decisions section, and validates item 8 of the Deployment Checklist ("Run load tests (5x peak: 100K votes/sec, sustain for 10 min) and validate results"). It depends on the full vote pipeline being implemented (I-001 through I-020) — at minimum, vote acceptance, the processor, and the result path must be in place for a load test to be meaningful.

---

## Solution

Build a **Locust** load-testing suite that drives the four scenarios from SPECIFICATION.md's Test Module 8 against a staging (or production-like) deployment, and run it pre-launch plus weekly thereafter.

### Why Locust over JMeter
SPECIFICATION.md leaves the choice open ("JMeter or Locust scripts"). This issue picks **Locust** explicitly:
- **Stack fit**: Locust is Python-native, matching the rest of this codebase (FastAPI, pytest). Load test scenarios can share request-building code, auth-token helpers, and response-shape assertions with the application and test suites, instead of maintaining a parallel Java/XML JMeter project.
- **Scriptability**: Locust's `HttpUser`/`FastHttpUser` classes let us express realistic vote-arrival patterns (steady-state, sudden spikes, single-poll floods) as plain Python, which is easier to review and version-control than JMeter's XML test plans.
- **Distributed load generation**: Locust supports a master/worker model out of the box, which is required to actually generate 50K-100K requests/sec — a single machine cannot produce that load, and Locust's distributed mode is simpler to automate in CI/Kubernetes than JMeter's equivalent.
- **Live metrics**: Locust's web UI and stats API make it easy to watch P99 latency and failure rate in real time during a run, which matters for a 30+ minute test.

---

## User Stories

1. As an operator, I want to run a scenario simulating 1000 votes/sec sustained for 60 seconds, so that I can confirm normal peak traffic produces zero 503s and P99 < 200ms
2. As an operator, I want to run a scenario simulating a sudden spike from 0 to 10K votes/sec within 1 second, so that I can confirm the queue auto-scales and latency stays under 500ms during the spike
3. As an operator, I want to run a scenario simulating a viral single poll receiving 50K votes/sec, so that I can confirm results stay accurate and no votes are lost during an influencer-driven burst — the core risk this whole architecture is designed to survive
4. As an operator, I want to run a scenario sustaining 100K votes/sec (5x normal peak) for an extended duration, so that I can confirm the API scales to its full 5 replicas and the queue remains stable rather than growing unbounded
5. As an operator, I want each scenario to report P50/P95/P99 latency, error rate, and 503 count, so that I can evaluate pass/fail against spec targets
6. As an operator, I want these tests scripted and repeatable (not manual), so that I can run them weekly without re-deriving the scenario each time
7. As an operator, I want the viral-poll scenario to specifically target a single `poll_id` (not spread across many polls), so that it actually stresses shard-hot-spot and single-poll cache-contention behavior rather than being diluted across the dataset
8. As an on-call engineer, I want load test results archived with timestamps, so that I can compare week-over-week and catch performance regressions before they reach production

---

## Implementation Decisions

### Scenario Definitions (Test Module 8, verbatim from spec)

| Scenario | Load Profile | Duration | Pass Criteria |
|---|---|---|---|
| **Sustained normal peak** | 1000 votes/sec | 60s | No 503s, P99 < 200ms |
| **Sudden spike** | 0 → 10K votes/sec in 1s | Spike + 60s hold | Queue auto-scales, latency < 500ms during spike |
| **Viral single poll** | 50K votes/sec, single `poll_id` | 60s+ | Results accurate, no data loss |
| **Sustained 5x peak** | 100K votes/sec | 10 min (per Deployment Checklist item 8) | API scales to 5 replicas, queue stays stable |

**Overall pass criteria** (per spec): no 503 errors during normal peak, P99 < 200ms sustained.

### Locust Project Structure
```
loadtest/
  locustfile.py              # base VoteUser (HttpUser/FastHttpUser), auth token pool
  scenarios/
    sustained_peak.py        # 1000 votes/sec, 60s
    sudden_spike.py          # 0 -> 10K votes/sec, 1s ramp
    viral_single_poll.py     # 50K votes/sec, one poll_id
    sustained_5x_peak.py     # 100K votes/sec, 10 min
  shapes/
    spike_shape.py           # LoadTestShape subclass for the 0->10K in 1s ramp
    sustained_5x_shape.py    # LoadTestShape subclass for step-up to 100K/sec
  utils/
    tokens.py                # pre-provisioned pool of fake Adidos test tokens
    report.py                # pulls Locust stats API, writes timestamped JSON/CSV report
```

### Base User (locustfile.py)
```python
from locust import HttpUser, task, between
from loadtest.utils.tokens import get_test_token

class VoteUser(HttpUser):
    wait_time = between(0, 0)  # scenario shapes control pacing, not per-user wait

    @task
    def cast_vote(self):
        token = get_test_token()
        self.client.post(
            "/v1/vote",
            json={"poll_id": self.poll_id, "answer_id": self.answer_id},
            headers={"Authorization": f"Bearer {token}"},
            name="/v1/vote",
        )
```

### Custom Load Shape for the Spike Scenario
```python
from locust import LoadTestShape

class SpikeShape(LoadTestShape):
    """0 -> 10K req/s within 1 second, then hold for 60s."""
    def tick(self):
        run_time = self.get_run_time()
        if run_time < 1:
            return (10_000, 10_000)   # (user_count, spawn_rate) ramps near-instantly
        elif run_time < 61:
            return (10_000, 1000)
        return None  # stop
```
(`FastHttpUser` and a properly sized Locust worker fleet are required to actually reach 10K-100K req/sec — see distributed execution below.)

### Distributed Execution
A single Locust process cannot generate 50K-100K requests/sec. Run Locust in **master/worker mode**, deployed as Kubernetes Jobs alongside (not on) the service under test, with enough worker pods to reach target throughput (validated empirically — start with 1 worker per ~1K req/sec of target load and scale up).

```bash
# master
locust -f loadtest/scenarios/sustained_5x_peak.py --master --headless \
  --run-time 10m --csv=reports/sustained_5x_peak

# workers (N pods)
locust -f loadtest/scenarios/sustained_5x_peak.py --worker --master-host=locust-master
```

### Target Environment
Load tests run against a **staging environment provisioned identically to production** (same shard count, same Redis cluster size, same replica counts/autoscaling config) — never against a shared dev environment, and never against production directly except as the pre-launch validation run called out in the Deployment Checklist.

### Reporting
Each run produces a timestamped report (`reports/<scenario>-<timestamp>.json`) capturing P50/P95/P99 latency, error rate, 503 count, and queue-depth/replica-count samples pulled from monitoring (I-017) during the run, so results are comparable week over week.

---

## Acceptance Criteria

- [ ] All 4 scenarios from Test Module 8 implemented as Locust scenarios
- [ ] Sustained normal peak (1000/sec, 60s) passes: zero 503s, P99 < 200ms
- [ ] Sudden spike (0→10K/sec in 1s) passes: queue auto-scales (observed via I-017 dashboards), latency < 500ms during spike
- [ ] Viral single-poll (50K/sec, one poll) passes: final vote count matches votes sent (no data loss), results remain accurate
- [ ] Sustained 5x peak (100K/sec, 10 min) passes: API autoscaler reaches 5 replicas, queue depth stabilizes (does not grow unbounded)
- [ ] Locust configured for distributed (master/worker) execution capable of generating 100K req/sec
- [ ] Each run produces an archived, timestamped report with P50/P95/P99, error rate, and 503 count
- [ ] Suite is runnable on-demand and is scheduled to run weekly (post-launch) in addition to the mandatory pre-launch run
- [ ] Runbook step documented for what to do if a scenario fails (do not launch; escalate to on-call/infra)

---

## Testing Strategy

- **Load Tests**: This issue *is* the load test suite — the deliverable is the Locust project plus a passing report for all 4 scenarios
- **Tool**: Locust (Python-native, matches stack — see rationale above)
- **Environment**: Staging environment matching production topology (shard count, Redis cluster size, replica/autoscaling config)
- **Cadence**: Run pre-launch (mandatory, blocks I-024) and weekly thereafter (per spec's Test Execution Strategy: "Load Tests: Run weekly and before launch (30+ min)")
- **Prior Art**: None internally (first load-testing effort for this service); Locust's official distributed-load documentation is the reference for scaling worker count

---

## Out of Scope

- Chaos/failure-injection testing beyond what's already covered by I-022's Redis-node-loss and shard-failover integration tests
- Load testing of admin endpoints (low-traffic, not the bottleneck this service is designed to survive)
- Cost/infrastructure-spend analysis of running these tests (tracked separately if it becomes a concern)

---

## Related Issues

- I-021-unit-tests.md (fast per-commit correctness suite this load test suite does not replace)
- I-022-integration-tests.md (correctness at real-infrastructure but small scale; this issue tests the same pipeline at production scale)
- I-017-monitoring.md (dashboards this issue's runs are observed through — queue depth, replica count, P95/P99, error rate)
- I-019-alerting.md (alert thresholds this issue should trigger and validate under load)
- I-020-runbooks.md (scaling/rollback runbook this issue's failure scenarios should map to)
- I-024-launch-checklist.md (a passing load test run is a hard gate for this issue)

---

## Implementation Checklist

- [ ] Add `locust` to project dependencies (`loadtest/requirements.txt` or equivalent extras group)
- [ ] Create `loadtest/locustfile.py` (base `VoteUser`, token pool helper)
- [ ] Create `loadtest/scenarios/sustained_peak.py`
- [ ] Create `loadtest/scenarios/sudden_spike.py` + `loadtest/shapes/spike_shape.py`
- [ ] Create `loadtest/scenarios/viral_single_poll.py`
- [ ] Create `loadtest/scenarios/sustained_5x_peak.py` + `loadtest/shapes/sustained_5x_shape.py`
- [ ] Create `loadtest/utils/tokens.py` (pre-provisioned test-token pool, does not hit real Adidos auth)
- [ ] Create `loadtest/utils/report.py` (pulls Locust stats + I-017 monitoring snapshot into an archived report)
- [ ] Create Kubernetes Job manifests for Locust master + worker pods (`k8s/loadtest/`)
- [ ] Run all 4 scenarios against staging, capture and store baseline reports in `reports/baseline/`
- [ ] Schedule weekly recurring run (cron job or CI scheduled pipeline)
- [ ] Document how to run each scenario locally/on-demand in `docs/setup/load-testing.md`

---

**Acceptance**: All 4 scenarios pass against a production-topology staging environment with an archived report, weekly run scheduled, PR reviewed and merged.
