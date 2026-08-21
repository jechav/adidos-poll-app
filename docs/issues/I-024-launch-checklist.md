# I-024: Pre-Launch Checklist

**Status**: Ready for Implementation
**Epic**: Observability & Launch
**Priority**: P0 (Blocker — this IS the launch gate)
**Estimated Effort**: 3 days

---

## Problem Statement

Every other issue in this tracker builds a piece of the system. None of them, individually, answers the question "are we actually ready to serve production traffic?" Without a single, explicit go/no-go checklist:
- Infrastructure provisioning (shards, Redis cluster, autoscaling) could be partially done and nobody would notice until traffic hits it
- Monitoring/alerting could be half-wired, leaving the team blind during the highest-risk moment (launch)
- Load tests (I-023) could be skipped or stale by the time launch actually happens
- The ops team could be handed a pager without ever having read the incident runbooks (I-020)
- There is no single artifact anyone can point to and say "yes, we verified all of this before flipping the switch"

This issue is not a code deliverable. It is the **launch gate**: a concrete, checkable transcription of SPECIFICATION.md's Deployment Checklist (Pre-Launch), cross-referenced against PROJECT_STATUS.md's Success Criteria (Pre-Launch), that must be fully checked off before production traffic is allowed to hit this service.

---

## Solution

Transcribe SPECIFICATION.md's 10-item **Deployment Checklist (Pre-Launch)** into concrete, individually verifiable checklist items (below, under Acceptance Criteria and the Implementation Checklist). Each item names what "done" means and which issue produced the thing being verified — this issue does not re-implement or re-design any of that work, it **verifies** it.

Cross-reference PROJECT_STATUS.md's **Success Criteria (Pre-Launch)** section (Functionality / Performance / Reliability / Monitoring) as the sign-off criteria for launch — those checkmarks get formally verified here, not re-invented. Sign-off requires both the Deployment Checklist items and the Success Criteria to be satisfied.

Finally, this issue formalizes and documents the **Big Bang Launch** decision (SPECIFICATION.md, Implementation Decision #18): the team deploys directly to production with close monitoring, rather than a gradual/canary rollout.

### Why Big Bang, Not Canary
- **Canary can't simulate the scenario that matters most.** The single highest-risk pattern this service must survive is an influencer-driven burst on one poll — 50K-100K votes/sec arriving with no warning. A canary rollout (e.g. 5% of traffic for an hour) would only ever see a *fraction* of normal steady-state load; it structurally cannot reproduce a concentrated burst on a single poll, because burst traffic isn't drawn from a population you can sample down — it's an event.
- **Gradual rollout delays the real signal.** The team already has synthetic evidence the system survives burst conditions (I-023's load tests, run pre-launch against a production-topology staging environment). What canary would add — real user behavior at low volume — doesn't validate the thing that's actually uncertain (burst handling), so its marginal value here is low relative to its cost (slower time-to-full-confidence, more deployment complexity).
- **The mitigation is readiness, not gradualism.** Instead of a canary, the team invests in: pre-validated load tests (I-023), live dashboards and alerting (I-017/I-019), a rehearsed incident runbook (I-020), and an on-call engineer ready to scale replicas or roll back the moment monitoring shows trouble. This trades "slow, staged exposure" for "fast, fully-monitored exposure with a fast abort path" — appropriate given the burst-driven risk profile.

---

## User Stories

1. As a launch owner, I want a single checklist that must be 100% complete before production traffic is allowed, so that launch readiness isn't a matter of individual judgment
2. As an operator, I want confirmation that all 8+ PostgreSQL shards are provisioned and replicating, so that I know the write path won't fail on day one
3. As an operator, I want confirmation that the Redis cluster (3+ nodes, persistence enabled) is provisioned, so that I know the cache/queue layer is HA from the start
4. As an operator, I want confirmation that API replicas and vote processor pods are deployed with autoscaling configured, so that I know the system can absorb bursts without manual intervention
5. As an on-call engineer, I want confirmation that monitoring dashboards and alerting rules are live before launch, so that I have visibility from the first vote onward, not after an incident
6. As an on-call engineer, I want confirmation that log aggregation is configured correctly (JSON format, correct sampling rates), so that I can debug incidents without missing data
7. As a launch owner, I want confirmation that load tests (I-023) have passed against a production-topology environment, so that I know performance targets are achievable before real traffic arrives
8. As an operator, I want confirmation that the reconciliation job has been validated end-to-end, so that I know drift detection actually works, not just that the code exists
9. As an on-call engineer, I want to have read and rehearsed the incident runbooks before go-live, so that I'm not learning the escalation process during an actual incident
10. As a launch owner, I want the Big Bang launch decision and its rationale documented, so that anyone questioning "why no canary?" post-launch has a clear answer
11. As a launch owner, I want the Success Criteria from PROJECT_STATUS.md formally checked off (not just implied by "the issues are done"), so that sign-off is explicit and auditable

---

## Implementation Decisions

### This Issue's Deliverable Is a Process Artifact, Not Source Code
Unlike I-001 through I-023, there is no `src/` output here. The deliverable is:
1. A completed, dated, signed-off checklist (this document's Acceptance Criteria, checked)
2. A go/no-go decision recorded (who signed off, when, and any exceptions granted)
3. A documented rationale for the Big Bang launch strategy (captured above; also referenced from `docs/setup/launch.md` if the team keeps a runbook-adjacent doc there)

### Verification, Not Re-Implementation
Every checklist item below maps to work done in an earlier issue. This issue's job is to **confirm** each item against the running staging/production environment — checking a box here means someone verified it directly (dashboard screenshot, terminal output, a passing test report), not that they assumed it was done because the corresponding issue was marked closed.

### Deployment Checklist (from SPECIFICATION.md, Further Notes → Deployment Checklist (Pre-Launch))

| # | Item | Verified By | Produced By |
|---|---|---|---|
| 1 | 8+ PostgreSQL shards provisioned (user_id sharding), replication configured | Connect to each shard, confirm schema + replication status | I-001 |
| 2 | Redis Cluster provisioned (3+ nodes), persistence enabled | `CLUSTER INFO` shows all nodes healthy; persistence (AOF/RDB) confirmed on disk | I-002 |
| 3 | 2 initial API server replicas deployed, autoscaling configured (queue depth > 1000 trigger) | Confirm replica count in Kubernetes, trigger a synthetic queue-depth spike, observe scale-up | I-003, I-008 (scaling config) |
| 4 | Vote processor pods deployed (1 per shard), autoscaling configured | Confirm 1 pod per shard baseline, trigger scale-up via queue depth | I-008 |
| 5 | Monitoring dashboards live (Prometheus/Grafana): P95/P99 latency, queue depth, error rate, Redis hit rate | Open dashboards, confirm live data flowing (not blank panels) | I-017 |
| 6 | Alerting rules configured: queue > 10K, error rate > 1%, Redis hit < 95%, drift > 1% | Trigger each condition synthetically (or review rule config + a test firing), confirm alert delivered | I-019 |
| 7 | Logging aggregation configured (ELK/CloudWatch): JSON format, 1-in-1000 sampling for votes, 100% for errors | Query aggregated logs, confirm format and sampling rate match spec | I-018 |
| 8 | Load tests run (5x peak: 100K votes/sec, sustained 10 min) and results validated | Review I-023's archived report; all 4 scenarios pass | I-023 |
| 9 | Reconciliation job validated: runs hourly, alerts on drift | Manually induce drift in staging, confirm job detects and alerts within one cycle | I-016 (reporting), I-022 (integration test coverage) |
| 10 | Ops team briefed on incident runbook (queue backlog → scale workers, shard down → queue + retry, Redis down → fallback to DB) | Runbook walkthrough session held, attendance recorded, on-call rotation confirmed | I-020 |

### Success Criteria Sign-Off (from PROJECT_STATUS.md, Success Criteria (Pre-Launch))

This issue is where each of the following gets **verified**, not re-invented:

**Functionality**
- Users can vote on active polls (202 accepted)
- Results updated in real-time (from Redis cache)
- Users blocked from voting twice (409 Conflict)
- Admins can create/activate/close/archive polls
- Bot attacks detected and reported

**Performance**
- P95 vote latency < 50ms (target)
- P99 vote latency < 200ms (target)
- 1000 votes/sec sustained without 503 errors
- 100K votes/sec peak (5x normal) accepted, queue auto-scales

**Reliability**
- No vote loss (all votes either in queue or DB)
- Zero duplicate votes (unique constraint enforced)
- Redis cluster survives single node failure
- Database shard failover queues votes for retry
- Hourly reconciliation detects drift < 1%

**Monitoring**
- P95/P99 latency measured per endpoint
- Queue depth tracked (alert > 10K)
- Error rate tracked (alert > 1%)
- Redis hit rate tracked (alert < 95%)
- Constraint violations tracked (alert > 0 anomalies/min)

Each of these should be checked off with a concrete piece of evidence (a dashboard link, a test report, a log query result) attached in the sign-off record — not just a checkbox ticked from memory.

### Big Bang Launch Runbook Snapshot
At the moment of launch:
1. Deploy directly to production (no canary/staged traffic split)
2. On-call engineer(s) actively watching dashboards for the first monitoring window post-launch (minimum first hour, ideally first 24h)
3. Pre-agreed thresholds for manual intervention: queue depth trending toward 10K, error rate approaching 1%, P99 approaching 200ms sustained — any of these trigger the scale-up or rollback runbook (I-020), not a wait-and-see approach
4. Rollback path confirmed and rehearsed before launch (not designed for the first time during an incident)

---

## Acceptance Criteria

- [ ] All 10 Deployment Checklist items verified with evidence (not assumed) and recorded
- [ ] All Functionality success criteria verified against the live/staging environment
- [ ] All Performance success criteria verified against I-023's load test report
- [ ] All Reliability success criteria verified (including a deliberate shard-down and Redis-node-loss drill, not just a design review)
- [ ] All Monitoring success criteria verified (dashboards show live data, alerts have been test-fired at least once each)
- [ ] Big Bang launch rationale documented and shared with the team (not just implied by "we didn't build a canary")
- [ ] Rollback procedure rehearsed at least once before go-live
- [ ] Ops team runbook walkthrough completed, attendance/acknowledgment recorded
- [ ] Explicit go/no-go decision recorded with sign-off name(s) and timestamp
- [ ] Any exceptions or deferred items explicitly called out with an owner and a follow-up date (nothing silently skipped)

---

## Testing Strategy

This issue does not introduce new automated tests — it **consumes** the results of I-021, I-022, and I-023 as gating evidence:
- **Unit Tests (I-021)**: Must be green in CI at the launch commit
- **Integration Tests (I-022)**: Must be green in CI at the launch commit
- **Load Tests (I-023)**: Must have a passing report dated within the pre-launch window (not a stale run from weeks earlier)
- **Manual Verification**: Shard failover drill, Redis node-loss drill, and alert-firing drills are performed manually as part of this issue's sign-off, since they exercise real infrastructure behavior under deliberately induced failure rather than a scripted test assertion

---

## Out of Scope

- Building any new monitoring, alerting, or infrastructure — that belongs to I-002, I-017, I-018, I-019, I-020
- Designing a canary/gradual-rollout mechanism — explicitly rejected per the Big Bang decision
- Post-launch operational cadence beyond the first monitoring window (ongoing on-call rotation, weekly load test cadence) — owned by standard ops process and I-023's weekly schedule, respectively

---

## Related Issues

- I-001-database-schema.md (shard provisioning verified in checklist item 1)
- I-003-api-framework.md (API replica deployment verified in checklist item 3)
- I-013-admin-endpoints.md (admin poll lifecycle functionality verified in Success Criteria sign-off)
- I-017-monitoring.md (dashboards verified in checklist item 5)
- I-019-alerting.md (alert rules verified in checklist item 6)
- I-020-runbooks.md (ops briefing verified in checklist item 10; rollback procedure source)
- I-021-unit-tests.md (must be green at launch commit)
- I-022-integration-tests.md (must be green at launch commit)
- I-023-load-tests.md (load test report is the evidence for checklist item 8 and the Performance success criteria)

---

## Implementation Checklist

*(This is the actual go/no-go checklist — items are verification actions, not source files.)*

- [ ] Verify all 8+ PostgreSQL shards provisioned and replicating (checklist item 1)
- [ ] Verify Redis Cluster (3+ nodes) provisioned with persistence enabled (checklist item 2)
- [ ] Verify API replicas deployed with autoscaling on queue depth > 1000 (checklist item 3)
- [ ] Verify vote processor pods deployed (1/shard) with autoscaling (checklist item 4)
- [ ] Verify monitoring dashboards live with real data (checklist item 5)
- [ ] Verify alerting rules configured and test-fired (checklist item 6)
- [ ] Verify log aggregation configured with correct format/sampling (checklist item 7)
- [ ] Verify I-023 load test report passed all 4 scenarios, dated within launch window (checklist item 8)
- [ ] Verify reconciliation job detects induced drift within one cycle (checklist item 9)
- [ ] Hold ops team runbook walkthrough, record attendance (checklist item 10)
- [ ] Verify all Functionality success criteria with evidence
- [ ] Verify all Performance success criteria with evidence
- [ ] Verify all Reliability success criteria with evidence (including failure drills)
- [ ] Verify all Monitoring success criteria with evidence
- [ ] Document and circulate Big Bang launch rationale
- [ ] Rehearse rollback procedure
- [ ] Record explicit go/no-go decision with sign-off and timestamp
- [ ] Log any exceptions/deferrals with owner and follow-up date

---

**Acceptance**: All checklist items verified with evidence, Success Criteria sign-off recorded, go/no-go decision documented, launch approved.
