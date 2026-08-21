# I-020: Incident Runbooks

**Status**: Ready for Implementation  
**Epic**: Observability & Launch  
**Priority**: P1  
**Estimated Effort**: 3 days  
**Depends On**: I-017, I-019

---

## Problem Statement

`docs/runbooks/` currently exists as an empty directory. SPECIFICATION.md's Deployment Checklist item 10 requires briefing the ops team with an incident runbook before launch, and I-019's critical alerts already point their `runbook_url` annotations at files that don't exist yet. Without them, an on-call engineer paged at 3am has no documented diagnosis or mitigation path — they'd be improvising against a system they may not have built.

**This issue's deliverable is the runbook documents themselves, written in plain Markdown under `docs/runbooks/` — not code.** There is no `src/` change here, and the Implementation Checklist below lists runbook filenames to create, not source files.

The five scenarios to cover come from reconciling two overlapping-but-not-identical lists in the existing docs:

- **Deployment Checklist item 10** (SPECIFICATION.md) names three scenarios explicitly: queue backlog → scale workers, shard down → queue + retry, Redis down → fallback to materialized views
- **Known Risks & Mitigations** (SPECIFICATION.md) and the **Risk Register** (PROJECT_STATUS.md) describe substantially the same risks with different framing, plus two more: PostgreSQL primary bottleneck, Redis cluster node failure (the same underlying risk as "Redis down," folded together), and bot attack exhausting rate limit quota

Reconciled, that's five distinct runbooks, not eight — see Implementation Decisions for how the overlapping risks map onto them. One Risk Register entry (load tests not catching hot-poll bottlenecks) is a testing gap, not an incident, and is explicitly out of scope here (it belongs to I-023).

---

## Solution

Write five runbook documents in `docs/runbooks/`, each following a consistent **Symptom → Diagnosis → Mitigation → Escalation** format, so an engineer unfamiliar with the underlying code can still follow one during an incident. Each runbook names the specific I-019 alert that would page for that scenario, and references the I-017 dashboards and I-018 log fields an engineer would use to diagnose it.

---

## User Stories

(from SPECIFICATION.md)

20. As an admin, I want to be notified of critical anomalies (e.g., 100K votes in 1 second), so that I can respond to emergencies
34. As an operator, I want failed votes (e.g., shard down) to remain in the queue and retry later, so that no votes are abandoned
35. As an operator, I want a big-bang launch with close monitoring (rather than canary), so that I maximize exposure to real traffic and catch edge cases early

Also directly implements **Deployment Checklist item 10**: "Brief ops team: incident runbook (queue backlog → scale workers, shard down → queue + retry, Redis down → fallback to DB)."

---

## Implementation Decisions

### Reconciling the Risk Lists into Five Runbooks

| Runbook | Deployment Checklist #10 | Known Risks (SPEC) | Risk Register (PROJECT_STATUS) |
|---|---|---|---|
| `queue-backlog.md` | "queue backlog → scale workers" | "Async queue builds up if processors can't keep pace" | (covered under queue backlog framing) |
| `shard-down.md` | "shard down → queue + retry" | — | — |
| `redis-down.md` | "Redis down → fallback to DB" | "Redis cluster node failure causes cache miss storm" | Risk 2: Redis cluster node failure |
| `db-bottleneck.md` | — | "Single PostgreSQL primary becomes bottleneck" | Risk 1: Single PostgreSQL primary becomes bottleneck |
| `bot-attack.md` | — | "Bot attack exhausts all rate limit quota" | Risk 3: Bot attack exhausts all rate limit quota |

A single node failing within the Redis cluster and the cluster being fully unavailable are points on the same failure spectrum with the same mitigation (materialized view fallback), so they're one runbook, not two. Risk Register item 4 ("load test doesn't catch hot-poll bottleneck") is a pre-launch testing gap, not something an on-call engineer diagnoses live — it's owned by I-023 (Load Tests) and intentionally has no runbook here.

### Runbook Skeletons

The following are the intended contents of each file, to guide the writer and reviewer — the files themselves are not created by this issue's author writing this document, they're created per the Implementation Checklist below.

#### `queue-backlog.md`
- **Symptom**: `PollQueueDepthWarning`/`PollQueueDepthCritical` fires (I-019: `poll_queue_depth` > 5,000 / > 10,000)
- **Diagnosis**: Check the Infra Health dashboard's queue depth trend; compare vote-processor pod count against the autoscaling target of 1 pod per 500 queued votes (spec decision #9); check whether dequeue rate is zero (a stalled worker) versus positive-but-insufficient (a genuine burst outpacing capacity)
- **Mitigation**: If autoscaling hasn't kicked in, manually scale the vote processor deployment (`kubectl scale deployment vote-processor --replicas=N`); if a specific worker is stalled, check its logs (I-018) for repeated DB errors and restart the pod; confirm depth returns below the warning threshold
- **Escalation**: If depth keeps growing after manual scale-out, the bottleneck is likely DB write capacity, not worker count — escalate to `db-bottleneck.md`

#### `shard-down.md`
- **Symptom**: Vote processor worker logs (I-018) show connection errors for a specific PostgreSQL shard; queue depth rising, but only for that shard's partition of `queue:votes` (i.e., only affecting the subset of users hashing to that shard)
- **Diagnosis**: Identify the affected shard (`user_id % num_shards`, per I-001's sharding scheme); check the shard's health directly (connectivity, `pg_isready` or equivalent)
- **Mitigation**: Per spec decision #7, no immediate action is required for a transient blip — affected votes remain queued and the worker retries with backoff; once the shard recovers, the queue for that shard drains automatically. If the outage persists, engage the DBA/infra team to restart or fail over the PostgreSQL instance for that shard. Users whose votes hash to unaffected shards see no impact.
- **Escalation**: If the shard doesn't recover within ~15 minutes, or data corruption is suspected, engage DB on-call / infra team; consider promoting a replica for that shard

#### `redis-down.md`
- **Symptom**: `poll_redis_cache_hits_total`/`_misses_total` (I-017) stop incrementing, or the Redis hit rate alert can't evaluate at all (no data) — treat this as more severe than an ordinary "hit rate < 95%" breach
- **Diagnosis**: Check Redis Cluster node status (I-002) — is this a single node down (the cluster's gossip protocol should self-heal automatically) or a full cluster outage?
- **Mitigation**: Per spec decision #16, the **read path** (serving poll results) falls back to I-010's materialized `vote_counts` view — up to 5 minutes stale, but available, and this requires no manual intervention if implemented per I-010. The **write path** is a harder problem worth calling out explicitly: vote acceptance (I-005/I-006) depends on Redis for rate limiting and the `SET NX` uniqueness check. In a full Redis outage, graceful degradation for reads does not extend to writes — accepting votes without the uniqueness guard risks duplicate votes reaching the queue. The documented behavior is to fail vote *acceptance* closed (503) rather than risk that, even though *read* traffic keeps serving stale-but-available results.
- **Escalation**: Page Redis/infra on-call if the cluster doesn't self-heal within a few minutes

#### `db-bottleneck.md`
- **Symptom**: DB write latency climbing; vote processor logs (I-018) show slow inserts; queue depth rising despite a healthy, fully-scaled worker count (workers are up but draining slowly, not stalled)
- **Diagnosis**: Check per-shard write throughput against the ">10K writes/sec sustained" threshold named in spec decision #7 and Known Risk #1; confirm whether load is concentrated on one shard (a hot shard despite even `user_id` hashing) or spread evenly and simply exceeding aggregate capacity
- **Mitigation**: This is a capacity risk, not a transient outage — there is no single switch to flip. Confirm that further worker autoscaling isn't making things worse (more workers hammering an already-saturated primary doesn't help, and may need a concurrency cap per shard). This is the exact trigger condition SPECIFICATION.md calls out for planning read-write splitting or additional sharding (decision #7)
- **Escalation**: Not a 5-minute on-call fix — escalate to team lead / architecture review if sustained above 10K writes/sec; the resolution is a capacity-planning decision, not an incident action

#### `bot-attack.md`
- **Symptom**: `poll_db_constraint_violations_total` (I-017) or duplicate-attempt anomalies spike; rate-limit 429 responses spike across many distinct IPs; `AnomalyAlert` rows (I-014/I-015/I-016) accumulate with `alert_type = rate_limit_exceeded` or `bot_pattern_detected`
- **Diagnosis**: Check `GET /v1/admin/anomalies` (I-013/I-016) for volume and pattern — a single hot IP is different from load spread thinly across many distinct IPs (the latter is a coordinated botnet staying under each individual IP's 50/min limit)
- **Mitigation**: Local defenses (5/user/min, 50/IP/min rate limits and the 3-strike duplicate block, decisions #5/#13) apply automatically — no manual action needed for that layer. For a distributed attack spreading load across many IPs specifically to stay under per-IP limits, local rate limiting is **not sufficient** per Known Risk #3 — the real mitigation is upstream: the 30-minute batch anomaly report to Adidos (I-016) is what enables Adidos-side IP blacklisting. If urgent, manually trigger the anomaly report job early rather than waiting for the next 30-minute cycle, and contact the Adidos platform team directly with the anomaly summary.
- **Escalation**: Escalate to the Adidos platform team for upstream IP blacklist / account-ban action — the poll app's local defenses are a delay tactic against a sophisticated distributed attack, not a solution on their own

### Format Consistency

Every runbook uses the same four headings (`## Symptom`, `## Diagnosis`, `## Mitigation`, `## Escalation`) so an engineer can pattern-match across runbooks under pressure, and each opens with a one-line pointer to the I-019 alert(s) that would have paged for it.

---

## Acceptance Criteria

- [ ] Five runbook documents exist under `docs/runbooks/`, one per scenario in the table above
- [ ] Each runbook follows the Symptom → Diagnosis → Mitigation → Escalation format
- [ ] Each runbook names the specific I-019 alert(s) (warning and/or critical) that would trigger a page for that scenario
- [ ] `redis-down.md` explicitly distinguishes the read-path fallback (materialized views, always available) from the write-path risk (uniqueness enforcement depends on Redis, fails closed)
- [ ] `db-bottleneck.md` is framed as a capacity-planning escalation, not a 5-minute on-call fix
- [ ] `bot-attack.md` explicitly states that local rate limiting is a delay tactic, not a standalone solution, per Known Risk #3
- [ ] Every I-019 alert's `runbook_url` annotation resolves to one of these five files
- [ ] Runbooks reviewed and walked through with the on-call/ops team before launch (Deployment Checklist item 10)

---

## Testing Strategy

Since the deliverable is documentation, "testing" here means validating the runbooks are actually usable, not running automated tests against them:

- **Tabletop / Fire-Drill Review**: Walk each runbook with the on-call team during I-019's staging fire-drill (synthetic alert firing) — can an engineer unfamiliar with the internals follow the Diagnosis and Mitigation steps to a correct outcome?
- **Cross-Check Against Reality**: At review time, verify every command, dashboard panel, and log field a runbook references actually exists (I-017's dashboards, I-018's log schema) rather than describing something aspirational
- **Prior Art**: Mirror Adidos's existing on-call runbook format and tone for consistency across the org's runbook library

---

## Out of Scope

- Building the systems these runbooks describe (I-017 for metrics, I-019 for alerting; the fallback/retry mechanics themselves live in I-005, I-008, I-009, I-010)
- The load-testing gap risk (Risk Register item 4 — "load test doesn't catch hot-poll bottleneck") — owned by I-023
- On-call scheduling and PagerDuty rotation setup — people/process, not documentation
- Actually creating the five files under `docs/runbooks/` as part of writing *this* issue document — that work is tracked by this issue's Implementation Checklist and happens when the issue is implemented, not as a side effect of specifying it

---

## Related Issues

- I-017: Metrics & Monitoring (the metrics each runbook's Diagnosis step queries)
- I-019: Alerting & Dashboards (the alerts that trigger each runbook, and the `runbook_url` annotations pointing here)
- I-002: Redis Cluster Setup (topology referenced in `redis-down.md`)
- I-010: Materialized Views (Fallback) (the fallback mechanism `redis-down.md` documents)

---

## Implementation Checklist

- [ ] Create `docs/runbooks/queue-backlog.md`
- [ ] Create `docs/runbooks/shard-down.md`
- [ ] Create `docs/runbooks/redis-down.md`
- [ ] Create `docs/runbooks/db-bottleneck.md`
- [ ] Create `docs/runbooks/bot-attack.md`
- [ ] Add an index in `docs/runbooks/README.md` linking all five
- [ ] Update each corresponding I-019 alert's `runbook_url` annotation to point at its file
- [ ] Walk through all five runbooks with the on-call team before launch and record sign-off

---

**Acceptance**: All five runbooks written, reviewed and walked through with the on-call team, linked from I-019's alert annotations, PR reviewed and merged.
