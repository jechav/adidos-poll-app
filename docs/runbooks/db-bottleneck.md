# Runbook: PostgreSQL Primary Bottleneck

**Pages from**: multiple I-019 alerts can indicate a saturated shard
rather than an isolated failure — `PollErrorRateWarning` /
`PollErrorRateCritical` (5xx rate > 0.5% / > 1%),
`PollP99LatencyWarning` / `PollP99LatencyCritical` (P99 vote latency >
200ms SLO target / > 500ms spec alert threshold), and
`PollReconciliationDriftWarning` / `PollReconciliationDriftCritical`
(reconciliation drift > 0.5% / > 1%, which can appear if writes are
falling behind badly enough to skew the hourly drift comparison). See
[I-019](../issues/I-019-alerting.md) for the exact thresholds.

---

## Symptom

DB write latency climbing; vote processor worker logs (I-018) show slow
inserts; queue depth (I-017) rising **despite a healthy, fully-scaled
worker count** — workers are up and dequeuing, but draining slowly, not
stalled. This is the key distinction from
[`queue-backlog.md`](queue-backlog.md): there, adding workers helps;
here, it doesn't.

## Diagnosis

1. Check per-shard write throughput against the **>10K writes/sec
   sustained** threshold named in SPECIFICATION.md decision #7 and
   Known Risk #1, using the **Infra Health** dashboard alongside the
   **API SLOs** dashboard's **Vote Latency (P50/P95/P99)** panel.
2. Determine whether load is concentrated on one shard (a hot shard
   despite even `user_id` hashing — e.g. a single viral poll skewing
   traffic) or spread evenly across shards and simply exceeding
   aggregate write capacity.

## Mitigation

This is a **capacity risk, not a transient outage** — there is no
single switch to flip, and it is explicitly **not a 5-minute on-call
fix**. What an on-call engineer can and should do in the moment:

- Confirm that further vote-processor autoscaling isn't making things
  worse. More workers hammering an already-saturated primary doesn't
  help throughput and may need a concurrency cap per shard instead of
  more replicas.
- Do **not** treat this the same as [`queue-backlog.md`](queue-backlog.md)
  by scaling workers up further — that runbook's mitigation assumes the
  DB can absorb more concurrent writes, which this scenario's Symptom
  section rules out by definition.

Beyond that, there is nothing to "fix" during the page itself — this is
exactly the trigger condition SPECIFICATION.md calls out for planning
read-write splitting or additional sharding (decision #7), which is
capacity-planning work, not incident response.

## Escalation

Escalate to the team lead / an architecture review if throughput stays
sustained above 10K writes/sec — the resolution here is a
capacity-planning decision (read replicas, additional shards,
read-write splitting), not an action an individual on-call engineer can
take unilaterally mid-incident.
