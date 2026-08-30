# Runbook: PostgreSQL Shard Down

**Pages from**: no dedicated I-019 alert exists for a single shard
outage — it surfaces as a partial, shard-scoped symptom underneath the
same queue-depth alerts as [`queue-backlog.md`](queue-backlog.md):
`PollQueueDepthWarning` / `PollQueueDepthCritical` (`poll_queue_depth` >
5,000 / > 10,000). The distinguishing signal that tells you it's a
shard problem rather than a general capacity problem is in the worker
logs, per Diagnosis below.

---

## Symptom

Vote processor worker logs (I-018) show connection errors (e.g.
`pg_isready` failures, connection refused/timeout) for one specific
PostgreSQL shard. Queue depth rises, but **only for the subset of votes
hashing to that shard** — `poll_queue_depth` overall may cross the
warning/critical thresholds above if that shard carries a large enough
slice of traffic, but the rise is concentrated, not uniform.

## Diagnosis

1. Identify the affected shard from worker error logs — writes are
   routed by `user_id % num_shards` (I-001's sharding scheme), and
   worker log lines (I-018) carry the `shard` field on write attempts,
   so a shard erroring out repeatedly stands out immediately.
2. Check that shard's health directly: `pg_isready` against its
   connection string, or the **PostgreSQL Shard Status** panel on the
   **Infra Health** Grafana dashboard (I-017).
3. Confirm the failure is isolated to one shard (other shards' write
   latency and error rates on the **API SLOs** dashboard stay normal) —
   this rules out a cluster-wide DB problem, which would instead be
   [`db-bottleneck.md`](db-bottleneck.md).

## Mitigation

Per SPECIFICATION.md decision #7 ("availability over consistency"), a
**transient blip requires no immediate action**: affected votes remain
queued (I-005/I-008's retry-with-backoff), and once the shard recovers,
that shard's backlog drains automatically without operator
intervention. Users whose votes hash to unaffected shards see no
impact at all — this is a partial-availability degradation, not a
full outage.

If the outage persists past a transient blip, engage the DBA/infra
team to restart or fail over the PostgreSQL instance for that shard.

## Escalation

If the shard doesn't recover within roughly 15 minutes, or data
corruption is suspected, engage DB on-call / the infra team directly;
consider promoting a replica for that shard rather than waiting further.
