# Runbook: Redis Down / Cache Miss Storm

**Pages from**: `PollRedisHitRateWarning` (hit rate < 97%, warning) /
`PollRedisHitRateCritical` (hit rate < 95%, critical, the spec-defined
threshold) — see [I-019](../issues/I-019-alerting.md). Treat the alert
being **unable to evaluate at all** (no data from
`poll_redis_cache_hits_total` / `poll_redis_cache_misses_total`) as
**more severe** than an ordinary threshold breach — it usually means
the cluster stopped responding entirely, not just serving slower.

---

## Symptom

`poll_redis_cache_hits_total` / `poll_redis_cache_misses_total` (I-017)
stop incrementing, or the hit-rate alert can't evaluate (no data).

## Diagnosis

Check the **Redis Cluster Node Status** panel on the **Infra Health**
Grafana dashboard, or `redis-cli --cluster check` / `CLUSTER NODES`
directly against the cluster (I-002), to determine which of two very
different situations this is:

- **A single node down**: the cluster's gossip protocol (I-002) should
  self-heal automatically as remaining nodes redistribute the affected
  hash slots.
- **A full cluster outage**: no nodes reachable, or the cluster can't
  reach quorum.

## Mitigation

The read and write paths behave very differently under a Redis outage,
and conflating them is the main way this runbook gets misapplied
mid-incident:

- **Read path (serving poll results)** falls back to I-010's
  materialized `vote_counts` view automatically — results stay
  available, up to 5 minutes stale, with **no manual intervention**
  required if I-010 is implemented and wired up correctly. Confirm on
  the **API SLOs** dashboard that the results endpoint's error rate
  stays flat despite the Redis outage; if it doesn't, I-010's fallback
  itself is broken and this becomes a incident of its own.
- **Write path (vote acceptance) is the harder problem** and does
  **not** get the same free pass. Vote acceptance (I-005/I-006) depends
  on Redis for rate limiting and the `SET NX` uniqueness check. In a
  full Redis outage, accepting votes without that uniqueness guard
  risks duplicate votes reaching the queue — so the documented,
  intended behavior is to **fail vote acceptance closed (HTTP 503)**
  rather than risk double-counted votes, even though the read path
  keeps serving stale-but-available results. This is a deliberate
  asymmetry, not a bug: reads degrade gracefully, writes fail closed.
- No manual action is required to trigger either behavior — both are
  implemented behaviors of I-005/I-009/I-010. This section exists so
  an on-call engineer isn't surprised that votes are being rejected
  (503) while results still look fine, and doesn't try to "fix" the
  503s by bypassing the uniqueness guard.

## Escalation

Page Redis/infra on-call if the cluster doesn't self-heal within a few
minutes of a single-node failure, or immediately for a full cluster
outage — every additional minute of the outage is additional minutes
of vote acceptance failing closed (503s), which directly blocks the
launch's "big-bang, high exposure" monitoring goal (user story #35).
