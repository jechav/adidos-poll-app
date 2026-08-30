# Incident Runbooks

Five runbooks, one per operational scenario named in SPECIFICATION.md's
Deployment Checklist item 10 and Known Risks / PROJECT_STATUS.md's Risk
Register. See [I-020](../issues/I-020-runbooks.md) for how the two
overlapping risk lists were reconciled into these five files (not
eight).

Every runbook follows the same **Symptom → Diagnosis → Mitigation →
Escalation** format, and opens with a pointer to the [I-019](../issues/I-019-alerting.md)
alert(s) that would page for it, so an on-call engineer can pattern-match
across runbooks under pressure.

| Runbook | Scenario | Pages from (I-019) |
|---|---|---|
| [`queue-backlog.md`](queue-backlog.md) | Vote queue backlog outpacing worker capacity | `PollQueueDepthWarning` / `PollQueueDepthCritical` |
| [`shard-down.md`](shard-down.md) | A single PostgreSQL shard unreachable | No dedicated alert — surfaces under the queue-depth alerts, scoped to one shard |
| [`redis-down.md`](redis-down.md) | Redis cluster degraded or fully down | `PollRedisHitRateWarning` / `PollRedisHitRateCritical` |
| [`db-bottleneck.md`](db-bottleneck.md) | PostgreSQL primary saturated across shards | `PollErrorRateCritical`, `PollP99LatencyCritical`, `PollReconciliationDriftCritical` |
| [`bot-attack.md`](bot-attack.md) | Distributed bot attack exhausting rate-limit quota | `PollDbConstraintViolationsWarning` / `PollDbConstraintViolationsCritical` |

## Diagnosing which runbook applies

Several of these scenarios look similar at first glance (queue depth
rising, errors climbing). The fastest disambiguator is usually **is the
worker fleet keeping up or not**:

- Workers stalled or under-scaled, DB otherwise healthy → `queue-backlog.md`
- Workers healthy and scaled, but one shard specifically erroring → `shard-down.md`
- Workers healthy and scaled, but draining slowly across all shards → `db-bottleneck.md`
- Redis metrics stop reporting or hit rate collapses → `redis-down.md`
- Constraint violations or 429s spike across many distinct IPs → `bot-attack.md`

## Before launch

Per Deployment Checklist item 10, these runbooks must be walked through
with the on-call/ops team (a tabletop review, ideally during I-019's
staging fire-drill) before launch — see I-020's Testing Strategy. Record
sign-off in [I-020](../issues/I-020-runbooks.md) once that review has
happened; nothing in this repository can automate that step.
