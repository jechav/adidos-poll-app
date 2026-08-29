# Metrics & Monitoring (I-017)

Implements [I-017](../issues/I-017-monitoring.md). Covers the metric
catalog served at `GET /metrics`, who populates each signal, and the
hourly reconciliation job. This document is what I-019 (alerting/
dashboards) builds PromQL alert rules against.

## Metric catalog

All six metrics live on one process-local `prometheus_client.CollectorRegistry`
(`src/metrics/registry.py`'s `REGISTRY`) and are exposed together at
`GET /metrics` (`src/api/routes/metrics.py`) in Prometheus text exposition
format — unauthenticated on the internal network, distinct from
`GET /health` (see that route's docstring for why the two aren't merged).

| Metric | Type | Labels | Target / alert threshold | Populated by |
|---|---|---|---|---|
| `poll_vote_latency_seconds` | Histogram | `route`, `method` | P95 < 50ms, P99 < 200ms | `src/api/middleware/request_context.py` (every request) |
| `poll_requests_total` | Counter | `route`, `status_code` | error rate = 5xx / total, alert if > 1% | same middleware |
| `poll_queue_depth` | Gauge | `queue` (`"votes"`) | alert if > 10,000 | `src/services/vote_queue.py` (enqueue), `src/worker/queue_consumer.py` (dequeue) |
| `poll_redis_cache_hits_total` / `poll_redis_cache_misses_total` | Counter | `cache` (`"answers"`) | hit rate = hits / (hits+misses), alert if < 95% | `src/cache/answer_cache.py` |
| `poll_db_constraint_violations_total` | Counter | `shard` | alert if > 0/min | `src/worker/db.py::insert_votes_individually` |
| `poll_reconciliation_drift_max_ratio` | Gauge | — (worst poll per run) | alert if > 1% | `src/jobs/reconciliation.py` (hourly) |

The actual PromQL alert expressions and severity bands are I-019's job —
this table only documents that the underlying numbers exist, are
correctly labeled, and are scrapeable.

### Histogram buckets

`poll_vote_latency_seconds` uses `(0.005, 0.01, 0.025, 0.05, 0.1, 0.2,
0.5, 1.0, 2.0, 5.0)` seconds. `0.05` (50ms) and `0.2` (200ms) both sit
exactly on a bucket edge, so `histogram_quantile(0.95, ...)` and
`histogram_quantile(0.99, ...)` resolve the spec's SLO thresholds without
interpolation error.

### Route labeling

Requests are labeled by the *matched route* (`request.scope["route"].path`),
not the raw request path — a request that never matched a route (404) is
labeled `route="unmatched"` instead. This keeps cardinality bounded
regardless of how many distinct path-parameter values (poll IDs, etc.)
are requested.

## Reconciliation job (hourly)

`src/jobs/reconciliation.py`, deployed as the `reconciliation` CronJob
(`deploy/cronjobs/reconciliation.yaml`, `schedule: "0 * * * *"`).
Implements user story #30.

For every poll returned by `get_active_and_recently_closed_polls()`
(state `active`, or `closed` within the last hour — a poll closed longer
ago has already been reconciled by prior runs and its counts have
stopped changing):

1. `get_redis_vote_counts` — live per-answer counts via I-009's
   `compute_poll_results` (reused, not re-derived, so this job and the
   results endpoint never disagree about what a "live count" is).
2. `get_materialized_vote_counts` — I-010's `vote_counts` table, if every
   row for that poll was refreshed within the last 5 minutes (I-010's
   own refresh cadence); otherwise a live `COUNT(*) FROM votes` fan-out
   across every shard (`votes` is sharded by `user_id`, so no single
   shard has the full count).
3. `compute_drift(redis_total, db_total)` — `abs(redis_total - db_total)
   / db_total`, or `None` when `db_total == 0` (nothing to compare
   against yet — never reported as 100% drift).
4. The single largest drift ratio observed across every poll in the run
   is published as `poll_reconciliation_drift_max_ratio` — one gauge
   value, since one alert threshold compares against one number, not a
   per-poll vector. Any poll whose drift exceeds 1% also gets a
   structured `reconciliation_drift_detected` warning log line (I-018)
   with the full per-poll detail (`poll_id`, `redis_total`, `db_total`,
   `drift`) that the gauge alone doesn't carry.

### Operational note: CronJob metrics need a push path

`GET /metrics` is served by the long-running API process's in-memory
registry. The reconciliation job runs as a separate, short-lived CronJob
pod — its `RECONCILIATION_DRIFT.set(...)` call updates a registry that
belongs to *that pod's own process*, which exits once the run finishes.
Left as-is, nothing ever scrapes that value.

Making the gauge actually reach Prometheus requires one of:

- Push the value to a
  [Pushgateway](https://github.com/prometheus/pushgateway) at the end of
  each run (`prometheus_client.push_to_gateway`), with Prometheus
  scraping the gateway instead of (or in addition to) the API pod, or
- Have the job write the metric to a file
  (`prometheus_client.write_to_textfile`) on a volume the node's
  `node_exporter` textfile collector reads.

Neither is wired up by this issue — I-017's scope is the metric
existing and being computed correctly (unit- and integration-tested
against real Postgres in `tests/unit/test_reconciliation.py` /
`tests/integration/test_reconciliation_db.py`), not the CronJob's
delivery mechanism to Prometheus. Whoever wires up I-019's Grafana
dashboards needs one of the two options above in place first, or
`poll_reconciliation_drift_max_ratio` will read as perpetually absent
rather than stale.
