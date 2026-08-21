# I-017: Metrics & Monitoring (Prometheus)

**Status**: Ready for Implementation  
**Epic**: Observability & Launch  
**Priority**: P0 (Blocker)  
**Estimated Effort**: 5 days  
**Depends On**: I-003, I-005, I-008, I-009

---

## Problem Statement

The poll app has no visibility into its own health once traffic starts flowing. Every claim in SPECIFICATION.md's Performance and Reliability success criteria — P95 < 50ms, P99 < 200ms, no vote loss, reconciliation drift < 1% — is unverifiable without instrumentation. This issue builds that instrumentation:

- Expose the six signals named in Implementation Decision #15 (Monitoring & Observability) as Prometheus metrics: P95/P99 vote latency, queue depth, error rate, Redis hit rate, DB constraint violation count, and reconciliation drift
- Extend I-003's per-request latency middleware from a stub that measures and drops a number into a real histogram, labeled by route, that Prometheus can query
- Implement the hourly reconciliation job (user story #30) that produces the reconciliation drift metric — comparing I-009's live Redis vote counters against I-010's materialized view / raw `votes` table counts
- Expose a dedicated `GET /metrics` endpoint for Prometheus to scrape, distinct from I-003's `GET /health` liveness check

Without this, I-019 (alerting/dashboards) and I-020 (runbooks) have nothing to point at — this issue is the foundation the rest of Phase 5 builds on.

---

## Solution

Instrument the FastAPI app with `prometheus-fastapi-instrumentator` for baseline HTTP metrics (request count, in-progress requests, default latency buckets), then add hand-rolled collectors for the poll-domain-specific signals that don't come for free from generic HTTP instrumentation: queue depth, Redis hit/miss counters, DB constraint violations, and reconciliation drift. All metrics live in a process-local `prometheus_client.CollectorRegistry` and are served at `GET /metrics` in Prometheus text exposition format.

The hourly reconciliation job is implemented **in this issue** (not I-009 or I-010) because its sole output is a metric — it has no user-facing behavior of its own. It reads live counts from I-009's Redis aggregates and authoritative counts from I-010's materialized `vote_counts` table (falling back to `COUNT(*) FROM votes` if the materialized view hasn't refreshed within its 5-minute window), computes drift per poll, and publishes the worst offender as a gauge.

---

## User Stories

(from SPECIFICATION.md, Infrastructure & Reliability block)

26. As an operator, I want the service to auto-scale API replicas when queue depth exceeds 1000 votes, so that bursts are absorbed without manual intervention
27. As an operator, I want the service to auto-scale vote processors when queue depth exceeds 1000 votes, so that votes are written to the database quickly
28. As an operator, I want Redis to be clustered (3+ nodes) so that it survives single node failures
29. As an operator, I want votes queued in Redis to survive brief service restarts, so that no votes are lost
30. As an operator, I want hourly reconciliation comparing Redis vote counts vs. database counts, so that I catch drift and alert if it exceeds 1%
31. As an operator, I want to monitor P95 and P99 vote latency (time from vote request to Redis update), so that I detect when bursts degrade performance
32. As an operator, I want to monitor queue depth, error rate, and Redis hit rate, so that I have end-to-end visibility
33. As an operator, I want database backups with 5-minute RPO (recovery point objective), so that data loss is bounded

Stories #30, #31, and #32 are directly implemented here (the metrics and the reconciliation job). Stories #26–29 and #33 describe infrastructure behaviors (autoscaling, clustering, persistence, backups) that this issue's metrics *feed* (e.g., `poll_queue_depth` is what a Kubernetes HPA would key off of) but does not itself configure — see Out of Scope.

---

## Implementation Decisions

### Metric Catalog

Per Implementation Decision #15 in SPECIFICATION.md, exactly these six signals must be observable:

| Metric | Prometheus Type | Labels | Target / Alert Threshold | Populated By |
|---|---|---|---|---|
| `poll_vote_latency_seconds` | Histogram | `route`, `method` | P95 < 50ms, P99 < 200ms | HTTP middleware (extends I-003) |
| `poll_queue_depth` | Gauge | `queue` | alert if > 10,000 | Vote queue writer (I-005) / worker (I-008) |
| `poll_requests_total` | Counter | `route`, `status_code` | error rate = 5xx / total, alert if > 1% | HTTP middleware |
| `poll_redis_cache_hits_total` / `poll_redis_cache_misses_total` | Counter | `cache` | hit rate = hits / (hits+misses), alert if < 95% | Results cache reader (I-009) |
| `poll_db_constraint_violations_total` | Counter | `shard` | alert if > 0/min | Vote processor insert path (I-008) |
| `poll_reconciliation_drift_max_ratio` | Gauge | — (worst poll per run) | alert if > 1% | Reconciliation job (this issue) |

The actual PromQL alert expressions and thresholds (warning vs. critical bands) are defined in I-019 — this issue's job is to make sure the underlying numbers exist and are scrapeable, not to define alerting policy.

### Extending I-003's Latency Middleware into a Histogram

I-003 sketched a middleware that measures latency and calls `metrics.record_latency(...)` without defining what that does. This issue replaces the stub with a real Prometheus histogram, labeled by the matched route (not the raw path, to avoid unbounded cardinality from path parameters):

```python
from prometheus_client import Histogram

VOTE_LATENCY = Histogram(
    "poll_vote_latency_seconds",
    "Request latency in seconds, labeled by route",
    labelnames=["route", "method"],
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.2, 0.5, 1.0, 2.0, 5.0),
)

@app.middleware("http")
async def record_latency(request: Request, call_next):
    start_time = time.monotonic()
    response = await call_next(request)
    latency_s = time.monotonic() - start_time
    route = request.scope.get("route")
    route_label = route.path if route else "unmatched"
    VOTE_LATENCY.labels(route=route_label, method=request.method).observe(latency_s)
    REQUEST_COUNT.labels(route=route_label, status_code=response.status_code).inc()
    return response
```

The bucket boundaries are chosen to resolve both SLO targets precisely: 50ms and 200ms both sit on bucket edges, so `histogram_quantile(0.95, ...)` and `histogram_quantile(0.99, ...)` are accurate without interpolation error at the exact thresholds the spec cares about.

### `GET /metrics` Endpoint

```python
from prometheus_client import generate_latest, CONTENT_TYPE_LATEST
from fastapi import APIRouter, Response

metrics_router = APIRouter()

@metrics_router.get("/metrics")
async def metrics():
    return Response(generate_latest(REGISTRY), media_type=CONTENT_TYPE_LATEST)
```

This is deliberately **not** wrapped in I-003's JSON success/error envelope — Prometheus's scraper expects the plain text exposition format, not JSON. It is also distinct from `GET /health` (I-003): `/health` answers "is this process alive and ready to serve traffic" (used by Kubernetes liveness/readiness probes), while `/metrics` answers "what happened" (used by the Prometheus scrape target). Conflating the two would mean a slow-to-render metrics payload could fail a liveness probe, or a metrics scrape could be gated behind readiness logic it doesn't need.

### Reconciliation Job (Hourly)

Implements user story #30. Runs as a Kubernetes CronJob (hourly schedule) invoking a standalone async script:

```python
async def run_reconciliation():
    worst_drift = 0.0
    for poll in await get_active_and_recently_closed_polls():
        redis_counts = await get_redis_vote_counts(poll.poll_id)      # I-009
        db_counts = await get_materialized_vote_counts(poll.poll_id)  # I-010, falls back to
                                                                        # COUNT(*) FROM votes if stale
        redis_total = sum(redis_counts.values())
        db_total = sum(db_counts.values())
        if db_total == 0:
            continue
        drift = abs(redis_total - db_total) / db_total
        worst_drift = max(worst_drift, drift)
        if drift > 0.01:
            logger.warning("reconciliation_drift_detected", poll_id=poll.poll_id,
                            redis_total=redis_total, db_total=db_total, drift=drift)
    RECONCILIATION_DRIFT.set(worst_drift)
```

`worst_drift` (the single largest per-poll drift ratio observed in the run) is published as the gauge value, since a single alert threshold needs one number, not a per-poll vector, to compare against 1%. Per-poll detail is only in the structured log line (I-018), which an operator can grep if the alert fires.

### Redis Hit Rate

Implemented as two counters (`poll_redis_cache_hits_total`, `poll_redis_cache_misses_total`) incremented in I-009's cache-read path, rather than a single pre-computed gauge. The ratio is computed at query time via PromQL (`rate(hits[5m]) / (rate(hits[5m]) + rate(misses[5m]))`) — this is idiomatic Prometheus practice (rates over raw counters, not app-computed ratios) and keeps the threshold/alerting logic entirely in I-019 rather than duplicated in application code.

### DB Constraint Violation Counter

Incremented in I-008's vote processor whenever an insert into `votes` (I-001's schema) fails the `UNIQUE(user_id, poll_id)` constraint. Per spec decision #4, this is an expected, non-retried, logged-and-skipped outcome — the counter's purpose is to catch a rate anomaly (many violations/min could mean the Redis `SET NX` uniqueness layer, I-006, is failing to catch duplicates before they reach the queue), not to treat every violation as an incident on its own.

---

## Acceptance Criteria

- [ ] `GET /metrics` returns Prometheus text exposition format, unauthenticated on the internal network, distinct from `GET /health`
- [ ] `poll_vote_latency_seconds` histogram is labeled by route and populated by every request through the extended I-003 middleware
- [ ] `poll_queue_depth` gauge reflects the live length of `queue:votes` (I-005/I-008)
- [ ] `poll_requests_total` counter is labeled by route and status_code (sufficient to compute error rate in PromQL)
- [ ] `poll_redis_cache_hits_total` / `poll_redis_cache_misses_total` counters increment on every I-009 cache read
- [ ] `poll_db_constraint_violations_total` counter increments on every unique-constraint rejection in I-008's insert path
- [ ] Reconciliation job runs hourly, compares I-009 Redis counts against I-010 materialized/raw DB counts, and publishes `poll_reconciliation_drift_max_ratio`
- [ ] All histogram bucket boundaries resolve the 50ms and 200ms SLO thresholds without interpolation
- [ ] Metric catalog documented (names, types, labels) for I-019 to build alert rules against

---

## Testing Strategy

- **Unit Tests**: Reconciliation drift calculation (given known Redis/DB counts, assert correct drift ratio); `is_sampled`-style edge cases (zero DB count, exact 1% boundary)
- **Integration Tests**: Scrape `/metrics` after issuing known requests, parse the exposition format, assert counters/histograms reflect the traffic; run the reconciliation job against a seeded shard with a deliberately introduced drift, assert the gauge picks it up
- **Load Tests**: Confirm histogram observation doesn't materially add to per-request latency at 1000 votes/sec (feeds into I-023)
- **Prior Art**: Mirror I-001's seeded test data for reconciliation job fixtures; mirror I-003's route testing structure for the `/metrics` endpoint

---

## Out of Scope

- Actual autoscaling of API/worker replicas based on `poll_queue_depth` (Kubernetes HPA configuration is deployment/ops scope — this issue only makes the metric available; see stories #26/#27)
- Redis clustering and persistence configuration (I-002; story #28/#29)
- Database backup/RPO tooling (story #33 — ops/infra scope, not application metrics)
- Alert thresholds, severity routing, and Grafana dashboards (I-019)
- Reporting anomalies to Adidos (I-016)

---

## Related Issues

- I-003: API Framework & Routing (this issue extends its latency middleware stub and adds `/metrics` alongside its `/health`)
- I-005: Vote Acceptance & Queueing (source of `poll_queue_depth` on the enqueue side)
- I-008: Vote Processor Workers (source of `poll_queue_depth` on the drain side and `poll_db_constraint_violations_total`)
- I-009: Result Aggregation (source of Redis hit/miss counters and one half of the reconciliation comparison)
- I-010: Materialized Views (Fallback) (other half of the reconciliation comparison)
- I-018: Structured Logging (reconciliation job logs per-poll drift detail that this issue's gauge only summarizes)
- I-019: Alerting & Dashboards (consumes every metric defined here)
- I-020: Incident Runbooks (diagnosis steps reference these metrics/dashboards)

---

## Implementation Checklist

- [ ] Add `prometheus-client` and `prometheus-fastapi-instrumentator` to dependencies
- [ ] Create `src/metrics/registry.py` (shared `CollectorRegistry` and metric definitions)
- [ ] Extend `src/api/middleware/latency.py` (builds on I-003's `record_latency`) to observe `poll_vote_latency_seconds` and increment `poll_requests_total`
- [ ] Create `src/api/routes/metrics.py` exposing `GET /metrics`
- [ ] Add `poll_queue_depth` gauge updates in `src/services/vote_queue.py` (I-005/I-008)
- [ ] Add `poll_redis_cache_hits_total` / `_misses_total` counters in `src/services/results_cache.py` (I-009)
- [ ] Add `poll_db_constraint_violations_total` counter in the vote processor's insert path (I-008)
- [ ] Create `src/jobs/reconciliation.py` (hourly job, drift gauge)
- [ ] Schedule the reconciliation job as a Kubernetes CronJob, document the schedule in `docs/architecture/metrics.md`
- [ ] Write unit and integration tests covering the metric catalog and reconciliation job
- [ ] Document the metric catalog in `docs/architecture/metrics.md`

---

**Acceptance**: All acceptance criteria met, `/metrics` scrapeable and reconciliation job validated against seeded drift, PR reviewed and merged.
