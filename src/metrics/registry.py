"""Prometheus metric catalog (I-017), per Implementation Decision #15 in
SPECIFICATION.md.

Every metric lives on one process-local `CollectorRegistry` (`REGISTRY`)
rather than prometheus_client's global default registry, so `GET /metrics`
(`src/api/routes/metrics.py`) serves exactly this catalog and nothing an
unrelated import happened to register elsewhere.

The six signals named in Implementation Decision #15, and who populates
each one:

| Metric                                   | Type      | Populated by                                   |
|-------------------------------------------|-----------|-------------------------------------------------|
| `poll_vote_latency_seconds`               | Histogram | `record_request` — I-003's latency middleware    |
| `poll_requests_total`                     | Counter   | `record_request` — same middleware               |
| `poll_queue_depth`                        | Gauge     | `set_queue_depth` — I-005 enqueue / I-008 dequeue |
| `poll_redis_cache_hits_total` / `_misses` | Counter   | `record_cache_hit` / `record_cache_miss` — I-009 |
| `poll_db_constraint_violations_total`     | Counter   | `record_constraint_violation` — I-008 insert path|
| `poll_reconciliation_drift_max_ratio`     | Gauge     | `set_reconciliation_drift` — I-017's own job      |

Callers never touch the `prometheus_client` objects directly — they go
through the recording functions below, so the label contract (what a
"route" or "shard" string looks like) lives in one place instead of being
re-derived at every call site.
"""

from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram

REGISTRY = CollectorRegistry()

# `poll_queue_depth{queue=...}` label for `queue:votes` — shared by both
# the enqueue side (`src/services/vote_queue.py`, I-005) and the dequeue
# side (`src/worker/queue_consumer.py`, I-008) so the two call sites
# can't drift into labeling the same queue two different ways.
QUEUE_LABEL_VOTES = "votes"

# `poll_redis_cache_hits_total`/`_misses_total{cache=...}` label for the
# I-009 answer-metadata cache (`src/cache/answer_cache.py`) — the only
# I-009 cache-read path today.
CACHE_LABEL_ANSWERS = "answers"

# 50ms and 200ms (the spec's P95/P99 SLO targets) both sit exactly on a
# bucket edge, so `histogram_quantile(0.95, ...)` / `(0.99, ...)` resolve
# those thresholds without interpolation error.
VOTE_LATENCY_BUCKETS = (0.005, 0.01, 0.025, 0.05, 0.1, 0.2, 0.5, 1.0, 2.0, 5.0)

VOTE_LATENCY = Histogram(
    "poll_vote_latency_seconds",
    "Request latency in seconds, labeled by route and method",
    labelnames=["route", "method"],
    buckets=VOTE_LATENCY_BUCKETS,
    registry=REGISTRY,
)

REQUEST_COUNT = Counter(
    "poll_requests_total",
    "Total HTTP requests, labeled by route and status_code",
    labelnames=["route", "status_code"],
    registry=REGISTRY,
)

QUEUE_DEPTH = Gauge(
    "poll_queue_depth",
    "Live length of a vote queue",
    labelnames=["queue"],
    registry=REGISTRY,
)

REDIS_CACHE_HITS = Counter(
    "poll_redis_cache_hits_total",
    "Redis cache hits, labeled by cache name",
    labelnames=["cache"],
    registry=REGISTRY,
)

REDIS_CACHE_MISSES = Counter(
    "poll_redis_cache_misses_total",
    "Redis cache misses, labeled by cache name",
    labelnames=["cache"],
    registry=REGISTRY,
)

DB_CONSTRAINT_VIOLATIONS = Counter(
    "poll_db_constraint_violations_total",
    "Unique-constraint rejections in the vote insert path, labeled by shard",
    labelnames=["shard"],
    registry=REGISTRY,
)

RECONCILIATION_DRIFT = Gauge(
    "poll_reconciliation_drift_max_ratio",
    "Largest per-poll Redis-vs-DB drift ratio observed in the last "
    "reconciliation run",
    registry=REGISTRY,
)


def record_request(*, route: str, method: str, status_code: int, latency_s: float) -> None:
    """One HTTP request's worth of observation: the latency histogram and
    the request counter, both labeled the same way (by route) so their
    ratio (error rate) and their quantiles (P95/P99) can be computed in
    PromQL without a join across differently-labeled series.
    """
    VOTE_LATENCY.labels(route=route, method=method).observe(latency_s)
    REQUEST_COUNT.labels(route=route, status_code=str(status_code)).inc()


def set_queue_depth(queue: str, depth: int) -> None:
    QUEUE_DEPTH.labels(queue=queue).set(depth)


def record_cache_hit(cache: str) -> None:
    REDIS_CACHE_HITS.labels(cache=cache).inc()


def record_cache_miss(cache: str) -> None:
    REDIS_CACHE_MISSES.labels(cache=cache).inc()


def record_constraint_violation(shard: int | str) -> None:
    DB_CONSTRAINT_VIOLATIONS.labels(shard=str(shard)).inc()


def set_reconciliation_drift(ratio: float) -> None:
    """`ratio` is the single worst per-poll drift observed in one
    reconciliation run (see `src/jobs/reconciliation.py`) — one number,
    since a single alert threshold compares against one gauge, not a
    per-poll vector.
    """
    RECONCILIATION_DRIFT.set(ratio)
