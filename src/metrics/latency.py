"""In-memory per-route latency recording (I-003).

A minimal P95/P99 tracker so request latency is observable without pulling
in a metrics backend yet — I-017 replaces this with a real Prometheus
histogram exposed at `/metrics`; this module only needs to hold the last
window of samples per route for local inspection and tests.
"""

from collections import defaultdict, deque

_WINDOW_SIZE = 1000
_samples: dict[str, deque] = defaultdict(lambda: deque(maxlen=_WINDOW_SIZE))


def record_latency(route: str, latency_ms: float) -> None:
    _samples[route].append(latency_ms)


def _percentile(values: list[float], pct: float) -> float:
    ordered = sorted(values)
    index = min(len(ordered) - 1, int(len(ordered) * pct))
    return ordered[index]


def get_percentiles(route: str) -> dict:
    values = list(_samples.get(route, ()))
    if not values:
        return {"p95": None, "p99": None, "count": 0}
    return {
        "p95": _percentile(values, 0.95),
        "p99": _percentile(values, 0.99),
        "count": len(values),
    }


def reset() -> None:
    """Test helper — clear all recorded samples."""
    _samples.clear()
