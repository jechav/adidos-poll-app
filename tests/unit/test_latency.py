import pytest

from src.metrics.latency import get_percentiles, record_latency, reset


@pytest.fixture(autouse=True)
def reset_metrics():
    reset()
    yield
    reset()


def test_get_percentiles_empty_route():
    stats = get_percentiles("/v1/unused")
    assert stats == {"p95": None, "p99": None, "count": 0}


def test_get_percentiles_computes_p95_p99():
    for ms in range(1, 101):
        record_latency("/v1/polls", ms)

    stats = get_percentiles("/v1/polls")
    assert stats["count"] == 100
    assert stats["p95"] >= 95
    assert stats["p99"] >= 99
