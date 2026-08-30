"""Unit tests for I-017's metric catalog (`src/metrics/registry.py`).

Exercises the recording helpers directly against the shared
`CollectorRegistry`, parsing `generate_latest` output rather than reaching
into prometheus_client's internal sample objects — this is exactly what a
Prometheus scraper does, so it doubles as a cheap exposition-format check.
"""

from prometheus_client import generate_latest
from prometheus_client.parser import text_string_to_metric_families

from src.metrics import registry


def _samples(sample_name: str) -> dict[tuple, float]:
    """All samples across every family whose *sample* name (not the
    family name — prometheus_client's parser strips the `_total`/`_sum`
    suffix from that) matches `sample_name` exactly.
    """
    text = generate_latest(registry.REGISTRY).decode("utf-8")
    result: dict[tuple, float] = {}
    for family in text_string_to_metric_families(text):
        for s in family.samples:
            if s.name == sample_name:
                result[tuple(sorted(s.labels.items()))] = s.value
    return result


def test_record_request_observes_latency_histogram_and_increments_count():
    registry.record_request(route="/v1/vote", method="POST", status_code=202, latency_s=0.01)

    bucket_samples = _samples("poll_vote_latency_seconds_bucket")
    bucket_keys = [
        k
        for k in bucket_samples
        if ("route", "/v1/vote") in k and ("le", "0.025") in k
    ]
    assert any(bucket_samples[k] >= 1 for k in bucket_keys)

    request_samples = _samples("poll_requests_total")
    key = (("route", "/v1/vote"), ("status_code", "202"))
    assert request_samples[key] >= 1


def test_histogram_buckets_resolve_both_slo_thresholds_exactly():
    assert 0.05 in registry.VOTE_LATENCY_BUCKETS
    assert 0.2 in registry.VOTE_LATENCY_BUCKETS


def test_set_queue_depth_reflects_last_value():
    registry.set_queue_depth("votes", 42)
    gauge_samples = _samples("poll_queue_depth")
    assert gauge_samples[(("queue", "votes"),)] == 42

    registry.set_queue_depth("votes", 7)
    gauge_samples = _samples("poll_queue_depth")
    assert gauge_samples[(("queue", "votes"),)] == 7


def test_cache_hit_and_miss_counters_increment_independently():
    before_hits = _samples("poll_redis_cache_hits_total").get((("cache", "answers"),), 0)
    before_misses = _samples("poll_redis_cache_misses_total").get((("cache", "answers"),), 0)

    registry.record_cache_hit("answers")
    registry.record_cache_hit("answers")
    registry.record_cache_miss("answers")

    after_hits = _samples("poll_redis_cache_hits_total")[(("cache", "answers"),)]
    after_misses = _samples("poll_redis_cache_misses_total")[(("cache", "answers"),)]
    assert after_hits == before_hits + 2
    assert after_misses == before_misses + 1


def test_constraint_violation_counter_is_labeled_by_shard():
    before = _samples("poll_db_constraint_violations_total").get((("shard", "3"),), 0)

    registry.record_constraint_violation(3)

    after = _samples("poll_db_constraint_violations_total")[(("shard", "3"),)]
    assert after == before + 1


def test_reconciliation_drift_gauge_holds_last_set_value():
    registry.set_reconciliation_drift(0.013)
    gauge_samples = _samples("poll_reconciliation_drift_max_ratio")
    assert gauge_samples[()] == 0.013
