"""I-023: pass/fail evaluation and archived reporting (`loadtest/utils/report.py`).

Seam choice: `report.py` never imports `locust` or talks to a real
Prometheus/Locust stats API -- it's pure functions over plain dicts,
exactly like the rest of the app's `services/` modules operate on
already-fetched data rather than reaching out themselves (see e.g.
`src/services/result_aggregator.py`). The caller (`loadtest/locustfile.py`'s
test-stop hook, not exercised here) is responsible for pulling Locust's
`environment.runner.stats` and a monitoring snapshot into the plain dict
shapes these functions take, and for network I/O this suite has no way
to exercise without a live deployment. This keeps `report.py` importable
by ordinary pytest (see `loadtest/tests/test_shapes.py`'s docstring for
why anything that imports real `locust` can't share a process with the
app's asyncio-based tests) and keeps the pass/fail *logic* -- the part
with actual branching worth testing -- fully unit-testable.

Threshold source: each scenario's pass criteria are copied verbatim from
I-023's own scenario table; the queue-depth "stays stable" check for the
5x-peak scenario reuses I-019's `PollQueueDepthCritical` threshold
(10,000) rather than inventing a new number, since "the queue does not
grow unbounded" and "the queue does not reach the state I-019 already
considers a critical incident" are the same claim.
"""

from __future__ import annotations

from pathlib import Path

from loadtest.utils.report import (
    QUEUE_DEPTH_CRITICAL_THRESHOLD,
    build_report,
    evaluate_pass_fail,
    write_report,
)


def _stats(**overrides):
    base = {
        "num_requests": 60_000,
        "num_failures": 0,
        "num_503": 0,
        "response_time_p50_ms": 40,
        "response_time_p95_ms": 90,
        "response_time_p99_ms": 150,
    }
    base.update(overrides)
    return base


def test_sustained_peak_passes_with_zero_503s_and_p99_under_200ms():
    passed, reasons = evaluate_pass_fail("sustained_peak", _stats(), {})
    assert passed is True
    assert reasons == []


def test_sustained_peak_fails_on_any_503():
    passed, reasons = evaluate_pass_fail(
        "sustained_peak", _stats(num_503=1), {}
    )
    assert passed is False
    assert any("503" in r for r in reasons)


def test_sustained_peak_fails_when_p99_at_or_above_200ms():
    passed, reasons = evaluate_pass_fail(
        "sustained_peak", _stats(response_time_p99_ms=200), {}
    )
    assert passed is False
    assert any("p99" in r.lower() for r in reasons)


def test_sudden_spike_passes_under_500ms_p99():
    passed, _ = evaluate_pass_fail(
        "sudden_spike", _stats(response_time_p99_ms=480), {}
    )
    assert passed is True


def test_sudden_spike_fails_at_or_above_500ms_p99():
    passed, reasons = evaluate_pass_fail(
        "sudden_spike", _stats(response_time_p99_ms=500), {}
    )
    assert passed is False
    assert any("p99" in r.lower() for r in reasons)


def test_viral_single_poll_passes_when_recorded_matches_accepted():
    stats = _stats(num_requests=50_000, num_failures=0)
    snapshot = {"recorded_vote_count": 50_000}
    passed, reasons = evaluate_pass_fail("viral_single_poll", stats, snapshot)
    assert passed is True
    assert reasons == []


def test_viral_single_poll_fails_on_any_data_loss():
    stats = _stats(num_requests=50_000, num_failures=0)
    snapshot = {"recorded_vote_count": 49_997}
    passed, reasons = evaluate_pass_fail("viral_single_poll", stats, snapshot)
    assert passed is False
    assert any("data loss" in r.lower() or "lost" in r.lower() for r in reasons)


def test_viral_single_poll_only_counts_accepted_votes_as_sent():
    # 500 of the 50,500 attempts were rejected (409/429/400) before
    # reaching the queue -- those were never supposed to be recorded,
    # so they must not count as "lost" votes.
    stats = _stats(num_requests=50_500, num_failures=500)
    snapshot = {"recorded_vote_count": 50_000}
    passed, reasons = evaluate_pass_fail("viral_single_poll", stats, snapshot)
    assert passed is True, reasons


def test_sustained_5x_peak_passes_when_queue_depth_stays_below_critical_threshold():
    snapshot = {"queue_depth_samples": [500, 2000, 4000, 3500, 3000]}
    passed, reasons = evaluate_pass_fail("sustained_5x_peak", _stats(), snapshot)
    assert passed is True
    assert reasons == []


def test_sustained_5x_peak_fails_when_queue_depth_reaches_critical_threshold():
    snapshot = {
        "queue_depth_samples": [500, 5000, QUEUE_DEPTH_CRITICAL_THRESHOLD]
    }
    passed, reasons = evaluate_pass_fail("sustained_5x_peak", _stats(), snapshot)
    assert passed is False
    assert any("queue" in r.lower() for r in reasons)


def test_sustained_5x_peak_fails_when_replica_count_never_reaches_five():
    snapshot = {
        "queue_depth_samples": [500, 1000],
        "max_replica_count": 3,
    }
    passed, reasons = evaluate_pass_fail("sustained_5x_peak", _stats(), snapshot)
    assert passed is False
    assert any("replica" in r.lower() for r in reasons)


def test_build_report_includes_scenario_stats_snapshot_and_verdict():
    report = build_report(
        scenario="sustained_peak",
        stats=_stats(),
        monitoring_snapshot={},
        timestamp="2026-08-29T00:00:00+00:00",
    )
    assert report["scenario"] == "sustained_peak"
    assert report["timestamp"] == "2026-08-29T00:00:00+00:00"
    assert report["stats"]["response_time_p99_ms"] == 150
    assert report["passed"] is True
    assert report["failure_reasons"] == []


def test_write_report_archives_a_timestamped_json_file(tmp_path: Path):
    report = build_report(
        scenario="sudden_spike",
        stats=_stats(),
        monitoring_snapshot={},
        timestamp="2026-08-29T12-00-00Z",
    )
    path = write_report(report, reports_dir=tmp_path)
    assert path.parent == tmp_path
    assert path.name == "sudden_spike-2026-08-29T12-00-00Z.json"
    assert path.is_file()
    import json

    assert json.loads(path.read_text()) == report
