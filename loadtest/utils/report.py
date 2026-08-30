"""Pass/fail evaluation and archived reporting for I-023's scenarios.

Deliberately has no dependency on `locust` or the network: it takes
already-collected `stats` (from Locust's stats API) and a
`monitoring_snapshot` (pulled from I-017's dashboards/`/metrics` during
the run) as plain dicts, and returns/archives plain dicts. Fetching
those two inputs for a real run is the job of a thin CLI wrapper around
`loadtest/locustfile.py`'s test-stop hook -- not implemented here, since
it requires a live Locust master and a live Prometheus instance this
suite has no way to exercise without the staging environment the ticket
calls for (see docs/setup/testing.md's "Running against staging"
section).

`stats` dict shape (matches what Locust's `/stats/requests` JSON API and
`environment.runner.stats` expose, trimmed to what this module needs):
    num_requests, num_failures, num_503,
    response_time_p50_ms, response_time_p95_ms, response_time_p99_ms

`monitoring_snapshot` dict shape (all keys optional; only the ones the
scenario being evaluated cares about are read):
    recorded_vote_count   -- authoritative vote count after the run
                             (viral_single_poll)
    queue_depth_samples   -- list of poll_queue_depth samples taken
                             during the run (sustained_5x_peak)
    max_replica_count     -- highest API replica count observed during
                             the run (sustained_5x_peak)
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

# Reused verbatim from I-019's PollQueueDepthCritical threshold
# (deploy/prometheus/alerts.rules.yml) -- "the queue doesn't grow
# unbounded" and "the queue never reaches the state I-019 already pages
# on as a critical incident" are the same claim, not two numbers to keep
# in sync by hand.
QUEUE_DEPTH_CRITICAL_THRESHOLD = 10_000

# API replica count the Acceptance Criteria say the autoscaler must
# reach during the sustained 5x peak scenario.
TARGET_REPLICA_COUNT = 5

_SUSTAINED_PEAK_MAX_P99_MS = 200
_SUDDEN_SPIKE_MAX_P99_MS = 500


def _evaluate_sustained_peak(stats: dict, _snapshot: dict) -> list[str]:
    reasons = []
    if stats.get("num_503", 0) > 0:
        reasons.append(f"{stats['num_503']} requests returned 503 (expected 0)")
    p99 = stats.get("response_time_p99_ms", 0)
    if p99 >= _SUSTAINED_PEAK_MAX_P99_MS:
        reasons.append(
            f"p99 latency {p99}ms >= {_SUSTAINED_PEAK_MAX_P99_MS}ms target"
        )
    return reasons


def _evaluate_sudden_spike(stats: dict, _snapshot: dict) -> list[str]:
    reasons = []
    p99 = stats.get("response_time_p99_ms", 0)
    if p99 >= _SUDDEN_SPIKE_MAX_P99_MS:
        reasons.append(
            f"p99 latency {p99}ms >= {_SUDDEN_SPIKE_MAX_P99_MS}ms target during spike"
        )
    return reasons


def _evaluate_viral_single_poll(stats: dict, snapshot: dict) -> list[str]:
    reasons = []
    accepted = stats.get("num_requests", 0) - stats.get("num_failures", 0)
    recorded = snapshot.get("recorded_vote_count")
    if recorded is None:
        reasons.append("no recorded_vote_count in monitoring snapshot")
    elif recorded != accepted:
        reasons.append(
            f"data loss: {accepted} votes accepted but {recorded} recorded"
        )
    return reasons


def _evaluate_sustained_5x_peak(_stats: dict, snapshot: dict) -> list[str]:
    reasons = []
    samples = snapshot.get("queue_depth_samples", [])
    if samples and max(samples) >= QUEUE_DEPTH_CRITICAL_THRESHOLD:
        reasons.append(
            f"queue depth reached {max(samples)} "
            f"(>= critical threshold {QUEUE_DEPTH_CRITICAL_THRESHOLD})"
        )
    max_replicas = snapshot.get("max_replica_count")
    if max_replicas is not None and max_replicas < TARGET_REPLICA_COUNT:
        reasons.append(
            f"autoscaler only reached {max_replicas} replicas "
            f"(target {TARGET_REPLICA_COUNT})"
        )
    return reasons


_EVALUATORS = {
    "sustained_peak": _evaluate_sustained_peak,
    "sudden_spike": _evaluate_sudden_spike,
    "viral_single_poll": _evaluate_viral_single_poll,
    "sustained_5x_peak": _evaluate_sustained_5x_peak,
}


def evaluate_pass_fail(
    scenario: str, stats: dict, monitoring_snapshot: dict
) -> tuple[bool, list[str]]:
    """Return (passed, failure_reasons) for one scenario's collected stats.

    `failure_reasons` is empty iff `passed` is True -- callers should
    treat an empty list, not the boolean alone, as authoritative when
    deciding whether to print/log anything.
    """
    try:
        evaluator = _EVALUATORS[scenario]
    except KeyError:
        raise ValueError(
            f"unknown scenario {scenario!r}; expected one of {sorted(_EVALUATORS)}"
        ) from None
    reasons = evaluator(stats, monitoring_snapshot)
    return (len(reasons) == 0, reasons)


def build_report(
    scenario: str,
    stats: dict,
    monitoring_snapshot: dict,
    timestamp: str,
) -> dict[str, Any]:
    """Assemble one archivable report dict for a completed scenario run."""
    passed, reasons = evaluate_pass_fail(scenario, stats, monitoring_snapshot)
    return {
        "scenario": scenario,
        "timestamp": timestamp,
        "stats": stats,
        "monitoring_snapshot": monitoring_snapshot,
        "passed": passed,
        "failure_reasons": reasons,
    }


def write_report(report: dict[str, Any], reports_dir: Path) -> Path:
    """Write `report` to `<reports_dir>/<scenario>-<timestamp>.json`.

    `reports_dir` is created if it doesn't already exist (mirrors
    `reports/` for on-demand runs, `reports/baseline/` for the
    pre-launch baseline captures the Implementation Checklist calls for).
    """
    reports_dir = Path(reports_dir)
    reports_dir.mkdir(parents=True, exist_ok=True)
    path = reports_dir / f"{report['scenario']}-{report['timestamp']}.json"
    path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    return path
