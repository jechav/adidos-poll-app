"""On-demand/scheduled runner for one of I-023's four Locust scenarios.

Usage::

    python -m scripts.loadtest.run_scenario sustained_peak \\
        --host https://staging.example.internal \\
        --reports-dir reports/

Runs the named scenario's `locustfile` headless via `subprocess`, parses
the resulting `--csv` output with `loadtest/utils/locust_csv.py`, and
archives a pass/fail report via `loadtest/utils/report.py::write_report`.

This is the thin, deliberately un-clever wiring the ticket's Reporting
section describes ("pulls the Locust stats API ... into an archived
report"); the actual worker-fleet sizing and distributed master/worker
invocation for the higher-throughput scenarios (`sudden_spike`,
`viral_single_poll`, `sustained_5x_peak`) is a deployment concern (see
`k8s/loadtest/`), not something this single-process convenience runner
does itself -- it targets `--master` mode being driven by the Kubernetes
Job manifests for those, and is most directly useful as-is for
`sustained_peak`, which a single process can generate.

`monitoring_snapshot` (I-017 dashboard data: recorded vote counts, queue
depth samples, replica counts) is not fetched here -- there is no
generic way to pull it that doesn't depend on the target environment's
specific Prometheus/Grafana deployment, and this script has no live
environment to validate that integration against. Pass `--monitoring-
snapshot-json` with a file already containing that data (e.g. exported
from Grafana's API, or hand-assembled from the dashboards I-017 ships)
when you have one; omitting it still archives the run's own stats with
an empty snapshot, and `evaluate_pass_fail` degrades gracefully (see its
docstring/tests) rather than crashing.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

from loadtest.utils.locust_csv import count_503_failures, parse_aggregated_stats
from loadtest.utils.report import build_report, write_report

REPO_ROOT = Path(__file__).resolve().parents[2]

# Scenarios with a `LoadTestShape` (sudden_spike, sustained_5x_peak) own
# their own ramp/hold/stop timing -- passing --users/--run-time alongside
# one is contradictory, so those keys are simply absent for them.
SCENARIOS: dict[str, dict] = {
    "sustained_peak": {
        "locustfile": "loadtest/scenarios/sustained_peak.py",
        "users": "1000",
        "run_time": "60s",
    },
    "sudden_spike": {
        "locustfile": "loadtest/scenarios/sudden_spike.py",
    },
    "viral_single_poll": {
        "locustfile": "loadtest/scenarios/viral_single_poll.py",
        "users": "50000",
        "run_time": "90s",
    },
    "sustained_5x_peak": {
        "locustfile": "loadtest/scenarios/sustained_5x_peak.py",
    },
}


def build_locust_command(scenario: str, host: str, csv_prefix: str) -> list[str]:
    """Build the `locust` argv for one scenario's on-demand run.

    Pure and subprocess-free by design (see module docstring) so it's
    unit-testable without invoking Locust or a network -- see
    `tests/unit/test_loadtest_runner.py`.
    """
    try:
        config = SCENARIOS[scenario]
    except KeyError:
        raise ValueError(
            f"unknown scenario {scenario!r}; expected one of {sorted(SCENARIOS)}"
        ) from None

    cmd = [
        "locust",
        "-f",
        config["locustfile"],
        "--headless",
        "--host",
        host,
        "--csv",
        csv_prefix,
    ]
    if "users" in config:
        cmd += ["--users", config["users"], "--run-time", config["run_time"]]
    return cmd


def _run(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("scenario", choices=sorted(SCENARIOS))
    parser.add_argument("--host", required=True)
    parser.add_argument("--reports-dir", default="reports")
    parser.add_argument("--monitoring-snapshot-json", default=None)
    args = parser.parse_args(argv)

    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H-%M-%SZ")
    csv_prefix = str(Path(args.reports_dir) / f"{args.scenario}-{timestamp}")
    cmd = build_locust_command(args.scenario, host=args.host, csv_prefix=csv_prefix)

    print(f"+ {' '.join(cmd)}", file=sys.stderr)
    subprocess.run(cmd, cwd=REPO_ROOT, check=True)

    stats = parse_aggregated_stats(Path(f"{csv_prefix}_stats.csv").read_text())
    failures_path = Path(f"{csv_prefix}_failures.csv")
    stats["num_503"] = (
        count_503_failures(failures_path.read_text()) if failures_path.exists() else 0
    )

    monitoring_snapshot = {}
    if args.monitoring_snapshot_json:
        monitoring_snapshot = json.loads(Path(args.monitoring_snapshot_json).read_text())

    report = build_report(
        scenario=args.scenario,
        stats=stats,
        monitoring_snapshot=monitoring_snapshot,
        timestamp=timestamp,
    )
    report_path = write_report(report, reports_dir=Path(args.reports_dir))
    print(f"wrote {report_path} (passed={report['passed']})", file=sys.stderr)
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(_run())
