"""I-023: structural checks for the Locust project layout, k8s manifests,
weekly CronJob, and docs the Implementation Checklist calls for.

Mirrors `tests/unit/test_runbooks.py`'s approach for the same reason:
these are infra/config/doc deliverables, not business logic -- there's
nothing for pytest to execute (running the actual scenarios needs a live
staging environment; see docs/setup/load-testing.md). What's checkable by
machine is that the described project structure, manifest cross-
references, and documentation sections actually exist and stay
consistent with each other (e.g. the k8s worker Job's pod count matches
what its master Job expects).

None of this imports `locust` -- everything here is read as plain text
or parsed as YAML, so it's safe to run in the app's main pytest process
(see `loadtest/tests/test_shapes.py`'s docstring for why importing
`locust` itself can't be).
"""

from __future__ import annotations

from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
LOADTEST_DIR = REPO_ROOT / "loadtest"


def read(path: Path) -> str:
    return path.read_text()


def test_locustfile_and_utils_exist():
    assert (LOADTEST_DIR / "locustfile.py").is_file()
    assert (LOADTEST_DIR / "utils" / "tokens.py").is_file()
    assert (LOADTEST_DIR / "utils" / "report.py").is_file()
    assert (LOADTEST_DIR / "requirements.txt").is_file()


def test_all_four_scenarios_exist():
    scenarios_dir = LOADTEST_DIR / "scenarios"
    for filename in (
        "sustained_peak.py",
        "sudden_spike.py",
        "viral_single_poll.py",
        "sustained_5x_peak.py",
    ):
        assert (scenarios_dir / filename).is_file(), filename


def test_both_shapes_exist():
    shapes_dir = LOADTEST_DIR / "shapes"
    assert (shapes_dir / "spike_shape.py").is_file()
    assert (shapes_dir / "sustained_5x_shape.py").is_file()


def test_locustfile_has_no_default_target_so_a_forgetful_scenario_fails_loudly():
    text = read(LOADTEST_DIR / "locustfile.py")
    # cast_vote must not silently fall back to a hardcoded poll/answer --
    # see locustfile.py's own docstring on this.
    assert "poll_id: str | None = None" in text
    assert "answer_id: str | None = None" in text


def test_vote_user_never_hits_real_adidos_auth():
    text = read(LOADTEST_DIR / "locustfile.py")
    assert "get_test_token" in text
    assert "adidos" not in text.lower() or "never" in text.lower()


def test_scenario_thresholds_match_the_i023_scenario_table():
    expectations = {
        "sustained_peak.py": ["1_000", "60", "200", "MAX_503_COUNT = 0"],
        "sudden_spike.py": ["500"],
        "sustained_5x_peak.py": ["5"],
    }
    for filename, needles in expectations.items():
        text = read(LOADTEST_DIR / "scenarios" / filename)
        for needle in needles:
            assert needle in text, f"{filename} missing {needle!r}"


def test_shapes_match_the_i023_scenario_table():
    spike_text = read(LOADTEST_DIR / "shapes" / "spike_shape.py")
    assert "10_000" in spike_text
    assert "RAMP_SECONDS = 1" in spike_text
    assert "HOLD_SECONDS = 60" in spike_text

    peak_text = read(LOADTEST_DIR / "shapes" / "sustained_5x_shape.py")
    assert "100_000" in peak_text
    assert "HOLD_SECONDS = 600" in peak_text  # 10 minutes


def test_viral_single_poll_targets_a_single_poll_not_a_pool():
    text = read(LOADTEST_DIR / "scenarios" / "viral_single_poll.py")
    # user story #7: must target exactly one poll_id, never spread
    # across a pool the way the steady-state scenarios do.
    assert "shared_pool" not in text
    assert "POLL_APP_LOADTEST_TARGET_POLL_ID" in text


def test_report_documents_stats_and_monitoring_snapshot_shapes():
    text = read(LOADTEST_DIR / "utils" / "report.py")
    for key in (
        "recorded_vote_count",
        "queue_depth_samples",
        "max_replica_count",
        "response_time_p99_ms",
    ):
        assert key in text


def test_k8s_master_and_worker_manifests_exist():
    k8s_dir = REPO_ROOT / "k8s" / "loadtest"
    assert (k8s_dir / "master-job.yaml").is_file()
    assert (k8s_dir / "worker-job.yaml").is_file()


def test_k8s_worker_pod_count_matches_master_expected_workers():
    k8s_dir = REPO_ROOT / "k8s" / "loadtest"
    master_docs = list(yaml.safe_load_all(read(k8s_dir / "master-job.yaml")))
    worker_docs = list(yaml.safe_load_all(read(k8s_dir / "worker-job.yaml")))

    master_env = {
        e["name"]: e["value"]
        for e in master_docs[0]["spec"]["template"]["spec"]["containers"][0]["env"]
        if "value" in e
    }
    expected_workers = int(master_env["EXPECTED_WORKERS"])

    worker_job = next(d for d in worker_docs if d["kind"] == "Job")
    assert worker_job["spec"]["parallelism"] == expected_workers
    assert worker_job["spec"]["completions"] == expected_workers


def test_k8s_manifests_never_run_load_generator_on_the_service_under_test():
    # I-023's Distributed Execution section: "deployed as Kubernetes Jobs
    # alongside (not on) the service under test." A dedicated loadtest
    # image name is the concrete, checkable proxy for that separation.
    for filename in ("master-job.yaml", "worker-job.yaml"):
        text = read(REPO_ROOT / "k8s" / "loadtest" / filename)
        assert "poll-app-loadtest" in text
        assert "poll-app:latest" not in text


def test_weekly_cronjob_exists_and_is_scheduled():
    path = REPO_ROOT / "deploy" / "cronjobs" / "loadtest-weekly.yaml"
    assert path.is_file()
    doc = yaml.safe_load(read(path))
    assert doc["kind"] == "CronJob"
    assert doc["spec"]["schedule"]  # non-empty cron expression
    assert doc["spec"]["concurrencyPolicy"] == "Forbid"


def test_testing_doc_covers_running_each_scenario_and_the_failure_runbook_mapping():
    text = read(REPO_ROOT / "docs" / "setup" / "load-testing.md")
    for needle in (
        "sustained_peak",
        "sudden_spike",
        "viral_single_poll",
        "sustained_5x_peak",
        "k8s/loadtest",
        "Do not launch",
        "docs/runbooks",
    ):
        assert needle in text, f"docs/setup/load-testing.md missing {needle!r}"


def test_baseline_reports_directory_is_honest_about_having_no_staging_environment():
    # This is the one assertion in this suite that's really about
    # honesty, not structure: the Implementation Checklist calls for
    # baseline reports in reports/baseline/, and none exist because no
    # staging environment was available to produce them against. The
    # README must say so rather than the directory silently being empty
    # or (worse) containing fabricated "passing" reports.
    readme = read(REPO_ROOT / "reports" / "baseline" / "README.md")
    assert "No reports are checked in here yet" in readme
    assert "staging environment" in readme.lower()

    json_reports = list((REPO_ROOT / "reports" / "baseline").glob("*.json"))
    assert json_reports == [], (
        "reports/baseline/ contains JSON reports but no staging "
        "environment was used to produce them -- these would be "
        "fabricated results"
    )
