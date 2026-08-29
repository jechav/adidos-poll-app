"""I-019: Prometheus Alertmanager rules for the six I-017 signals.

`promtool` isn't installed in this environment (there's no Prometheus
binary anywhere in this Python-only repo/CI image), so
`deploy/prometheus/alerts.rules.yml` is validated here with PyYAML
instead of `promtool check rules` / `promtool test rules`. These tests
cover what `promtool check rules` would (required fields, one
warning+critical pair per signal, `for` durations) plus the ticket's
specific acceptance criteria (thresholds, runbook_url wiring, and the
SLO-target-vs-alert-threshold distinction on P99 latency) that a bare
syntax check wouldn't catch anyway.
"""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

RULES_PATH = (
    Path(__file__).resolve().parents[2]
    / "deploy"
    / "prometheus"
    / "alerts.rules.yml"
)

# The five I-020 runbooks this ticket's alerts must link to (I-020 creates
# the files; this ticket only wires the URLs).
KNOWN_RUNBOOKS = {
    "queue-backlog.md",
    "shard-down.md",
    "redis-down.md",
    "db-bottleneck.md",
    "bot-attack.md",
}


def load_rules() -> dict:
    return yaml.safe_load(RULES_PATH.read_text())


def all_alerts() -> list[dict]:
    doc = load_rules()
    alerts = []
    for group in doc["groups"]:
        for rule in group["rules"]:
            alerts.append(rule)
    return alerts


def alerts_by_name() -> dict[str, dict]:
    return {a["alert"]: a for a in all_alerts()}


def test_rules_file_exists():
    assert RULES_PATH.is_file()


def test_every_alert_has_required_fields():
    for alert in all_alerts():
        assert alert.get("alert"), "every rule needs an `alert` name"
        assert alert.get("expr"), f"{alert.get('alert')} missing expr"
        labels = alert.get("labels", {})
        assert labels.get("severity") in ("warning", "critical"), alert["alert"]
        annotations = alert.get("annotations", {})
        assert annotations.get("summary"), f"{alert['alert']} missing summary"


def test_every_alert_links_to_a_known_i020_runbook():
    for alert in all_alerts():
        runbook_url = alert["annotations"].get("runbook_url")
        assert runbook_url, f"{alert['alert']} missing runbook_url"
        filename = runbook_url.rsplit("/", 1)[-1]
        assert filename in KNOWN_RUNBOOKS, f"{alert['alert']} -> {runbook_url}"


@pytest.mark.parametrize(
    "warning_alert,critical_alert",
    [
        ("PollQueueDepthWarning", "PollQueueDepthCritical"),
        ("PollErrorRateWarning", "PollErrorRateCritical"),
        ("PollRedisHitRateWarning", "PollRedisHitRateCritical"),
        ("PollReconciliationDriftWarning", "PollReconciliationDriftCritical"),
        ("PollP99LatencyWarning", "PollP99LatencyCritical"),
        ("PollDbConstraintViolationsWarning", "PollDbConstraintViolationsCritical"),
    ],
)
def test_each_signal_has_a_warning_and_critical_band(warning_alert, critical_alert):
    alerts = alerts_by_name()
    assert warning_alert in alerts
    assert critical_alert in alerts
    assert alerts[warning_alert]["labels"]["severity"] == "warning"
    assert alerts[critical_alert]["labels"]["severity"] == "critical"


def test_queue_depth_thresholds_and_for_durations():
    alerts = alerts_by_name()
    warn, crit = alerts["PollQueueDepthWarning"], alerts["PollQueueDepthCritical"]
    assert "poll_queue_depth" in warn["expr"]
    assert "5000" in warn["expr"]
    assert warn["for"] == "2m"
    assert "10000" in crit["expr"]
    assert crit["for"] == "1m"


def test_error_rate_thresholds_and_for_durations():
    alerts = alerts_by_name()
    warn = alerts["PollErrorRateWarning"]
    crit = alerts["PollErrorRateCritical"]
    assert "poll_requests_total" in warn["expr"]
    assert "status_code=~\"5..\"" in warn["expr"]
    assert "0.005" in warn["expr"]
    assert warn["for"] == "5m"
    assert "0.01" in crit["expr"]
    assert crit["for"] == "2m"


def test_redis_hit_rate_thresholds_and_for_durations():
    alerts = alerts_by_name()
    warn = alerts["PollRedisHitRateWarning"]
    crit = alerts["PollRedisHitRateCritical"]
    assert "poll_redis_cache_hits_total" in warn["expr"]
    assert "poll_redis_cache_misses_total" in warn["expr"]
    assert "0.97" in warn["expr"]
    assert warn["for"] == "10m"
    assert "0.95" in crit["expr"]
    assert crit["for"] == "5m"


def test_reconciliation_drift_thresholds_and_no_for_clause():
    # The reconciliation job runs once/hour, so `for` (which requires the
    # condition to hold across repeated evaluations) would either delay
    # every alert by re-firing scrape intervals or never fire at all
    # between runs -- both wrong. The rule fires on the single hourly
    # sample instead.
    alerts = alerts_by_name()
    warn = alerts["PollReconciliationDriftWarning"]
    crit = alerts["PollReconciliationDriftCritical"]
    assert "poll_reconciliation_drift_max_ratio" in warn["expr"]
    assert "0.005" in warn["expr"]
    assert "for" not in warn
    assert "0.01" in crit["expr"]
    assert "for" not in crit


def test_p99_latency_distinguishes_slo_target_from_alert_threshold():
    alerts = alerts_by_name()
    warn = alerts["PollP99LatencyWarning"]
    crit = alerts["PollP99LatencyCritical"]

    assert "histogram_quantile(0.99" in warn["expr"]
    assert "0.2" in warn["expr"]
    assert warn["for"] == "5m"
    assert "0.5" in crit["expr"]
    assert crit["for"] == "2m"

    # Acceptance criterion: the two numbers must never be confusable by
    # anyone reading a fired alert's annotations.
    assert "SLO target" in warn["annotations"]["summary"]
    assert "200ms" in warn["annotations"]["summary"]
    assert "500ms" in crit["annotations"]["summary"]
    assert "alert" in crit["annotations"]["summary"].lower()


def test_db_constraint_violations_warning_is_any_occurrence_no_for():
    # Per I-006/I-008, a small number of retry-driven constraint
    # violations is expected and non-incident -- the warning band is
    # visibility only (any occurrence, evaluated once), not paging.
    alerts = alerts_by_name()
    warn = alerts["PollDbConstraintViolationsWarning"]
    assert "poll_db_constraint_violations_total" in warn["expr"]
    assert "> 0" in warn["expr"]
    assert "for" not in warn


def test_db_constraint_violations_critical_is_sustained_rate():
    alerts = alerts_by_name()
    crit = alerts["PollDbConstraintViolationsCritical"]
    assert "poll_db_constraint_violations_total" in crit["expr"]
    assert "5" in crit["expr"]
    assert crit["for"] == "5m"
