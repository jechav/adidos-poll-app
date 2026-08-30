"""I-019: Grafana dashboards provisioned as code (JSON) from I-017's
metric catalog.

There's no live Grafana instance in this environment, so these tests
validate the dashboard JSON structurally (valid JSON, expected panels,
threshold lines/targets, titles) rather than rendering against a running
Grafana + Prometheus stack, per docs/issues/I-019-alerting.md's
Integration Tests note ("deploy the rule set and dashboards against a
running I-017 /metrics endpoint") -- that step needs live infra this
unattended run doesn't have.
"""

from __future__ import annotations

import json
from pathlib import Path

DASHBOARDS_DIR = (
    Path(__file__).resolve().parents[2] / "deploy" / "grafana" / "dashboards"
)
API_SLOS_PATH = DASHBOARDS_DIR / "api-slos.json"
INFRA_HEALTH_PATH = DASHBOARDS_DIR / "infra-health.json"


def load(path: Path) -> dict:
    return json.loads(path.read_text())


def panel_titles(dashboard: dict) -> list[str]:
    return [p["title"] for p in dashboard["panels"]]


def all_expr(dashboard: dict) -> str:
    """Flatten every panel target's `expr` into one string for substring checks."""
    exprs = []
    for panel in dashboard["panels"]:
        for target in panel.get("targets", []):
            if "expr" in target:
                exprs.append(target["expr"])
    return " | ".join(exprs)


# --- API SLOs dashboard -----------------------------------------------


def test_api_slos_dashboard_exists_and_is_valid_json():
    assert API_SLOS_PATH.is_file()
    dashboard = load(API_SLOS_PATH)
    assert dashboard["title"] == "API SLOs"


def test_api_slos_has_latency_percentile_panel_with_both_target_lines():
    dashboard = load(API_SLOS_PATH)
    titles = panel_titles(dashboard)
    assert any("Latency" in t for t in titles)

    exprs = all_expr(dashboard)
    assert "histogram_quantile(0.5" in exprs
    assert "histogram_quantile(0.95" in exprs
    assert "histogram_quantile(0.99" in exprs

    latency_panel = next(p for p in dashboard["panels"] if "Latency" in p["title"])
    # 50ms and 200ms are SLO target lines; 500ms is the separate alert
    # threshold line -- all three must be visually distinct per the
    # ticket's framing.
    threshold_values = {t["value"] for t in latency_panel["thresholds"]}
    assert {0.05, 0.2, 0.5}.issubset(threshold_values)
    slo_thresholds = [t for t in latency_panel["thresholds"] if t["value"] in (0.05, 0.2)]
    assert all("SLO target" in t["label"] for t in slo_thresholds)
    alert_threshold = next(t for t in latency_panel["thresholds"] if t["value"] == 0.5)
    assert "alert" in alert_threshold["label"].lower()


def test_api_slos_has_request_rate_and_error_rate_panels():
    dashboard = load(API_SLOS_PATH)
    titles = panel_titles(dashboard)
    assert any("Request Rate" in t for t in titles)
    assert any("Error Rate" in t for t in titles)
    assert any("Error" in t and "Status" in t for t in titles)

    exprs = all_expr(dashboard)
    assert "poll_requests_total" in exprs


# --- Infra Health dashboard ---------------------------------------------


def test_infra_health_dashboard_exists_and_is_valid_json():
    assert INFRA_HEALTH_PATH.is_file()
    dashboard = load(INFRA_HEALTH_PATH)
    assert dashboard["title"] == "Infra Health"


def test_infra_health_has_queue_depth_gauge_with_threshold_lines():
    dashboard = load(INFRA_HEALTH_PATH)
    queue_panel = next(p for p in dashboard["panels"] if "Queue Depth" in p["title"])
    exprs = " ".join(t["expr"] for t in queue_panel["targets"])
    assert "poll_queue_depth" in exprs
    threshold_values = {t["value"] for t in queue_panel["thresholds"]}
    assert {5000, 10000}.issubset(threshold_values)


def test_infra_health_has_redis_and_shard_status_and_drift_panels():
    dashboard = load(INFRA_HEALTH_PATH)
    titles = panel_titles(dashboard)

    assert any("Redis Hit Rate" in t for t in titles)
    assert any("Redis" in t and ("Cluster" in t or "Node" in t) for t in titles)
    assert any("Shard" in t for t in titles)
    assert any("Reconciliation Drift" in t for t in titles)
    assert any("Constraint Violation" in t for t in titles)


def test_dashboards_are_provisioned_via_config_not_manual_import():
    provisioning_path = (
        Path(__file__).resolve().parents[2]
        / "deploy"
        / "grafana"
        / "provisioning"
        / "dashboards.yml"
    )
    assert provisioning_path.is_file()
