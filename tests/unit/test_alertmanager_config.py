"""I-019: Alertmanager routing config (warning -> Slack, critical -> PagerDuty).

`promtool` isn't installed in this environment, so `deploy/alertmanager/
config.yml` is validated structurally with PyYAML instead of `amtool
check-config` / `promtool check rules` — see docs/issues/I-019-alerting.md's
Testing Strategy and the note in this ticket's implementation commit.
"""

from __future__ import annotations

from pathlib import Path

import yaml

CONFIG_PATH = (
    Path(__file__).resolve().parents[2] / "deploy" / "alertmanager" / "config.yml"
)


def load_config() -> dict:
    return yaml.safe_load(CONFIG_PATH.read_text())


def test_config_file_exists():
    assert CONFIG_PATH.is_file()


def test_route_sends_warning_to_slack_and_critical_to_pagerduty():
    config = load_config()
    routes = config["route"]["routes"]

    by_severity = {r["match"]["severity"]: r["receiver"] for r in routes}

    assert by_severity["warning"] == "slack-poll-app-alerts"
    assert by_severity["critical"] == "pagerduty-oncall"


def test_receivers_are_configured_as_code():
    config = load_config()
    receivers = {r["name"]: r for r in config["receivers"]}

    assert "slack-poll-app-alerts" in receivers
    slack = receivers["slack-poll-app-alerts"]["slack_configs"][0]
    assert slack["channel"] == "#poll-app-alerts"

    assert "pagerduty-oncall" in receivers
    pagerduty = receivers["pagerduty-oncall"]["pagerduty_configs"][0]
    # Secret is sourced from the environment, never hardcoded.
    assert pagerduty["service_key"].startswith("${") and pagerduty[
        "service_key"
    ].endswith("}")


def test_alerts_are_grouped_by_alertname_to_avoid_duplicate_pages():
    config = load_config()
    assert config["route"]["group_by"] == ["alertname"]
