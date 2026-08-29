"""I-020: Incident runbooks under `docs/runbooks/`.

The deliverable here is documentation, not code (see I-020's Problem
Statement), so there's no behavior to unit-test in the usual sense.
What *is* checkable by machine, per I-020's own Acceptance Criteria and
Testing Strategy ("Cross-Check Against Reality"), is structural:

- the five files actually exist, with the required Symptom/Diagnosis/
  Mitigation/Escalation section format
- each names the I-019 alert(s) that would page for it
- the handful of specific content requirements the Acceptance Criteria
  call out verbatim (redis-down's read/write split, db-bottleneck's
  capacity framing, bot-attack's delay-tactic framing)
- every `runbook_url` in I-019's `deploy/prometheus/alerts.rules.yml`
  resolves to one of these five files, and `docs/runbooks/README.md`
  indexes all five

This does not (and cannot) verify the *content* is operationally
correct -- that's the tabletop/fire-drill review I-020 calls for with
the on-call team, not something pytest can stand in for.
"""

from __future__ import annotations

from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
RUNBOOKS_DIR = REPO_ROOT / "docs" / "runbooks"
ALERTS_PATH = REPO_ROOT / "deploy" / "prometheus" / "alerts.rules.yml"

REQUIRED_HEADINGS = ["## Symptom", "## Diagnosis", "## Mitigation", "## Escalation"]

RUNBOOK_FILES = [
    "queue-backlog.md",
    "shard-down.md",
    "redis-down.md",
    "db-bottleneck.md",
    "bot-attack.md",
]

# Which I-019 alert name(s) each runbook must call out by name, so an
# on-call engineer landing on the file from a page can confirm they're
# in the right place.
EXPECTED_ALERT_MENTIONS = {
    "queue-backlog.md": ["PollQueueDepthWarning", "PollQueueDepthCritical"],
    "redis-down.md": ["PollRedisHitRateWarning", "PollRedisHitRateCritical"],
    "db-bottleneck.md": ["PollErrorRateCritical", "PollP99LatencyCritical"],
    "bot-attack.md": [
        "PollDbConstraintViolationsWarning",
        "PollDbConstraintViolationsCritical",
    ],
    # shard-down.md has no dedicated I-019 alert (it surfaces as a
    # partial queue-depth rise plus worker logs, not a distinct metric)
    # -- see the runbook's own Symptom section.
}


def read(filename: str) -> str:
    return (RUNBOOKS_DIR / filename).read_text()


def test_runbooks_directory_exists():
    assert RUNBOOKS_DIR.is_dir()


def test_all_five_runbooks_exist():
    for filename in RUNBOOK_FILES:
        assert (RUNBOOKS_DIR / filename).is_file(), filename


def test_each_runbook_has_required_headings_in_order():
    for filename in RUNBOOK_FILES:
        text = read(filename)
        positions = [text.index(h) for h in REQUIRED_HEADINGS]
        assert positions == sorted(positions), f"{filename} headings out of order"


def test_each_runbook_names_its_i019_alerts():
    for filename, alert_names in EXPECTED_ALERT_MENTIONS.items():
        text = read(filename)
        for alert_name in alert_names:
            assert alert_name in text, f"{filename} missing mention of {alert_name}"


def test_shard_down_names_its_symptom_without_a_dedicated_alert():
    # No I-019 alert exists solely for shard failure -- it's diagnosed
    # from a combination of queue depth + worker logs. The runbook must
    # still point at the queue-depth alerts an on-call engineer would
    # actually be paged by.
    text = read("shard-down.md")
    assert "PollQueueDepthWarning" in text or "PollQueueDepthCritical" in text


def test_redis_down_distinguishes_read_and_write_paths():
    text = read("redis-down.md")
    assert "materialized" in text.lower()
    # write path must fail closed (503), not silently accept unsafe votes
    assert "503" in text
    assert "fail" in text.lower() and "closed" in text.lower()


def test_db_bottleneck_is_framed_as_capacity_planning_not_a_quick_fix():
    text = read("db-bottleneck.md").lower()
    assert "capacity" in text
    assert "not a" in text or "not a 5-minute" in text or "no single switch" in text


def test_bot_attack_frames_local_rate_limiting_as_a_delay_tactic():
    text = read("bot-attack.md").lower()
    assert "delay tactic" in text
    assert "not a solution" in text or "not sufficient" in text or "not enough" in text


def test_readme_indexes_all_five_runbooks():
    readme = (RUNBOOKS_DIR / "README.md").read_text()
    for filename in RUNBOOK_FILES:
        assert filename in readme, f"README.md missing link to {filename}"


def test_every_i019_alert_runbook_url_resolves_to_an_existing_file():
    doc = yaml.safe_load(ALERTS_PATH.read_text())
    for group in doc["groups"]:
        for rule in group["rules"]:
            runbook_url = rule["annotations"]["runbook_url"]
            resolved = REPO_ROOT / runbook_url
            assert resolved.is_file(), f"{rule['alert']} -> {runbook_url} (missing)"
