"""I-023: Locust CLI command construction for on-demand/scheduled runs
(`scripts/loadtest/run_scenario.py`).

Seam choice: `build_locust_command` turns a scenario name + a target host
into the argv list that would be passed to `subprocess.run`, without
actually invoking Locust or a network -- that invocation (`main()`) is
what actually needs a live target environment and is exercised manually
per docs/setup/testing.md, not by pytest. This mirrors `report.py`'s
split between pure evaluation logic (tested here) and I/O (not).
"""

from __future__ import annotations

import pytest

from scripts.loadtest.run_scenario import SCENARIOS, build_locust_command


@pytest.mark.parametrize("scenario", sorted(SCENARIOS))
def test_every_known_scenario_builds_a_command_targeting_its_own_locustfile(
    scenario,
):
    cmd = build_locust_command(scenario, host="https://staging.example", csv_prefix="reports/x")
    assert cmd[0] == "locust"
    assert "-f" in cmd
    locustfile = cmd[cmd.index("-f") + 1]
    assert locustfile == SCENARIOS[scenario]["locustfile"]
    assert "--headless" in cmd
    assert "--host" in cmd
    assert cmd[cmd.index("--host") + 1] == "https://staging.example"
    assert "--csv" in cmd
    assert cmd[cmd.index("--csv") + 1] == "reports/x"


def test_sustained_peak_passes_explicit_users_and_run_time():
    cmd = build_locust_command("sustained_peak", host="h", csv_prefix="p")
    assert "--users" in cmd
    assert cmd[cmd.index("--users") + 1] == "1000"
    assert "--run-time" in cmd
    assert cmd[cmd.index("--run-time") + 1] == "60s"


def test_shape_driven_scenarios_omit_users_and_run_time():
    # SpikeShape/Sustained5xShape own the ramp/hold/stop logic -- passing
    # --users/--run-time alongside a LoadTestShape is contradictory
    # (Locust logs a warning and the shape wins), so the command must
    # not include them for shape-driven scenarios.
    for scenario in ("sudden_spike", "sustained_5x_peak"):
        cmd = build_locust_command(scenario, host="h", csv_prefix="p")
        assert "--users" not in cmd
        assert "--run-time" not in cmd


def test_unknown_scenario_raises_value_error():
    with pytest.raises(ValueError):
        build_locust_command("not_a_real_scenario", host="h", csv_prefix="p")
