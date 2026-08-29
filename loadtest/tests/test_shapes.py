"""I-023: custom Locust `LoadTestShape` classes (`loadtest/shapes/`).

Seam choice: `tick()` is a pure function of elapsed run time to a
`(user_count, spawn_rate)` tuple (or `None` to stop) -- Locust itself
never has to be running to test it. We monkeypatch `get_run_time` (the
one method Locust's base class supplies at runtime) rather than sleeping
in real time, so these tests are instant and deterministic.

This exercises exactly the load profile the ticket's Acceptance Criteria
and scenario table specify:

- `SpikeShape`: 0 -> 10K within 1s, hold through the 60s window, then stop
- `Sustained5xShape`: ramp to 100K over the ramp window, hold for the
  full 10 minute (600s) sustain window per the Deployment Checklist's
  "100K votes/sec, sustain for 10 min", then stop

Why this file lives in `loadtest/tests/` and not `tests/unit/`: importing
`locust` runs `gevent.monkey.patch_all()` as an import-time side effect,
which globally patches `socket`/`ssl`/threading for the rest of the
Python process. That's fine in isolation, but it corrupts the app's own
asyncio-based tests (FastAPI's async routes, the async Redis client) if
both run in the same pytest process -- confirmed empirically while
building this suite: adding a locust-importing test file under
`tests/unit/` made unrelated asyncio tests hang/fail. `pytest.ini`'s
`testpaths = tests` already keeps this directory out of the default
`pytest` run; see `docs/setup/testing.md` for the separate command that
runs it.

What this can't verify (and doesn't try to): that a given worker fleet
actually converts a given `user_count` into that many requests/sec
against a real deployment -- that's the empirical, per-environment
worker-sizing step the ticket itself calls out as "validated
empirically" under Distributed Execution.
"""

from __future__ import annotations

from loadtest.shapes.spike_shape import SpikeShape
from loadtest.shapes.sustained_5x_shape import Sustained5xShape


def _tick_at(shape, run_time: float):
    shape.get_run_time = lambda: run_time  # type: ignore[method-assign]
    return shape.tick()


def test_spike_shape_ramps_to_10k_within_first_second():
    shape = SpikeShape()
    assert _tick_at(shape, 0.0) == (10_000, 10_000)
    assert _tick_at(shape, 0.9) == (10_000, 10_000)


def test_spike_shape_holds_10k_users_after_the_initial_ramp():
    shape = SpikeShape()
    assert _tick_at(shape, 1.0) == (10_000, 1_000)
    assert _tick_at(shape, 60.9) == (10_000, 1_000)


def test_spike_shape_stops_after_the_60s_hold_window():
    shape = SpikeShape()
    assert _tick_at(shape, 61.0) is None


def test_sustained_5x_shape_ramps_up_in_stages_toward_100k():
    shape = Sustained5xShape()
    # Ramp window is 60s; user_count should be monotonically
    # non-decreasing across it and reach the 100K target by the end.
    samples = [_tick_at(shape, t) for t in (0.0, 15.0, 30.0, 45.0, 59.9)]
    user_counts = [s[0] for s in samples]
    assert user_counts == sorted(user_counts)
    assert all(0 < uc <= 100_000 for uc in user_counts)


def test_sustained_5x_shape_holds_100k_for_the_full_10_minute_window():
    shape = Sustained5xShape()
    assert _tick_at(shape, 60.0) == (100_000, 100_000 // 10)
    assert _tick_at(shape, 659.9) == (100_000, 100_000 // 10)


def test_sustained_5x_shape_stops_after_ramp_plus_10_minute_hold():
    shape = Sustained5xShape()
    # 60s ramp + 600s hold = 660s total.
    assert _tick_at(shape, 660.0) is None
