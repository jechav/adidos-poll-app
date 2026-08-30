"""Custom load shape for the "sustained 5x peak" scenario (I-023 scenario
table / Deployment Checklist item 8: "100K votes/sec, sustain for 10 min").

Ramps to 100K over a 60s staircase (four 15s steps) rather than an
instant jump like `SpikeShape` -- unlike the sudden-spike scenario,
which is explicitly testing an unannounced burst, this scenario is
validating the API autoscaler reaching its full 5 replicas (Acceptance
Criteria), and an autoscaler needs a few scrape/decision cycles to react;
an instant 0->100K jump would conflate "did the autoscaler keep up with
a gradual ramp" with "did the system survive an impossible instant
step", which isn't what this scenario is for (that's `SpikeShape`'s job).
Holds at 100K for the full 10 minute (600s) window the Deployment
Checklist calls for, then stops.
"""

from __future__ import annotations

from locust import LoadTestShape

PEAK_USERS = 100_000
RAMP_SECONDS = 60
RAMP_STEPS = 4
HOLD_SECONDS = 600


class Sustained5xShape(LoadTestShape):
    """Staircase ramp to 100K users over 60s, then hold for 10 minutes."""

    def tick(self):
        run_time = self.get_run_time()
        total = RAMP_SECONDS + HOLD_SECONDS
        if run_time >= total:
            return None

        if run_time < RAMP_SECONDS:
            step_seconds = RAMP_SECONDS / RAMP_STEPS
            step = int(run_time // step_seconds) + 1  # 1-indexed step
            users = (PEAK_USERS * step) // RAMP_STEPS
            spawn_rate = PEAK_USERS // RAMP_STEPS // step_seconds \
                if step_seconds else PEAK_USERS
            return (users, max(1, int(spawn_rate)))

        # Hold at peak; spawn_rate only backfills users that died mid-run.
        return (PEAK_USERS, PEAK_USERS // 10)
