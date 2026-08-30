"""Custom load shape for the "sudden spike" scenario (I-023 scenario table).

Target profile: 0 -> 10K votes/sec within 1 second, then hold for 60s.
`user_count` here approximates req/sec under `VoteUser`'s zero wait_time
(see `loadtest/locustfile.py`) -- the actual req/sec a given user_count
produces depends on the worker fleet's throughput and must be confirmed
empirically per the ticket's Distributed Execution section; this class
only encodes the *shape* of the ramp, not a guarantee of the resulting
rate.
"""

from __future__ import annotations

from locust import LoadTestShape

PEAK_USERS = 10_000
RAMP_SECONDS = 1
HOLD_SECONDS = 60


class SpikeShape(LoadTestShape):
    """0 -> 10K users within 1 second, then hold through a 60s window."""

    def tick(self):
        run_time = self.get_run_time()
        if run_time >= RAMP_SECONDS + HOLD_SECONDS:
            return None
        if run_time < RAMP_SECONDS:
            # Ramp near-instantly: spawn_rate == user_count so Locust
            # reaches the full 10K within the 1s window rather than
            # trickling up at a fixed users/sec rate.
            return (PEAK_USERS, PEAK_USERS)
        # Hold at 10K; spawn_rate here only matters for filling in any
        # users that died/reconnected during the hold, not for further
        # ramping.
        return (PEAK_USERS, 1_000)
