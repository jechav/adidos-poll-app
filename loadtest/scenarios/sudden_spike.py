"""I-023 scenario: sudden spike.

Load profile: 0 -> 10K votes/sec within 1 second, then hold for 60s
(`loadtest/shapes/spike_shape.py::SpikeShape`).
Pass criteria: queue auto-scales (observed via I-017 dashboards during
the run), latency < 500ms during the spike.

Distributed run (a single process cannot generate 10K req/sec -- see
the ticket's Distributed Execution section and k8s/loadtest/ manifests):

    # master
    locust -f loadtest/scenarios/sudden_spike.py --master --headless \\
        --host https://staging.example.internal \\
        --csv=reports/sudden_spike
    # workers (N pods, sized per docs/setup/testing.md)
    locust -f loadtest/scenarios/sudden_spike.py --worker \\
        --master-host=locust-master

`SpikeShape` (imported below) controls user count/spawn-rate and when
the run stops; `--run-time` is not needed alongside a `LoadTestShape`.
"""

from __future__ import annotations

from loadtest.locustfile import PooledVoteUser
from loadtest.shapes.spike_shape import SpikeShape  # noqa: F401 (registers the shape)

MAX_LATENCY_DURING_SPIKE_MS = 500


class SuddenSpikeUser(PooledVoteUser):
    pass
