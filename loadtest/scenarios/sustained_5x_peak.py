"""I-023 scenario: sustained 5x peak.

Load profile: 100K votes/sec (5x normal peak), sustained for 10 minutes
per the Deployment Checklist's item 8
(`loadtest/shapes/sustained_5x_shape.py::Sustained5xShape`).
Pass criteria: API autoscaler reaches 5 replicas, queue depth stabilizes
(does not grow unbounded -- see
`loadtest/utils/report.py::QUEUE_DEPTH_CRITICAL_THRESHOLD`).

This is the scenario the ticket's own Problem Statement calls out by
name ("the single highest-named risk in PROJECT_STATUS.md's risk
register") -- it requires the largest worker fleet of the four
scenarios; see k8s/loadtest/ and docs/setup/testing.md for sizing.

    # master
    locust -f loadtest/scenarios/sustained_5x_peak.py --master --headless \\
        --host https://staging.example.internal \\
        --csv=reports/sustained_5x_peak
    # workers (N pods -- start at 1 per ~1K req/sec of target load)
    locust -f loadtest/scenarios/sustained_5x_peak.py --worker \\
        --master-host=locust-master

`Sustained5xShape` controls user count/spawn-rate and stop time; no
`--run-time`/`--users` flags are needed alongside a `LoadTestShape`.
"""

from __future__ import annotations

from loadtest.locustfile import PooledVoteUser
from loadtest.shapes.sustained_5x_shape import Sustained5xShape  # noqa: F401

TARGET_REPLICA_COUNT = 5


class Sustained5xPeakUser(PooledVoteUser):
    pass
