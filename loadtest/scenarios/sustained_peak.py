"""I-023 scenario: sustained normal peak.

Load profile: 1000 votes/sec, sustained for 60s.
Pass criteria: zero 503s, P99 < 200ms.

Run standalone (single process, sufficient at this rate -- see
docs/setup/testing.md):

    locust -f loadtest/scenarios/sustained_peak.py --headless \\
        --users 1000 --spawn-rate 1000 --run-time 60s \\
        --host https://staging.example.internal \\
        --csv=reports/sustained_peak

`--users 1000` with `VoteUser`'s zero wait_time approximates 1000
votes/sec; confirm the actual achieved rate from the run's own CSV/stats
output (`Requests/s`), per the ticket's Distributed Execution note that
the users->req/sec conversion is environment-dependent.
"""

from __future__ import annotations

from loadtest.locustfile import PooledVoteUser

TARGET_VOTES_PER_SEC = 1_000
DURATION_SECONDS = 60
MAX_P99_MS = 200
MAX_503_COUNT = 0


class SustainedPeakUser(PooledVoteUser):
    pass
