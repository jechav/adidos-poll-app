"""I-023 scenario: viral single poll.

Load profile: 50K votes/sec, all directed at one `poll_id` (deliberately
*not* spread across many polls -- spec user story #7 -- so it actually
stresses shard-hot-spot and single-poll cache-contention behavior rather
than being diluted across the dataset).
Pass criteria: final recorded vote count matches votes accepted (no data
loss); results remain accurate.

Requires `POLL_APP_LOADTEST_TARGET_POLL_ID` and
`POLL_APP_LOADTEST_TARGET_ANSWER_ID` (a single pair, unlike the
`POLL_APP_LOADTEST_POLL_IDS` pool the steady-state scenarios use) --
see docs/setup/testing.md.

Distributed run (50K req/sec needs a worker fleet -- see
k8s/loadtest/ and the ticket's Distributed Execution section):

    locust -f loadtest/scenarios/viral_single_poll.py --master --headless \\
        --users 50000 --spawn-rate 5000 --run-time 90s \\
        --host https://staging.example.internal \\
        --csv=reports/viral_single_poll

After the run, feed the run's accepted-request count and the
environment's authoritative post-run vote count for this poll into
`loadtest/utils/report.py::evaluate_pass_fail("viral_single_poll", ...)`
to check for data loss.
"""

from __future__ import annotations

import os

from loadtest.locustfile import VoteUser


def _required_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(
            f"{name} is not set -- viral_single_poll targets exactly one "
            "seeded poll (see docs/setup/testing.md)"
        )
    return value


class ViralSinglePollUser(VoteUser):
    def on_start(self):
        self.poll_id = _required_env("POLL_APP_LOADTEST_TARGET_POLL_ID")
        self.answer_id = _required_env("POLL_APP_LOADTEST_TARGET_ANSWER_ID")
