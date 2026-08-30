"""Base Locust user for I-023's vote-pipeline load tests.

Every scenario under `loadtest/scenarios/` imports `VoteUser` from here
rather than redefining the request-building logic, so a change to the
vote request shape (headers, body, endpoint) only needs to happen once.
`wait_time` is `between(0, 0)`: pacing is controlled entirely by each
scenario's user count and (for the spike/5x-peak scenarios) its
`LoadTestShape` in `loadtest/shapes/`, not by a per-user think-time --
matching the ticket's own base-user sketch.

Poll/answer targets: `VoteUser.poll_id`/`answer_id` default to `None` and
must be set by the scenario (either as class attributes on a subclass,
per `viral_single_poll.py`, or per-instance in `on_start`, per the
steady-state scenarios that spread load across a poll/answer pool) --
`cast_vote` intentionally has no fallback default, so a scenario that
forgets to set a target fails fast and loudly (an `AttributeError` on
the first request) rather than silently hammering `poll_id=None` and
producing a confusing 400 flood.
"""

from __future__ import annotations

from locust import HttpUser, task, between

from loadtest.utils.poll_targets import shared_pool
from loadtest.utils.tokens import get_test_token


class VoteUser(HttpUser):
    # `abstract = True` keeps Locust's auto-discovery from spawning this
    # base class directly (it has no `on_start` and would vote with
    # poll_id=None/answer_id=None) whenever a scenario module imports it
    # alongside its own concrete subclass -- confirmed via Locust's
    # `UserMeta`, which only auto-clears `abstract` on classes that
    # don't set it themselves, so every subclass below is picked up
    # normally without repeating this flag.
    abstract = True

    wait_time = between(0, 0)

    poll_id: str | None = None
    answer_id: str | None = None

    @task
    def cast_vote(self):
        token = get_test_token()
        self.client.post(
            "/v1/vote",
            json={"poll_id": self.poll_id, "answer_id": self.answer_id},
            headers={"Authorization": f"Bearer {token}"},
            name="/v1/vote",
        )


class PooledVoteUser(VoteUser):
    """`VoteUser` that draws its target from `poll_targets.shared_pool()`.

    All three steady-state scenarios (`sustained_peak`, `sudden_spike`,
    `sustained_5x_peak`) spread load across the same seeded poll/answer
    pool identically -- only `viral_single_poll` differs (a single fixed
    target, per user story #7), so it defines its own `on_start` instead
    of using this class.
    """

    abstract = True

    def on_start(self):
        self.poll_id, self.answer_id = shared_pool().next_target()
