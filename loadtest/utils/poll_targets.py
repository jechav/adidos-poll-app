"""Poll/answer target pool for steady-state scenarios (I-023).

Real `poll_id`/`answer_id` UUIDs only exist once a target environment has
been seeded with active polls -- there is no way for this module to know
them ahead of time, and hardcoding fixtures here would silently drift
from whatever staging/prod-topology environment a given run targets. So
`sustained_peak.py` and `sudden_spike.py` (which spread load across a
realistic pool of active polls, unlike `viral_single_poll.py`'s
deliberately single-target flood) read the pool from the
`POLL_APP_LOADTEST_POLL_IDS` env var: a comma-separated list of
`poll_id:answer_id` pairs, seeded by whoever provisions the run (see
docs/setup/testing.md).
"""

from __future__ import annotations

import os
from functools import lru_cache
from itertools import cycle

ENV_VAR = "POLL_APP_LOADTEST_POLL_IDS"


def parse_poll_targets(value: str) -> list[tuple[str, str]]:
    """Parse `"poll:answer,poll:answer,..."` into a list of pairs.

    Raises `ValueError` on an empty string or any pair missing its
    `poll_id:answer_id` separator, so a misconfigured run fails at
    startup rather than silently voting for nothing.
    """
    value = value.strip()
    if not value:
        raise ValueError(
            f"{ENV_VAR} must be a non-empty comma-separated list of "
            "poll_id:answer_id pairs"
        )
    targets = []
    for raw_pair in value.split(","):
        parts = raw_pair.split(":")
        if len(parts) != 2:
            raise ValueError(
                f"malformed poll target {raw_pair!r} in {ENV_VAR} "
                "(expected 'poll_id:answer_id')"
            )
        poll_id, answer_id = (p.strip() for p in parts)
        targets.append((poll_id, answer_id))
    return targets


class PollTargetPool:
    """Round-robin pool of `(poll_id, answer_id)` targets for one scenario run."""

    def __init__(self, targets: list[tuple[str, str]]) -> None:
        if not targets:
            raise ValueError("PollTargetPool needs at least one target")
        self.targets = list(targets)
        self._cycle = cycle(self.targets)

    @classmethod
    def from_env(cls) -> "PollTargetPool":
        value = os.environ.get(ENV_VAR)
        if not value:
            raise RuntimeError(
                f"{ENV_VAR} is not set -- seed the target environment with "
                "active polls first and export their poll_id:answer_id "
                "pairs (see docs/setup/testing.md)"
            )
        return cls(parse_poll_targets(value))

    def next_target(self) -> tuple[str, str]:
        return next(self._cycle)


@lru_cache(maxsize=1)
def shared_pool() -> "PollTargetPool":
    """One `PollTargetPool.from_env()` per worker process, memoized.

    Every simulated `HttpUser`'s `on_start` calls this instead of
    constructing its own pool -- a fresh `PollTargetPool` per user would
    each restart its own round-robin at index 0, piling all traffic onto
    the pool's first target instead of spreading it across the seeded
    polls. `lru_cache` gives every user in the same process the same
    pool instance, and the round-robin cursor is what actually
    distributes the load.
    """
    return PollTargetPool.from_env()
