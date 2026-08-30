"""Pre-provisioned pool of fake load-test identities (I-023).

The load-test suite must never call real Adidos auth (it doesn't exist
from this service's perspective anyway -- I-004's `get_current_user`
never calls out to Adidos synchronously; see
`src/api/dependencies/auth.py`'s module docstring on trust-on-first-use).
Instead this pool hands out plain opaque strings that satisfy I-004's
`decode_adidos_token` convention directly: the token content *is* the
`user_id`, and an `admin:` prefix would grant the admin role -- which
load-test traffic must never do, since it exists to simulate voting end
users, not to exercise admin endpoints (I-023's "Out of Scope").

Sizing: `DEFAULT_POOL_SIZE` (10,000) is chosen so that, at the viral
single-poll scenario's target of 50K votes/sec, the pool cycles roughly
5 times per second. That's deliberate -- I-006/I-015 block a second vote
from the same (user_id, poll_id) pair, so re-using an identity against
the *same* poll_id inside I-015's 3-strike window would self-inflict
409s/blocks instead of exercising the vote-acceptance hot path. A larger
pool would need proportionally more setup time and memory for no benefit
the load test's assertions actually need; a caller running a bespoke
scenario that needs to avoid repeats entirely can pass a larger `size`.
"""

from __future__ import annotations

from itertools import cycle
from typing import Iterator

DEFAULT_POOL_SIZE = 10_000


class TokenPool:
    """A fixed, deterministic set of fake `loadtest-user-*` identities.

    `next_token()` cycles round-robin through the pool so a long-running
    (e.g. 10 minute) scenario reuses a bounded set of identities instead
    of growing memory for the run's duration.
    """

    def __init__(self, size: int = DEFAULT_POOL_SIZE) -> None:
        if size < 1:
            raise ValueError("TokenPool size must be at least 1")
        self.tokens: tuple[str, ...] = tuple(
            f"loadtest-user-{i:08d}" for i in range(size)
        )
        self._cycle: Iterator[str] = cycle(self.tokens)

    def next_token(self) -> str:
        return next(self._cycle)


_default_pool = TokenPool(DEFAULT_POOL_SIZE)


def get_test_token() -> str:
    """Return the next token from the shared default-size pool.

    This is what `loadtest/locustfile.py`'s `VoteUser` calls per request
    -- a module-level convenience over `TokenPool` so every simulated
    user in a Locust worker process shares one cycling pool rather than
    each spinning up its own.
    """
    return _default_pool.next_token()
