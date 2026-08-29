"""I-023: `VoteUser` must never be auto-spawned as its own runnable user.

Lives in `loadtest/tests/` rather than `tests/unit/`, like
`test_shapes.py`, because it imports real `locust` -- see that file's
docstring for why that can't share a process with the app's own
asyncio-based tests.

Regression coverage for a real bug found while building this suite:
Locust auto-discovers every concrete `HttpUser` subclass present in a
locustfile module's namespace, including ones only *imported* for reuse.
Before `VoteUser.abstract = True` was added, every scenario module (e.g.
`sustained_peak.py`, which does `from loadtest.locustfile import
VoteUser`) caused Locust to spawn both the concrete scenario's user
class *and* the bare `VoteUser` base itself -- which has no `on_start`
and would vote with `poll_id=None`/`answer_id=None`, corrupting a run's
results and request counts. Confirmed with a live headless run against
a throwaway local HTTP server before and after the fix.
"""

from __future__ import annotations

from loadtest.locustfile import VoteUser
from loadtest.scenarios.sustained_peak import SustainedPeakUser


def test_vote_user_is_abstract():
    assert VoteUser.abstract is True


def test_concrete_scenario_user_is_not_abstract():
    # Locust's UserMeta only auto-clears `abstract` on classes that
    # don't set it themselves in their own class body -- this asserts
    # that behavior actually holds for a real scenario subclass, not
    # just that VoteUser declares the flag.
    assert SustainedPeakUser.abstract is False
