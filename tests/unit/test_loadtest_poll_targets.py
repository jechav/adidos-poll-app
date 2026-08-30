"""I-023: parsing the seeded poll/answer target pool for steady-state
scenarios (`loadtest/utils/poll_targets.py`).

Real poll/answer UUIDs only exist once a target environment has been
seeded (the staging environment I-023's own "Target Environment" section
calls for); this module can't know them ahead of time, so it takes them
from an env var and only owns the parsing/round-robin-selection logic,
which is exactly the seam worth a real unit test -- same shape as
`loadtest/utils/tokens.py`'s `TokenPool`.
"""

from __future__ import annotations

import pytest

from loadtest.utils.poll_targets import PollTargetPool, parse_poll_targets, shared_pool


def test_parse_poll_targets_splits_pairs():
    targets = parse_poll_targets("poll-1:answer-1,poll-2:answer-2")
    assert targets == [("poll-1", "answer-1"), ("poll-2", "answer-2")]


def test_parse_poll_targets_strips_whitespace():
    targets = parse_poll_targets(" poll-1 : answer-1 , poll-2:answer-2 ")
    assert targets == [("poll-1", "answer-1"), ("poll-2", "answer-2")]


def test_parse_poll_targets_rejects_empty_string():
    with pytest.raises(ValueError):
        parse_poll_targets("")


def test_parse_poll_targets_rejects_malformed_pair():
    with pytest.raises(ValueError):
        parse_poll_targets("poll-1-missing-answer")


def test_pool_cycles_round_robin_across_targets():
    pool = PollTargetPool([("poll-1", "answer-1"), ("poll-2", "answer-2")])
    assert pool.next_target() == ("poll-1", "answer-1")
    assert pool.next_target() == ("poll-2", "answer-2")
    assert pool.next_target() == ("poll-1", "answer-1")


def test_pool_from_env_parses_and_wraps(monkeypatch):
    monkeypatch.setenv("POLL_APP_LOADTEST_POLL_IDS", "poll-1:answer-1")
    pool = PollTargetPool.from_env()
    assert pool.next_target() == ("poll-1", "answer-1")


def test_pool_from_env_raises_with_actionable_message_when_unset(monkeypatch):
    monkeypatch.delenv("POLL_APP_LOADTEST_POLL_IDS", raising=False)
    with pytest.raises(RuntimeError, match="POLL_APP_LOADTEST_POLL_IDS"):
        PollTargetPool.from_env()


def test_shared_pool_memoizes_a_single_instance_per_process(monkeypatch):
    monkeypatch.setenv("POLL_APP_LOADTEST_POLL_IDS", "poll-1:answer-1")
    shared_pool.cache_clear()
    first = shared_pool()
    second = shared_pool()
    assert first is second
    shared_pool.cache_clear()
