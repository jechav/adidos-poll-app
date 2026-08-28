"""Unit tests for I-013's `validate_transition()`: pure decision logic,
no I/O. `validate_transition` is only ever called by the route *after*
the same-state (idempotent no-op) case has already been handled — see
its docstring — so this suite covers every remaining (current,
requested) pair: 4 possible current states x 3 requestable states
(`active`/`closed`/`archived`, the only values `UpdatePollStateRequest`
accepts) minus the 3 same-state pairs = 9 genuine transition attempts,
of which exactly 3 are one step forward and must succeed, and the other
6 (backward or a forward skip) must raise.
"""

import pytest

from src.services.polls import POLL_STATE_ORDER, InvalidRequestError, validate_transition

REQUESTABLE_STATES = ("active", "closed", "archived")
ALL_STATES = ("draft", "active", "closed", "archived")

TRANSITION_ATTEMPTS = [
    (current, requested)
    for current in ALL_STATES
    for requested in REQUESTABLE_STATES
    if current != requested
]


@pytest.mark.parametrize(
    "current,requested",
    [
        ("draft", "active"),
        ("active", "closed"),
        ("closed", "archived"),
    ],
)
def test_one_step_forward_transitions_succeed(current, requested):
    validate_transition(current, requested)  # must not raise


@pytest.mark.parametrize(
    "current,requested",
    [
        pair
        for pair in TRANSITION_ATTEMPTS
        if POLL_STATE_ORDER[pair[1]] != POLL_STATE_ORDER[pair[0]] + 1
    ],
)
def test_backward_and_skip_transitions_are_rejected(current, requested):
    with pytest.raises(InvalidRequestError):
        validate_transition(current, requested)


def test_transition_attempts_partition_into_3_forward_and_6_rejected():
    forward = sum(
        1
        for c, r in TRANSITION_ATTEMPTS
        if POLL_STATE_ORDER[r] == POLL_STATE_ORDER[c] + 1
    )
    rejected = len(TRANSITION_ATTEMPTS) - forward
    assert forward == 3
    assert rejected == 6
