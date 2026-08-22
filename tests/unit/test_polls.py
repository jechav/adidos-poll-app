"""Unit tests for I-005's poll/answer validation logic.

`validate_vote_target` is the pure decision function extracted from the
`POST /v1/vote` handler: given already-loaded `Poll`/`Answer` objects (or
`None`, standing in for "not found"), decide whether the vote is allowed
and raise the right domain error otherwise. It never touches Postgres —
`load_poll_and_answer`'s actual query is exercised separately in
tests/integration (real DB required), mirroring how I-006 splits its
Redis-backed and DB-backed layers into separately testable pieces.
"""

import uuid

import pytest

from src.services.polls import (
    Answer,
    InvalidRequestError,
    Poll,
    PollClosedError,
    PollNotFoundError,
    validate_vote_target,
)


def test_active_poll_with_matching_answer_passes():
    poll_id = uuid.uuid4()
    answer_id = uuid.uuid4()
    poll = Poll(poll_id=poll_id, state="active")
    answer = Answer(answer_id=answer_id, poll_id=poll_id)

    validate_vote_target(poll, answer, poll_id, answer_id)


def test_missing_poll_raises_not_found():
    poll_id, answer_id = uuid.uuid4(), uuid.uuid4()
    answer = Answer(answer_id=answer_id, poll_id=poll_id)

    with pytest.raises(PollNotFoundError):
        validate_vote_target(None, answer, poll_id, answer_id)


def test_missing_answer_raises_not_found():
    poll_id, answer_id = uuid.uuid4(), uuid.uuid4()
    poll = Poll(poll_id=poll_id, state="active")

    with pytest.raises(PollNotFoundError):
        validate_vote_target(poll, None, poll_id, answer_id)


@pytest.mark.parametrize("state", ["draft", "closed", "archived"])
def test_non_active_poll_raises_closed(state):
    poll_id, answer_id = uuid.uuid4(), uuid.uuid4()
    poll = Poll(poll_id=poll_id, state=state)
    answer = Answer(answer_id=answer_id, poll_id=poll_id)

    with pytest.raises(PollClosedError):
        validate_vote_target(poll, answer, poll_id, answer_id)


def test_answer_belonging_to_different_poll_raises_invalid_request():
    poll_id = uuid.uuid4()
    other_poll_id = uuid.uuid4()
    answer_id = uuid.uuid4()
    poll = Poll(poll_id=poll_id, state="active")
    answer = Answer(answer_id=answer_id, poll_id=other_poll_id)

    with pytest.raises(InvalidRequestError):
        validate_vote_target(poll, answer, poll_id, answer_id)
