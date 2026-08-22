"""Poll/answer lookups and vote-target validation (I-005).

Polls and answers are replicated identically to every shard (I-001), so a
lookup is served from a single shared connection — no shard routing needed
here, unlike votes themselves (I-008, routed by `user_id`).

`validate_vote_target` is kept separate from `load_poll_and_answer` on
purpose: it's pure decision logic (no I/O), so it's unit-testable with
plain `Poll`/`Answer` values, while the DB query is exercised against a
real Postgres instance in tests/integration — mirroring how I-006 splits
its Redis-backed and DB-backed layers.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from uuid import UUID

import psycopg

from src.config import settings

_conn: psycopg.AsyncConnection | None = None
_conn_lock = asyncio.Lock()


@dataclass(frozen=True)
class Poll:
    poll_id: UUID
    state: str


@dataclass(frozen=True)
class Answer:
    answer_id: UUID
    poll_id: UUID


class PollNotFoundError(Exception):
    """Raised when `poll_id` or `answer_id` doesn't resolve to a row.
    Callers (I-005's route) should translate this into `404 NOT_FOUND`.
    """

    def __init__(self, poll_id: UUID, answer_id: UUID):
        self.poll_id = poll_id
        self.answer_id = answer_id
        super().__init__(f"poll {poll_id!r} or answer {answer_id!r} not found")


class PollClosedError(Exception):
    """Raised when the poll exists but isn't in `active` state. Callers
    should translate this into `400 POLL_CLOSED`.
    """

    def __init__(self, poll_id: UUID, state: str):
        self.poll_id = poll_id
        self.state = state
        super().__init__(f"poll {poll_id!r} is not accepting votes (state={state!r})")


class InvalidRequestError(Exception):
    """Raised for a request-shape problem that Pydantic's schema
    validation can't catch on its own — e.g. an `answer_id` that doesn't
    belong to the given `poll_id`. Callers should translate this into
    `400 INVALID_REQUEST`.
    """


def validate_vote_target(
    poll: Poll | None, answer: Answer | None, poll_id: UUID, answer_id: UUID
) -> None:
    """Decide whether a vote against (poll_id, answer_id) may proceed.

    Raises `PollNotFoundError`, `PollClosedError`, or `InvalidRequestError`;
    returns normally if the vote target is valid. Order matches I-005's
    spec: not-found is checked before state, state before the
    answer/poll relationship, so the most fundamental problem is reported
    first.
    """
    if poll is None or answer is None:
        raise PollNotFoundError(poll_id, answer_id)
    if poll.state != "active":
        raise PollClosedError(poll.poll_id, poll.state)
    if answer.poll_id != poll.poll_id:
        raise InvalidRequestError("answer_id does not belong to poll_id")


async def load_poll_and_answer(
    poll_id: UUID, answer_id: UUID
) -> tuple[Poll | None, Answer | None]:
    """Look up a poll and an answer by id. Either may come back `None` if
    not found; a mismatched `poll.state`/`answer.poll_id` is left to
    `validate_vote_target` to reject, not this function's job to decide.

    A single shared connection is reused across calls (like I-002's Redis
    client), guarded by a lock: `psycopg.AsyncConnection` isn't safe for
    concurrent queries from multiple coroutines, and this endpoint sits on
    every vote request.
    """
    async with _conn_lock:
        conn = await _get_connection()
        async with conn.cursor() as cur:
            await cur.execute(
                "SELECT poll_id, state FROM polls WHERE poll_id = %s", (poll_id,)
            )
            poll_row = await cur.fetchone()
            await cur.execute(
                "SELECT answer_id, poll_id FROM answers WHERE answer_id = %s",
                (answer_id,),
            )
            answer_row = await cur.fetchone()

    poll = Poll(poll_id=poll_row[0], state=poll_row[1]) if poll_row else None
    answer = Answer(answer_id=answer_row[0], poll_id=answer_row[1]) if answer_row else None
    return poll, answer


async def _get_connection() -> psycopg.AsyncConnection:
    """Assumes the caller already holds `_conn_lock`."""
    global _conn
    if _conn is None or _conn.closed:
        _conn = await psycopg.AsyncConnection.connect(
            settings.database_url, connect_timeout=2
        )
    return _conn
