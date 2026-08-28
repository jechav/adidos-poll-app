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
from uuid import UUID, uuid4

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


# --- Admin poll management (I-013) -----------------------------------
#
# `AdminAnswer`/`AdminPoll` are separate dataclasses from `Answer`/`Poll`
# above: those two carry only the fields I-005's vote-acceptance path
# needs (poll_id/state, answer_id/poll_id); admin management needs the
# full row (question, per-*_at timestamps, answer text/order), so
# reusing the narrower shapes would mean widening them for a use case
# they were never meant to serve.

# draft -> active -> closed -> archived, strictly one-way, one step at a
# time (DOMAIN_MODEL.md's `PollState` value object).
POLL_STATE_ORDER = {"draft": 0, "active": 1, "closed": 2, "archived": 3}

_TIMESTAMP_COLUMN_BY_STATE = {
    "active": "activated_at",
    "closed": "closed_at",
    "archived": "archived_at",
}


@dataclass(frozen=True)
class AdminAnswer:
    answer_id: UUID
    text: str
    order: int


@dataclass(frozen=True)
class AdminPoll:
    poll_id: UUID
    question: str
    state: str
    answers: list[AdminAnswer]
    created_at: object
    activated_at: object | None
    closed_at: object | None
    archived_at: object | None


def validate_transition(current: str, requested: str) -> None:
    """Raise `InvalidRequestError` unless `requested` is exactly one step
    forward of `current` in `POLL_STATE_ORDER`. Same-state requests are
    handled by the caller as an idempotent no-op *before* this is called
    — this function only validates an actual transition attempt.
    """
    if POLL_STATE_ORDER[requested] != POLL_STATE_ORDER[current] + 1:
        raise InvalidRequestError(
            f"Cannot transition poll from '{current}' to '{requested}'; "
            "states move forward one step at a time "
            "(draft -> active -> closed -> archived)"
        )


async def create_admin_poll(question: str, answer_texts: list[str]) -> AdminPoll:
    """Create a poll + its answers, always landing in `draft` state
    (I-001's schema default) — activation is a separate, explicit
    transition (`transition_poll`).
    """
    poll_id = uuid4()
    answers = [
        AdminAnswer(answer_id=uuid4(), text=text, order=i)
        for i, text in enumerate(answer_texts)
    ]

    async with _conn_lock:
        conn = await _get_connection()
        try:
            async with conn.cursor() as cur:
                await cur.execute(
                    "INSERT INTO polls (poll_id, question, state) "
                    "VALUES (%s, %s, 'draft') RETURNING created_at",
                    (str(poll_id), question),
                )
                (created_at,) = await cur.fetchone()
                for answer in answers:
                    await cur.execute(
                        'INSERT INTO answers (answer_id, poll_id, answer_text, "order") '
                        "VALUES (%s, %s, %s, %s)",
                        (str(answer.answer_id), str(poll_id), answer.text, answer.order),
                    )
            await conn.commit()
        except Exception:
            await conn.rollback()
            raise

    return AdminPoll(
        poll_id=poll_id,
        question=question,
        state="draft",
        answers=answers,
        created_at=created_at,
        activated_at=None,
        closed_at=None,
        archived_at=None,
    )


async def get_admin_poll(poll_id: UUID) -> AdminPoll | None:
    """Full poll row (state + all lifecycle timestamps) plus its answers,
    for `PUT /v1/admin/polls/{poll_id}/state`'s existence/idempotency
    checks. Returns `None` if `poll_id` doesn't exist.
    """
    async with _conn_lock:
        conn = await _get_connection()
        async with conn.cursor() as cur:
            await cur.execute(
                "SELECT poll_id, question, state, created_at, activated_at, "
                "closed_at, archived_at FROM polls WHERE poll_id = %s",
                (str(poll_id),),
            )
            poll_row = await cur.fetchone()
            if poll_row is None:
                return None
            await cur.execute(
                'SELECT answer_id, answer_text, "order" FROM answers '
                'WHERE poll_id = %s ORDER BY "order"',
                (str(poll_id),),
            )
            answer_rows = await cur.fetchall()

    return AdminPoll(
        poll_id=poll_row[0],
        question=poll_row[1],
        state=poll_row[2],
        answers=[
            AdminAnswer(answer_id=row[0], text=row[1], order=row[2])
            for row in answer_rows
        ],
        created_at=poll_row[3],
        activated_at=poll_row[4],
        closed_at=poll_row[5],
        archived_at=poll_row[6],
    )


async def transition_poll(poll_id: UUID, new_state: str) -> AdminPoll:
    """Write `new_state` and stamp the matching `*_at` column. Assumes the
    caller (the route handler) has already validated the transition via
    `validate_transition` — this function performs the write
    unconditionally.
    """
    timestamp_column = _TIMESTAMP_COLUMN_BY_STATE[new_state]
    async with _conn_lock:
        conn = await _get_connection()
        try:
            async with conn.cursor() as cur:
                await cur.execute(
                    f"UPDATE polls SET state = %s, {timestamp_column} = now() "
                    "WHERE poll_id = %s",
                    (new_state, str(poll_id)),
                )
            await conn.commit()
        except Exception:
            await conn.rollback()
            raise

    updated = await get_admin_poll(poll_id)
    assert updated is not None  # just updated it; must still exist
    return updated
