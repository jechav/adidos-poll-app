"""Integration tests for I-006's Layer 2 backstop: `insert_vote_or_log_duplicate`
exercised against a real PostgreSQL instance with I-001's migrations applied
(mirrors tests/integration/test_schema.py's fixtures and skip behavior).

These simulate the acceptance criteria's failure mode directly: two votes
for the same (user_id, poll_id) reach the insert path (as if Layer 1's
Redis key had been manually cleared or was never reserved). The DB's
UNIQUE(user_id, poll_id) constraint (I-001) must still allow exactly one
row, skip the second silently, and raise nothing to the caller.
"""

import logging
import os
import uuid

import psycopg
import pytest

from src.services.uniqueness import insert_vote_or_log_duplicate

DATABASE_URL = os.environ.get(
    "DATABASE_URL", "postgresql://postgres@localhost:5432/poll_app"
)


def _connect():
    return psycopg.connect(DATABASE_URL, connect_timeout=2)


@pytest.fixture
def conn():
    try:
        connection = _connect()
    except psycopg.OperationalError:
        pytest.skip(f"no PostgreSQL reachable at {DATABASE_URL}")
    yield connection
    connection.rollback()
    connection.close()


@pytest.fixture
def poll_and_answer(conn):
    poll_id = uuid.uuid4()
    answer_id = uuid.uuid4()
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO polls (poll_id, question, state) VALUES (%s, %s, 'active')",
            (poll_id, "test poll"),
        )
        cur.execute(
            'INSERT INTO answers (answer_id, poll_id, answer_text, "order") '
            "VALUES (%s, %s, %s, 0)",
            (answer_id, poll_id, "yes"),
        )
    conn.commit()
    yield poll_id, answer_id


def _make_insert_fn(conn):
    """Adapts insert_vote_or_log_duplicate's async insert_fn contract to a
    synchronous psycopg connection, matching I-008's eventual real insert
    path closely enough to exercise the actual constraint."""

    async def insert_fn(vote: dict) -> None:
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO votes (vote_id, user_id, poll_id, answer_id) "
                    "VALUES (%s, %s, %s, %s)",
                    (vote["vote_id"], vote["user_id"], vote["poll_id"], vote["answer_id"]),
                )
            conn.commit()
        except psycopg.errors.UniqueViolation:
            # A real insert path (I-008) would roll back to clear the
            # aborted-transaction state before continuing; mirrored here
            # so the connection stays usable for the assertions below.
            conn.rollback()
            raise

    return insert_fn


@pytest.mark.asyncio
async def test_duplicate_at_db_layer_yields_one_row_no_error(
    conn, poll_and_answer, caplog
):
    poll_id, answer_id = poll_and_answer
    user_id = f"user-{uuid.uuid4()}"
    insert_fn = _make_insert_fn(conn)

    vote_a = {
        "vote_id": uuid.uuid4(),
        "user_id": user_id,
        "poll_id": poll_id,
        "answer_id": answer_id,
    }
    vote_b = {
        "vote_id": uuid.uuid4(),
        "user_id": user_id,
        "poll_id": poll_id,
        "answer_id": answer_id,
    }

    first = await insert_vote_or_log_duplicate(
        insert_fn, vote_a, unique_violation=psycopg.errors.UniqueViolation
    )

    with caplog.at_level(logging.WARNING, logger="poll_app.uniqueness"):
        second = await insert_vote_or_log_duplicate(
            insert_fn, vote_b, unique_violation=psycopg.errors.UniqueViolation
        )

    assert first is True
    assert second is False
    assert any(
        record.message == "duplicate_vote_at_db_layer" for record in caplog.records
    )

    with conn.cursor() as cur:
        cur.execute(
            "SELECT count(*) FROM votes WHERE user_id = %s AND poll_id = %s",
            (user_id, poll_id),
        )
        (count,) = cur.fetchone()
    assert count == 1
