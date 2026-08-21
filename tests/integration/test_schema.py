"""Integration tests for I-001's schema, run against a real PostgreSQL
instance with the migrations applied (see docs/setup/sharding.md).

Skips automatically if DATABASE_URL isn't reachable, so `pytest tests/`
still passes in environments without Postgres up.
"""

import os
import uuid

import psycopg
import pytest

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


def test_duplicate_vote_rejected_by_unique_constraint(conn, poll_and_answer):
    poll_id, answer_id = poll_and_answer
    user_id = f"user-{uuid.uuid4()}"

    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO votes (vote_id, user_id, poll_id, answer_id) VALUES (%s, %s, %s, %s)",
            (uuid.uuid4(), user_id, poll_id, answer_id),
        )
    conn.commit()

    with pytest.raises(psycopg.errors.UniqueViolation):
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO votes (vote_id, user_id, poll_id, answer_id) VALUES (%s, %s, %s, %s)",
                (uuid.uuid4(), user_id, poll_id, answer_id),
            )
    conn.rollback()


def test_vote_rejected_for_unknown_poll(conn):
    with pytest.raises(psycopg.errors.ForeignKeyViolation):
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO votes (vote_id, user_id, poll_id, answer_id) VALUES (%s, %s, %s, %s)",
                (uuid.uuid4(), "orphan-user", uuid.uuid4(), uuid.uuid4()),
            )
    conn.rollback()


def test_anonymization_nulls_old_votes_only(conn, poll_and_answer):
    poll_id, answer_id = poll_and_answer
    old_user = f"user-{uuid.uuid4()}"
    recent_user = f"user-{uuid.uuid4()}"

    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO votes (vote_id, user_id, poll_id, answer_id, created_at) "
            "VALUES (%s, %s, %s, %s, now() - interval '100 days')",
            (uuid.uuid4(), old_user, poll_id, answer_id),
        )
        cur.execute(
            "INSERT INTO votes (vote_id, user_id, poll_id, answer_id, created_at) "
            "VALUES (%s, %s, %s, %s, now())",
            (uuid.uuid4(), recent_user, poll_id, answer_id),
        )
        cur.execute("SELECT anonymize_votes_older_than_90_days()")
    conn.commit()

    with conn.cursor() as cur:
        cur.execute(
            "SELECT user_id FROM votes WHERE poll_id = %s AND user_id IS NULL", (poll_id,)
        )
        anonymized_rows = cur.fetchall()
        cur.execute(
            "SELECT user_id FROM votes WHERE poll_id = %s AND user_id = %s",
            (poll_id, recent_user),
        )
        recent_row = cur.fetchone()

    assert len(anonymized_rows) == 1
    assert recent_row == (recent_user,)
