"""Integration tests for I-010's `refresh_vote_counts` against a real
PostgreSQL instance — verifies the actual SQL (the `answers` LEFT JOIN
`votes`, and the `vote_counts` broadcast upsert) that the fake-shard unit
tests in tests/unit/test_refresh_vote_counts.py can't exercise.

Local dev stands in one Postgres instance for every shard (see
`src/config.py`'s `database_url` fallback and
tests/integration/test_vote_processor_pipeline.py's identical pattern),
so this suite runs the job against a single "shard" (`num_shards=1`) —
enough to validate the query and upsert SQL for real, while the
cross-shard summing/merge arithmetic itself is already covered against
fakes in the unit suite.

Skips automatically if DATABASE_URL isn't reachable, mirroring
tests/integration/test_vote_processor_pipeline.py.
"""

import os
import uuid

import psycopg
import pytest

from scripts.jobs.refresh_vote_counts import refresh_vote_counts
from src.worker.db import ShardConnectionPool

DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://postgres@localhost:5432/poll_app")


@pytest.fixture
def sync_conn():
    try:
        conn = psycopg.connect(DATABASE_URL, connect_timeout=2)
    except psycopg.OperationalError:
        pytest.skip(f"no PostgreSQL reachable at {DATABASE_URL}")
    yield conn
    conn.rollback()
    conn.close()


@pytest.fixture
def poll_with_answers(sync_conn):
    poll_id = uuid.uuid4()
    answer_yes = uuid.uuid4()
    answer_no = uuid.uuid4()
    with sync_conn.cursor() as cur:
        cur.execute(
            "INSERT INTO polls (poll_id, question, state) VALUES (%s, %s, 'active')",
            (poll_id, "refresh_vote_counts test poll"),
        )
        cur.execute(
            'INSERT INTO answers (answer_id, poll_id, answer_text, "order") '
            "VALUES (%s, %s, %s, 0)",
            (answer_yes, poll_id, "yes"),
        )
        cur.execute(
            'INSERT INTO answers (answer_id, poll_id, answer_text, "order") '
            "VALUES (%s, %s, %s, 1)",
            (answer_no, poll_id, "no"),
        )
    sync_conn.commit()
    yield poll_id, answer_yes, answer_no


@pytest.fixture
def three_yes_one_no_votes(sync_conn, poll_with_answers):
    poll_id, answer_yes, answer_no = poll_with_answers
    with sync_conn.cursor() as cur:
        for i in range(3):
            cur.execute(
                "INSERT INTO votes (vote_id, user_id, poll_id, answer_id) VALUES (%s, %s, %s, %s)",
                (uuid.uuid4(), f"refresh-job-user-yes-{i}", poll_id, answer_yes),
            )
        cur.execute(
            "INSERT INTO votes (vote_id, user_id, poll_id, answer_id) VALUES (%s, %s, %s, %s)",
            (uuid.uuid4(), "refresh-job-user-no-0", poll_id, answer_no),
        )
    sync_conn.commit()
    return poll_with_answers


@pytest.fixture
async def pool():
    p = ShardConnectionPool(lambda shard_id: DATABASE_URL)
    yield p
    for shard_id in list(p._connections):
        await p.invalidate(shard_id)


def _read_vote_counts(sync_conn, poll_id):
    with sync_conn.cursor() as cur:
        cur.execute(
            "SELECT answer_id, count, percentage, last_updated_at FROM vote_counts WHERE poll_id = %s",
            (poll_id,),
        )
        return {row[0]: (row[1], float(row[2]), row[3]) for row in cur.fetchall()}


@pytest.mark.asyncio
async def test_refresh_computes_correct_counts_and_percentages(
    sync_conn, three_yes_one_no_votes, pool
):
    poll_id, answer_yes, answer_no = three_yes_one_no_votes

    await refresh_vote_counts(pool, num_shards=1)

    rows = _read_vote_counts(sync_conn, poll_id)
    assert rows[answer_yes][:2] == (3, 75.0)
    assert rows[answer_no][:2] == (1, 25.0)


@pytest.mark.asyncio
async def test_refresh_includes_zero_vote_answers(sync_conn, poll_with_answers, pool):
    poll_id, answer_yes, answer_no = poll_with_answers
    # No votes cast at all — both answers must still appear with count=0.

    await refresh_vote_counts(pool, num_shards=1)

    rows = _read_vote_counts(sync_conn, poll_id)
    assert rows[answer_yes][:2] == (0, 0.0)
    assert rows[answer_no][:2] == (0, 0.0)


@pytest.mark.asyncio
async def test_refresh_is_idempotent_aside_from_last_updated_at(
    sync_conn, three_yes_one_no_votes, pool
):
    poll_id, answer_yes, answer_no = three_yes_one_no_votes

    await refresh_vote_counts(pool, num_shards=1)
    first = _read_vote_counts(sync_conn, poll_id)

    await refresh_vote_counts(pool, num_shards=1)
    second = _read_vote_counts(sync_conn, poll_id)

    assert first[answer_yes][:2] == second[answer_yes][:2] == (3, 75.0)
    assert first[answer_no][:2] == second[answer_no][:2] == (1, 25.0)
    assert second[answer_yes][2] >= first[answer_yes][2]
