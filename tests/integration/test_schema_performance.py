"""Performance test for I-001's acceptance criterion: "Performance
validated: 1000 votes/sec write throughput on single shard" and the
Testing Strategy's "< 5ms per vote" insert latency target.

Skips automatically if DATABASE_URL isn't reachable.
"""

import os
import time
import uuid

import psycopg
import pytest

DATABASE_URL = os.environ.get(
    "DATABASE_URL", "postgresql://postgres@localhost:5432/poll_app"
)

VOTES_TO_INSERT = 2000
MIN_THROUGHPUT_PER_SEC = 1000
MAX_AVG_LATENCY_MS = 5


@pytest.fixture
def conn():
    try:
        connection = psycopg.connect(DATABASE_URL, connect_timeout=2)
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
            (poll_id, "perf test poll"),
        )
        cur.execute(
            'INSERT INTO answers (answer_id, poll_id, answer_text, "order") '
            "VALUES (%s, %s, %s, 0)",
            (answer_id, poll_id, "yes"),
        )
    conn.commit()
    yield poll_id, answer_id


def test_vote_write_throughput_and_latency(conn, poll_and_answer):
    poll_id, answer_id = poll_and_answer

    start = time.perf_counter()
    with conn.cursor() as cur:
        for i in range(VOTES_TO_INSERT):
            cur.execute(
                "INSERT INTO votes (vote_id, user_id, poll_id, answer_id) VALUES (%s, %s, %s, %s)",
                (uuid.uuid4(), f"perf-user-{i}-{uuid.uuid4()}", poll_id, answer_id),
            )
    conn.commit()
    elapsed = time.perf_counter() - start

    throughput = VOTES_TO_INSERT / elapsed
    avg_latency_ms = (elapsed / VOTES_TO_INSERT) * 1000

    assert throughput >= MIN_THROUGHPUT_PER_SEC, (
        f"throughput {throughput:.0f} votes/sec below the {MIN_THROUGHPUT_PER_SEC}/sec target"
    )
    assert avg_latency_ms < MAX_AVG_LATENCY_MS, (
        f"avg insert latency {avg_latency_ms:.2f}ms exceeds the {MAX_AVG_LATENCY_MS}ms target"
    )
