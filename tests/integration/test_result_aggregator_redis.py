"""Integration tests for I-009's `compute_poll_results`/
`compute_poll_results_batch` against a real Redis cluster (I-002) and a
real PostgreSQL instance (I-001 schema, answers-cache miss path).

Seeds `cache:poll:{poll_id}:answer:{answer_id}` counters directly (the
same key format I-008's worker `INCR`s in production) rather than driving
votes through the whole pipeline — this suite's job is the aggregation
math against real Redis round trips, not the vote-acceptance/processor
path already covered by tests/integration/test_vote_acceptance.py and
tests/integration/test_vote_processor_pipeline.py.

Skips automatically if either dependency isn't reachable, mirroring
tests/integration/test_vote_acceptance.py.
"""

import asyncio
import os
import uuid

import psycopg
import pytest

from src.cache.answer_cache import answers_cache_key
from src.cache.redis_client import get_redis
from src.services.result_aggregator import (
    compute_poll_results,
    compute_poll_results_batch,
)

DATABASE_URL = os.environ.get(
    "DATABASE_URL", "postgresql://postgres@localhost:5432/poll_app"
)


def _connect():
    return psycopg.connect(DATABASE_URL, connect_timeout=2)


@pytest.fixture
def db_conn():
    try:
        connection = _connect()
    except psycopg.OperationalError:
        pytest.skip(f"no PostgreSQL reachable at {DATABASE_URL}")
    yield connection
    connection.rollback()
    connection.close()


@pytest.fixture
async def redis():
    client = await get_redis()
    try:
        await asyncio.wait_for(client.ping(), timeout=3)
    except Exception:
        pytest.skip("no Redis cluster reachable")
    yield client


def _make_poll(conn):
    poll_id = uuid.uuid4()
    answer_id = uuid.uuid4()
    other_answer_id = uuid.uuid4()
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO polls (poll_id, question, state) VALUES (%s, %s, %s)",
            (poll_id, "test poll", "active"),
        )
        cur.execute(
            'INSERT INTO answers (answer_id, poll_id, answer_text, "order") '
            "VALUES (%s, %s, %s, 0)",
            (answer_id, poll_id, "yes"),
        )
        cur.execute(
            'INSERT INTO answers (answer_id, poll_id, answer_text, "order") '
            "VALUES (%s, %s, %s, 1)",
            (other_answer_id, poll_id, "no"),
        )
    conn.commit()
    return poll_id, answer_id, other_answer_id


async def _seed_counters(redis, poll_id, counts: dict) -> None:
    for answer_id, count in counts.items():
        key = f"cache:poll:{poll_id}:answer:{answer_id}"
        await redis.delete(key)
        if count:
            await redis.set(key, count)


@pytest.mark.asyncio
async def test_compute_poll_results_reads_seeded_counters(db_conn, redis):
    poll_id, answer_id, other_answer_id = _make_poll(db_conn)
    await redis.delete(answers_cache_key(poll_id))
    await _seed_counters(redis, poll_id, {answer_id: 6, other_answer_id: 4})

    result = await compute_poll_results(poll_id, redis)

    assert result.total_votes == 10
    by_id = {str(a.answer_id): a for a in result.answers}
    assert by_id[str(answer_id)].vote_count == 6
    assert by_id[str(answer_id)].percentage == 60.0
    assert by_id[str(other_answer_id)].vote_count == 4
    assert by_id[str(other_answer_id)].percentage == 40.0


@pytest.mark.asyncio
async def test_compute_poll_results_populates_answers_cache_on_miss(db_conn, redis):
    poll_id, answer_id, other_answer_id = _make_poll(db_conn)
    cache_key = answers_cache_key(poll_id)
    await redis.delete(cache_key)
    await _seed_counters(redis, poll_id, {answer_id: 0, other_answer_id: 0})

    await compute_poll_results(poll_id, redis)

    ttl = await redis.ttl(cache_key)
    assert ttl > 0  # populated with a TTL, not left unset


@pytest.mark.asyncio
async def test_compute_poll_results_new_poll_no_counters_is_zero(db_conn, redis):
    poll_id, answer_id, other_answer_id = _make_poll(db_conn)
    await redis.delete(answers_cache_key(poll_id))
    # Never INCRed at all -- both keys absent, not present-and-zero.
    await redis.delete(f"cache:poll:{poll_id}:answer:{answer_id}")
    await redis.delete(f"cache:poll:{poll_id}:answer:{other_answer_id}")

    result = await compute_poll_results(poll_id, redis)

    assert result.total_votes == 0
    assert all(a.vote_count == 0 and a.percentage == 0.0 for a in result.answers)


@pytest.mark.asyncio
async def test_batch_matches_per_poll_results_against_real_redis(db_conn, redis):
    poll_a_id, a_answer, a_other = _make_poll(db_conn)
    poll_b_id, b_answer, b_other = _make_poll(db_conn)
    for poll_id in (poll_a_id, poll_b_id):
        await redis.delete(answers_cache_key(poll_id))
    await _seed_counters(redis, poll_a_id, {a_answer: 6, a_other: 4})
    await _seed_counters(redis, poll_b_id, {b_answer: 1, b_other: 2})

    individual_a = await compute_poll_results(poll_a_id, redis)
    individual_b = await compute_poll_results(poll_b_id, redis)

    batch = await compute_poll_results_batch([poll_a_id, poll_b_id], redis)

    assert batch[poll_a_id] == individual_a
    assert batch[poll_b_id] == individual_b
