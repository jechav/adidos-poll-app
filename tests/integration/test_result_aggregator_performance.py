"""Performance test for I-009's acceptance criterion: "P95 latency < 50ms
for `compute_poll_results` under concurrent load (measured, not assumed)".

Seeds one poll's counters in a real Redis cluster (I-002), then fires many
concurrent `compute_poll_results` calls against it and asserts on the
measured P95. Skips automatically if Redis/Postgres aren't reachable,
mirroring the rest of this suite's integration tests.
"""

import asyncio
import os
import time
import uuid

import psycopg
import pytest

from src.cache.answer_cache import answers_cache_key
from src.cache.redis_client import get_redis
from src.services.result_aggregator import compute_poll_results

DATABASE_URL = os.environ.get(
    "DATABASE_URL", "postgresql://postgres@localhost:5432/poll_app"
)

CONCURRENT_CALLS = 80  # stays under the shared RedisCluster client's
# per-node max_connections=100 (src/cache/redis_client.py) — each
# concurrent compute_poll_results call briefly holds a connection for
# its answers-cache GET, so a count above that pool size trips
# MaxConnectionsError rather than measuring real request latency.
P95_TARGET_SECONDS = 0.050


@pytest.fixture
def db_conn():
    try:
        connection = psycopg.connect(DATABASE_URL, connect_timeout=2)
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
            (poll_id, "perf test poll", "active"),
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


def _percentile(values: list[float], pct: float) -> float:
    ordered = sorted(values)
    index = min(int(len(ordered) * pct), len(ordered) - 1)
    return ordered[index]


@pytest.mark.asyncio
async def test_compute_poll_results_p95_under_50ms_concurrent(db_conn, redis):
    poll_id, answer_id, other_answer_id = _make_poll(db_conn)
    await redis.delete(answers_cache_key(poll_id))
    await redis.set(f"cache:poll:{poll_id}:answer:{answer_id}", 6)
    await redis.set(f"cache:poll:{poll_id}:answer:{other_answer_id}", 4)

    # Warm the answers cache so the timed run only measures the counter
    # MGET path, not a one-time DB populate.
    await compute_poll_results(poll_id, redis)

    async def _timed_call():
        start = time.perf_counter()
        await compute_poll_results(poll_id, redis)
        return time.perf_counter() - start

    durations = await asyncio.gather(*[_timed_call() for _ in range(CONCURRENT_CALLS)])

    p95 = _percentile(durations, 0.95)
    p99 = _percentile(durations, 0.99)
    assert p95 < P95_TARGET_SECONDS, (
        f"P95 latency {p95 * 1000:.2f}ms exceeded the {P95_TARGET_SECONDS * 1000:.0f}ms "
        f"target (P99 was {p99 * 1000:.2f}ms)"
    )
