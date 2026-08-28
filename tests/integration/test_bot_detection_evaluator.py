"""Integration tests for I-014 against a real Redis cluster (I-002) and a
real PostgreSQL instance (I-001 schema): the counter writer, the
evaluator's rules end-to-end, and the anomalies rows they actually write.

Skips automatically if either dependency isn't reachable, mirroring
tests/integration/test_result_aggregator_redis.py and
tests/integration/test_vote_acceptance.py.
"""

import asyncio
import os
import time
import uuid

import psycopg
import pytest

from src.cache.redis_client import get_redis
from src.jobs.bot_detection import (
    evaluate_global_spike,
    evaluate_low_and_slow,
    evaluate_single_ip_bursts,
)
from src.services.bot_detection import (
    global_bucket_key,
    ip_burst_key,
    ip_users_key,
    record_vote_attempt,
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
async def redis(db_conn):
    client = await get_redis()
    try:
        await asyncio.wait_for(client.ping(), timeout=3)
    except Exception:
        pytest.skip("no Redis cluster reachable")
    yield client


def _recent_anomalies(conn, *, alert_type, ip_address):
    with conn.cursor() as cur:
        cur.execute(
            "SELECT alert_id, severity FROM anomalies "
            "WHERE alert_type = %s AND ip_address = %s "
            "AND created_at > now() - interval '1 minute'",
            (alert_type, ip_address),
        )
        return cur.fetchall()


@pytest.mark.asyncio
async def test_100_attempts_from_one_ip_produces_one_warning_row(db_conn, redis):
    ip = f"10.0.0.{uuid.uuid4().int % 250}"
    await redis.delete(ip_burst_key(ip))
    await redis.delete(f"botcheck:cooldown:single_ip_burst:{ip}")
    for i in range(100):
        await redis.zadd(ip_burst_key(ip), {f"seed-{i}-{uuid.uuid4()}": time.time()})

    await evaluate_single_ip_bursts(redis)

    rows = _recent_anomalies(db_conn, alert_type="bot_pattern_detected", ip_address=ip)
    assert len(rows) == 1
    assert rows[0][1] == "warning"


@pytest.mark.asyncio
async def test_20_distinct_users_produces_warning_without_burst_or_dup_trigger(
    db_conn, redis
):
    ip = f"10.0.1.{uuid.uuid4().int % 250}"
    await redis.delete(ip_burst_key(ip))
    await redis.delete(ip_users_key(ip))
    await redis.delete(f"botcheck:cooldown:low_and_slow:{ip}")
    # One vote attempt per distinct user — never crosses the 100-in-10s
    # burst threshold, matching the "no individual user or the IP itself
    # crosses I-007's quota" acceptance criterion.
    for i in range(20):
        await record_vote_attempt(redis, ip=ip, user_id=f"low-and-slow-user-{i}")

    await evaluate_low_and_slow(redis)

    rows = _recent_anomalies(db_conn, alert_type="bot_pattern_detected", ip_address=ip)
    assert len(rows) == 1
    assert rows[0][1] == "warning"


@pytest.mark.asyncio
async def test_global_spike_produces_critical_row(db_conn, redis):
    bucket = int(time.time())
    key = global_bucket_key(bucket)
    await redis.set(key, 100_000, ex=5)
    await redis.delete("botcheck:cooldown:global_spike:global")

    await evaluate_global_spike(redis)

    rows = _recent_anomalies(
        db_conn, alert_type="bot_pattern_detected", ip_address="global"
    )
    assert len(rows) == 1
    assert rows[0][1] == "critical"
