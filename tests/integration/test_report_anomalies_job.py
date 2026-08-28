"""Integration tests for I-016's `report_anomalies()` job against a real
PostgreSQL instance (I-001 schema + migration 003) and, for the
concurrency test, a real Redis cluster (I-002).

The Adidos HTTP call itself is mocked (`post_anomaly_batch`) — this
suite's job is the DB read/write cycle and the lock, not a live network
integration with Adidos. Skips automatically if DATABASE_URL isn't
reachable, mirroring tests/integration/test_user_votes.py; the
concurrency test additionally skips if no Redis cluster is reachable.
"""

import asyncio
import os
import uuid

import httpx
import psycopg
import pytest

import src.jobs.report_anomalies as job
import src.services.anomalies as anomalies_module
from src.cache.redis_client import get_redis

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


def _seed_unreported(conn, *, marker: str, alert_type="rate_limit_exceeded"):
    alert_id = uuid.uuid4()
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO anomalies (alert_id, user_id, alert_type, description, severity) "
            "VALUES (%s, %s, %s, %s, 'warning')",
            (alert_id, marker, alert_type, f"seeded for {marker}"),
        )
    conn.commit()
    return alert_id


def _reported_at(conn, alert_id):
    with conn.cursor() as cur:
        cur.execute("SELECT reported_at FROM anomalies WHERE alert_id = %s", (alert_id,))
        (value,) = cur.fetchone()
    return value


class TrivialLockRedis:
    """A trivial always-succeeds lock stand-in so the DB-focused tests in
    this file don't need a real Redis cluster reachable — only the
    dedicated concurrency test at the bottom exercises the real lock.
    """

    async def set(self, key, value, nx=False, ex=None):
        return True

    async def delete(self, key):
        return 1


@pytest.fixture
def trivial_lock(monkeypatch):
    fake = TrivialLockRedis()

    async def _get_redis():
        return fake

    monkeypatch.setattr(job, "get_redis", _get_redis)


@pytest.mark.asyncio
async def test_seeded_row_reported_on_2xx_and_excluded_from_next_run(
    db_conn, monkeypatch, trivial_lock
):
    marker = f"report-test-{uuid.uuid4()}"
    alert_id = _seed_unreported(db_conn, marker=marker)

    sent_payloads = []

    async def fake_post(payload):
        sent_payloads.append(payload)
        return httpx.Response(200, request=httpx.Request("POST", "https://x"))

    monkeypatch.setattr(job, "post_anomaly_batch", fake_post)

    await job.report_anomalies()

    assert _reported_at(db_conn, alert_id) is not None
    assert any(p["alert_id"] == str(alert_id) for batch in sent_payloads for p in batch)

    # A second run must not resend the now-reported row.
    sent_payloads.clear()
    await job.report_anomalies()
    assert sent_payloads == [] or not any(
        p["alert_id"] == str(alert_id) for batch in sent_payloads for p in batch
    )


@pytest.mark.asyncio
async def test_500_response_leaves_row_unreported_and_it_reappears_next_run(
    db_conn, monkeypatch, trivial_lock
):
    marker = f"report-fail-test-{uuid.uuid4()}"
    alert_id = _seed_unreported(db_conn, marker=marker)

    async def fake_post_500(payload):
        return httpx.Response(500, request=httpx.Request("POST", "https://x"))

    monkeypatch.setattr(job, "post_anomaly_batch", fake_post_500)
    await job.report_anomalies()
    assert _reported_at(db_conn, alert_id) is None

    seen_second_run = []

    async def fake_post_second(payload):
        seen_second_run.extend(payload)
        return httpx.Response(200, request=httpx.Request("POST", "https://x"))

    monkeypatch.setattr(job, "post_anomaly_batch", fake_post_second)
    await job.report_anomalies()

    assert any(p["alert_id"] == str(alert_id) for p in seen_second_run)
    assert _reported_at(db_conn, alert_id) is not None


@pytest.mark.asyncio
async def test_zero_unreported_rows_makes_no_http_call(db_conn, monkeypatch, trivial_lock):
    async def fake_list_unreported(*, limit):
        return []

    called = {"post": False}

    async def fake_post(payload):
        called["post"] = True

    monkeypatch.setattr(anomalies_module, "list_unreported", fake_list_unreported)
    monkeypatch.setattr(job, "post_anomaly_batch", fake_post)

    await job.report_anomalies()

    assert called["post"] is False


@pytest.mark.asyncio
async def test_reported_at_independent_of_acknowledged_at(db_conn, monkeypatch, trivial_lock):
    marker = f"independent-fields-{uuid.uuid4()}"
    alert_id = _seed_unreported(db_conn, marker=marker)
    with db_conn.cursor() as cur:
        cur.execute(
            "UPDATE anomalies SET acknowledged_at = now() WHERE alert_id = %s", (alert_id,)
        )
    db_conn.commit()

    async def fake_post(payload):
        return httpx.Response(200, request=httpx.Request("POST", "https://x"))

    monkeypatch.setattr(job, "post_anomaly_batch", fake_post)
    await job.report_anomalies()

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT acknowledged_at, reported_at FROM anomalies WHERE alert_id = %s",
            (alert_id,),
        )
        acknowledged_at, reported_at = cur.fetchone()
    assert acknowledged_at is not None
    assert reported_at is not None
    assert acknowledged_at != reported_at


@pytest.mark.asyncio
async def test_concurrent_runs_only_one_sends_via_redis_lock(db_conn, monkeypatch):
    client = await get_redis()
    try:
        await asyncio.wait_for(client.ping(), timeout=3)
    except Exception:
        pytest.skip("no Redis cluster reachable")

    marker = f"concurrency-test-{uuid.uuid4()}"
    _seed_unreported(db_conn, marker=marker)
    await client.delete(job.LOCK_KEY)

    send_count = 0

    async def fake_post(payload):
        nonlocal send_count
        send_count += 1
        return httpx.Response(200, request=httpx.Request("POST", "https://x"))

    import src.jobs.report_anomalies as job_module

    original = job_module.post_anomaly_batch
    job_module.post_anomaly_batch = fake_post
    try:
        await asyncio.gather(job.report_anomalies(), job.report_anomalies())
    finally:
        job_module.post_anomaly_batch = original
        await client.delete(job.LOCK_KEY)

    assert send_count == 1
