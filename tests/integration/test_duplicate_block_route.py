"""Integration tests for I-015: the full `POST /v1/vote` request/response
cycle — same user+IP duplicating a poll 3 times blocks the pair; a 4th
request (even for a different poll) is rejected before reaching the
uniqueness check; the block lifts once its TTL expires.

Skips automatically if either DATABASE_URL or a Redis cluster isn't
reachable, mirroring tests/integration/test_vote_acceptance.py.
"""

import asyncio
import os
import uuid

import psycopg
import pytest
from httpx import ASGITransport, AsyncClient

from src.api.app import app
from src.cache.redis_client import get_redis
from src.services.duplicate_block import _block_key, _strike_key

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


@pytest.fixture
async def client():
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        yield ac


def _make_poll(conn, *, question="dup block test poll"):
    poll_id = uuid.uuid4()
    answer_id = uuid.uuid4()
    other_answer_id = uuid.uuid4()
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO polls (poll_id, question, state) VALUES (%s, %s, 'active')",
            (poll_id, question),
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


@pytest.mark.asyncio
async def test_third_duplicate_blocks_pair_and_writes_one_anomaly(
    client, db_conn, redis
):
    # httpx's ASGITransport always reports a fixed client address
    # ("127.0.0.1", 123) regardless of any forwarding header, and the
    # handler reads `request.client.host` (not a forwarded-for header)
    # — so every request through this fixture shares the same `ip`; only
    # `user_id` varies test-to-test, keeping the block-key scoping
    # (user_id, ip) unique per test.
    poll_id, answer_id, _ = _make_poll(db_conn, question="3-strike test")
    user_id = f"user-{uuid.uuid4()}"
    ip = "127.0.0.1"
    headers = {"Authorization": f"Bearer {user_id}"}
    body = {"poll_id": str(poll_id), "answer_id": str(answer_id)}

    await redis.delete(_block_key(user_id, ip))
    await redis.delete(_strike_key(user_id, ip, poll_id))

    first = await client.post("/v1/vote", headers=headers, json=body)
    assert first.status_code == 202  # first vote is accepted, not a duplicate

    second = await client.post("/v1/vote", headers=headers, json=body)
    assert second.status_code == 409  # 1st duplicate

    third = await client.post("/v1/vote", headers=headers, json=body)
    assert third.status_code == 409  # 2nd duplicate

    fourth = await client.post("/v1/vote", headers=headers, json=body)
    assert fourth.status_code == 409  # 3rd duplicate — crosses STRIKE_THRESHOLD

    assert await redis.exists(_block_key(user_id, ip))

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT count(*) FROM anomalies WHERE alert_type = 'duplicate_attempts_blocked' "
            "AND user_id = %s AND ip_address = %s",
            (user_id, ip),
        )
        (count,) = cur.fetchone()
    assert count == 1


@pytest.mark.asyncio
async def test_blocked_pair_rejected_on_a_different_poll_without_uniqueness_check(
    client, db_conn, redis
):
    poll_b, answer_b, _ = _make_poll(db_conn, question="different poll, still blocked")
    user_id = f"user-{uuid.uuid4()}"
    ip = "127.0.0.1"
    headers = {"Authorization": f"Bearer {user_id}"}

    await redis.delete(_block_key(user_id, ip))
    await redis.set(_block_key(user_id, ip), "1", ex=3600)

    resp = await client.post(
        "/v1/vote",
        headers=headers,
        json={"poll_id": str(poll_b), "answer_id": str(answer_b)},
    )

    assert resp.status_code == 429
    assert resp.json()["error"]["code"] == "RATE_LIMIT_EXCEEDED"


@pytest.mark.asyncio
async def test_block_lifts_after_ttl_expires(client, db_conn, redis):
    poll_id, answer_id, _ = _make_poll(db_conn, question="ttl expiry test")
    user_id = f"user-{uuid.uuid4()}"
    ip = "127.0.0.1"

    await redis.set(_block_key(user_id, ip), "1", ex=3600)
    await redis.delete(_block_key(user_id, ip))  # simulate TTL having elapsed

    resp = await client.post(
        "/v1/vote",
        headers={"Authorization": f"Bearer {user_id}"},
        json={"poll_id": str(poll_id), "answer_id": str(answer_id)},
    )

    assert resp.status_code == 202
