"""Integration tests for I-005's `POST /v1/vote`: the full request/response
cycle through the real ASGI app, a real PostgreSQL instance (I-001 schema)
and a real Redis cluster (I-002) — rate limiting (I-007), uniqueness
(I-006), and the queue hand-off (I-008 will drain it) all run for real,
not mocked.

Skips automatically if either dependency isn't reachable, mirroring
tests/integration/test_uniqueness_db.py (Postgres) and
tests/integration/test_auth_integration.py (Redis cluster, bounded ping
so an unreachable cluster fails fast rather than hanging).
"""

import asyncio
import os
import uuid

import psycopg
import pytest
from httpx import ASGITransport, AsyncClient

from src.api.app import app
from src.cache.redis_client import get_redis
from src.services.uniqueness import vote_reservation_key

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


@pytest.fixture
async def client():
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        yield ac


def _make_poll(conn, *, state="active"):
    poll_id = uuid.uuid4()
    answer_id = uuid.uuid4()
    other_answer_id = uuid.uuid4()
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO polls (poll_id, question, state) VALUES (%s, %s, %s)",
            (poll_id, "test poll", state),
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


async def _clear_rate_limit(redis, user_id: str, ip: str):
    await redis.delete(f"rate_limit:{user_id}")
    await redis.delete(f"rate_limit_ip:{ip}")


@pytest.mark.asyncio
async def test_valid_vote_returns_202_and_queues_payload(client, db_conn, redis):
    poll_id, answer_id, _ = _make_poll(db_conn)
    user_id = f"user-{uuid.uuid4()}"
    await _clear_rate_limit(redis, user_id, "testclient")
    await redis.delete(vote_reservation_key(user_id, poll_id))
    await redis.delete("queue:votes")

    resp = await client.post(
        "/v1/vote",
        headers={"Authorization": f"Bearer {user_id}"},
        json={"poll_id": str(poll_id), "answer_id": str(answer_id)},
    )

    assert resp.status_code == 202
    body = resp.json()
    assert body["data"]["status"] == "queued"
    assert body["data"]["poll_id"] == str(poll_id)
    assert body["data"]["answer_id"] == str(answer_id)

    assert await redis.llen("queue:votes") == 1
    raw = await redis.lpop("queue:votes")
    import json

    queued = json.loads(raw)
    assert queued["user_id"] == user_id
    assert queued["poll_id"] == str(poll_id)
    assert queued["answer_id"] == str(answer_id)
    assert "vote_id" in queued
    assert "requested_at" in queued


@pytest.mark.asyncio
async def test_second_vote_by_same_user_returns_409_no_new_queue_entry(
    client, db_conn, redis
):
    poll_id, answer_id, _ = _make_poll(db_conn)
    user_id = f"user-{uuid.uuid4()}"
    await _clear_rate_limit(redis, user_id, "testclient")
    await redis.delete(vote_reservation_key(user_id, poll_id))
    await redis.delete("queue:votes")

    first = await client.post(
        "/v1/vote",
        headers={"Authorization": f"Bearer {user_id}"},
        json={"poll_id": str(poll_id), "answer_id": str(answer_id)},
    )
    assert first.status_code == 202
    assert await redis.llen("queue:votes") == 1

    second = await client.post(
        "/v1/vote",
        headers={"Authorization": f"Bearer {user_id}"},
        json={"poll_id": str(poll_id), "answer_id": str(answer_id)},
    )

    assert second.status_code == 409
    assert second.json()["error"]["code"] == "DUPLICATE_VOTE"
    # The rejected retry must not have queued a second entry.
    assert await redis.llen("queue:votes") == 1


@pytest.mark.asyncio
async def test_vote_on_closed_poll_returns_400_poll_closed(client, db_conn, redis):
    poll_id, answer_id, _ = _make_poll(db_conn, state="closed")
    user_id = f"user-{uuid.uuid4()}"
    await _clear_rate_limit(redis, user_id, "testclient")
    await redis.delete(vote_reservation_key(user_id, poll_id))

    resp = await client.post(
        "/v1/vote",
        headers={"Authorization": f"Bearer {user_id}"},
        json={"poll_id": str(poll_id), "answer_id": str(answer_id)},
    )

    assert resp.status_code == 400
    assert resp.json()["error"]["code"] == "POLL_CLOSED"


@pytest.mark.asyncio
async def test_vote_on_nonexistent_poll_returns_404(client, redis):
    user_id = f"user-{uuid.uuid4()}"
    poll_id, answer_id = uuid.uuid4(), uuid.uuid4()
    await _clear_rate_limit(redis, user_id, "testclient")

    resp = await client.post(
        "/v1/vote",
        headers={"Authorization": f"Bearer {user_id}"},
        json={"poll_id": str(poll_id), "answer_id": str(answer_id)},
    )

    assert resp.status_code == 404
    assert resp.json()["error"]["code"] == "NOT_FOUND"


@pytest.mark.asyncio
async def test_answer_not_belonging_to_poll_returns_400_invalid_request(
    client, db_conn, redis
):
    poll_id, answer_id, _ = _make_poll(db_conn)
    other_poll_id, other_answer_id, _ = _make_poll(db_conn)
    user_id = f"user-{uuid.uuid4()}"
    await _clear_rate_limit(redis, user_id, "testclient")

    resp = await client.post(
        "/v1/vote",
        headers={"Authorization": f"Bearer {user_id}"},
        # answer belongs to the *other* poll.
        json={"poll_id": str(poll_id), "answer_id": str(other_answer_id)},
    )

    assert resp.status_code == 400
    assert resp.json()["error"]["code"] == "INVALID_REQUEST"


@pytest.mark.asyncio
async def test_rate_limit_exceeded_returns_429_before_uniqueness_or_enqueue(
    client, db_conn, redis
):
    poll_id, answer_id, _ = _make_poll(db_conn)
    user_id = f"user-{uuid.uuid4()}"
    await redis.delete(f"rate_limit:{user_id}")
    await redis.delete(vote_reservation_key(user_id, poll_id))
    await redis.delete("queue:votes")

    # Exhaust the 5/min per-user quota directly (distinct polls per call,
    # so uniqueness never blocks any of these five).
    for _ in range(5):
        await redis.incr(f"rate_limit:{user_id}")

    resp = await client.post(
        "/v1/vote",
        headers={"Authorization": f"Bearer {user_id}"},
        json={"poll_id": str(poll_id), "answer_id": str(answer_id)},
    )

    assert resp.status_code == 429
    assert resp.json()["error"]["code"] == "RATE_LIMIT_EXCEEDED"
    # Rejected on rate limit before ever reaching uniqueness/enqueue.
    assert await redis.get(vote_reservation_key(user_id, poll_id)) is None
    assert await redis.llen("queue:votes") == 0
