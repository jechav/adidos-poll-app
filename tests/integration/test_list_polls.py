"""Integration tests for I-011's `GET /v1/polls`: state filtering and
pagination through the real ASGI app and a real PostgreSQL instance.

These assertions hold regardless of which path (I-009 Redis or I-010
fallback) served the response, so they don't need a Redis cluster —
mirroring tests/integration/test_user_votes.py's DB-only pattern. A
dedicated primary-path (`meta.stale: false`) test lives at the bottom and
skips if no Redis cluster is reachable, mirroring
tests/integration/test_result_aggregator_redis.py.

Skips automatically if DATABASE_URL isn't reachable.
"""

import asyncio
import os
import uuid

import psycopg
import pytest
from httpx import ASGITransport, AsyncClient

from src.api.app import app
from src.cache.answer_cache import answers_cache_key
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


@pytest.fixture
async def client():
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        yield ac


def _auth(user_id: str = "user-list-polls") -> dict:
    return {"Authorization": f"Bearer {user_id}"}


def _make_poll(conn, *, state="active", question="poll"):
    poll_id = uuid.uuid4()
    answer_a = uuid.uuid4()
    answer_b = uuid.uuid4()
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO polls (poll_id, question, state) VALUES (%s, %s, %s)",
            (poll_id, question, state),
        )
        cur.execute(
            'INSERT INTO answers (answer_id, poll_id, answer_text, "order") '
            "VALUES (%s, %s, %s, 0)",
            (answer_a, poll_id, "A"),
        )
        cur.execute(
            'INSERT INTO answers (answer_id, poll_id, answer_text, "order") '
            "VALUES (%s, %s, %s, 1)",
            (answer_b, poll_id, "B"),
        )
    conn.commit()
    return poll_id, answer_a, answer_b


@pytest.mark.asyncio
async def test_state_defaults_to_active(client, db_conn):
    active_id, *_ = _make_poll(db_conn, state="active", question="active default test")
    draft_id, *_ = _make_poll(db_conn, state="draft", question="draft not listed")

    resp = await client.get("/v1/polls", headers=_auth())

    assert resp.status_code == 200
    poll_ids = {p["poll_id"] for p in resp.json()["data"]["polls"]}
    assert str(active_id) in poll_ids
    assert str(draft_id) not in poll_ids


@pytest.mark.asyncio
async def test_pagination_limit_offset_and_total(client, db_conn):
    marker = f"page-test-{uuid.uuid4()}"
    ids = []
    for i in range(5):
        poll_id, *_ = _make_poll(db_conn, question=f"{marker}-{i}")
        ids.append(poll_id)

    resp_page1 = await client.get(
        "/v1/polls",
        headers=_auth(),
        params={"state": "active", "limit": 2, "offset": 0},
    )
    resp_page2 = await client.get(
        "/v1/polls",
        headers=_auth(),
        params={"state": "active", "limit": 2, "offset": 2},
    )

    assert resp_page1.status_code == 200
    assert resp_page2.status_code == 200
    body1 = resp_page1.json()
    body2 = resp_page2.json()
    assert body1["data"]["pagination"]["limit"] == 2
    assert body1["data"]["pagination"]["offset"] == 0
    assert body1["data"]["pagination"]["total"] >= 5
    assert len(body1["data"]["polls"]) == 2
    assert len(body2["data"]["polls"]) == 2
    ids_page1 = {p["poll_id"] for p in body1["data"]["polls"]}
    ids_page2 = {p["poll_id"] for p in body2["data"]["polls"]}
    assert ids_page1.isdisjoint(ids_page2)


@pytest.mark.asyncio
async def test_limit_over_100_is_rejected(client, db_conn):
    resp = await client.get(
        "/v1/polls", headers=_auth(), params={"limit": 101}
    )
    assert resp.status_code == 400


@pytest.mark.asyncio
async def test_empty_result_set_returns_200_not_404(client, db_conn):
    resp = await client.get(
        "/v1/polls",
        headers=_auth(),
        params={"state": "active", "limit": 1, "offset": 999999},
    )
    assert resp.status_code == 200
    assert resp.json()["data"]["polls"] == []


@pytest.mark.asyncio
async def test_unauthenticated_request_returns_401(client):
    resp = await client.get("/v1/polls")
    assert resp.status_code == 401
    assert resp.json()["error"]["code"] == "UNAUTHORIZED"


@pytest.fixture
async def redis():
    client = await get_redis()
    try:
        await asyncio.wait_for(client.ping(), timeout=3)
    except Exception:
        pytest.skip("no Redis cluster reachable")
    yield client


@pytest.mark.asyncio
async def test_primary_path_uses_redis_and_sets_stale_false(client, db_conn, redis):
    poll_id, answer_a, answer_b = _make_poll(db_conn, question="6a4b primary path")
    await redis.delete(answers_cache_key(poll_id))
    await redis.set(f"cache:poll:{poll_id}:answer:{answer_a}", 6)
    await redis.set(f"cache:poll:{poll_id}:answer:{answer_b}", 4)

    resp = await client.get("/v1/polls", headers=_auth(), params={"state": "active"})

    assert resp.status_code == 200
    body = resp.json()
    assert body["meta"]["stale"] is False
    poll = next(p for p in body["data"]["polls"] if p["poll_id"] == str(poll_id))
    assert poll["total_votes"] == 10
    by_text = {a["text"]: a for a in poll["answers"]}
    assert by_text["A"]["vote_count"] == 6
    assert by_text["A"]["percentage"] == 60.0
    assert by_text["B"]["vote_count"] == 4
    assert by_text["B"]["percentage"] == 40.0
