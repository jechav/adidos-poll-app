"""Integration tests for I-011's `GET /v1/polls` fallback path: when
Redis is unreachable, the endpoint must still return 200, sourced from
I-010's `vote_counts` table, with `meta.stale: true`.

The CI integration job runs this suite against a real, reachable Redis
cluster (I-022), so the `client` fixture overrides `redis_dependency`
with a stub that always raises a connection error — forcing every
request through the real ASGI app onto the fallback path deterministically.
This suite's job is asserting the fallback's correctness (counts,
percentages, `stale` flag), not whether Redis happens to be reachable.

Skips automatically if DATABASE_URL isn't reachable, mirroring
tests/integration/test_user_votes.py.
"""

import os
import uuid

import psycopg
import pytest
from httpx import ASGITransport, AsyncClient
from redis.exceptions import ConnectionError as RedisConnectionError

from src.api.app import app
from src.cache.redis_client import redis_dependency

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


class _UnreachableRedis:
    """Stands in for `redis_dependency` so every request exercises the
    fallback path deterministically, regardless of whether a real Redis
    cluster happens to be reachable from this test environment (the CI
    integration job runs the whole suite against a real cluster)."""

    async def get(self, *args, **kwargs):
        raise RedisConnectionError("simulated redis outage")


@pytest.fixture
async def client():
    app.dependency_overrides[redis_dependency] = lambda: _UnreachableRedis()
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        yield ac
    app.dependency_overrides.pop(redis_dependency, None)


def _auth(user_id: str = "user-fallback") -> dict:
    return {"Authorization": f"Bearer {user_id}"}


def _make_poll(conn, *, state="active", question="fallback poll"):
    poll_id = uuid.uuid4()
    answer_yes = uuid.uuid4()
    answer_no = uuid.uuid4()
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO polls (poll_id, question, state) VALUES (%s, %s, %s)",
            (poll_id, question, state),
        )
        cur.execute(
            'INSERT INTO answers (answer_id, poll_id, answer_text, "order") '
            "VALUES (%s, %s, %s, 0)",
            (answer_yes, poll_id, "Yes"),
        )
        cur.execute(
            'INSERT INTO answers (answer_id, poll_id, answer_text, "order") '
            "VALUES (%s, %s, %s, 1)",
            (answer_no, poll_id, "No"),
        )
    conn.commit()
    return poll_id, answer_yes, answer_no


def _seed_vote_counts(conn, poll_id, answer_id, count, percentage):
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO vote_counts (poll_id, answer_id, count, percentage) "
            "VALUES (%s, %s, %s, %s)",
            (poll_id, answer_id, count, percentage),
        )
    conn.commit()


@pytest.mark.asyncio
async def test_fallback_returns_200_with_stale_true_and_correct_counts(client, db_conn):
    poll_id, answer_yes, answer_no = _make_poll(db_conn, question="6 yes 4 no?")
    _seed_vote_counts(db_conn, poll_id, answer_yes, 6, 60.0)
    _seed_vote_counts(db_conn, poll_id, answer_no, 4, 40.0)

    resp = await client.get("/v1/polls", headers=_auth(), params={"state": "active"})

    assert resp.status_code == 200
    body = resp.json()
    assert body["meta"]["stale"] is True
    poll = next(p for p in body["data"]["polls"] if p["poll_id"] == str(poll_id))
    assert poll["total_votes"] == 10
    by_text = {a["text"]: a for a in poll["answers"]}
    assert by_text["Yes"]["vote_count"] == 6
    assert by_text["Yes"]["percentage"] == 60.0
    assert by_text["No"]["vote_count"] == 4
    assert by_text["No"]["percentage"] == 40.0


@pytest.mark.asyncio
async def test_fallback_poll_with_no_vote_counts_row_shows_zero_votes(client, db_conn):
    poll_id, answer_yes, answer_no = _make_poll(db_conn, question="never refreshed")

    resp = await client.get("/v1/polls", headers=_auth(), params={"state": "active"})

    assert resp.status_code == 200
    poll = next(
        p for p in resp.json()["data"]["polls"] if p["poll_id"] == str(poll_id)
    )
    assert poll["total_votes"] == 0
    assert poll["answers"] == []


@pytest.mark.asyncio
async def test_closed_and_archived_polls_reachable_via_state_param(client, db_conn):
    closed_id, closed_yes, closed_no = _make_poll(
        db_conn, state="closed", question="closed poll?"
    )
    _seed_vote_counts(db_conn, closed_id, closed_yes, 3, 100.0)
    _seed_vote_counts(db_conn, closed_id, closed_no, 0, 0.0)

    archived_id, archived_yes, archived_no = _make_poll(
        db_conn, state="archived", question="archived poll?"
    )
    _seed_vote_counts(db_conn, archived_id, archived_yes, 1, 50.0)
    _seed_vote_counts(db_conn, archived_id, archived_no, 1, 50.0)

    resp_closed = await client.get(
        "/v1/polls", headers=_auth(), params={"state": "closed"}
    )
    resp_archived = await client.get(
        "/v1/polls", headers=_auth(), params={"state": "archived"}
    )

    assert resp_closed.status_code == 200
    assert any(
        p["poll_id"] == str(closed_id) for p in resp_closed.json()["data"]["polls"]
    )
    assert resp_archived.status_code == 200
    assert any(
        p["poll_id"] == str(archived_id)
        for p in resp_archived.json()["data"]["polls"]
    )


@pytest.mark.asyncio
async def test_unauthenticated_request_returns_401(client):
    resp = await client.get("/v1/polls")
    assert resp.status_code == 401
    assert resp.json()["error"]["code"] == "UNAUTHORIZED"
