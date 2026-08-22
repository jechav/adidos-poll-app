"""Integration tests for I-012's `GET /v1/user/votes`: the full
request/response cycle through the real ASGI app and a real PostgreSQL
instance (I-001 schema) — no Redis involved at all, per the ticket's "No
Redis, No Aggregate Counts" decision.

Skips automatically if DATABASE_URL isn't reachable, mirroring
tests/integration/test_vote_acceptance.py and
tests/integration/test_schema_performance.py.
"""

import os
import uuid

import psycopg
import pytest
from httpx import ASGITransport, AsyncClient

from src.api.app import app

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


def _make_poll(conn, *, state="active", question="test poll"):
    poll_id = uuid.uuid4()
    answer_id = uuid.uuid4()
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO polls (poll_id, question, state) VALUES (%s, %s, %s)",
            (poll_id, question, state),
        )
        cur.execute(
            'INSERT INTO answers (answer_id, poll_id, answer_text, "order") '
            "VALUES (%s, %s, %s, 0)",
            (answer_id, poll_id, "yes"),
        )
    conn.commit()
    return poll_id, answer_id


def _cast_vote(conn, *, user_id, poll_id, answer_id, vote_id=None):
    vote_id = vote_id or uuid.uuid4()
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO votes (vote_id, user_id, poll_id, answer_id) VALUES (%s, %s, %s, %s)",
            (vote_id, user_id, poll_id, answer_id),
        )
    conn.commit()
    return vote_id


def _auth(user_id: str) -> dict:
    return {"Authorization": f"Bearer {user_id}"}


@pytest.mark.asyncio
async def test_user_with_zero_votes_returns_empty_list(client, db_conn):
    user_id = f"user-{uuid.uuid4()}"

    resp = await client.get("/v1/user/votes", headers=_auth(user_id))

    assert resp.status_code == 200
    body = resp.json()
    assert body["success"] is True
    assert body["data"]["votes"] == []
    assert body["data"]["pagination"] == {"limit": 20, "offset": 0, "total": 0}


@pytest.mark.asyncio
async def test_unauthenticated_request_returns_401(client):
    resp = await client.get("/v1/user/votes")
    assert resp.status_code == 401
    assert resp.json()["error"]["code"] == "UNAUTHORIZED"


@pytest.mark.asyncio
async def test_votes_across_all_poll_states_are_visible(client, db_conn):
    user_id = f"user-{uuid.uuid4()}"
    active_poll, active_answer = _make_poll(db_conn, state="active", question="active?")
    closed_poll, closed_answer = _make_poll(db_conn, state="closed", question="closed?")
    archived_poll, archived_answer = _make_poll(
        db_conn, state="archived", question="archived?"
    )

    for poll_id, answer_id in (
        (active_poll, active_answer),
        (closed_poll, closed_answer),
        (archived_poll, archived_answer),
    ):
        _cast_vote(db_conn, user_id=user_id, poll_id=poll_id, answer_id=answer_id)

    resp = await client.get("/v1/user/votes", headers=_auth(user_id))

    assert resp.status_code == 200
    body = resp.json()
    votes = body["data"]["votes"]
    assert len(votes) == 3
    states_by_poll = {v["poll_id"]: v["poll_state"] for v in votes}
    assert states_by_poll[str(active_poll)] == "active"
    assert states_by_poll[str(closed_poll)] == "closed"
    assert states_by_poll[str(archived_poll)] == "archived"
    for v in votes:
        assert set(v) == {
            "poll_id",
            "question",
            "poll_state",
            "answer_id",
            "answer_text",
            "voted_at",
        }
        assert v["answer_text"] == "yes"


@pytest.mark.asyncio
async def test_pagination_limit_offset_and_total(client, db_conn):
    user_id = f"user-{uuid.uuid4()}"
    poll_id, answer_id = _make_poll(db_conn, question="paginated poll")
    for _ in range(5):
        _cast_vote(
            db_conn,
            user_id=user_id,
            poll_id=poll_id,
            answer_id=answer_id,
            vote_id=uuid.uuid4(),
        )
        # Each vote targets a distinct (user_id, poll_id) pair since votes
        # has UNIQUE(user_id, poll_id) — so give each its own poll.
        poll_id, answer_id = _make_poll(db_conn, question="paginated poll")

    resp_page1 = await client.get(
        "/v1/user/votes", headers=_auth(user_id), params={"limit": 2, "offset": 0}
    )
    resp_page2 = await client.get(
        "/v1/user/votes", headers=_auth(user_id), params={"limit": 2, "offset": 2}
    )

    body1 = resp_page1.json()
    body2 = resp_page2.json()
    assert body1["data"]["pagination"] == {"limit": 2, "offset": 0, "total": 5}
    assert body2["data"]["pagination"] == {"limit": 2, "offset": 2, "total": 5}
    assert len(body1["data"]["votes"]) == 2
    assert len(body2["data"]["votes"]) == 2
    ids_page1 = {v["poll_id"] for v in body1["data"]["votes"]}
    ids_page2 = {v["poll_id"] for v in body2["data"]["votes"]}
    assert ids_page1.isdisjoint(ids_page2)


@pytest.mark.asyncio
async def test_limit_over_100_is_rejected(client, db_conn):
    user_id = f"user-{uuid.uuid4()}"
    resp = await client.get(
        "/v1/user/votes", headers=_auth(user_id), params={"limit": 101}
    )
    assert resp.status_code == 400


@pytest.mark.asyncio
async def test_anonymized_vote_no_longer_appears(client, db_conn):
    """Simulates I-001's 90-day anonymization job nulling `user_id`: once
    nulled, the vote is expected to disappear from that user's history —
    documented behavior, not a bug (see the module docstring in
    src/db/queries/user_votes.py).
    """
    user_id = f"user-{uuid.uuid4()}"
    poll_id, answer_id = _make_poll(db_conn, question="will be anonymized")
    vote_id = _cast_vote(db_conn, user_id=user_id, poll_id=poll_id, answer_id=answer_id)

    resp_before = await client.get("/v1/user/votes", headers=_auth(user_id))
    assert len(resp_before.json()["data"]["votes"]) == 1

    with db_conn.cursor() as cur:
        cur.execute("UPDATE votes SET user_id = NULL WHERE vote_id = %s", (vote_id,))
    db_conn.commit()

    resp_after = await client.get("/v1/user/votes", headers=_auth(user_id))
    assert resp_after.json()["data"]["votes"] == []
    assert resp_after.json()["data"]["pagination"]["total"] == 0


@pytest.mark.asyncio
async def test_user_a_never_sees_user_bs_votes(client, db_conn):
    user_a = f"user-a-{uuid.uuid4()}"
    user_b = f"user-b-{uuid.uuid4()}"
    poll_id, answer_id = _make_poll(db_conn, question="privacy test poll")
    _cast_vote(db_conn, user_id=user_b, poll_id=poll_id, answer_id=answer_id)

    resp = await client.get("/v1/user/votes", headers=_auth(user_a))

    assert resp.status_code == 200
    assert resp.json()["data"]["votes"] == []


@pytest.mark.asyncio
async def test_no_user_id_query_param_is_accepted_or_honored(client, db_conn):
    """There is no code path to override `user_id` via a request
    parameter — confirms an attempted override is silently ignored
    (FastAPI drops unknown query params) rather than accepted.
    """
    user_a = f"user-a-{uuid.uuid4()}"
    user_b = f"user-b-{uuid.uuid4()}"
    poll_id, answer_id = _make_poll(db_conn, question="no override poll")
    _cast_vote(db_conn, user_id=user_b, poll_id=poll_id, answer_id=answer_id)

    resp = await client.get(
        "/v1/user/votes", headers=_auth(user_a), params={"user_id": user_b}
    )

    assert resp.status_code == 200
    assert resp.json()["data"]["votes"] == []


@pytest.mark.asyncio
async def test_query_plan_uses_an_index_not_a_sequential_scan(db_conn):
    """Acceptance criterion: the `WHERE v.user_id = $1` lookup must be an
    index scan (via the implicit unique index for `UNIQUE(user_id,
    poll_id)` on `votes`, I-001), not a sequential scan.

    The planner only prefers an index over a sequential scan once the
    table is large enough for that trade-off to pay off, so this seeds a
    few thousand unrelated votes (each its own poll, since `votes` has
    `UNIQUE(user_id, poll_id)`) before asking Postgres to plan the query
    for real, and `ANALYZE`s so the planner's row-count estimate is
    current rather than stale/default.
    """
    from src.db.queries.user_votes import USER_VOTES_QUERY

    target_user_id = f"user-{uuid.uuid4()}"
    target_poll_id, target_answer_id = _make_poll(db_conn, question="explain analyze poll")
    _cast_vote(
        db_conn, user_id=target_user_id, poll_id=target_poll_id, answer_id=target_answer_id
    )

    filler_poll_id, filler_answer_id = _make_poll(db_conn, question="filler poll")
    with db_conn.cursor() as cur:
        cur.executemany(
            "INSERT INTO votes (vote_id, user_id, poll_id, answer_id) VALUES (%s, %s, %s, %s)",
            [
                (uuid.uuid4(), f"filler-user-{i}", filler_poll_id, filler_answer_id)
                for i in range(5000)
            ],
        )
    db_conn.commit()
    with db_conn.cursor() as cur:
        cur.execute("ANALYZE votes")
    db_conn.commit()

    with db_conn.cursor() as cur:
        cur.execute(f"EXPLAIN ANALYZE {USER_VOTES_QUERY}", (target_user_id, 20, 0))
        plan = "\n".join(row[0] for row in cur.fetchall())

    assert "Seq Scan on votes" not in plan, plan
