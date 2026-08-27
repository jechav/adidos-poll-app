"""Integration tests for I-013's admin routes: full request/response
cycle against a real ASGI app and a real PostgreSQL instance.

Skips automatically if DATABASE_URL isn't reachable, mirroring
tests/integration/test_user_votes.py.
"""

import os
import uuid
from datetime import datetime, timedelta, timezone

import psycopg
import pytest
from httpx import ASGITransport, AsyncClient

from src.api.app import app

DATABASE_URL = os.environ.get(
    "DATABASE_URL", "postgresql://postgres@localhost:5432/poll_app"
)

ADMIN_AUTH = {"Authorization": "Bearer admin:ops-test"}
USER_AUTH = {"Authorization": "Bearer regular-user"}


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


# --- POST /v1/admin/polls ---------------------------------------------


@pytest.mark.asyncio
async def test_create_poll_with_2_answers_returns_201_draft(client):
    resp = await client.post(
        "/v1/admin/polls",
        headers=ADMIN_AUTH,
        json={"question": "Tabs or spaces?", "answers": [{"text": "Tabs"}, {"text": "Spaces"}]},
    )
    assert resp.status_code == 201
    data = resp.json()["data"]
    assert data["question"] == "Tabs or spaces?"
    assert data["state"] == "draft"
    assert [a["text"] for a in data["answers"]] == ["Tabs", "Spaces"]
    assert [a["order"] for a in data["answers"]] == [0, 1]


@pytest.mark.parametrize("answers", [[], [{"text": "Only one"}], [{"text": "A"}, {"text": "B"}, {"text": "C"}]])
@pytest.mark.asyncio
async def test_create_poll_wrong_answer_count_returns_400(client, answers):
    resp = await client.post(
        "/v1/admin/polls",
        headers=ADMIN_AUTH,
        json={"question": "Bad answer count?", "answers": answers},
    )
    assert resp.status_code == 400
    assert resp.json()["error"]["code"] == "INVALID_REQUEST"


@pytest.mark.asyncio
async def test_create_poll_non_admin_returns_403_before_any_write(client, db_conn):
    with db_conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM polls")
        (before,) = cur.fetchone()

    resp = await client.post(
        "/v1/admin/polls",
        headers=USER_AUTH,
        json={"question": "Should never be created", "answers": [{"text": "A"}, {"text": "B"}]},
    )

    assert resp.status_code == 403
    assert resp.json()["error"]["code"] == "FORBIDDEN"
    with db_conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM polls")
        (after,) = cur.fetchone()
    assert after == before


# --- PUT /v1/admin/polls/{poll_id}/state -------------------------------


async def _create_poll(client, question="state machine test") -> str:
    resp = await client.post(
        "/v1/admin/polls",
        headers=ADMIN_AUTH,
        json={"question": question, "answers": [{"text": "A"}, {"text": "B"}]},
    )
    return resp.json()["data"]["poll_id"]


@pytest.mark.asyncio
async def test_same_state_request_is_idempotent_no_op(client):
    poll_id = await _create_poll(client, "idempotent draft->draft")

    resp = await client.put(
        f"/v1/admin/polls/{poll_id}/state", headers=ADMIN_AUTH, json={"state": "active"}
    )
    assert resp.status_code == 200
    resp_again = await client.put(
        f"/v1/admin/polls/{poll_id}/state", headers=ADMIN_AUTH, json={"state": "active"}
    )
    assert resp_again.status_code == 200
    assert resp_again.json()["data"]["state"] == "active"
    assert resp_again.json()["data"]["activated_at"] == resp.json()["data"]["activated_at"]


@pytest.mark.asyncio
async def test_one_step_forward_transitions_succeed_and_stamp_timestamps(client):
    poll_id = await _create_poll(client, "full lifecycle")

    activate = await client.put(
        f"/v1/admin/polls/{poll_id}/state", headers=ADMIN_AUTH, json={"state": "active"}
    )
    assert activate.status_code == 200
    assert activate.json()["data"]["activated_at"] is not None

    close = await client.put(
        f"/v1/admin/polls/{poll_id}/state", headers=ADMIN_AUTH, json={"state": "closed"}
    )
    assert close.status_code == 200
    assert close.json()["data"]["closed_at"] is not None

    archive = await client.put(
        f"/v1/admin/polls/{poll_id}/state", headers=ADMIN_AUTH, json={"state": "archived"}
    )
    assert archive.status_code == 200
    assert archive.json()["data"]["archived_at"] is not None


@pytest.mark.asyncio
async def test_backward_transition_returns_400(client):
    poll_id = await _create_poll(client, "backward transition")
    await client.put(f"/v1/admin/polls/{poll_id}/state", headers=ADMIN_AUTH, json={"state": "active"})
    await client.put(f"/v1/admin/polls/{poll_id}/state", headers=ADMIN_AUTH, json={"state": "closed"})

    resp = await client.put(
        f"/v1/admin/polls/{poll_id}/state", headers=ADMIN_AUTH, json={"state": "active"}
    )
    assert resp.status_code == 400
    assert resp.json()["error"]["code"] == "INVALID_REQUEST"


@pytest.mark.asyncio
async def test_forward_skip_returns_400(client):
    poll_id = await _create_poll(client, "forward skip")

    resp = await client.put(
        f"/v1/admin/polls/{poll_id}/state", headers=ADMIN_AUTH, json={"state": "closed"}
    )
    assert resp.status_code == 400
    assert resp.json()["error"]["code"] == "INVALID_REQUEST"


@pytest.mark.asyncio
async def test_state_transition_on_nonexistent_poll_returns_404(client):
    resp = await client.put(
        f"/v1/admin/polls/{uuid.uuid4()}/state", headers=ADMIN_AUTH, json={"state": "active"}
    )
    assert resp.status_code == 404
    assert resp.json()["error"]["code"] == "NOT_FOUND"


@pytest.mark.asyncio
async def test_state_transition_non_admin_returns_403(client):
    poll_id = await _create_poll(client, "non-admin transition attempt")
    resp = await client.put(
        f"/v1/admin/polls/{poll_id}/state", headers=USER_AUTH, json={"state": "active"}
    )
    assert resp.status_code == 403
    assert resp.json()["error"]["code"] == "FORBIDDEN"


# --- GET /v1/admin/anomalies --------------------------------------------


def _seed_anomaly(
    conn, *, alert_type, severity, user_id=None, ip_address=None, description="test anomaly"
):
    alert_id = uuid.uuid4()
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO anomalies (alert_id, user_id, ip_address, alert_type, "
            "description, severity) VALUES (%s, %s, %s, %s, %s, %s)",
            (alert_id, user_id, ip_address, alert_type, description, severity),
        )
    conn.commit()
    return alert_id


@pytest.mark.asyncio
async def test_anomalies_filterable_by_severity_and_type(client, db_conn):
    marker = f"filter-test-{uuid.uuid4()}"
    warn_id = _seed_anomaly(
        db_conn,
        alert_type="rate_limit_exceeded",
        severity="warning",
        user_id=marker,
        description=marker,
    )
    crit_id = _seed_anomaly(
        db_conn,
        alert_type="bot_pattern_detected",
        severity="critical",
        ip_address="203.0.113.9",
        description=marker,
    )

    resp = await client.get(
        "/v1/admin/anomalies",
        headers=ADMIN_AUTH,
        params={"severity": "critical", "alert_type": "bot_pattern_detected"},
    )

    assert resp.status_code == 200
    ids = {a["alert_id"] for a in resp.json()["data"]["anomalies"]}
    assert str(crit_id) in ids
    assert str(warn_id) not in ids


@pytest.mark.asyncio
async def test_anomalies_counts_summary_present_for_all_types(client, db_conn):
    _seed_anomaly(
        db_conn,
        alert_type="duplicate_attempts_blocked",
        severity="critical",
        user_id="counts-test-user",
        ip_address="203.0.113.10",
    )

    resp = await client.get("/v1/admin/anomalies", headers=ADMIN_AUTH)

    assert resp.status_code == 200
    counts = resp.json()["data"]["counts"]
    assert set(counts) == {
        "rate_limit_exceeded",
        "duplicate_attempts_blocked",
        "bot_pattern_detected",
    }
    for type_counts in counts.values():
        assert set(type_counts) == {"warning", "critical"}
    assert counts["duplicate_attempts_blocked"]["critical"] >= 1


@pytest.mark.asyncio
async def test_anomalies_since_filter_excludes_older_rows(client, db_conn):
    old_id = _seed_anomaly(
        db_conn,
        alert_type="rate_limit_exceeded",
        severity="warning",
        user_id=f"since-test-old-{uuid.uuid4()}",
    )
    with db_conn.cursor() as cur:
        cur.execute(
            "UPDATE anomalies SET created_at = %s WHERE alert_id = %s",
            (datetime.now(timezone.utc) - timedelta(days=2), old_id),
        )
    db_conn.commit()

    recent_id = _seed_anomaly(
        db_conn,
        alert_type="rate_limit_exceeded",
        severity="warning",
        user_id=f"since-test-recent-{uuid.uuid4()}",
    )

    since = (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat()
    resp = await client.get(
        "/v1/admin/anomalies", headers=ADMIN_AUTH, params={"since": since, "limit": 500}
    )

    ids = {a["alert_id"] for a in resp.json()["data"]["anomalies"]}
    assert str(recent_id) in ids
    assert str(old_id) not in ids


@pytest.mark.asyncio
async def test_anomalies_never_touches_votes_table(client, db_conn, monkeypatch):
    """Contract test: mock the repo layer, assert only `anomalies_repo`
    is called — `GET /v1/admin/anomalies` must never query `votes`.
    """
    import src.api.routes.admin as admin_routes

    called = {"list": False, "counts": False}

    async def fake_list(**kwargs):
        called["list"] = True
        return []

    async def fake_counts(**kwargs):
        called["counts"] = True
        return {t: {"warning": 0, "critical": 0} for t in ("rate_limit_exceeded", "duplicate_attempts_blocked", "bot_pattern_detected")}

    monkeypatch.setattr(admin_routes.anomalies_repo, "list_anomalies", fake_list)
    monkeypatch.setattr(admin_routes.anomalies_repo, "counts_by_type", fake_counts)

    resp = await client.get("/v1/admin/anomalies", headers=ADMIN_AUTH)

    assert resp.status_code == 200
    assert called["list"] and called["counts"]
    assert resp.json()["data"]["anomalies"] == []


@pytest.mark.asyncio
async def test_anomalies_limit_capped_at_500(client):
    resp = await client.get(
        "/v1/admin/anomalies", headers=ADMIN_AUTH, params={"limit": 501}
    )
    assert resp.status_code == 400


@pytest.mark.asyncio
async def test_anomalies_non_admin_returns_403(client):
    resp = await client.get("/v1/admin/anomalies", headers=USER_AUTH)
    assert resp.status_code == 403
    assert resp.json()["error"]["code"] == "FORBIDDEN"
