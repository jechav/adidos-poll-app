"""Integration tests for I-003's API framework: routing, envelopes,
auth wiring, and error handling — exercised through a real ASGI request
via httpx, not by calling handlers directly.
"""

import pytest
from httpx import ASGITransport, AsyncClient

from src.api.app import app

AUTH = {"Authorization": "Bearer user-token"}
ADMIN_AUTH = {"Authorization": "Bearer admin:ops-1"}


@pytest.fixture
async def client():
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        yield ac


@pytest.mark.asyncio
async def test_health_check(client):
    resp = await client.get("/health")
    assert resp.status_code == 200
    body = resp.json()
    assert body["success"] is True
    assert body["data"]["status"] == "ok"
    assert "request_id" in body["meta"]
    assert "timestamp" in body["meta"]


@pytest.mark.asyncio
async def test_list_polls_requires_auth(client):
    resp = await client.get("/v1/polls")
    assert resp.status_code == 401
    body = resp.json()
    assert body["success"] is False
    assert body["error"]["code"] == "UNAUTHORIZED"
    assert "request_id" in body["meta"]


@pytest.mark.asyncio
async def test_list_polls_success_envelope(client):
    # Real handler as of I-011: no Redis is reachable in this test process,
    # so it falls back to I-010's `vote_counts` path (`meta.stale: true`)
    # rather than raising. `polls`/`pagination` content depends on
    # whatever rows other tests have seeded into the shared DB — see
    # tests/integration/test_list_polls.py and
    # tests/integration/test_list_polls_fallback.py for content coverage
    # against an isolated fixture; this test only asserts envelope shape.
    resp = await client.get("/v1/polls", headers=AUTH)
    assert resp.status_code == 200
    body = resp.json()
    assert body["success"] is True
    assert isinstance(body["data"]["polls"], list)
    assert body["data"]["pagination"]["limit"] == 20
    assert body["data"]["pagination"]["offset"] == 0
    assert "total" in body["data"]["pagination"]
    assert "timestamp" in body["meta"]
    assert "request_id" in body["meta"]
    assert resp.headers["x-request-id"] == body["meta"]["request_id"]


@pytest.mark.asyncio
async def test_cast_vote_invalid_body_returns_400(client):
    resp = await client.post("/v1/vote", headers=AUTH, json={"poll_id": "not-a-uuid"})
    assert resp.status_code == 400
    body = resp.json()
    assert body["error"]["code"] == "INVALID_REQUEST"
    assert body["error"]["details"]


@pytest.mark.asyncio
async def test_user_votes_requires_auth(client):
    resp = await client.get("/v1/user/votes")
    assert resp.status_code == 401


@pytest.mark.asyncio
async def test_admin_routes_reject_regular_user(client):
    resp = await client.get("/v1/admin/anomalies", headers=AUTH)
    assert resp.status_code == 403
    assert resp.json()["error"]["code"] == "FORBIDDEN"


@pytest.mark.asyncio
async def test_admin_create_poll(client):
    resp = await client.post(
        "/v1/admin/polls",
        headers=ADMIN_AUTH,
        json={"question": "Cats or dogs?", "answers": [{"text": "Cats"}, {"text": "Dogs"}]},
    )
    assert resp.status_code == 201
    assert resp.json()["data"]["question"] == "Cats or dogs?"


@pytest.mark.asyncio
async def test_admin_update_poll_state(client):
    # Real handler as of I-013: the poll must actually exist, so create
    # one first rather than PUTting against a made-up id — see
    # tests/integration/test_admin_endpoints.py for full I-013 coverage
    # (idempotency, backward/skip rejection, 404).
    created = await client.post(
        "/v1/admin/polls",
        headers=ADMIN_AUTH,
        json={
            "question": "State transition smoke test?",
            "answers": [{"text": "Yes"}, {"text": "No"}],
        },
    )
    poll_id = created.json()["data"]["poll_id"]

    resp = await client.put(
        f"/v1/admin/polls/{poll_id}/state",
        headers=ADMIN_AUTH,
        json={"state": "active"},
    )
    assert resp.status_code == 200
    assert resp.json()["data"]["state"] == "active"


@pytest.mark.asyncio
async def test_admin_update_poll_state_not_found_returns_404(client):
    resp = await client.put(
        "/v1/admin/polls/8f14e45f-ceea-4f5a-9d5a-6c7a3f2f1a1a/state",
        headers=ADMIN_AUTH,
        json={"state": "active"},
    )
    assert resp.status_code == 404
    assert resp.json()["error"]["code"] == "NOT_FOUND"


@pytest.mark.asyncio
async def test_admin_anomalies(client):
    resp = await client.get("/v1/admin/anomalies", headers=ADMIN_AUTH)
    assert resp.status_code == 200
    data = resp.json()["data"]
    assert isinstance(data["anomalies"], list)
    assert set(data["counts"]) == {
        "rate_limit_exceeded",
        "duplicate_attempts_blocked",
        "bot_pattern_detected",
    }


@pytest.mark.asyncio
async def test_body_over_1mb_rejected(client):
    huge_payload = "x" * (1_000_001)
    resp = await client.post(
        "/v1/admin/polls",
        headers={**ADMIN_AUTH, "Content-Length": str(len(huge_payload))},
        content=huge_payload,
    )
    assert resp.status_code == 400
    assert resp.json()["error"]["code"] == "INVALID_REQUEST"


@pytest.mark.asyncio
async def test_body_over_1mb_rejected_without_content_length(client):
    async def chunks():
        for _ in range(11):
            yield b"x" * 100_000  # 1.1MB total, streamed with no declared length

    resp = await client.post("/v1/admin/polls", headers=ADMIN_AUTH, content=chunks())
    assert resp.status_code == 400
    assert resp.json()["error"]["code"] == "INVALID_REQUEST"
