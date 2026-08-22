"""Unit tests for I-005's `POST /v1/vote` composition logic, independent of
live Postgres/Redis: `load_poll_and_answer`, `check_rate_limit`,
`check_and_reserve_uniqueness`, and `enqueue_vote` are monkeypatched so
these exercise only the handler's own sequencing and error-recovery
decisions (tests/integration/test_vote_acceptance.py covers the same
paths against real infra).
"""

import uuid

import pytest
from httpx import ASGITransport, AsyncClient

import src.api.routes.user as user_routes
from src.api.app import app
from src.api.dependencies.auth import get_current_user
from src.cache.redis_client import redis_dependency
from src.schemas.user_context import UserContext
from src.services.polls import Answer, Poll
from src.services.vote_queue import VoteQueueUnavailableError


class FakeRedis:
    def __init__(self):
        self.deleted: list[str] = []

    async def delete(self, key: str) -> int:
        self.deleted.append(key)
        return 1


@pytest.fixture
async def client():
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        yield ac
    app.dependency_overrides.clear()


@pytest.fixture
def fake_redis():
    return FakeRedis()


def _override_auth(user_id="user-1"):
    app.dependency_overrides[get_current_user] = lambda: UserContext(user_id=user_id)


@pytest.mark.asyncio
async def test_enqueue_failure_releases_uniqueness_reservation_and_returns_503(
    client, fake_redis, monkeypatch
):
    poll_id, answer_id = uuid.uuid4(), uuid.uuid4()
    _override_auth("user-1")
    app.dependency_overrides[redis_dependency] = lambda: fake_redis

    async def fake_load_poll_and_answer(pid, aid):
        return Poll(poll_id=poll_id, state="active"), Answer(answer_id=answer_id, poll_id=poll_id)

    async def fake_check_rate_limit(**kwargs):
        return None

    async def fake_check_and_reserve_uniqueness(redis, user_id, pid):
        return None

    async def fake_enqueue_vote(redis, payload):
        raise VoteQueueUnavailableError()

    monkeypatch.setattr(user_routes, "load_poll_and_answer", fake_load_poll_and_answer)
    monkeypatch.setattr(user_routes, "check_rate_limit", fake_check_rate_limit)
    monkeypatch.setattr(
        user_routes, "check_and_reserve_uniqueness", fake_check_and_reserve_uniqueness
    )
    monkeypatch.setattr(user_routes, "enqueue_vote", fake_enqueue_vote)

    resp = await client.post(
        "/v1/vote",
        headers={"Authorization": "Bearer user-1"},
        json={"poll_id": str(poll_id), "answer_id": str(answer_id)},
    )

    assert resp.status_code == 503
    assert resp.json()["error"]["code"] == "SERVICE_UNAVAILABLE"
    assert fake_redis.deleted == [f"vote:user:user-1:poll:{poll_id}"]


@pytest.mark.asyncio
async def test_valid_vote_calls_rate_limit_before_uniqueness(client, fake_redis, monkeypatch):
    poll_id, answer_id = uuid.uuid4(), uuid.uuid4()
    _override_auth("user-2")
    app.dependency_overrides[redis_dependency] = lambda: fake_redis

    call_order: list[str] = []

    async def fake_load_poll_and_answer(pid, aid):
        return Poll(poll_id=poll_id, state="active"), Answer(answer_id=answer_id, poll_id=poll_id)

    async def fake_check_rate_limit(**kwargs):
        call_order.append("rate_limit")

    async def fake_check_and_reserve_uniqueness(redis, user_id, pid):
        call_order.append("uniqueness")

    async def fake_enqueue_vote(redis, payload):
        call_order.append("enqueue")

    monkeypatch.setattr(user_routes, "load_poll_and_answer", fake_load_poll_and_answer)
    monkeypatch.setattr(user_routes, "check_rate_limit", fake_check_rate_limit)
    monkeypatch.setattr(
        user_routes, "check_and_reserve_uniqueness", fake_check_and_reserve_uniqueness
    )
    monkeypatch.setattr(user_routes, "enqueue_vote", fake_enqueue_vote)

    resp = await client.post(
        "/v1/vote",
        headers={"Authorization": "Bearer user-2"},
        json={"poll_id": str(poll_id), "answer_id": str(answer_id)},
    )

    assert resp.status_code == 202
    assert call_order == ["rate_limit", "uniqueness", "enqueue"]
