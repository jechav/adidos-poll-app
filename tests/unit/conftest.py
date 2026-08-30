"""Shared fixtures for I-021's vote-acceptance/rate-limit unit suite.

Everything here stands in for real infrastructure per the ticket's
"Implementation Decisions": `fakeredis` in place of the Redis cluster,
and an in-memory poll/answer store (monkeypatched over
`src.services.polls.load_poll_and_answer`) in place of PostgreSQL.

A real SQLite-backed `test_db` fixture was considered (as the ticket's
sample `conftest.py` sketches) but rejected: `src/services/polls.py`
talks to PostgreSQL directly via `psycopg`, not through a swappable
`get_db`/SQLAlchemy session dependency, so there is no seam that a SQLite
engine could stand behind without rewriting the production data-access
layer. `FakePollStore` monkeypatches the same seam
`tests/unit/test_cast_vote_route.py` already established for I-005's
route tests, which keeps this suite's contract identical (HTTP status
codes, response bodies, queue contents) while never touching a real
database — real-Postgres coverage of the query layer itself is
`tests/integration/test_vote_acceptance.py`'s job (I-022), not this
suite's.
"""

from __future__ import annotations

import uuid

import pytest
import pytest_asyncio
from fakeredis.aioredis import FakeRedis
from httpx import ASGITransport, AsyncClient

import src.api.routes.user as user_routes
import src.services.rate_limit as rate_limit_module
from src.api.app import app
from src.cache.redis_client import redis_dependency
from src.services.polls import Answer, Poll


@pytest_asyncio.fixture
async def fake_redis():
    """A real (async, in-memory) Redis protocol implementation — not a
    hand-rolled stub — so this suite exercises the actual `SET NX`,
    `INCR`/`EXPIRE`, pipeline, and TTL semantics the production code
    depends on, per the ticket's "Test Doubles, Not Real Infrastructure"
    section.
    """
    redis = FakeRedis(decode_responses=True)
    yield redis
    await redis.flushall()
    await redis.aclose()


class FakePollStore:
    """In-memory stand-in for I-001's `polls`/`answers` tables."""

    def __init__(self) -> None:
        self._polls: dict[uuid.UUID, Poll] = {}
        self._answers: dict[uuid.UUID, Answer] = {}

    def add_poll(self, state: str = "active") -> tuple[uuid.UUID, uuid.UUID]:
        """Register a poll with one answer belonging to it; returns
        `(poll_id, answer_id)`.
        """
        poll_id = uuid.uuid4()
        answer_id = self.add_answer(poll_id)
        self._polls[poll_id] = Poll(poll_id=poll_id, state=state)
        return poll_id, answer_id

    def add_answer(self, poll_id: uuid.UUID) -> uuid.UUID:
        answer_id = uuid.uuid4()
        self._answers[answer_id] = Answer(answer_id=answer_id, poll_id=poll_id)
        return answer_id

    async def load(
        self, poll_id: uuid.UUID, answer_id: uuid.UUID
    ) -> tuple[Poll | None, Answer | None]:
        return self._polls.get(poll_id), self._answers.get(answer_id)


@pytest.fixture
def poll_store(monkeypatch) -> FakePollStore:
    store = FakePollStore()
    monkeypatch.setattr(user_routes, "load_poll_and_answer", store.load)
    return store


@pytest_asyncio.fixture
async def client_factory(fake_redis, monkeypatch):
    """Factory for `httpx.AsyncClient`s against the real ASGI app
    (routing, middleware, dependency injection, exception handlers) —
    no socket bound, per the ticket's "HTTP layer" decision. Each call
    can bind a different simulated client IP (`request.client.host` in
    `cast_vote`), which a single fixed-IP client can't do -- needed for
    the per-IP rate-limit test cases in test_rate_limiting.py.

    `src/services/rate_limit.py` fetches its own client via the module-
    level `get_redis()` rather than the `redis_dependency` FastAPI
    dependency (unlike `redis_dependency`'s other consumers), so it needs
    its own monkeypatch onto `fake_redis` -- otherwise it would try to
    reach a real Redis Cluster. Same seam `tests/unit/test_rate_limit.py`
    already uses.
    """
    app.dependency_overrides[redis_dependency] = lambda: fake_redis

    async def _get_fake_redis():
        return fake_redis

    monkeypatch.setattr(rate_limit_module, "get_redis", _get_fake_redis)

    clients: list[AsyncClient] = []

    def _make(ip: str = "127.0.0.1") -> AsyncClient:
        transport = ASGITransport(app=app, client=(ip, 0))
        ac = AsyncClient(transport=transport, base_url="http://test")
        clients.append(ac)
        return ac

    yield _make

    for ac in clients:
        await ac.aclose()
    app.dependency_overrides.clear()


@pytest_asyncio.fixture
async def client(client_factory) -> AsyncClient:
    """A single default-IP client -- the common case for tests that
    don't care about per-IP behavior.
    """
    return client_factory()


def auth_header(user_id: str) -> dict[str, str]:
    """`get_current_user` (I-004) treats the bearer token's content as
    the user_id (see `src/api/dependencies/auth.py`'s module docstring),
    so this is real auth going through the real dependency and its
    `fake_redis`-backed cache -- not overridden/bypassed.
    """
    return {"Authorization": f"Bearer {user_id}"}


async def cast_vote(client: AsyncClient, poll_id: uuid.UUID, answer_id: uuid.UUID, user_id: str):
    """`POST /v1/vote` as `user_id`, for the common case of a caller that
    only cares about the response (status code / error code / body) --
    factored out since most test cases in this suite make this exact
    call, differing only in poll/answer/user.
    """
    return await client.post(
        "/v1/vote",
        headers=auth_header(user_id),
        json={"poll_id": str(poll_id), "answer_id": str(answer_id)},
    )
