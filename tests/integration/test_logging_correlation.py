"""Integration tests for I-018's cross-process correlation story: cast a
vote through the real `POST /v1/vote` route, drain the same queue entry
through the real worker dequeue/process pipeline, and assert the same
`request_id` and `vote_id` appear in both processes' log output — the
`grep request_id=...` scenario the ticket is built around.

Runs entirely against fakes (no live PostgreSQL/Redis needed): the goal
here is verifying the *contract* between the two independently-declared
`VotePayload` classes (`src.services.vote_queue.VotePayload`, the API
side; `src.worker.models.VotePayload`, the worker side) and the shared
`is_sampled`/`configure_logging` modules actually agree end-to-end — a
field-name mismatch between the two would be caught here even though
neither module's own unit tests would ever exercise the other's code.
Live-Postgres/Redis coverage of the individual halves already exists in
tests/integration/test_vote_acceptance.py and
tests/integration/test_vote_processor_pipeline.py.
"""

import json
import uuid

import pytest
from httpx import ASGITransport, AsyncClient

import src.api.routes.user as user_routes
from src.api.app import app
from src.api.dependencies.auth import get_current_user
from src.cache.redis_client import redis_dependency
from src.logging.config import configure_logging
from src.schemas.user_context import UserContext
from src.services.polls import Answer, Poll
from src.services.uniqueness import DuplicateVoteError
from src.worker.db import ShardConnectionPool
from src.worker.processor import process_batch
from src.worker.queue_consumer import QUEUE_KEY, dequeue_batch

# uuid.UUID(int=N * 1000) is always sampled (is_sampled checks % 1000 == 0).
SAMPLED_VOTE_ID = uuid.UUID(int=42_000)


class FakeRedis:
    """Minimal fake covering both the route's needs (I-015's
    `enforce_not_blocked`, the real `enqueue_vote`'s `lpush`) and the
    worker's needs (`brpop`, `incr`) against the same underlying
    `queue:votes` list — this is what makes it possible to drain, in the
    same test, exactly what the route enqueued.
    """

    def __init__(self):
        self.lists: dict[str, list[str]] = {}
        self.counters: dict[str, int] = {}
        self.deleted: list[str] = []

    async def exists(self, key: str) -> int:
        return 0

    async def delete(self, key: str) -> int:
        self.deleted.append(key)
        return 1

    async def lpush(self, key: str, value: str) -> int:
        lst = self.lists.setdefault(key, [])
        lst.insert(0, value)
        return len(lst)

    async def brpop(self, key: str, timeout: int = 1):
        lst = self.lists.get(key)
        if lst:
            return key, lst.pop()
        return None

    async def incr(self, key: str) -> int:
        self.counters[key] = self.counters.get(key, 0) + 1
        return self.counters[key]

    async def llen(self, key: str) -> int:
        return len(self.lists.get(key, []))


class FakePool:
    async def get_connection(self, shard_id: int):
        return object()

    async def invalidate(self, shard_id: int) -> None:
        pass


def _json_lines(capsys):
    out = capsys.readouterr().out.strip()
    return [json.loads(line) for line in out.splitlines() if line.strip()]


@pytest.fixture(autouse=True)
def _configure(capsys):
    configure_logging()
    capsys.readouterr()


@pytest.fixture
async def client():
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        yield ac
    app.dependency_overrides.clear()


def _override_auth(user_id: str):
    app.dependency_overrides[get_current_user] = lambda: UserContext(user_id=user_id)


@pytest.mark.asyncio
async def test_a_sampled_votes_request_id_and_vote_id_correlate_api_to_worker(
    client, monkeypatch, capsys
):
    poll_id, answer_id = uuid.uuid4(), uuid.uuid4()
    fake_redis = FakeRedis()
    _override_auth("user-corr")
    app.dependency_overrides[redis_dependency] = lambda: fake_redis

    async def fake_load_poll_and_answer(pid, aid):
        return Poll(poll_id=poll_id, state="active"), Answer(answer_id=answer_id, poll_id=poll_id)

    async def fake_check_rate_limit(**kwargs):
        return None

    async def fake_check_and_reserve_uniqueness(redis, user_id, pid):
        return None

    async def fake_batch_insert(conn, votes):
        return list(votes)

    monkeypatch.setattr(user_routes, "load_poll_and_answer", fake_load_poll_and_answer)
    monkeypatch.setattr(user_routes, "check_rate_limit", fake_check_rate_limit)
    monkeypatch.setattr(
        user_routes, "check_and_reserve_uniqueness", fake_check_and_reserve_uniqueness
    )
    # The route generates vote_id itself; pin it to a value `is_sampled`
    # always selects so this test doesn't depend on random UUID luck.
    monkeypatch.setattr(user_routes, "uuid4", lambda: SAMPLED_VOTE_ID)

    import src.worker.processor as processor_module

    monkeypatch.setattr(processor_module, "batch_insert_votes", fake_batch_insert)

    # --- API side: cast the vote ---
    resp = await client.post(
        "/v1/vote",
        headers={"Authorization": "Bearer user-corr", "x-request-id": "req-correlate-me"},
        json={"poll_id": str(poll_id), "answer_id": str(answer_id)},
    )
    assert resp.status_code == 202

    api_lines = _json_lines(capsys)
    api_accepted = [line for line in api_lines if line["event"] == "vote_accepted"]
    assert len(api_accepted) == 1
    assert api_accepted[0]["request_id"] == "req-correlate-me"
    assert api_accepted[0]["vote_id"] == str(SAMPLED_VOTE_ID)

    # --- Worker side: drain the exact same queue entry the route pushed ---
    batch = await dequeue_batch(fake_redis, min_size=1, max_size=1, timeout_s=1)
    assert len(batch) == 1
    assert batch[0].vote_id == SAMPLED_VOTE_ID
    assert batch[0].request_id == "req-correlate-me"

    await process_batch(0, batch, FakePool(), fake_redis)

    worker_lines = _json_lines(capsys)
    dequeued = [line for line in worker_lines if line["event"] == "vote_dequeued"]
    written = [line for line in worker_lines if line["event"] == "vote_written"]
    assert len(dequeued) == 1
    assert len(written) == 1

    # The whole point: one grep-able request_id (and vote_id) ties the
    # API accept line to the worker dequeue/write lines together.
    for line in (dequeued[0], written[0]):
        assert line["request_id"] == "req-correlate-me"
        assert line["vote_id"] == str(SAMPLED_VOTE_ID)


@pytest.mark.asyncio
async def test_a_forced_rejection_is_logged_regardless_of_sampling(client, monkeypatch, capsys):
    """Duplicate-vote rejection must be logged at 100% even though this
    vote_id is deliberately *not* sampled (decision #15's unconditional
    OR: errors/rejections bypass the sampling decision entirely).
    """
    poll_id, answer_id = uuid.uuid4(), uuid.uuid4()
    fake_redis = FakeRedis()
    _override_auth("user-dup")
    app.dependency_overrides[redis_dependency] = lambda: fake_redis

    async def fake_load_poll_and_answer(pid, aid):
        return Poll(poll_id=poll_id, state="active"), Answer(answer_id=answer_id, poll_id=poll_id)

    async def fake_check_rate_limit(**kwargs):
        return None

    async def fake_check_and_reserve_uniqueness(redis, user_id, pid):
        raise DuplicateVoteError(user_id, str(pid))

    async def fake_record_duplicate_attempt(user_id, ip, pid, redis):
        return None

    monkeypatch.setattr(user_routes, "load_poll_and_answer", fake_load_poll_and_answer)
    monkeypatch.setattr(user_routes, "check_rate_limit", fake_check_rate_limit)
    monkeypatch.setattr(
        user_routes, "check_and_reserve_uniqueness", fake_check_and_reserve_uniqueness
    )
    monkeypatch.setattr(user_routes, "record_duplicate_attempt", fake_record_duplicate_attempt)
    # uuid.UUID(int=1) is never sampled — proves the rejection log doesn't
    # depend on is_sampled at all.
    monkeypatch.setattr(user_routes, "uuid4", lambda: uuid.UUID(int=1))

    resp = await client.post(
        "/v1/vote",
        headers={"Authorization": "Bearer user-dup", "x-request-id": "req-dup-case"},
        json={"poll_id": str(poll_id), "answer_id": str(answer_id)},
    )

    assert resp.status_code == 409
    lines = _json_lines(capsys)
    rejected = [line for line in lines if line["event"] == "vote_rejected"]
    assert len(rejected) == 1
    assert rejected[0]["request_id"] == "req-dup-case"
    assert rejected[0]["outcome"] == "duplicate"
