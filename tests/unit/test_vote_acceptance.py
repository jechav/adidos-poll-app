"""I-021 Test Module 1: Vote Acceptance Tests.

Exercises `POST /v1/vote` end-to-end through the real ASGI app (routing,
auth, rate limiting, uniqueness, queueing, error handlers) with
`fakeredis` standing in for the Redis cluster and `FakePollStore`
(conftest.py) standing in for PostgreSQL's `polls`/`answers` tables.

Per SPECIFICATION.md's testing philosophy (quoted in the ticket): these
tests assert only on external behavior -- HTTP status codes, response
bodies, and `queue:votes` contents (the observable "this vote is
counted" signal at this layer; I-008's worker turning a queued vote into
a durable count is covered by tests/unit/test_worker_processor.py and
tests/unit/test_result_aggregator.py, not repeated here). No test reads
`vote:user:...` reservation keys or `queue:votes`' internal JSON shape
beyond the fields a client-observable contract would care about.
"""

from __future__ import annotations

import json
import uuid

import pytest

from tests.unit.conftest import cast_vote


@pytest.mark.asyncio
async def test_vote_on_active_poll_returns_202_and_is_counted(client, poll_store, fake_redis):
    poll_id, answer_id = poll_store.add_poll(state="active")

    resp = await cast_vote(client, poll_id, answer_id, "user-1")

    assert resp.status_code == 202
    body = resp.json()
    assert body["data"]["status"] == "queued"
    assert body["data"]["poll_id"] == str(poll_id)
    assert body["data"]["answer_id"] == str(answer_id)

    # The vote reaching queue:votes -- the hand-off I-008's worker drains
    # into a durable, counted vote -- is this layer's observable "it was
    # counted" signal.
    assert await fake_redis.llen("queue:votes") == 1
    queued = json.loads(await fake_redis.lpop("queue:votes"))
    assert queued["user_id"] == "user-1"
    assert queued["poll_id"] == str(poll_id)
    assert queued["answer_id"] == str(answer_id)


@pytest.mark.asyncio
async def test_duplicate_vote_returns_409(client, poll_store):
    poll_id, answer_id = poll_store.add_poll(state="active")

    first = await cast_vote(client, poll_id, answer_id, "user-2")
    assert first.status_code == 202

    second = await cast_vote(client, poll_id, answer_id, "user-2")

    assert second.status_code == 409
    assert second.json()["error"]["code"] == "DUPLICATE_VOTE"


@pytest.mark.asyncio
async def test_vote_on_closed_poll_returns_400(client, poll_store):
    poll_id, answer_id = poll_store.add_poll(state="closed")

    resp = await cast_vote(client, poll_id, answer_id, "user-3")

    assert resp.status_code == 400
    assert resp.json()["error"]["code"] == "POLL_CLOSED"


@pytest.mark.asyncio
async def test_vote_with_invalid_token_returns_401(client, poll_store):
    poll_id, answer_id = poll_store.add_poll(state="active")

    # No Authorization header at all.
    missing = await client.post(
        "/v1/vote",
        json={"poll_id": str(poll_id), "answer_id": str(answer_id)},
    )
    assert missing.status_code == 401
    assert missing.json()["error"]["code"] == "UNAUTHORIZED"

    # Malformed scheme (not "Bearer ...").
    malformed = await client.post(
        "/v1/vote",
        headers={"Authorization": "Token abc123"},
        json={"poll_id": str(poll_id), "answer_id": str(answer_id)},
    )
    assert malformed.status_code == 401
    assert malformed.json()["error"]["code"] == "UNAUTHORIZED"


@pytest.mark.asyncio
async def test_vote_with_missing_answer_id_returns_400(client, poll_store):
    poll_id, _ = poll_store.add_poll(state="active")

    resp = await client.post(
        "/v1/vote",
        headers={"Authorization": "Bearer user-4"},
        json={"poll_id": str(poll_id)},
    )

    assert resp.status_code == 400
    assert resp.json()["error"]["code"] == "INVALID_REQUEST"


@pytest.mark.asyncio
async def test_vote_on_nonexistent_poll_returns_404(client, poll_store):
    """Not one of the ticket's five enumerated cases, but the same
    `load_poll_and_answer`/`validate_vote_target` seam exercised above
    with the poll/answer simply absent from the store -- worth a line to
    pin down the not-found branch's status code since the other four
    branches of `validate_vote_target` are already covered.
    """
    resp = await cast_vote(client, uuid.uuid4(), uuid.uuid4(), "user-5")

    assert resp.status_code == 404
    assert resp.json()["error"]["code"] == "NOT_FOUND"
