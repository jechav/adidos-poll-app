"""Integration tests for I-022's Module 6 (Admin Poll Management): the
poll lifecycle enforced end to end through the real admin + vote-casting
routes, against a real ASGI app and real PostgreSQL.

`tests/integration/test_admin_endpoints.py` already covers most of this
module's individual state-machine mechanics for real (poll creation
landing in `draft`, one-step-forward transitions, idempotent same-state
requests, backward/skip transitions returning 400, and non-admin 403 on
both `POST /v1/admin/polls` and the state-transition route) -- this file
does not re-implement those, to avoid two suites asserting the same
thing on the same code path. Its job is the two things that file's
scope (the admin routes in isolation) can't show: **whether a state
transition actually changes what `POST /v1/vote` and `GET /v1/polls`
do** -- i.e. that the write-side state machine and the read/vote-side
enforcement agree, per this ticket's user stories #9-12 and #14.

Skips automatically if PostgreSQL isn't reachable, mirroring every other
file in this directory.
"""

import pytest

from tests.integration.conftest import ADMIN_AUTH, auth_header, cast_vote


async def _create_poll(client, question: str) -> tuple[str, str, str]:
    resp = await client.post(
        "/v1/admin/polls",
        headers=ADMIN_AUTH,
        json={"question": question, "answers": [{"text": "A"}, {"text": "B"}]},
    )
    assert resp.status_code == 201, resp.text
    data = resp.json()["data"]
    return data["poll_id"], data["answers"][0]["answer_id"], data["answers"][1]["answer_id"]


async def _transition(client, poll_id: str, state: str):
    return await client.put(
        f"/v1/admin/polls/{poll_id}/state", headers=ADMIN_AUTH, json={"state": state}
    )


@pytest.mark.asyncio
async def test_admin_creates_poll_lands_in_draft_and_draft_polls_reject_votes(client, redis):
    """User story #9: a freshly created poll lands in `draft` -- and,
    tying creation to enforcement, a vote against it is rejected (draft
    is not `active`) rather than silently accepted.
    """
    poll_id, answer_a, _answer_b = await _create_poll(client, "draft creation + vote rejection")

    resp = await cast_vote(client, redis, user_id="draft-voter", poll_id=poll_id, answer_id=answer_a)

    assert resp.status_code == 400
    assert resp.json()["error"]["code"] == "POLL_CLOSED"


@pytest.mark.asyncio
async def test_admin_activates_poll_enables_voting(client, redis):
    """User story #10: the draft -> active transition is what actually
    makes `POST /v1/vote` start accepting votes for this poll -- not
    just a state label that voting ignores.
    """
    poll_id, answer_a, _answer_b = await _create_poll(client, "activation enables voting")

    rejected = await cast_vote(
        client, redis, user_id="pre-activation-voter", poll_id=poll_id, answer_id=answer_a
    )
    assert rejected.status_code == 400

    activated = await _transition(client, poll_id, "active")
    assert activated.status_code == 200
    assert activated.json()["data"]["state"] == "active"

    accepted = await cast_vote(
        client, redis, user_id="post-activation-voter", poll_id=poll_id, answer_id=answer_a
    )
    assert accepted.status_code == 202


@pytest.mark.asyncio
async def test_admin_closes_poll_immediately_rejects_new_votes(client, redis):
    """User story #11: closing an active poll takes effect immediately --
    a vote attempted right after the close transition (not one seeded
    directly into a `closed`-state row, unlike
    `tests/integration/test_vote_acceptance.py::test_vote_on_closed_poll_returns_400_poll_closed`)
    is rejected with 400.
    """
    poll_id, answer_a, _answer_b = await _create_poll(client, "close rejects new votes")
    await _transition(client, poll_id, "active")
    accepted = await cast_vote(
        client, redis, user_id="before-close-voter", poll_id=poll_id, answer_id=answer_a
    )
    assert accepted.status_code == 202

    closed = await _transition(client, poll_id, "closed")
    assert closed.status_code == 200

    rejected = await cast_vote(
        client, redis, user_id="after-close-voter", poll_id=poll_id, answer_id=answer_a
    )
    assert rejected.status_code == 400
    assert rejected.json()["error"]["code"] == "POLL_CLOSED"


@pytest.mark.asyncio
async def test_admin_archives_poll_removed_from_active_list_visible_in_history(client):
    """User story #12: archiving changes *visibility*, not history --
    the poll disappears from `GET /v1/polls?state=active` but is still
    reachable via `GET /v1/polls?state=archived`.
    """
    poll_id, _answer_a, _answer_b = await _create_poll(client, "archive visibility test")
    await _transition(client, poll_id, "active")
    await _transition(client, poll_id, "closed")
    archived = await _transition(client, poll_id, "archived")
    assert archived.status_code == 200

    active_list = await client.get(
        "/v1/polls", headers=auth_header("archive-reader"), params={"state": "active"}
    )
    archived_list = await client.get(
        "/v1/polls", headers=auth_header("archive-reader"), params={"state": "archived"}
    )

    assert poll_id not in {p["poll_id"] for p in active_list.json()["data"]["polls"]}
    assert poll_id in {p["poll_id"] for p in archived_list.json()["data"]["polls"]}


@pytest.mark.asyncio
async def test_closed_to_active_backward_transition_rejected_then_vote_still_rejected(
    client, redis
):
    """User story #14, tied to enforcement: not only does the admin route
    reject a `closed -> active` request with 400 (already covered by
    `tests/integration/test_admin_endpoints.py::test_backward_transition_returns_400`),
    the poll's *actual* votability is unaffected by the rejected attempt
    -- a vote right after still gets 400, proving the rejected request
    was a true no-op rather than a partial write.
    """
    poll_id, answer_a, _answer_b = await _create_poll(client, "backward transition is a true no-op")
    await _transition(client, poll_id, "active")
    await _transition(client, poll_id, "closed")

    backward = await _transition(client, poll_id, "active")
    assert backward.status_code == 400
    assert backward.json()["error"]["code"] == "INVALID_REQUEST"

    still_rejected = await cast_vote(
        client, redis, user_id="backward-transition-voter", poll_id=poll_id, answer_id=answer_a
    )
    assert still_rejected.status_code == 400
    assert still_rejected.json()["error"]["code"] == "POLL_CLOSED"
