"""I-021 Test Module 2: Rate Limit Tests.

Exercises `POST /v1/vote`'s rate limiting (I-007) and 3-strike duplicate
block (I-015) end-to-end through the real ASGI app, with `fakeredis`
standing in for the Redis cluster.

Per SPECIFICATION.md's testing philosophy, no test here asserts on the
sliding-window/fixed-window bucket math or on Redis key names -- only on
HTTP status codes and response bodies. The actual limits enforced by
`src/services/rate_limit.py` -- 5 votes/minute per user, 50 votes/minute
per IP -- differ from the illustrative 25/50 figures in this ticket's
prose (written before I-007 was implemented); the test cases below use
the real, implemented limits, which is the whole point of a suite
exercising the real code path rather than restating the spec's numbers.

Window-reset uses `freezegun` (added to requirements.txt for this
ticket) -- verified to work with `fakeredis.aioredis.FakeRedis`'s
`EXPIRE`/TTL handling, since `fakeredis` reads the same `time.time()`
freezegun patches.
"""

from __future__ import annotations

from datetime import timedelta

import pytest
from freezegun import freeze_time

import src.services.duplicate_block as duplicate_block
from tests.unit.conftest import cast_vote


@pytest.fixture(autouse=True)
def no_anomaly_db_write(monkeypatch):
    """I-015's 3rd-strike block writes an `anomalies` row via a real
    Postgres connection (`src/services/anomalies.py`), which this unit
    suite has no access to. The anomaly *write* is I-016's concern
    (already covered by tests/unit/test_duplicate_block.py and
    tests/unit/test_report_anomalies.py); this suite's job is only the
    externally-observable HTTP contract (429 during the block window),
    so the write is stubbed to a no-op here rather than skipped/faked
    with a real DB.
    """

    async def _noop_create(**kwargs):
        return None

    monkeypatch.setattr(duplicate_block.anomalies_repo, "create", _noop_create)


@pytest.mark.asyncio
async def test_fifth_vote_within_minute_succeeds(client, poll_store):
    # 5 distinct polls so I-006 uniqueness never blocks any of these.
    for _ in range(5):
        poll_id, answer_id = poll_store.add_poll(state="active")
        resp = await cast_vote(client, poll_id, answer_id, "user-1")
        assert resp.status_code == 202


@pytest.mark.asyncio
async def test_sixth_vote_within_minute_returns_429(client, poll_store):
    for _ in range(5):
        poll_id, answer_id = poll_store.add_poll(state="active")
        resp = await cast_vote(client, poll_id, answer_id, "user-2")
        assert resp.status_code == 202

    poll_id, answer_id = poll_store.add_poll(state="active")
    sixth = await cast_vote(client, poll_id, answer_id, "user-2")

    assert sixth.status_code == 429
    assert sixth.json()["error"]["code"] == "RATE_LIMIT_EXCEEDED"


@pytest.mark.asyncio
async def test_rate_limit_resets_after_window(client, poll_store):
    with freeze_time("2026-01-01 00:00:00") as frozen:
        for _ in range(5):
            poll_id, answer_id = poll_store.add_poll(state="active")
            resp = await cast_vote(client, poll_id, answer_id, "user-3")
            assert resp.status_code == 202

        blocked_poll_id, blocked_answer_id = poll_store.add_poll(state="active")
        blocked = await cast_vote(client, blocked_poll_id, blocked_answer_id, "user-3")
        assert blocked.status_code == 429

        # The window's 61s TTL elapses.
        frozen.tick(timedelta(seconds=62))

        after_poll_id, after_answer_id = poll_store.add_poll(state="active")
        after_reset = await cast_vote(client, after_poll_id, after_answer_id, "user-3")
        assert after_reset.status_code == 202


@pytest.mark.asyncio
async def test_two_ips_each_voting_up_to_the_per_ip_limit_both_succeed(
    client_factory, poll_store
):
    """Ticket story #9's shape (two IPs, each under the per-IP ceiling,
    both succeed) exercised against the real per-IP limit implemented by
    I-007 -- 50/minute, not the ticket prose's illustrative 25 -- using a
    distinct user per vote so the per-user 5/minute limit never trips
    first and masks the per-IP behavior being tested.
    """
    client_a = client_factory("1.1.1.1")
    client_b = client_factory("2.2.2.2")

    for i in range(50):
        poll_id, answer_id = poll_store.add_poll(state="active")
        resp = await cast_vote(client_a, poll_id, answer_id, f"user-a-{i}")
        assert resp.status_code == 202, f"ip 1.1.1.1 vote {i} failed: {resp.json()}"

    for i in range(50):
        poll_id, answer_id = poll_store.add_poll(state="active")
        resp = await cast_vote(client_b, poll_id, answer_id, f"user-b-{i}")
        assert resp.status_code == 202, f"ip 2.2.2.2 vote {i} failed: {resp.json()}"


@pytest.mark.asyncio
async def test_fifty_first_vote_from_either_ip_returns_429(client_factory, poll_store):
    """The 51st vote from an IP is the first to be rejected under I-007's
    real 50/minute per-IP ceiling (the ticket prose's 26th assumed a
    25/minute limit that isn't what I-007 implements). Checked against
    both IPs to confirm the ceiling applies independently to each.
    """
    for ip in ("3.3.3.3", "4.4.4.4"):
        client = client_factory(ip)
        for i in range(50):
            poll_id, answer_id = poll_store.add_poll(state="active")
            resp = await cast_vote(client, poll_id, answer_id, f"user-{ip}-{i}")
            assert resp.status_code == 202

        poll_id, answer_id = poll_store.add_poll(state="active")
        fifty_first = await cast_vote(client, poll_id, answer_id, f"user-{ip}-50")

        assert fifty_first.status_code == 429
        assert fifty_first.json()["error"]["code"] == "RATE_LIMIT_EXCEEDED"


@pytest.mark.asyncio
async def test_user_blocked_for_1_hour_after_3_duplicate_attempts(client, poll_store):
    poll_id, answer_id = poll_store.add_poll(state="active")

    first = await cast_vote(client, poll_id, answer_id, "user-dup")
    assert first.status_code == 202

    # 3 duplicate attempts on the same already-voted poll: I-015 counts
    # strikes on every 409, blocking the (user, ip) pair on the 3rd.
    for attempt in range(3):
        dup = await cast_vote(client, poll_id, answer_id, "user-dup")
        assert dup.status_code == 409, f"attempt {attempt} should still be 409 DUPLICATE_VOTE"

    # A subsequent attempt from the same user+ip during the block window
    # is rejected before ever reaching the duplicate check again.
    other_poll_id, other_answer_id = poll_store.add_poll(state="active")
    blocked = await cast_vote(client, other_poll_id, other_answer_id, "user-dup")

    assert blocked.status_code == 429
    assert blocked.json()["error"]["code"] == "RATE_LIMIT_EXCEEDED"
