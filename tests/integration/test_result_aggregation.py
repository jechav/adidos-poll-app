"""Integration tests for I-022's Module 3 (Result Aggregation): the full
vote -> queue -> worker -> DB -> cache -> `GET /v1/polls` pipeline
against real, dockerized PostgreSQL and Redis -- no mocked queue, no
mocked worker, no seeded cache counters.

This is the key distinction from the aggregation-math coverage that
already exists: `tests/integration/test_result_aggregator_redis.py` and
`tests/integration/test_list_polls.py::test_primary_path_uses_redis_and_sets_stale_false`
both seed `cache:poll:*:answer:*` counters *directly* to isolate the
aggregation math from the pipeline that produces those counters -- their
docstrings say so explicitly. This file drives votes through the real
`POST /v1/vote` endpoint and drains them with I-008's real worker loop
(`conftest.drain_queue`), so a bug anywhere in that pipeline (not just in
`compute_poll_results`) would fail these tests.

Skips automatically if PostgreSQL and/or the Redis cluster aren't
reachable -- see conftest.py's module docstring for why a Redis-cluster
skip from the bare host is expected, not a regression.
"""

import asyncio
import time
import uuid

import pytest

from src.cache.answer_cache import answers_cache_key
from tests.integration.conftest import (
    ADMIN_AUTH,
    REDIS_COMPOSE_SERVICES,
    auth_header,
    cast_vote,
    compose,
    drain_queue,
    make_poll,
)


async def _cast_n_votes(client, redis, poll_id, answer_id, n, *, prefix):
    for i in range(n):
        resp = await cast_vote(
            client, redis, user_id=f"{prefix}-{i}-{uuid.uuid4()}", poll_id=poll_id, answer_id=answer_id
        )
        assert resp.status_code == 202, resp.text


@pytest.mark.asyncio
async def test_60_40_split_after_10_votes_through_full_pipeline(
    client, db_conn, redis, shard_pool
):
    """User story #1: 6 votes for A, 4 for B -> `GET /v1/polls` shows
    60%/40%, produced by the real queue+worker, not a seeded counter.
    """
    poll_id, (answer_a, answer_b) = make_poll(db_conn, question="60/40 pipeline test")
    await redis.delete(answers_cache_key(poll_id))

    await _cast_n_votes(client, redis, poll_id, answer_a, 6, prefix="agg-a")
    await _cast_n_votes(client, redis, poll_id, answer_b, 4, prefix="agg-b")
    await drain_queue(redis, shard_pool)

    resp = await client.get(
        "/v1/polls", headers=auth_header("agg-reader"), params={"state": "active"}
    )
    assert resp.status_code == 200
    poll = next(p for p in resp.json()["data"]["polls"] if p["poll_id"] == str(poll_id))

    by_text = {a["text"]: a for a in poll["answers"]}
    assert by_text["A"]["vote_count"] == 6
    assert by_text["A"]["percentage"] == 60.0
    assert by_text["B"]["vote_count"] == 4
    assert by_text["B"]["percentage"] == 40.0


@pytest.mark.asyncio
async def test_results_include_total_and_per_answer_counts(client, db_conn, redis, shard_pool):
    """User story #2: the response shape carries `total_votes` at the
    poll level and a `vote_count` per answer, matching the API contract
    (`src/schemas/polls.py`), not just correct numbers embedded somewhere.
    """
    poll_id, (answer_a, answer_b) = make_poll(db_conn, question="shape test")
    await redis.delete(answers_cache_key(poll_id))

    await _cast_n_votes(client, redis, poll_id, answer_a, 3, prefix="shape-a")
    await drain_queue(redis, shard_pool)

    resp = await client.get(
        "/v1/polls", headers=auth_header("shape-reader"), params={"state": "active"}
    )
    poll = next(p for p in resp.json()["data"]["polls"] if p["poll_id"] == str(poll_id))

    assert poll["total_votes"] == 3
    assert {a["answer_id"] for a in poll["answers"]} == {str(answer_a), str(answer_b)}
    assert all("vote_count" in a and "percentage" in a for a in poll["answers"])


@pytest.mark.asyncio
async def test_closed_poll_results_still_queryable(client, db_conn, redis, shard_pool):
    """User story #3: votes cast while active remain visible in
    `GET /v1/polls?state=closed` once the poll is closed -- historical
    results aren't wiped or hidden by the state transition.
    """
    poll_id, (answer_a, answer_b) = make_poll(db_conn, question="closed poll results")
    await redis.delete(answers_cache_key(poll_id))

    await _cast_n_votes(client, redis, poll_id, answer_a, 2, prefix="closed-a")
    await drain_queue(redis, shard_pool)

    close = await client.put(
        f"/v1/admin/polls/{poll_id}/state", headers=ADMIN_AUTH, json={"state": "closed"}
    )
    assert close.status_code == 200

    resp = await client.get(
        "/v1/polls", headers=auth_header("closed-reader"), params={"state": "closed"}
    )
    assert resp.status_code == 200
    poll = next(p for p in resp.json()["data"]["polls"] if p["poll_id"] == str(poll_id))
    assert poll["total_votes"] == 2


@pytest.mark.asyncio
async def test_results_visible_within_5_seconds_of_vote(client, db_conn, redis, shard_pool):
    """User story #4: the async queue -> worker -> cache pipeline's
    latency budget. Casts one vote, then polls `GET /v1/polls` (draining
    the queue between polls) until the vote is reflected or 5s elapse,
    whichever comes first.
    """
    poll_id, (answer_a, _answer_b) = make_poll(db_conn, question="5s visibility test")
    await redis.delete(answers_cache_key(poll_id))
    user_id = f"latency-user-{uuid.uuid4()}"

    started = time.monotonic()
    resp = await cast_vote(client, redis, user_id=user_id, poll_id=poll_id, answer_id=answer_a)
    assert resp.status_code == 202, resp.text

    deadline = started + 5
    total_votes = 0
    while time.monotonic() < deadline:
        await drain_queue(redis, shard_pool, iterations_per_shard=1)
        resp = await client.get(
            "/v1/polls", headers=auth_header("latency-reader"), params={"state": "active"}
        )
        poll = next(p for p in resp.json()["data"]["polls"] if p["poll_id"] == str(poll_id))
        total_votes = poll["total_votes"]
        if total_votes >= 1:
            break
        await asyncio.sleep(0.1)

    elapsed = time.monotonic() - started
    assert total_votes == 1, f"vote never became visible (waited {elapsed:.1f}s)"
    assert elapsed < 5, f"vote took {elapsed:.1f}s to become visible, budget is 5s"


@pytest.mark.asyncio
async def test_results_fall_back_to_materialized_view_when_redis_down(
    client, db_conn, redis, require_redis_cluster_running, restart_redis_nodes
):
    """User story #5: stop the Redis cluster mid-test (a real failure,
    not just an environment where Redis was never reachable, unlike
    `tests/integration/test_list_polls_fallback.py`) and confirm results
    are still computable -- slower, but accurate -- from I-010's
    `vote_counts` materialized view rather than a 503.
    """
    poll_id, (answer_a, answer_b) = make_poll(db_conn, question="redis down mid-test fallback")
    with db_conn.cursor() as cur:
        cur.execute(
            "INSERT INTO vote_counts (poll_id, answer_id, count, percentage) "
            "VALUES (%s, %s, %s, %s)",
            (poll_id, answer_a, 6, 60.0),
        )
        cur.execute(
            "INSERT INTO vote_counts (poll_id, answer_id, count, percentage) "
            "VALUES (%s, %s, %s, %s)",
            (poll_id, answer_b, 4, 40.0),
        )
    db_conn.commit()

    # Sanity check: the primary (Redis) path is actually live before we
    # take it down, so a pass below can't be explained by Redis having
    # been unreachable the whole time.
    sanity = await client.get(
        "/v1/polls", headers=auth_header("fallback-reader"), params={"state": "active"}
    )
    assert sanity.json()["meta"]["stale"] is False

    # `restart_redis_nodes` (above) guarantees the nodes come back up and
    # are ready again after this test, pass or fail.
    compose("stop", *REDIS_COMPOSE_SERVICES)
    resp = await client.get(
        "/v1/polls", headers=auth_header("fallback-reader"), params={"state": "active"}
    )

    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["meta"]["stale"] is True
    poll = next(p for p in body["data"]["polls"] if p["poll_id"] == str(poll_id))
    assert poll["total_votes"] == 10
    by_text = {a["text"]: a for a in poll["answers"]}
    assert by_text["A"]["vote_count"] == 6 and by_text["A"]["percentage"] == 60.0
    assert by_text["B"]["vote_count"] == 4 and by_text["B"]["percentage"] == 40.0
