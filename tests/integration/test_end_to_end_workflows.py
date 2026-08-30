"""Integration tests for I-022's Module 7 (Integration / End-to-End
Workflow Tests): cross-user consistency, a scaled-down high-volume
burst, Redis node loss, and the read/write state agreement on closed
polls -- all against real, dockerized PostgreSQL and Redis.

Out of scope, recorded deliberately: user story #18 ("reconciliation job
... detects and alerts on a deliberately introduced Redis/DB drift").
This repo has no reconciliation/drift-detection job to test yet --
`scripts/jobs/refresh_vote_counts.py` (I-010) *recomputes* `vote_counts`
from the authoritative `votes` table every run rather than comparing two
independently-maintained counters, so there is nothing in it that can
"drift" in the sense this story describes, and `src/jobs/report_anomalies.py`
(I-016) reports anomalies that some other detector already wrote to the
`anomalies` table -- it doesn't compute Redis-vs-DB drift itself either.
A dedicated drift-detection job is future work under I-017 (Monitoring)
/ I-019 (Alerting), both still unchecked in docs/issues/INDEX.md as of
this suite. `test_reconciliation_job_detects_drift_over_1_percent` below
is written against that not-yet-existing job and is marked
`pytest.mark.skip` with this reasoning, rather than deleted, so it is
easy to un-skip once I-017/I-019 land the job it needs.
"""

import asyncio
import time
import uuid

import pytest

from src.cache.answer_cache import answers_cache_key
from tests.integration.conftest import (
    ADMIN_AUTH,
    TEST_CLIENT_IP,
    auth_header,
    cast_vote,
    cluster_info,
    compose,
    drain_queue,
    make_poll,
    wait_until,
)


@pytest.mark.asyncio
async def test_two_users_vote_and_see_each_others_impact_on_results(
    client, db_conn, redis, shard_pool
):
    """User story #15: two distinct users vote for different answers on
    the same poll; each sees the *other's* vote reflected in results,
    not just their own.
    """
    poll_id, (answer_a, answer_b) = make_poll(db_conn, question="two users, shared results")
    await redis.delete(answers_cache_key(poll_id))
    user_1, user_2 = f"e2e-user-1-{uuid.uuid4()}", f"e2e-user-2-{uuid.uuid4()}"

    first = await cast_vote(client, redis, user_id=user_1, poll_id=poll_id, answer_id=answer_a)
    second = await cast_vote(client, redis, user_id=user_2, poll_id=poll_id, answer_id=answer_b)
    assert first.status_code == 202 and second.status_code == 202
    await drain_queue(redis, shard_pool)

    # Both users query the same public results -- there's no per-user
    # results view, so "each other's impact" means the shared response
    # reflects both votes regardless of who's asking.
    resp_as_user_1 = await client.get(
        "/v1/polls", headers=auth_header(user_1), params={"state": "active"}
    )
    resp_as_user_2 = await client.get(
        "/v1/polls", headers=auth_header(user_2), params={"state": "active"}
    )

    for resp in (resp_as_user_1, resp_as_user_2):
        poll = next(p for p in resp.json()["data"]["polls"] if p["poll_id"] == str(poll_id))
        assert poll["total_votes"] == 2
        by_text = {a["text"]: a for a in poll["answers"]}
        assert by_text["A"]["vote_count"] == 1
        assert by_text["B"]["vote_count"] == 1


@pytest.mark.asyncio
async def test_high_volume_burst_p99_under_200ms_no_data_loss(client, db_conn, redis):
    """User story #16, scaled down for the < 5 minute CI budget per the
    ticket's "Scale-down for CI" strategy (the full 1M-votes/10s scenario
    is I-023's job): a concurrent burst of `POST /v1/vote` requests must
    show P99 request latency under 200ms and zero data loss -- every
    accepted vote is actually sitting on `queue:votes` afterward, none
    silently dropped.
    """
    poll_id, (answer_a, _answer_b) = make_poll(db_conn, question="burst latency test")
    # Kept comfortably under `src.cache.redis_client`'s `max_connections=100`
    # per cluster node: each concurrent `POST /v1/vote` briefly holds a Redis
    # connection across several round trips (rate limit, uniqueness, bot
    # detection, enqueue), so a burst much larger than the pool itself would
    # measure `MaxConnectionsError` contention, not the vote-acceptance path
    # this story is actually about.
    burst_size = 50
    # Every user_id is a fresh uuid4, so `rate_limit:{user_id}` is guaranteed
    # unused -- no pre-clear needed, unlike the single-vote tests elsewhere
    # in this suite that reuse fixed-ish prefixes across runs.
    user_ids = [f"burst-user-{i}-{uuid.uuid4()}" for i in range(burst_size)]
    await redis.delete("queue:votes")
    # Every request in this suite shares I-007's per-IP counter (see
    # `TEST_CLIENT_IP`'s docstring) -- clear it once, up front, rather than
    # per-request like `cast_vote` does, since 50 concurrent clears of the
    # same key would just race each other.
    await redis.delete(f"rate_limit_ip:{TEST_CLIENT_IP}")

    async def _timed_vote(user_id: str) -> tuple[int, float]:
        started = time.perf_counter()
        resp = await client.post(
            "/v1/vote",
            headers=auth_header(user_id),
            json={"poll_id": str(poll_id), "answer_id": str(answer_a)},
        )
        return resp.status_code, time.perf_counter() - started

    results = await asyncio.gather(*(_timed_vote(u) for u in user_ids))

    statuses = [status for status, _latency in results]
    assert all(status == 202 for status in statuses), (
        f"expected every burst request to be accepted, got statuses: {set(statuses)}"
    )

    latencies_ms = sorted(latency * 1000 for _status, latency in results)
    p99_index = max(0, int(len(latencies_ms) * 0.99) - 1)
    p99_ms = latencies_ms[p99_index]
    assert p99_ms < 200, f"P99 latency was {p99_ms:.1f}ms, budget is 200ms (n={burst_size})"

    queued = await redis.llen("queue:votes")
    assert queued == burst_size, (
        f"expected all {burst_size} accepted votes on queue:votes, found {queued} -- data loss"
    )


@pytest.mark.asyncio
async def test_redis_node_loss_keeps_service_up_eventually_consistent(
    client, db_conn, redis, shard_pool, require_redis_cluster_running, restart_redis_nodes
):
    """User story #17: dropping one Redis cluster node must not take
    voting down -- `POST /v1/vote` (which touches rate-limiting,
    uniqueness, and the queue, all Redis-backed) must keep succeeding
    while the node is down, using slots the surviving nodes still serve,
    and the vote must still be fully processed (eventually consistent)
    once the node recovers and the queue is drained.

    Mirrors `tests/integration/test_redis_failover.py`'s node-loss
    mechanics (stop one node via `docker compose`, wait for degraded-but-
    `ok` cluster state) one level up the stack, at the application's
    actual vote-casting behavior rather than raw `redis-cli` commands.
    """
    poll_id, (answer_a, _answer_b) = make_poll(db_conn, question="redis node loss e2e")
    await redis.delete(answers_cache_key(poll_id))
    user_id = f"node-loss-user-{uuid.uuid4()}"

    compose("stop", "redis-node-2")
    degraded = wait_until(
        lambda: (
            info := cluster_info()
        ).get("cluster_slots_ok") != "16384"
        and info.get("cluster_state") == "ok"
        and info
    )
    assert degraded, "redis-node-2's slots never degraded, or cluster_state left 'ok'"

    accepted = None
    for _ in range(20):
        resp = await cast_vote(client, redis, user_id=user_id, poll_id=poll_id, answer_id=answer_a)
        if resp.status_code == 202:
            accepted = resp
            break
        # A vote whose keys happen to hash to node-2's now-unreachable
        # slots may 5xx; retry with a fresh user_id (different hash slot)
        # rather than treating one unlucky slot as "the service is down".
        user_id = f"node-loss-user-{uuid.uuid4()}"
    assert accepted is not None, "no vote succeeded against any hash slot while one node was down"

    compose("start", "redis-node-2")
    recovered = wait_until(lambda: cluster_info().get("cluster_slots_ok") == "16384")
    assert recovered, "cluster did not recover full slot coverage after restarting redis-node-2"

    await drain_queue(redis, shard_pool)
    resp = await client.get(
        "/v1/polls", headers=auth_header("node-loss-reader"), params={"state": "active"}
    )
    poll = next(p for p in resp.json()["data"]["polls"] if p["poll_id"] == str(poll_id))
    assert poll["total_votes"] == 1, "the vote accepted during the outage must still land eventually"


@pytest.mark.skip(
    reason="No reconciliation/drift-detection job exists yet to test against -- "
    "see this module's docstring. Owned by I-017 (Monitoring) / I-019 (Alerting)."
)
def test_reconciliation_job_detects_drift_over_1_percent():
    """User story #18. Left as a documented placeholder: once a
    reconciliation job exists (comparing Redis's live counters against
    `votes`/`vote_counts` and writing an `anomalies` row on drift beyond
    1%), this test should seed a drift by writing a `vote_counts` row
    that doesn't match the real `votes` count, run that job, and assert
    an anomaly was recorded -- mirroring
    `tests/integration/test_admin_endpoints.py`'s `_seed_anomaly`/
    `GET /v1/admin/anomalies` pattern for the assertion side.
    """


@pytest.mark.asyncio
async def test_closed_poll_visible_in_history_but_not_votable(client, db_conn, redis, shard_pool):
    """User story #19: the read path (`GET /v1/user/votes`) and write
    path (`POST /v1/vote`) must agree on a closed poll's state -- the
    vote stays in the user's history, but casting a new one is rejected.
    """
    poll_id, (answer_a, _answer_b) = make_poll(db_conn, question="closed: history yes, votes no")
    user_id = f"history-user-{uuid.uuid4()}"

    voted = await cast_vote(client, redis, user_id=user_id, poll_id=poll_id, answer_id=answer_a)
    assert voted.status_code == 202
    await drain_queue(redis, shard_pool)

    closed = await client.put(
        f"/v1/admin/polls/{poll_id}/state", headers=ADMIN_AUTH, json={"state": "closed"}
    )
    assert closed.status_code == 200

    history = await client.get("/v1/user/votes", headers=auth_header(user_id))
    assert any(v["poll_id"] == str(poll_id) for v in history.json()["data"]["votes"])

    rejected = await cast_vote(
        client, redis, user_id=f"new-{user_id}", poll_id=poll_id, answer_id=answer_a
    )
    assert rejected.status_code == 400
    assert rejected.json()["error"]["code"] == "POLL_CLOSED"
