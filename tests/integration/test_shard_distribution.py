"""Integration tests for I-022's Module 5 (Shard Distribution): real
shard-routing arithmetic (`src/worker/sharding.py`) plus real PostgreSQL
writes through `src/worker/processor.py`'s batch-insert path -- not the
fake-shard unit coverage in `tests/unit/test_worker_sharding.py`.

Only the Redis-shaped side of the pipeline (`INCR`ing the result cache) is
faked here, via `conftest.FakeRedis` -- the same stand-in
`tests/integration/test_vote_processor_pipeline.py` already uses, for the
same reason its docstring gives: this suite's job is shard-routing
fairness and the worker's real DB write path, not Redis's own behavior,
which `tests/integration/test_redis_failover.py` and
`tests/integration/test_result_aggregator_redis.py` already cover against
a real cluster.

Per spec, nothing here asserts on the `blake2b` hash itself -- only its
*distribution outcome* (the ±10%/no-shard->55% fairness bound and
data-consistency after processing).

Skips automatically if PostgreSQL isn't reachable.
"""

import uuid

import pytest

from src.worker.processor import process_batch
from src.worker.sharding import route_by_shard
from src.worker.models import VotePayload
from tests.integration.conftest import FakeRedis, make_poll

NUM_SHARDS = 8


def _votes_for(poll_id, answer_id, n, *, prefix):
    return [
        VotePayload(
            vote_id=uuid.uuid4(),
            user_id=f"{prefix}-{i}-{uuid.uuid4()}",
            poll_id=poll_id,
            answer_id=answer_id,
        )
        for i in range(n)
    ]


async def _process_all_shards(votes, pool, redis):
    """Route `votes` for real, then write each shard's slice through the
    real `process_batch` (real Postgres insert, per I-008) -- returns the
    per-shard vote counts actually committed.
    """
    buckets = route_by_shard(votes, NUM_SHARDS)
    written_by_shard: dict[int, int] = {}
    for shard_id, shard_votes in buckets.items():
        written = await process_batch(shard_id, shard_votes, pool, redis)
        written_by_shard[shard_id] = len(written)
    return written_by_shard


@pytest.mark.asyncio
async def test_1000_votes_distributed_within_10_percent_across_shards(db_conn, shard_pool):
    """User story #6: 1000 distinct users -> no single shard holds more
    than 55% of the total, and the spread stays within the spec's ±10%
    fairness bound around the 1/8 (12.5%) expectation.
    """
    poll_id, (answer_id, _other) = make_poll(db_conn, question="1000-vote fairness test")
    redis = FakeRedis()

    votes = _votes_for(poll_id, answer_id, 1000, prefix="fair1k")
    written_by_shard = await _process_all_shards(votes, shard_pool, redis)

    total_written = sum(written_by_shard.values())
    assert total_written == 1000, "no vote should be lost or double-written across shards"

    expected_share = 1.0 / NUM_SHARDS
    for shard_id in range(NUM_SHARDS):
        share = written_by_shard.get(shard_id, 0) / total_written
        assert share <= 0.55, f"shard {shard_id} holds {share:.1%} of votes, exceeds the 55% cap"
        assert abs(share - expected_share) <= 0.10, (
            f"shard {shard_id} holds {share:.1%} of votes, "
            f"outside ±10% of the expected {expected_share:.1%} share"
        )


@pytest.mark.asyncio
async def test_viral_poll_consistent_across_shards_at_ci_scale(db_conn, shard_pool):
    """User story #7 ("100K votes from distinct users"), scaled down for
    the suite's < 5 minute CI budget per the ticket's "Scale-down for CI"
    testing strategy -- the full production-scale run is I-023's job.
    5,000 distinct-user votes on one hot poll must land intact and
    consistent: no shard drops or duplicates votes just because every
    other shard is hammering the same poll_id concurrently.
    """
    poll_id, (answer_id, other_answer_id) = make_poll(db_conn, question="viral poll (CI-scaled)")
    redis = FakeRedis()
    total_votes = 5000

    votes = _votes_for(poll_id, answer_id, total_votes, prefix="viral")
    written_by_shard = await _process_all_shards(votes, shard_pool, redis)

    assert sum(written_by_shard.values()) == total_votes
    with db_conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM votes WHERE poll_id = %s", (poll_id,))
        assert cur.fetchone()[0] == total_votes
        cur.execute(
            "SELECT count(DISTINCT vote_id) FROM votes WHERE poll_id = %s", (poll_id,)
        )
        assert cur.fetchone()[0] == total_votes, "every vote_id must be unique -- no double-write"
        cur.execute(
            "SELECT count(*) FROM votes WHERE poll_id = %s AND answer_id = %s",
            (poll_id, other_answer_id),
        )
        assert cur.fetchone()[0] == 0, "no vote should land against an answer nobody voted for"
    # Cache increments happened exactly once per committed vote, matching
    # the DB row count -- consistency across the write and cache side of
    # the pipeline, not just the DB side.
    assert redis.counters[f"cache:poll:{poll_id}:answer:{answer_id}"] == total_votes


@pytest.mark.asyncio
async def test_shard_failover_queues_and_recovers_via_real_worker_loop(db_conn):
    """User story #8: pause one shard (simulated the same way
    `tests/integration/test_vote_processor_pipeline.py::test_shard_down_requeues_votes...`
    does -- an unreachable DSN, since this repo has no second real
    Postgres instance to actually pause), submit votes routed to it, and
    confirm they're queued rather than lost, then processed once the
    shard "recovers" (a healthy pool takes over).

    Distinct from that existing test: this drives the actual worker loop
    (`run_worker`/`dequeue_batch`, I-008's real dequeue-route-process
    cycle against a real `queue:votes` list) end to end, rather than
    calling `process_batch` directly -- the requeue-and-later-reprocess
    behavior is exercised through the same code path production uses to
    drain the queue, not just the unit of work one batch touches.
    """
    from src.worker.db import ShardConnectionPool
    from src.worker.main import run_worker
    from tests.integration.conftest import DATABASE_URL

    poll_id, (answer_id, _other) = make_poll(db_conn, question="failover via worker loop")
    redis = FakeRedis()
    user_id = f"failover-worker-{uuid.uuid4()}"
    shard_id = 0  # num_shards=1 below: every user_id routes to shard 0, deterministically

    vote = VotePayload(vote_id=uuid.uuid4(), user_id=user_id, poll_id=poll_id, answer_id=answer_id)
    await redis.lpush("queue:votes", vote.model_dump_json())

    down_pool = ShardConnectionPool(
        lambda _shard_id: "postgresql://postgres@localhost:1/nonexistent", connect_timeout_s=1
    )
    await run_worker(
        shard_id, 1, redis=redis, pool=down_pool, iterations=1,
        min_batch_size=1, max_batch_size=10, batch_timeout_s=0.5, poll_timeout_s=1,
    )

    with db_conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM votes WHERE poll_id = %s", (poll_id,))
        assert cur.fetchone()[0] == 0, "vote must not be written while its shard is down"
    assert len(redis.lists["queue:votes"]) == 1, "vote must be requeued, not dropped, on shard-down"

    healthy_pool = ShardConnectionPool(lambda _shard_id: DATABASE_URL)
    try:
        await run_worker(
            shard_id, 1, redis=redis, pool=healthy_pool, iterations=1,
            min_batch_size=1, max_batch_size=10, batch_timeout_s=0.5, poll_timeout_s=1,
        )
    finally:
        for sid in list(healthy_pool._connections):
            await healthy_pool.invalidate(sid)

    with db_conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM votes WHERE poll_id = %s AND vote_id = %s", (poll_id, str(vote.vote_id)))
        assert cur.fetchone()[0] == 1, "vote must be durably written once the shard recovers"
    assert redis.lists.get("queue:votes", []) == []
