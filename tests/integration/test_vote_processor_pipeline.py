"""Integration tests for I-008's vote processor against a real PostgreSQL
shard — verifies the actual round-trip (batch insert, per-row duplicate
fallback, shard-down requeue) that unit tests can only fake.

Uses an in-memory Redis-list stand-in for `queue:votes` /
`cache:poll:*:answer:*` rather than a live Redis cluster: I-002's cluster
tests (tests/integration/test_redis_failover.py) already cover Redis's
own failure modes, and this suite's job is the worker's DB-facing
behavior, not Redis's.

Skips automatically if DATABASE_URL isn't reachable, mirroring
tests/integration/test_schema_performance.py.
"""

import os
import uuid

import psycopg
import pytest
from prometheus_client import generate_latest
from prometheus_client.parser import text_string_to_metric_families

from src.metrics.registry import REGISTRY
from src.worker.db import ShardConnectionPool
from src.worker.models import VotePayload
from src.worker.processor import process_batch
from src.worker.queue_consumer import QUEUE_KEY


def _violations_count(shard: str) -> float:
    text = generate_latest(REGISTRY).decode("utf-8")
    for family in text_string_to_metric_families(text):
        for s in family.samples:
            if s.name == "poll_db_constraint_violations_total" and s.labels.get("shard") == shard:
                return s.value
    return 0.0

DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://postgres@localhost:5432/poll_app")


class FakeRedis:
    """Same in-memory stand-in used by the unit tests: LPUSH prepends,
    BRPOP-equivalent access pops the tail; INCR is a plain counter.
    """

    def __init__(self):
        self.lists: dict[str, list[str]] = {}
        self.counters: dict[str, int] = {}

    async def lpush(self, key: str, value: str) -> None:
        self.lists.setdefault(key, []).insert(0, value)

    async def incr(self, key: str) -> int:
        self.counters[key] = self.counters.get(key, 0) + 1
        return self.counters[key]


@pytest.fixture
def sync_conn():
    try:
        conn = psycopg.connect(DATABASE_URL, connect_timeout=2)
    except psycopg.OperationalError:
        pytest.skip(f"no PostgreSQL reachable at {DATABASE_URL}")
    yield conn
    conn.rollback()
    conn.close()


@pytest.fixture
def poll_and_answer(sync_conn):
    poll_id = uuid.uuid4()
    answer_id = uuid.uuid4()
    with sync_conn.cursor() as cur:
        cur.execute(
            "INSERT INTO polls (poll_id, question, state) VALUES (%s, %s, 'active')",
            (poll_id, "vote processor test poll"),
        )
        cur.execute(
            'INSERT INTO answers (answer_id, poll_id, answer_text, "order") '
            "VALUES (%s, %s, %s, 0)",
            (answer_id, poll_id, "yes"),
        )
    sync_conn.commit()
    yield poll_id, answer_id


@pytest.fixture
async def pool():
    p = ShardConnectionPool(lambda shard_id: DATABASE_URL)
    yield p
    for shard_id in list(p._connections):
        await p.invalidate(shard_id)


@pytest.mark.asyncio
async def test_process_batch_writes_votes_and_updates_cache(sync_conn, poll_and_answer, pool):
    poll_id, answer_id = poll_and_answer
    redis = FakeRedis()
    votes = [
        VotePayload(vote_id=uuid.uuid4(), user_id=f"pipeline-user-{i}", poll_id=poll_id, answer_id=answer_id)
        for i in range(5)
    ]

    successful = await process_batch(0, votes, pool, redis)

    assert len(successful) == 5
    with sync_conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM votes WHERE poll_id = %s", (poll_id,))
        assert cur.fetchone()[0] == 5
    assert redis.counters[f"cache:poll:{poll_id}:answer:{answer_id}"] == 5


@pytest.mark.asyncio
async def test_a_duplicate_in_the_batch_does_not_roll_back_the_rest(
    sync_conn, poll_and_answer, pool
):
    poll_id, answer_id = poll_and_answer
    redis = FakeRedis()
    dup_user = f"dup-user-{uuid.uuid4()}"

    with sync_conn.cursor() as cur:
        cur.execute(
            "INSERT INTO votes (vote_id, user_id, poll_id, answer_id) VALUES (%s, %s, %s, %s)",
            (uuid.uuid4(), dup_user, poll_id, answer_id),
        )
    sync_conn.commit()

    votes = [
        VotePayload(vote_id=uuid.uuid4(), user_id="pipeline-user-a", poll_id=poll_id, answer_id=answer_id),
        # Same (user_id, poll_id) as the row already committed above —
        # violates I-001's UNIQUE(user_id, poll_id) constraint.
        VotePayload(vote_id=uuid.uuid4(), user_id=dup_user, poll_id=poll_id, answer_id=answer_id),
        VotePayload(vote_id=uuid.uuid4(), user_id="pipeline-user-b", poll_id=poll_id, answer_id=answer_id),
    ]

    before_violations = _violations_count("0")

    successful = await process_batch(0, votes, pool, redis)

    assert {v.user_id for v in successful} == {"pipeline-user-a", "pipeline-user-b"}
    assert redis.counters[f"cache:poll:{poll_id}:answer:{answer_id}"] == 2
    assert _violations_count("0") == before_violations + 1
    with sync_conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM votes WHERE poll_id = %s", (poll_id,))
        # 1 pre-existing (dup_user) + 2 newly committed.
        assert cur.fetchone()[0] == 3


@pytest.mark.asyncio
async def test_shard_down_requeues_votes_without_writing_or_updating_cache(
    poll_and_answer, sync_conn
):
    poll_id, answer_id = poll_and_answer
    redis = FakeRedis()
    unreachable_pool = ShardConnectionPool(
        lambda shard_id: "postgresql://postgres@localhost:1/nonexistent",
        connect_timeout_s=1,
    )
    votes = [
        VotePayload(vote_id=uuid.uuid4(), user_id="pipeline-user-down", poll_id=poll_id, answer_id=answer_id)
    ]

    successful = await process_batch(7, votes, unreachable_pool, redis)

    assert successful == []
    assert redis.counters == {}
    requeued = redis.lists[QUEUE_KEY]
    assert len(requeued) == 1
    assert VotePayload.model_validate_json(requeued[0]).vote_id == votes[0].vote_id
    with sync_conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM votes WHERE poll_id = %s", (poll_id,))
        assert cur.fetchone()[0] == 0


@pytest.mark.asyncio
async def test_votes_are_processed_once_the_shard_recovers(sync_conn, poll_and_answer):
    """Mirrors the failover test's "processed later once shard recovers"
    step: the same votes that got requeued while a shard was unreachable
    are handed to a healthy pool afterwards and land durably, with the
    cache updated exactly once each.
    """
    poll_id, answer_id = poll_and_answer
    redis = FakeRedis()
    unreachable_pool = ShardConnectionPool(
        lambda shard_id: "postgresql://postgres@localhost:1/nonexistent",
        connect_timeout_s=1,
    )
    votes = [
        VotePayload(vote_id=uuid.uuid4(), user_id="pipeline-user-recovers", poll_id=poll_id, answer_id=answer_id)
    ]

    first_attempt = await process_batch(7, votes, unreachable_pool, redis)
    assert first_attempt == []
    requeued_votes = [VotePayload.model_validate_json(v) for v in redis.lists[QUEUE_KEY]]

    healthy_pool = ShardConnectionPool(lambda shard_id: DATABASE_URL)
    try:
        second_attempt = await process_batch(7, requeued_votes, healthy_pool, redis)
    finally:
        for shard_id in list(healthy_pool._connections):
            await healthy_pool.invalidate(shard_id)

    assert len(second_attempt) == 1
    assert redis.counters[f"cache:poll:{poll_id}:answer:{answer_id}"] == 1
    with sync_conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM votes WHERE poll_id = %s", (poll_id,))
        assert cur.fetchone()[0] == 1
