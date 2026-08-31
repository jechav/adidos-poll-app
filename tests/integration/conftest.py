"""Shared fixtures/helpers for I-022's four integration-test modules
(Modules 3, 5, 6, 7: `test_result_aggregation.py`,
`test_shard_distribution.py`, `test_admin_poll_management.py`,
`test_end_to_end_workflows.py`).

Mirrors the per-file `db_conn`/`client`/`redis` fixture pattern already
used throughout `tests/integration/` (e.g. `test_admin_endpoints.py`,
`test_vote_acceptance.py`, `test_vote_processor_pipeline.py`) rather than
inventing a new convention — this file exists so the four new I-022
modules share one copy of that pattern instead of pasting it four more
times. It does not touch or refactor any pre-existing test file.

**Deviation from I-022's illustrative `testcontainers-python` snippet,
recorded deliberately**: the ticket's own "Testing Strategy" section
lists "Prior Art: existing Adidos integration test conventions (mirror
structure for consistency)" as a requirement, and this repo's actual
prior art (every file above) binds to `docker-compose`'s already-running
`postgres`/`redis-node-*` services via `DATABASE_URL`/`get_redis()`,
skipping when they're unreachable — not `testcontainers`. Introducing a
second, parallel infrastructure-provisioning mechanism alongside the one
every existing integration test already uses would be the inconsistency
the ticket asks this suite to avoid, so this file follows the existing
pattern instead of the snippet literally.

**"2+ shards" without a second database, recorded deliberately**: like
`tests/integration/test_vote_processor_pipeline.py` and
`tests/integration/test_refresh_vote_counts_db.py`, `shard_pool` below
resolves every shard id to the same local Postgres instance (see
`src/worker/db.py`'s `default_shard_dsn` docstring: "a single Postgres
instance stands in for every shard" is the documented local-dev/test
posture). This exercises the real shard-routing arithmetic
(`shard_for_user`/`route_by_shard`) and real batch-insert/uniqueness SQL
against a real database — the two things Module 5's stories actually
ask to be verified against fakes-free — without standing up a second
container that no other test in this repo uses either.

Distinction from I-021's unit tests: I-021 mocks Redis (`fakeredis`) and
a throwaway DB to get sub-minute, hermetic feedback on vote-acceptance/
rate-limit *decision logic* on every commit. I-022 trades that speed for
realism: every fixture here binds to real, dockerized infrastructure, so
tests exercise actual Postgres constraint enforcement, actual shard-
routing arithmetic, and the actual worker dequeue/write loop, not fakes
standing in for them. Redis-touching fixtures skip (not fail) when the
cluster isn't reachable from wherever pytest is running -- see
docs/setup/redis-cluster.md's "Cluster clients must connect from inside
the compose network" note and I-025's write-up: this is an accepted,
by-design limitation of running `pytest` on the bare host, not something
this suite works around. `docs/setup/testing.md` documents the
compose-network workaround that lets CI exercise these for real.
"""

from __future__ import annotations

import asyncio
import os
import shutil
import subprocess
import time
import uuid

import psycopg
import pytest
from httpx import ASGITransport, AsyncClient

from src.api.app import app
from src.cache.redis_client import get_redis
from src.worker.db import ShardConnectionPool
from src.worker.main import run_worker

REDIS_COMPOSE_SERVICES = ["redis-node-1", "redis-node-2", "redis-node-3"]

DATABASE_URL = os.environ.get(
    "DATABASE_URL", "postgresql://postgres@localhost:5432/poll_app"
)

ADMIN_AUTH = {"Authorization": "Bearer admin:i022-test"}


def auth_header(user_id: str) -> dict:
    return {"Authorization": f"Bearer {user_id}"}


# httpx's `ASGITransport` reports every request's client host as this
# fixed address by default (there's no real socket) -- every test in this
# suite that casts a vote shares I-007's per-IP counter
# (`rate_limit_ip:127.0.0.1`, 50/minute) for that reason, not because real
# users share an IP. (`tests/integration/test_vote_acceptance.py` has a
# same-shaped `_clear_rate_limit` helper that clears `rate_limit_ip:testclient`
# instead -- a no-op against the real key, since ASGITransport's actual
# default client is `("127.0.0.1", 123)`, not the string "testclient".
# Harmless there because none of that file's tests depend on the IP-limit
# clear actually landing; this suite's burst test does, so it uses the
# real value.)
TEST_CLIENT_IP = "127.0.0.1"


async def cast_vote(client, redis, *, user_id: str, poll_id, answer_id):
    """POST /v1/vote after clearing both of I-007's rate-limit counters
    for this request -- per-user (`rate_limit:{user_id}`, always safe to
    clear since every test here uses a fresh uuid4-suffixed user_id) and
    per-IP (`rate_limit_ip:{TEST_CLIENT_IP}`, shared by every test in this
    *session* -- see that constant's docstring -- so it must be cleared
    too, or a test can be 429'd by vote volume from a completely
    unrelated, already-finished test). Returns the raw response so
    callers can assert on any status, not just the success path.
    """
    await redis.delete(f"rate_limit:{user_id}")
    await redis.delete(f"rate_limit_ip:{TEST_CLIENT_IP}")
    return await client.post(
        "/v1/vote",
        headers=auth_header(user_id),
        json={"poll_id": str(poll_id), "answer_id": str(answer_id)},
    )


@pytest.fixture
def db_conn():
    try:
        connection = psycopg.connect(DATABASE_URL, connect_timeout=2)
    except psycopg.OperationalError:
        pytest.skip(f"no PostgreSQL reachable at {DATABASE_URL}")
    yield connection
    connection.rollback()
    connection.close()


@pytest.fixture
async def client():
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        yield ac


@pytest.fixture(autouse=True)
def _reset_redis_singleton_per_test():
    """`get_redis()` caches one `RedisCluster` client at module scope for
    the life of the *process* (by design -- one pooled client per app
    instance, see `src.cache.redis_client`'s docstring). pytest-asyncio
    hands each `async def test_*` its own event loop by default, so
    reusing that cached client across tests -- including indirectly,
    through `client` fixture requests that exercise routes depending on
    `redis_dependency` without the test itself requesting the `redis`
    fixture -- binds it to a loop a later test's loop isn't the same
    object as. The second test to touch it then fails with "Event loop is
    closed" from inside redis-py's transport, not from anything this
    suite is actually testing (confirmed by first running this suite for
    real, inside the compose network per docs/setup/testing.md -- this
    never surfaces when pytest runs on the bare host, since every
    Redis-touching test skips there instead of ever reaching Redis).
    Forcing a fresh client at the start of every test (only reachable
    in-process here; real deployments run one event loop for the
    process's whole lifetime and never hit this) sidesteps that
    pytest-asyncio artifact without changing the production singleton's
    behavior. `autouse=True` and directly resetting the module global
    (rather than calling `close_redis()`) are both deliberate: the
    previous test's client belongs to a loop that's already gone, so
    there's nothing to gracefully close, and this must run before *every*
    test here, not just ones that request `redis` by name.
    """
    import src.cache.redis_client as redis_client

    redis_client._redis = None
    yield


@pytest.fixture
async def redis():
    r = await get_redis()
    try:
        await asyncio.wait_for(r.ping(), timeout=3)
    except Exception:
        pytest.skip("no Redis cluster reachable")
    yield r


@pytest.fixture
async def shard_pool():
    pool = ShardConnectionPool(lambda shard_id: DATABASE_URL)
    yield pool
    for shard_id in list(pool._connections):
        await pool.invalidate(shard_id)


def make_poll(conn, *, state: str = "active", question: str = "poll", answers=("A", "B")):
    """Insert a poll with `answers` (in order) directly via SQL -- the
    same shortcut every existing integration test in this directory uses
    to set up fixture data without going through the admin API.
    """
    poll_id = uuid.uuid4()
    answer_ids = [uuid.uuid4() for _ in answers]
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO polls (poll_id, question, state) VALUES (%s, %s, %s)",
            (poll_id, question, state),
        )
        for order, (answer_id, text) in enumerate(zip(answer_ids, answers)):
            cur.execute(
                'INSERT INTO answers (answer_id, poll_id, answer_text, "order") '
                "VALUES (%s, %s, %s, %s)",
                (answer_id, poll_id, text, order),
            )
    conn.commit()
    return poll_id, answer_ids


async def drain_queue(redis, pool, *, num_shards: int = 2, iterations_per_shard: int = 3) -> None:
    """Run I-008's real worker loop (`run_worker`) against `queue:votes`
    to completion, one shard at a time, using small batch/timeout knobs
    so an already-empty queue returns almost immediately instead of
    waiting out production-sized `BRPOP` timeouts.

    A worker only ever writes the votes routed to *its own* shard id and
    pushes everything else straight back onto the queue (see
    `src/worker/main.py`'s docstring) -- running shard 0 then shard 1
    (matching `shard_pool`'s 2-shard convention above) is enough for every
    vote to reach a worker that owns it, in either order, as long as no
    single shard's share of the batch exceeds `dequeue_batch`'s
    `max_size` (500 by default): tests that push more votes than that
    call `drain_queue` with a larger `iterations_per_shard` instead of
    inflating `max_size`, so the same, unmodified worker loop is what
    gets exercised in every case.
    """
    for shard_id in range(num_shards):
        await run_worker(
            shard_id,
            num_shards,
            redis=redis,
            pool=pool,
            iterations=iterations_per_shard,
            min_batch_size=1,
            max_batch_size=500,
            batch_timeout_s=0.5,
            poll_timeout_s=1,
        )


def compose(*args: str, timeout: float = 30) -> subprocess.CompletedProcess:
    """Drive the real `docker compose` cluster via subprocess, mirroring
    `tests/integration/test_redis_failover.py`'s identical helper -- reused
    here (not redefined) by the two I-022 tests that need to actually
    stop/start Redis nodes mid-test (a real failure, not just an
    unreachable-by-default environment).
    """
    try:
        return subprocess.run(
            ["docker", "compose", *args], capture_output=True, text=True, timeout=timeout
        )
    except subprocess.TimeoutExpired as exc:
        return subprocess.CompletedProcess(exc.cmd, returncode=124, stdout="", stderr="timeout")


def redis_cluster_running() -> bool:
    if shutil.which("docker") is None:
        return False
    for service in REDIS_COMPOSE_SERVICES:
        result = compose("ps", "-q", service)
        if result.returncode != 0 or not result.stdout.strip():
            return False
    return True


def wait_until(predicate, timeout_s: float = 45, interval_s: float = 1.0):
    deadline = time.monotonic() + timeout_s
    last = None
    while time.monotonic() < deadline:
        last = predicate()
        if last:
            return last
        time.sleep(interval_s)
    return last


@pytest.fixture
def require_redis_cluster_running():
    """Unlike the `redis` fixture (which just skips if unreachable), the
    two tests that use this need the cluster to genuinely exist as
    `docker compose` services so it can be stopped mid-test -- skip up
    front rather than fail confusingly on the first `docker compose
    stop`.
    """
    if not redis_cluster_running():
        pytest.skip(
            "redis-node-* compose services aren't running; start with "
            "`docker compose up -d redis-node-1 redis-node-2 redis-node-3 redis-cluster-init`"
        )


@pytest.fixture
def restart_redis_nodes():
    """Always restarts the Redis nodes after the test, even on failure --
    mirroring `tests/integration/test_redis_failover.py`'s
    `restart_node_2` fixture -- so one failing assertion doesn't leave
    every other Redis-dependent test in the suite skipping.
    """
    yield
    compose("start", *REDIS_COMPOSE_SERVICES)
    wait_until(
        lambda: compose(
            "exec", "-T", "redis-node-1", "redis-cli", "-p", "6379", "ping"
        ).stdout.strip()
        == "PONG"
    )


def cluster_info(service: str = "redis-node-1") -> dict:
    """`CLUSTER INFO`, parsed into a dict -- mirrors
    `tests/integration/test_redis_failover.py`'s identical helper.
    """
    result = compose("exec", "-T", service, "redis-cli", "-p", "6379", "cluster", "info")
    info: dict = {}
    for line in result.stdout.splitlines():
        if ":" in line:
            key, _, value = line.partition(":")
            info[key.strip()] = value.strip()
    return info


class FakeRedis:
    """The same in-memory `queue:votes`/cache-counter stand-in
    `tests/integration/test_vote_processor_pipeline.py` uses. Reused here
    (rather than redefined a second time) for Module 5's high-volume
    shard-distribution tests, which exercise `route_by_shard` and real
    Postgres writes -- not Redis's own behavior, which
    `tests/integration/test_redis_failover.py` and
    `tests/integration/test_result_aggregator_redis.py` already cover
    against a real cluster.
    """

    def __init__(self):
        self.lists: dict[str, list[str]] = {}
        self.counters: dict[str, int] = {}

    async def lpush(self, key: str, value: str) -> int:
        lst = self.lists.setdefault(key, [])
        lst.insert(0, value)
        return len(lst)

    async def incr(self, key: str) -> int:
        self.counters[key] = self.counters.get(key, 0) + 1
        return self.counters[key]

    async def llen(self, key: str) -> int:
        return len(self.lists.get(key, []))

    async def brpop(self, key: str, timeout: float = 0):
        """Non-blocking stand-in for `BRPOP`: an empty list returns `None`
        immediately (real Redis would block up to `timeout`) rather than
        actually sleeping -- fine for tests, which only use this via
        `src.worker.queue_consumer.dequeue_batch`'s own time-boxed loop,
        never expecting a real block.
        """
        values = self.lists.get(key)
        if not values:
            return None
        return key, values.pop()
