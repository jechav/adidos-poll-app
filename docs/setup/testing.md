# Testing

Implements [I-021](../issues/I-021-unit-tests.md) (unit tests) and
[I-022](../issues/I-022-integration-tests.md) (integration tests).

## Unit vs. integration: what's mocked, what's real

| | Unit (`tests/unit/`) | Integration (`tests/integration/`) |
|---|---|---|
| Redis | mocked (`fakeredis`) or in-memory stand-ins | real Redis Cluster (`docker-compose`'s `redis-node-*`) |
| PostgreSQL | throwaway/in-memory | real PostgreSQL (`docker-compose`'s `postgres`) |
| Goal | hermetic, sub-minute feedback on vote-acceptance/rate-limit/shard-routing *decision logic*, on every commit | correctness against the real protocols/constraints/hashing those decisions run against |
| Budget | < 1 minute | < 5 minutes |

Both suites run on every commit; integration tests are allowed a larger
budget because container startup and real I/O cost more than in-memory
fakes.

## Prerequisites

- Docker + `docker compose`
- Local Python environment with `requirements.txt` installed (`pytest`
  and `pytest-asyncio` are already listed there — no separate dev
  dependency file exists in this repo yet)

**Deviation from `testcontainers-python`, recorded deliberately**: I-022's
own "Testing Strategy" section lists this repo's existing integration
test conventions as required "Prior Art", and every pre-existing file in
`tests/integration/` binds to `docker-compose`'s already-running
`postgres`/`redis-node-*` services (via `DATABASE_URL`/`get_redis()`),
skipping cleanly when they're unreachable, rather than provisioning its
own ephemeral containers per test run. The four Module 3/5/6/7 files
this ticket added follow that same convention instead of introducing
`testcontainers` as a second, parallel infrastructure mechanism.

## Running the suites locally

```bash
docker compose up -d postgres redis-node-1 redis-node-2 redis-node-3 redis-cluster-init
scripts/db/run-sql.sh migrations   # apply the schema once
```

```bash
make test-unit          # tests/unit -- always fast, never touches Docker
make test-integration    # tests/integration -- see below for what actually gets exercised
```

(`pytest tests/unit` / `pytest tests/integration` work the same way if
you'd rather skip `make`.)

### A real constraint on where `pytest` runs, inherited from I-002/I-025

[docs/setup/redis-cluster.md](./redis-cluster.md) and
[I-025](../issues/I-025-redis-cluster-announce-address.md) already
document this for the app itself: `redis-py`'s `RedisCluster` client
follows `CLUSTER SLOTS`, which returns each node's **internal
Docker-network hostname** — unresolvable from the bare host. Running
`pytest` directly on the host (the normal case) means every
Redis-touching integration test in this directory skips with "no Redis
cluster reachable" — correctly, by design, not a bug in this suite.
I-025 goes further: even with a socket timeout set, a *sequence* of
calls against a genuinely-up cluster from the host can hang inside
`redis-py`'s own retry logic rather than fail fast, so **don't leave the
Redis containers up while also running `pytest` straight on the host** —
stop them first (`docker compose stop redis-node-1 redis-node-2
redis-node-3`), which turns "unreachable" into an instant, clean skip.

To actually exercise the Redis-backed integration tests for real (not
skip them), run `pytest` **inside the compose network**, the same way
I-025 put the app itself there — this was noted as a follow-up in that
ticket and is what this suite's CI job (`.github/workflows/tests.yml`)
does:

```bash
docker compose build app
docker compose run --rm \
  -v "$PWD":/app -w /app \
  -e DATABASE_URL=postgresql://poll_app:poll_app@postgres:5432/poll_app \
  -e POLL_APP_DATABASE_URL=postgresql://poll_app:poll_app@postgres:5432/poll_app \
  -e POLL_APP_REDIS_STARTUP_HOST=redis-node-1 \
  -e POLL_APP_REDIS_STARTUP_PORT=6379 \
  app python -m pytest tests/integration -v
```

Two tests in `tests/integration/test_result_aggregation.py` and
`tests/integration/test_end_to_end_workflows.py`
(`test_results_fall_back_to_materialized_view_when_redis_down` and
`test_redis_node_loss_keeps_service_up_eventually_consistent`) need
**both** of the above at once: real reachability to the cluster *and* a
`docker` CLI with control over it (to actually stop/start a node
mid-test, mirroring `test_redis_failover.py`). Neither the bare host
(reachability) nor the containerized run above (no `docker` CLI/socket
inside the `app` image, deliberately — it's the production image, not a
test runner) satisfies both, so these two skip in both of the setups
above. Running them for real needs a "docker outside of docker" setup
(the `app` container's process given the host's Docker socket) that
this repo doesn't have set up yet; they're written and will exercise
correctly the day that exists, and their skip messages/docstrings say so
explicitly rather than silently passing.

## What's out of scope in `tests/integration/`

- `test_reconciliation_job_detects_drift_over_1_percent`
  (`test_end_to_end_workflows.py`) is `@pytest.mark.skip`ped: there is no
  reconciliation/drift-detection job in this codebase yet to test against
  (`scripts/jobs/refresh_vote_counts.py` recomputes from source-of-truth
  `votes` rather than comparing two independently-maintained counters).
  Owned by I-017/I-019.
- Full production-scale load scenarios (1M votes/10s, 100K votes/sec
  sustained) are I-023's job — this suite's Module 5/7 tests use the same
  scenarios scaled down to fit the < 5 minute CI budget.
