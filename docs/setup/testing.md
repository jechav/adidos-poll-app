# Running the Test Suite

Implements [I-021](../issues/I-021-unit-tests.md) (unit tests) and
covers the `tests/unit/` layer specifically; `tests/integration/`
(I-022) and `tests/load/` (I-023) are separate suites with their own
infrastructure requirements, noted below.

## Unit tests (`tests/unit/`)

No external services required — `fakeredis` stands in for the Redis
cluster and, for the vote-acceptance/rate-limiting suite specifically
(`tests/unit/test_vote_acceptance.py`, `tests/unit/test_rate_limiting.py`),
an in-memory fake poll/answer store stands in for PostgreSQL. Install
`requirements.txt` into a local virtualenv, then:

```bash
make test-unit
# equivalent to:
pytest tests/unit
```

The full unit suite runs in well under a minute (a few hundred
milliseconds for the vote-acceptance/rate-limit tests specifically) —
it's meant to run on every commit, not just before a push.

## Integration tests (`tests/integration/`)

Need a real PostgreSQL instance (`DATABASE_URL`, defaults to
`postgresql://postgres@localhost:5432/poll_app`) and a real Redis
cluster (see [redis-cluster.md](./redis-cluster.md)) reachable from the
host. Tests skip themselves (rather than fail) when either dependency
isn't reachable — see any fixture named `db_conn`/`redis` in that
directory for the skip logic.

```bash
docker-compose up -d  # postgres + the 3-node redis cluster
pytest tests/integration
```

## Everything

```bash
pytest   # tests/unit + tests/integration, per pytest.ini's testpaths
```

## CI

[`.github/workflows/tests.yml`](../../.github/workflows/tests.yml) runs
`pytest tests/unit` on every push and pull request — no services needed,
so it never blocks on Postgres/Redis provisioning. Integration and load
tests aren't wired into that workflow yet (they need real infrastructure
provisioned in CI, tracked separately — see I-022/I-023).
