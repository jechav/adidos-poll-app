# Redis Cluster Setup

Implements [I-002](../issues/I-002-redis-cluster.md).

## Local development

```bash
docker-compose up -d redis-node-1 redis-node-2 redis-node-3 redis-cluster-init
```

`redis-cluster-init` runs once, forms the 3-node cluster
(`--cluster-replicas 0`, no replicas locally — see "HA behavior" below),
and exits. Verify:

```bash
docker-compose exec redis-node-1 redis-cli -p 6379 cluster info
# cluster_state:ok
```

Nodes are reachable from the host on `localhost:7001`/`7002`/`7003`, and
from other containers on the compose network as `redis-node-1:6379`, etc.

**Cluster clients must connect from inside the compose network, not from
the bare host.** `redis-py`'s `RedisCluster` follows `CLUSTER SLOTS`, which
returns the nodes' internal hostnames (`redis-node-2`, `redis-node-3`) —
those aren't resolvable from the host machine even though the *startup*
node (`localhost:7001`) is reachable, so a client run directly on the host
will time out after the first handshake. This is why the app runs as a
compose service (`app`, built from the root `Dockerfile`) on
`adidos-poll-app_default` (the network this file's `docker-compose.yml`
creates), the same way it will reach Redis in Kubernetes-managed
staging/production:

```bash
docker compose up -d          # postgres, all 3 redis nodes, cluster-init, and app
curl localhost:8000/health    # {"redis": "ok", ...} once the cluster is reachable
```

`pytest` still runs directly on the host (correctly — tests shouldn't
require a container to execute), so it still can't reach the cluster's
internal-hostname addresses and will keep skipping the live-cluster
integration tests locally; that's expected, not a regression. See
[I-025](../issues/I-025-redis-cluster-announce-address.md) for the full
story, including a documented dead end (`cluster-announce-ip`) worth not
re-attempting.

## Connecting from the app

```python
from src.cache.redis_client import get_redis, redis_dependency

redis = await get_redis()  # singleton, pooled
```

FastAPI route handlers should use `Depends(redis_dependency)` instead of
calling `get_redis()` directly, so the dependency can be overridden in
tests.

Configure the startup node via env vars (see [src/config.py](../../src/config.py)):

```bash
export POLL_APP_REDIS_STARTUP_HOST=localhost
export POLL_APP_REDIS_STARTUP_PORT=7001
```

## HA behavior: single-node failure

- **Local/dev topology (no replicas)**: a lost node's hash slots become
  unavailable until the node restarts and rejoins with its AOF-restored
  data; other masters' slots keep serving (`cluster-require-full-coverage
  no` in [scripts/redis/redis.conf](../../scripts/redis/redis.conf) — without
  it, Redis Cluster's default behavior takes the *entire* cluster down on
  any single master loss, not just that master's slots). This is fine for
  local dev but does **not** represent production failover — don't use it
  to validate HA behavior.
- **Production topology (1 replica per master, 6 nodes total)**: the
  replica for a failed master is promoted automatically; writes to that
  master's hash slots resume once promotion completes (typically within a
  few seconds).
- Detection happens via gossip: a node is marked `PFAIL` then `FAIL` once
  enough peers agree, bounded by `cluster-node-timeout` (15s, see
  [scripts/redis/redis.conf](../../scripts/redis/redis.conf)).

## Testing node failure locally

```bash
docker-compose stop redis-node-2
docker-compose exec redis-node-1 redis-cli -p 6379 cluster info
# cluster_state may report degraded slot coverage for node-2's slots;
# node-1/node-3's slots remain reachable.
docker-compose start redis-node-2
# AOF replay restores node-2's data on rejoin.
```

## Persistence

Both AOF (`appendfsync everysec`) and RDB snapshotting are enabled per
node — see [scripts/redis/redis.conf](../../scripts/redis/redis.conf) for
the exact config and the ticket for the durability trade-off rationale.
