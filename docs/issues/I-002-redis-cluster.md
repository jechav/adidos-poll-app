# I-002: Redis Cluster Setup

**Status**: Implemented
**Epic**: Voting Infrastructure
**Priority**: P0 (Blocker, parallel with I-001)
**Estimated Effort**: 3 days

---

## Problem Statement

Nearly every downstream issue in this project reads or writes Redis: the vote queue (I-005), uniqueness checks (I-006), rate limiting (I-007), result caching (I-009), the 3-strike duplicate block (I-015), and the auth token cache (I-004). None of that can be built until the cluster exists with an agreed-upon key namespace.

The cluster must:
- Survive a single node failure without taking voting down (spec decision #7, #9: "Availability over Consistency")
- Persist queued votes across restarts, so a brief service restart doesn't lose in-flight votes (user story #29)
- Support the mixed workload of a queue (list ops), counters (INCR), sets (SET NX), and short-TTL rate-limit keys, all from a single cluster
- Be reachable locally via Docker Compose for development, and via Kubernetes-managed Redis in staging/production

This issue provisions and configures the cluster itself. It does **not** implement any business logic (queueing, rate limiting, uniqueness, caching) — those are separate issues that depend on this one.

---

## Solution

Stand up a **Redis Cluster** (cluster mode enabled) with:
1. 3+ master nodes (gossip protocol for cluster state, hash slot distribution)
2. AOF (append-only file) + RDB snapshot persistence enabled, so queued votes and cached state survive node restarts
3. A documented key namespace (prefixes, types, TTLs) that all later issues implement against exactly as specified here
4. An async connection pool exposed to the FastAPI app (redis-py asyncio client), reused across requests
5. A cluster health check wired into the API's `/health` endpoint
6. Local dev parity via a `docker-compose` Redis service

No REST endpoints, no route handlers, no Pydantic schemas — this is pure infrastructure and a shared client module other issues import.

---

## User Stories

1. As an operator, I want Redis clustered across 3+ nodes, so that the service survives a single node failure (user story #28)
2. As an operator, I want AOF + RDB persistence enabled, so that votes queued in Redis survive a brief service restart without data loss (user story #29)
3. As a developer, I want a documented, stable key namespace, so that every issue that touches Redis (I-004 through I-015) uses consistent, non-colliding keys
4. As a developer, I want a shared async Redis client module, so that I don't reconnect per-request or duplicate connection logic across route handlers
5. As an operator, I want Redis cluster health surfaced in `GET /health`, so that I know immediately if the cache/queue layer degrades
6. As a developer, I want a `docker-compose` Redis service for local development, so that I can run the full stack without a hosted cluster

---

## Implementation Decisions

### Cluster Topology

- **Mode**: Redis Cluster (`cluster-enabled yes`), not Sentinel — hash-slot based sharding gives us horizontal scale for the queue/cache workload, not just failover
- **Node count**: 3 masters minimum (production), each cluster node gets no replica in the local/dev topology; production adds 1 replica per master (6 nodes total) for automatic failover
- **Gossip protocol**: Nodes discover and monitor each other via Redis Cluster's built-in gossip (`cluster-node-timeout 15000`); no external service discovery needed
- **Slot distribution**: 16384 hash slots divided evenly across masters; redis-py cluster client (`RedisCluster`) resolves the correct node per key automatically via `CLUSTER SLOTS`

### Persistence Configuration

Both AOF and RDB are enabled — RDB alone is not sufficient because it snapshots on an interval and could lose the last few seconds of queued votes; AOF gives us near-durability.

```conf
# redis.conf (per node)
cluster-enabled yes
cluster-config-file nodes.conf
cluster-node-timeout 15000

appendonly yes
appendfsync everysec        # fsync every second, bounded data loss window

save 900 1                  # RDB snapshot: 900s if >=1 key changed
save 300 10                 # RDB snapshot: 300s if >=10 keys changed
save 60 10000                # RDB snapshot: 60s if >=10000 keys changed
```

- **Rationale**: `appendfsync everysec` balances durability (max 1 second of lost writes) against write throughput — `always` would fsync every write and tank latency under burst load
- **Trade-off accepted**: On the worst-case node crash, up to 1 second of queued votes may be lost from that node's slots. This is acceptable per spec decision #7 (availability over consistency) and is why the DB unique constraint in I-001 remains the ultimate source of truth, not Redis.

### Key Namespace (Reference Table)

This is the contract every downstream issue implements against. Do not introduce new prefixes without updating this table.

| Key Pattern | Type | Written By | Read By | TTL | Notes |
|---|---|---|---|---|---|
| `queue:votes` | List | I-005 (vote acceptance, `LPUSH`) | I-008 (vote processor, `RPOP`/`BRPOP`) | None (persisted via AOF) | FIFO queue of pending vote payloads (JSON-encoded) |
| `rate_limit:{user_id}` | String (counter) | I-007 | I-007 | 61s | Sliding-window counter, per-user (5/min quota) |
| `rate_limit_ip:{ip}` | String (counter) | I-007 | I-007 | 61s | Sliding-window counter, per-IP (50/min quota) |
| `vote:user:{user_id}:poll:{poll_id}` | String (`SET NX`) | I-005 / I-006 | I-006 | None (persists until poll archived + anonymized) | Fast-path uniqueness check before DB write |
| `blocked:{user_id}:{ip}` | String (flag) | I-015 | I-005 (pre-vote check) | 3600s | 3-strike duplicate block (1-hour lockout) |
| `cache:poll:{poll_id}:answer:{answer_id}` | String (counter, `INCR`) | I-009 (worker updates on vote write) | I-009, I-011 (result reads) | None (ages naturally, no explicit invalidation per spec decision #16) | Real-time vote count per answer |
| `auth:token:{token_hash}` | Hash (`user_id`, `role`) | I-004 | I-004 | 300-600s (5-10 min) | Cached Adidos token validation result |

- **Rationale for `SET NX` on the uniqueness key rather than a Redis Set**: a per-(user, poll) string key lets every check be O(1) with no key growth cleanup; a single `SET(...)` per pair, versus a shared Set per poll that grows unbounded and requires scanning for anonymization.
- **Rationale for hashing the token in `auth:token:{token_hash}`**: avoids storing raw Adidos tokens in Redis; only a hash is cached, consistent with "trust but don't persist the credential" from spec decision #1.
- **No key ever stores raw PII beyond `user_id`** (already opaque per Adidos) and `ip_address`, both already permitted by the domain model's data constraints.

### Local Dev Setup (docker-compose)

```yaml
# docker-compose.yml (excerpt)
services:
  redis-node-1:
    image: redis:7-alpine
    command: redis-server /usr/local/etc/redis/redis.conf
    ports: ["7001:6379"]
    volumes:
      - ./scripts/redis/redis.conf:/usr/local/etc/redis/redis.conf
      - redis-node-1-data:/data
  redis-node-2:
    image: redis:7-alpine
    command: redis-server /usr/local/etc/redis/redis.conf
    ports: ["7002:6379"]
    volumes:
      - ./scripts/redis/redis.conf:/usr/local/etc/redis/redis.conf
      - redis-node-2-data:/data
  redis-node-3:
    image: redis:7-alpine
    command: redis-server /usr/local/etc/redis/redis.conf
    ports: ["7003:6379"]
    volumes:
      - ./scripts/redis/redis.conf:/usr/local/etc/redis/redis.conf
      - redis-node-3-data:/data
  redis-cluster-init:
    image: redis:7-alpine
    depends_on: [redis-node-1, redis-node-2, redis-node-3]
    command: >
      redis-cli --cluster create
      redis-node-1:6379 redis-node-2:6379 redis-node-3:6379
      --cluster-replicas 0 --cluster-yes

volumes:
  redis-node-1-data:
  redis-node-2-data:
  redis-node-3-data:
```

### Connection Pooling (FastAPI Integration)

```python
# src/cache/redis_client.py
from redis.asyncio.cluster import RedisCluster

_redis: RedisCluster | None = None

async def get_redis() -> RedisCluster:
    global _redis
    if _redis is None:
        _redis = RedisCluster(
            host=settings.redis_startup_host,
            port=settings.redis_startup_port,
            decode_responses=True,
            max_connections=100,        # pool cap, shared across requests
        )
    return _redis

# FastAPI dependency for route handlers
async def redis_dependency() -> RedisCluster:
    return await get_redis()
```

- **Rationale**: a single pooled `RedisCluster` client is created at app startup (lifespan hook) and shared across all requests via `Depends(redis_dependency)`, avoiding per-request connection overhead. `redis-py`'s asyncio cluster client resolves hash slots and re-routes on `MOVED`/`ASK` automatically.

### Health Check Contribution

`GET /health` (owned by I-003) calls into this module to report cluster status:

```python
async def redis_health() -> dict:
    redis = await get_redis()
    try:
        await redis.ping()
        return {"redis": "ok"}
    except Exception:
        return {"redis": "degraded"}  # cluster reachable but a check failed
```

- A `degraded` (not `down`) status is intentional: per spec decision #7 and the risk register, a single-node failure should not flip the whole service to unhealthy — the cluster keeps serving other slots.

### HA Behavior: Single-Node Failure

- **Detection**: Gossip protocol marks the node `PFAIL` then `FAIL` after enough peer nodes agree (default `cluster-node-timeout` 15s)
- **Production topology (1 replica per master)**: the replica for the failed master is promoted automatically; writes to that master's hash slots resume once promotion completes (typically within a few seconds)
- **Local/dev topology (no replicas)**: a lost node's hash slots become unavailable until the node restarts and rejoins with its AOF-restored data; this is acceptable for local dev but not representative of production HA and should not be used to validate failover behavior
- **Client behavior during failover**: `redis-py`'s cluster client retries against the new topology after a `CLUSTERDOWN`/`MOVED` response; requests to unaffected slots are unaffected
- **Consistency during failover**: per spec decision #7, results may be briefly stale or a rate-limit/uniqueness check may momentarily fail closed (safe direction — reject rather than double-count); this is the accepted trade-off, not a bug

---

## Acceptance Criteria

- [x] Redis Cluster running with 3+ master nodes (`CLUSTER INFO` reports `cluster_state:ok`)
- [x] AOF and RDB persistence both enabled and verified (restart a node, confirm data present)
- [x] Key namespace table (above) documented in `docs/architecture/redis-keys.md` and treated as the canonical reference for I-004 through I-015
- [x] `docker-compose` brings up a working 3-node cluster locally with a single command
- [x] Shared async connection pool (`src/cache/redis_client.py`) importable by any future module
- [x] `GET /health` reports Redis cluster status (`ok` / `degraded`)
- [x] Killing one node (`docker stop`) does not take the whole cluster down; other slots remain writable
- [x] Connection pool survives a node failover without requiring app restart (verified with a scripted test)
- [x] No API routes, Pydantic schemas, or business logic included in this issue — verified by review

---

## Testing Strategy

- **Unit Tests**: Connection pool initialization (singleton behavior, config parsing)
- **Integration Tests**: Write a key, kill a node, confirm cluster stays reachable and unaffected slots still serve reads/writes; restart node, confirm AOF replay restores its data
- **Failover Tests**: Simulate `docker stop redis-node-2` mid-write-loop, confirm client retries and eventually succeeds once topology stabilizes (production replica topology only)
- **Prior Art**: None — this is new infrastructure with no direct Adidos precedent to mirror

---

## Out of Scope

- Business logic for the queue, uniqueness, rate limiting, result cache, or duplicate blocking — those are I-005, I-006, I-007, I-009, I-015 respectively, all of which depend on this issue but are implemented separately
- Any HTTP endpoint or route handler (this issue has no API surface)
- Cross-region replication or multi-cluster setups
- Redis ACL / auth hardening beyond what's needed for cluster bring-up (tracked separately if required before launch)

---

## Related Issues

- I-001: Database Schema (parallel, no hard dependency)
- I-004: Authentication Middleware (depends on this for `auth:token:{token_hash}` cache)
- I-005: Vote Acceptance & Queueing (depends on this for `queue:votes`)
- I-006: Uniqueness Enforcement (depends on this for `vote:user:{user_id}:poll:{poll_id}`)
- I-007: Rate Limiting (depends on this for `rate_limit:*` keys)
- I-009: Result Aggregation (depends on this for `cache:poll:*:answer:*` keys)
- I-015: Duplicate Detection / 3-Strike Block (depends on this for `blocked:{user_id}:{ip}`)

---

## Implementation Checklist

- [x] Create `scripts/redis/redis.conf` (cluster mode, AOF + RDB config)
- [x] Create `docker-compose.yml` Redis service definitions (3 nodes + cluster-init)
- [x] Create `src/cache/redis_client.py` (async `RedisCluster` client, pooled, FastAPI dependency)
- [x] Create `src/cache/health.py` (`redis_health()` contribution to `/health`)
- [x] Document key namespace table in `docs/architecture/redis-keys.md`
- [x] Document local cluster bring-up in `docs/setup/redis-cluster.md`
- [x] Test node failure locally (`docker stop` one node, confirm cluster + client behavior)
- [x] Test AOF persistence (restart a node, confirm queued data present)

---

**Acceptance**: Cluster provisioned and documented, key namespace agreed and published, health check integrated, PR reviewed and merged.
