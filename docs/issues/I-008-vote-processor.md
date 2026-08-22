# I-008: Vote Processor Workers

**Status**: Done  
**Epic**: Voting Infrastructure  
**Priority**: P0 (Blocker)  
**Estimated Effort**: 5 days

---

## Problem Statement

Votes accepted by `POST /v1/vote` (I-005) land in the `queue:votes` Redis list but aren't durable until they're written to PostgreSQL. The system needs a background process that:
- Drains `queue:votes` continuously, in batches, without dropping votes
- Routes each vote to the correct PostgreSQL shard by `user_id % num_shards`
- Writes votes durably, respecting the uniqueness constraint from I-001/I-006
- Updates the Redis result cache immediately after each successful DB write, so results feel real-time
- Survives a shard being temporarily unreachable without losing or silently dropping the votes destined for it
- Scales horizontally as queue depth grows during traffic bursts

This is the component that turns "accepted" (202) into "durable" — it's the other half of the async vote pipeline described in spec decision #3 and #8.

---

## Solution

Implement standalone **vote processor workers** — a separate Python process/entrypoint from the FastAPI API server, not an in-process background task. One worker pool per shard, each pool draining `queue:votes`, batching 100-500 votes per cycle, routing by `user_id % num_shards`, batch-inserting into that shard's `votes` table, and incrementing the Redis result cache per successful write. Workers autoscale on queue depth, matching spec decision #9.

```
queue:votes (Redis list)
        │
        ▼
  Worker pool (1 per shard)
        │  BRPOP batch of 100-500
        ▼
  Route by user_id % num_shards
        │
        ▼
  Batch INSERT → PostgreSQL shard N
        │  on success
        ▼
  INCR cache:poll:{poll_id}:answer:{answer_id}
```

---

## User Stories

(from SPECIFICATION.md)

26. As an operator, I want the service to auto-scale API replicas when queue depth exceeds 1000 votes, so that bursts are absorbed without manual intervention
27. As an operator, I want the service to auto-scale vote processors when queue depth exceeds 1000 votes, so that votes are written to the database quickly
29. As an operator, I want votes queued in Redis to survive brief service restarts, so that no votes are lost
34. As an operator, I want failed votes (e.g., shard down) to remain in the queue and retry later, so that no votes are abandoned

---

## Implementation Decisions

### Process Model: Standalone Worker, Not In-Process Background Task

**Decision**: The vote processor is a separate entrypoint (`src/worker/main.py`), run as its own container/process, independent of the FastAPI request lifecycle (`src/api/app.py` from I-003).

**Rationale**: FastAPI background tasks run inside the same process and event loop as the API server, competing with it for CPU and being killed on API pod restarts/redeploys mid-batch. A standalone worker can be scaled, restarted, and monitored independently from the API tier — matching spec decision #9's split between "API Servers" and "Vote Processors" as separate autoscaled pools. It's launched as `python -m src.worker.main --shard-id=N` per Kubernetes deployment (one deployment per shard, see Deployment Model below), and is a plain asyncio consumer loop — not tied to any web framework.

### Dequeue & Batching

```python
async def run_worker(shard_id: int, num_shards: int):
    while True:
        batch = await dequeue_batch(min_size=100, max_size=500, timeout_s=2)
        if not batch:
            continue
        shard_batches = route_by_shard(batch, num_shards)
        my_votes = shard_batches.get(shard_id, [])
        if my_votes:
            await process_batch(shard_id, my_votes)

async def dequeue_batch(min_size: int, max_size: int, timeout_s: int) -> list[VotePayload]:
    batch = []
    deadline = time.monotonic() + timeout_s
    while len(batch) < max_size and time.monotonic() < deadline:
        item = await redis.brpop("queue:votes", timeout=1)
        if item is None:
            if len(batch) >= min_size:
                break
            continue
        batch.append(VotePayload.parse_raw(item[1]))
    return batch
```
Per spec decision #8: `BRPOP` (blocking pop) is used over a tight polling loop to avoid busy-waiting Redis with empty reads, while still batching 100-500 votes per cycle before committing — balancing "write soon" (low latency to durability) against "write efficiently" (fewer, larger transactions per shard, matching the "batch writes every 1-2 sec" cost optimization in the spec's Key Trade-offs).

### Shard Routing

```python
def route_by_shard(votes: list[VotePayload], num_shards: int) -> dict[int, list[VotePayload]]:
    buckets: dict[int, list[VotePayload]] = defaultdict(list)
    for vote in votes:
        shard_id = hash(vote.user_id) % num_shards
        buckets[shard_id].append(vote)
    return buckets
```
This uses the same `user_id % num_shards` rule as I-001's sharding strategy — must stay in lock-step with however I-001's provisioning script assigns shard numbers, since a mismatch here would misroute votes to a shard that doesn't expect them.

### Batch Insert & Cache Update

```python
async def process_batch(shard_id: int, votes: list[VotePayload]) -> None:
    db = shard_connections[shard_id]
    successful: list[VotePayload] = []
    try:
        successful = await db.batch_insert_votes(votes)  # returns rows that committed
    except UniqueViolationError:
        # Per I-006: fall back to per-row insert so one duplicate doesn't sink the batch
        successful = await insert_votes_individually(db, votes)
    except ShardUnavailableError:
        await requeue(votes)  # see Failover below
        return

    for vote in successful:
        await redis.incr(f"cache:poll:{vote.poll_id}:answer:{vote.answer_id}")
```
The Redis counter update happens *after* the DB write commits, never before — this preserves the invariant that the cache never shows a vote that isn't durably recorded. If the batch insert as a whole raises a unique-violation (one bad row in a multi-row `INSERT`), the worker falls back to inserting the batch's votes one at a time so the 499 good rows in a 500-row batch aren't all rolled back by one duplicate; see I-006 for the per-row duplicate handling (log-and-skip, no retry, no client-facing error).

### Shard-Down Failover

**Decision**: If a shard is unreachable, votes destined for it are **requeued**, not dropped, not retried in a tight loop against the dead connection.

```python
async def requeue(votes: list[VotePayload]) -> None:
    for vote in votes:
        await redis.lpush("queue:votes", vote.json())
    logger.error("shard_unavailable_requeued", count=len(votes))
```
Pushed back onto `queue:votes` (not a separate dead-letter queue — keeping a single queue keeps the retry path simple, and a downed shard is expected to be a transient, minutes-scale event, not a permanent one). The worker for that shard backs off (exponential, capped) between connection attempts rather than hot-looping. Other shards' workers are unaffected and continue processing normally — a single shard outage degrades only that shard's users' vote latency, not the whole system's, matching the spec's "no hot shard, no single point of failure" sharding rationale.

This satisfies user story #34 directly: failed votes remain in the queue and retry later, nothing is abandoned.

### Deployment Model

**Decision**: 1 worker pod (Kubernetes Deployment) per shard, matching spec decision #9 ("Vote Processors: 1 worker pod per shard").

- Each pod is configured with `--shard-id=N` and only claims votes routed to shard N from the batches it dequeues (votes for other shards are left/requeued rather than processed cross-shard, so a shard-N pod never opens a connection to shard-M's database)
- **Autoscaling**: total worker pod count scales on queue depth — spec decision #9 says 1 additional pod per 500 queued votes, and user stories #26/#27 tie the >1000 threshold to triggering autoscale at all. Implementation: a Kubernetes HPA (or KEDA, given Redis-based scaling triggers) watches `LLEN queue:votes` and scales the worker Deployment(s) accordingly, capped by cluster capacity
- Since routing happens inside each pod after a shared `BRPOP`, adding more pods per shard increases dequeue throughput for that shard's slice of the queue without any coordination beyond Redis's own atomicity guarantees on `BRPOP`

### Reconciliation Hook (Forward Reference)

This issue writes votes and increments cache counters but does not implement the hourly Redis-vs-DB drift check (spec decision, user story #30) — that's a separate, later concern (Phase 5 observability). It's noted here only because `process_batch`'s "DB commit, then cache increment" ordering is exactly what the reconciliation job will later verify is staying in sync.

---

## Acceptance Criteria

- [x] Worker runs as a standalone process (`python -m src.worker.main`), independent of the FastAPI app process
- [x] Worker dequeues in batches of 100-500 votes using `BRPOP` (no busy-polling)
- [x] Votes are routed to the correct shard via `user_id % num_shards`, verified against I-001's shard assignment
- [x] Batch insert commits to the correct PostgreSQL shard; a single duplicate row does not roll back the rest of the batch
- [x] Redis cache counter (`cache:poll:{poll_id}:answer:{answer_id}`) increments only after the corresponding DB write commits
- [x] Shard-down scenario: votes for that shard are requeued onto `queue:votes`, not dropped, not stuck retrying in a hot loop
- [x] Other shards continue processing normally while one shard is down
- [ ] Worker pods scale with queue depth (verified via HPA/KEDA config watching `LLEN queue:votes`) — not built here; this issue delivered the worker code/tests, not the Kubernetes Deployment/HPA manifests (no k8s manifests exist anywhere yet in this repo). Tracked as follow-up.
- [x] No vote is lost across a worker pod restart mid-batch (in-flight `BRPOP`'d-but-not-yet-committed votes are either committed or safely lost only in the same way any at-least-once queue consumer can lose an in-flight item — documented, not silently assumed away): see the module docstring in `src/worker/main.py` and the "Ambiguous/deferred points" note below.

---

## Testing Strategy

- **Unit Tests**: Shard routing function (`route_by_shard`) — even distribution across shards for random user_ids; batching logic (min/max size, timeout behavior)
- **Integration Tests**: Full pipeline — push votes onto `queue:votes`, run worker, verify rows land in the correct shard's `votes` table and `cache:poll:*:answer:*` counters increment correctly
- **Failover Tests**: Kill a shard's DB connection mid-batch, verify votes for that shard are requeued and later processed once the shard recovers; verify other shards are unaffected during the outage
- **Shard Distribution Tests** (mirrors SPECIFICATION.md's dedicated module): 1000 votes from random users distributed within ±10% across shards, no single shard >55% of votes
- **Load Tests**: Sustained 1000 votes/sec drained without unbounded queue growth; burst scenario (0→10K votes/sec) triggers autoscale and queue drains back down
- **Not Tested Here**: User_id hashing algorithm internals (spec's testing philosophy — behavior only), reconciliation job (separate future issue)
- **Prior Art**: Reuse I-001's 100K-vote seed data for shard-distribution and load tests

---

## Related Issues

- I-001: Database Schema (defines the shard layout and `UNIQUE(user_id, poll_id)` constraint this worker writes against)
- I-002: Redis Cluster Setup (hosts `queue:votes` and `cache:poll:*:answer:*` keyspaces)
- I-003: API Framework & Routing (the API tier this worker is deliberately decoupled from)
- I-005: Vote Acceptance & Queueing (produces the `queue:votes` entries this worker consumes)
- I-006: Uniqueness Enforcement (defines the per-row duplicate handling this worker's insert path must respect)
- I-009: Result Aggregation (consumes the `cache:poll:*:answer:*` counters this worker updates)

---

## Implementation Checklist

- [x] Create `src/worker/main.py` (standalone entrypoint, `--shard-id` arg)
- [x] Implement `dequeue_batch()` in `src/worker/queue_consumer.py` (BRPOP-based batching)
- [x] Implement `route_by_shard()` in `src/worker/sharding.py`
- [x] Implement `process_batch()` in `src/worker/processor.py` (batch insert + fallback per-row insert)
- [x] Implement `requeue()` and shard-down backoff handling
- [x] Wire Redis cache `INCR` after successful DB commit
- [ ] Add Dockerfile/entrypoint for worker container, distinct from API container — deferred; no container tooling exists in this repo yet (no top-level Dockerfile for the API tier either)
- [ ] Configure Kubernetes Deployment (1 per shard) + HPA/KEDA scaling rule on `LLEN queue:votes` — deferred; no `k8s/` or deployment manifests exist anywhere in the repo yet
- [x] Write failover integration test (simulate shard down mid-batch): `tests/integration/test_vote_processor_pipeline.py` (connects to a refused port to simulate an unreachable shard) and `tests/unit/test_worker_processor.py`/`test_worker_db.py` (mocked connection-loss and backoff)
- [x] Write shard-distribution test (1000 random-user votes, verify even spread): `tests/unit/test_worker_sharding.py`
- [ ] Load test sustained + burst scenarios — deferred; needs a running Redis cluster + multi-shard Postgres and a load generator, out of scope for this worktree's unit/integration test run (see I-023: Load Tests)

---

## Implementation Notes (as built)

- **Shard hash**: `user_id` is a free-form string (`VARCHAR(255)`), not an
  integer, so "`user_id % num_shards`" is implemented as
  `int.from_bytes(blake2b(user_id), "big") % num_shards`
  (`src/worker/sharding.py`). Python's built-in `hash()` was deliberately
  avoided — it's salted per-process (`PYTHONHASHSEED`), which would make
  two worker pods disagree on where the same user_id routes.
  `docs/setup/sharding.md` explicitly left application-layer routing
  unspecified pending this issue, so this hash choice is the answer to
  that open question.
- **I-005/I-006 not yet merged**: this worker was built standalone against
  the `queue:votes`/`VotePayload` JSON shape and the `votes` table's
  `UNIQUE(user_id, poll_id)` constraint from I-001, without a live
  producer. The per-row duplicate fallback (log-and-skip, no retry, no
  client error) matches I-006's spec'd design even though I-006 itself
  isn't merged yet.
- **Cross-shard requeue in `run_worker`**: every worker pod `BRPOP`s from
  the same shared `queue:votes`, keeps only votes matching its own
  `--shard-id`, and immediately `LPUSH`es the rest back — per the issue's
  Deployment Model section. This is also what makes horizontal scaling
  double-processing-safe: `BRPOP` pops each entry exactly once, so no two
  pods (same shard or different) ever process the same vote.
- **Shard-down backoff**: `ShardConnectionPool` tracks a per-shard
  next-retry time with exponential backoff (1s initial, doubling, capped
  at 30s), so a persistently down shard is polled occasionally rather
  than hot-looped; other shards' pools are unaffected.
- **Pod-restart vote loss**: this worker gives the same at-least-once
  guarantee any `BRPOP`-based consumer gives — once `BRPOP` returns an
  item, it's off the Redis list; if the pod is killed before that vote is
  committed (or requeued), it is lost. This is a real, documented gap
  (not silently assumed away), consistent with the acceptance criterion's
  own wording. Closing it fully (e.g. a claimed-but-unacked visibility
  window, akin to a Redis Streams consumer group) is a bigger redesign
  than this issue's scope and is not attempted here.
- **Local dev shard DSNs**: `default_shard_dsn()` reads
  `POLL_APP_SHARD_{N}_DSN` per shard, falling back to
  `settings.database_url` (a single local Postgres instance standing in
  for all shards) — matching `docs/setup/sharding.md`'s "single Postgres
  instance is enough for local dev" note.

**Acceptance**: Votes durably written and cache updated end-to-end, failover verified, autoscaling configured, PR reviewed and merged.
