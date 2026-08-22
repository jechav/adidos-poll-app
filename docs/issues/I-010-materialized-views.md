# I-010: Materialized Views (Fallback)

**Status**: Done  
**Epic**: Result Aggregation & Caching  
**Priority**: P1  
**Estimated Effort**: 2 days

---

## Problem Statement

I-009 makes Redis the primary source of poll results, which is fast but not durable — if the Redis Cluster is unreachable, GET /v1/polls (I-011) still needs to return something rather than a 503. The spec is explicit about this trade-off (Implementation Decision #6, #16): "Graceful Degradation... fall back to computing results from materialized views (slower but accurate)" and "Availability over Consistency: Stale cached results > errors."

The `vote_counts` table itself already exists — I-001 created its schema (`poll_id`, `answer_id`, `count`, `percentage`, `last_updated_at`), replicated to all shards. What's missing is the job that actually keeps it populated: a periodic recomputation from the `votes` table, refreshed every 5 minutes, that becomes the fallback data source when Redis is down and later the reference point for drift detection between Redis and PostgreSQL.

This is meaningfully harder than the Redis path in one specific way: `votes` is **sharded** by `user_id` across 8+ PostgreSQL nodes, so no single shard has the full picture for any poll. Computing a poll's total requires a cross-shard fan-in (query all shards, sum), whereas Redis's counters are already globally correct the instant each vote lands, because there's only one Redis Cluster keyspace.

---

## Solution

A scheduled job (Kubernetes CronJob, `*/5 * * * *`) that:
1. Queries every shard in parallel for `(poll_id, answer_id) → vote count`, using an outer join against the replicated `answers` table so answers with zero votes still appear
2. Sums the per-shard counts into a single global count per `(poll_id, answer_id)`
3. Computes percentages using the exact same rounding rule as I-009, so the Redis path and the fallback path never visibly disagree
4. Upserts the merged rows into `vote_counts` — broadcast to every shard, since `vote_counts` is a replicated reference table, not itself sharded

The same table also becomes the comparison target for the hourly Redis-vs-DB drift check (spec user story #30). This issue does not implement that reconciliation job — it's an observability-phase concern (I-017+) — but it does need `vote_counts` to be a reliable, timestamped ground truth for that job to compare against.

---

## User Stories

1. As a user, I want to see poll results even if Redis is down, so that a cache outage doesn't make the app unusable (spec: "Graceful Degradation")
2. As an operator, I want stale-but-available results during a Redis outage rather than errors, so that the service stays up (spec principle: Availability over Consistency)
3. As an operator, I want an hourly job to compare Redis counts against a trustworthy database source, so that I catch drift greater than 1% (spec story #30) — this issue provides that trustworthy source
4. As a database admin, I want the recomputation job to aggregate across all 8+ shards correctly, so that a poll's total isn't silently undercounted by only reading one shard
5. As a developer, I want the fallback's percentage math to match the Redis path's rounding exactly, so results don't visibly jump when a client fails over between the two

---

## Implementation Decisions

### Zero-Vote Answers Must Appear

A naive `SELECT poll_id, answer_id, COUNT(*) FROM votes GROUP BY poll_id, answer_id` only returns rows for answers that have at least one vote — a freshly-activated poll with zero votes would be entirely absent from `vote_counts`. Since `answers` is replicated to every shard identically, each shard's query starts from `answers` and left-joins `votes`:

```sql
SELECT
  a.poll_id,
  a.answer_id,
  COUNT(v.vote_id) AS vote_count
FROM answers a
LEFT JOIN votes v ON v.answer_id = a.answer_id
GROUP BY a.poll_id, a.answer_id;
```

This is safe to run identically on every shard: `answers` is replicated (full set on every shard), while `votes` only contributes that shard's own rows via the join — exactly the partial count needed before cross-shard summing.

### Cross-Shard Fan-In

```python
async def refresh_vote_counts(shards: list[asyncpg.Pool]) -> None:
    per_shard_results = await asyncio.gather(
        *(shard.fetch(SHARD_COUNT_QUERY) for shard in shards),
        return_exceptions=True,
    )

    merged: dict[tuple[UUID, UUID], int] = defaultdict(int)
    for shard_result in per_shard_results:
        if isinstance(shard_result, Exception):
            logger.warning("shard unreachable during vote_counts refresh", exc_info=shard_result)
            continue  # partial results are acceptable; don't block the whole job on one bad shard
        for row in shard_result:
            merged[(row["poll_id"], row["answer_id"])] += row["vote_count"]

    await upsert_vote_counts(merged)
```

A shard being temporarily unreachable degrades this job's accuracy for that shard's users, but does not fail the whole run — consistent with the spec's availability-first posture. The next 5-minute run naturally self-heals once the shard recovers.

### Percentage Rounding (Shared With I-009)

Percentages are computed with the identical rule as I-009's `_percentage()`: independent rounding per answer to 2 decimal places, `0.0` when `total_votes == 0`, no largest-remainder correction. This is a hard requirement, not a suggestion — if the two paths round differently, a client that fails over from Redis to this fallback mid-session will see percentages visibly shift for reasons unrelated to new votes.

```python
def _percentage(count: int, total_votes: int) -> float:
    if total_votes == 0:
        return 0.0
    return round((count / total_votes) * 100, 2)
```

### Broadcast Upsert

Because `vote_counts` is replicated (every shard carries the full table, per I-001), the job writes the same merged rows to every shard connection, not just one:

```sql
INSERT INTO vote_counts (poll_id, answer_id, count, percentage, last_updated_at)
VALUES ($1, $2, $3, $4, now())
ON CONFLICT (poll_id, answer_id)
DO UPDATE SET
  count = EXCLUDED.count,
  percentage = EXCLUDED.percentage,
  last_updated_at = EXCLUDED.last_updated_at;
```

Executed against all 8+ shards in parallel after the merge step. This is a deliberate MVP simplification — true multi-node logical replication for `vote_counts` is out of scope (see below).

### Job Scheduling

Deployed as a Kubernetes CronJob (`*/5 * * * *`) running `scripts/jobs/refresh_vote_counts.py`, rather than an in-process scheduler — this keeps it operationally independent of the API server and vote processor deployments, and makes a stuck run trivially killable/restartable without touching live traffic.

### Fallback Trigger (Consumed by I-011)

`last_updated_at` is the client-visible staleness signal: I-011 surfaces it (or a derived `stale: true` flag) whenever it serves from this table instead of Redis, so the up-to-5-minutes staleness window (spec Decision #16) is honest rather than silent.

---

## Acceptance Criteria

- [x] CronJob runs every 5 minutes and completes well within that window at current data volumes (`deploy/cronjobs/refresh-vote-counts.yaml`, `activeDeadlineSeconds: 240`)
- [x] Job aggregates `votes` across all 8+ shards (cross-shard fan-in) before writing any row
- [x] Answers with zero votes still appear in `vote_counts` with `count = 0` (via `LEFT JOIN` from `answers`, not `GROUP BY` on `votes` alone)
- [x] Percentages match I-009's rounding rule exactly (independent per-answer rounding, `0.0` at zero votes) — reuses `_percentage()` directly, not reimplemented
- [x] Upserts are idempotent — re-running the job twice in a row produces identical `vote_counts` rows (aside from `last_updated_at`)
- [x] One shard being unreachable does not fail the entire job run; other shards are still processed and logged
- [x] `vote_counts` rows are written/refreshed on every shard (broadcast), not just one
- [ ] I-011's fallback path successfully reads correct results from `vote_counts` when Redis is simulated as down (integration/chaos test) — **deferred**: I-011 (GET /v1/polls) doesn't exist yet, so this end-to-end criterion can't be exercised from this ticket. I-011's own implementation must add this test when it builds the fallback-read path this issue's `vote_counts` table now supports.
- [x] `last_updated_at` accurately reflects the most recent successful job run, usable as a staleness signal

---

## Testing Strategy

- **Unit Tests**: Percentage rounding parity with I-009 (same fixtures, same expected outputs); merge logic across mocked shard results, including one shard returning an error
- **Integration Tests**: Seed votes across multiple real (or test-container) shards, run the job, verify `vote_counts` rows match hand-computed expectations; verify zero-vote answers are present
- **Fallback Test**: Disable/mock Redis in the API layer, confirm GET /v1/polls (I-011) reads from `vote_counts` instead of erroring
- **Prior Art**: Spec's Result Aggregation Tests module — "If Redis down, results computed from materialized view (slower, but accurate)"

---

## Out of Scope

- The hourly Redis-vs-DB drift reconciliation job itself (spec story #30) — owned by the observability phase (I-017+); this issue only guarantees `vote_counts` is a trustworthy, timestamped comparison target for it
- True cross-node logical replication for `vote_counts` — the broadcast-write approach here is an intentional MVP simplification
- Real-time/event-triggered refresh — this table is refresh-on-schedule only, never updated synchronously with a vote
- Cross-shard distributed transactions — the merge happens in application code, not a distributed SQL transaction

---

## Related Issues

- I-001: Database Schema & Migrations (`vote_counts` table schema, `votes`/`answers` tables this job reads)
- I-008: Vote Processor Workers (writes the `votes` rows this job aggregates)
- I-009: Result Aggregation (Redis Cache) (primary path; this issue's percentage rounding must match it exactly)
- I-011: GET /v1/polls Endpoint (consumes this table as its fallback when Redis is unavailable)

---

## Implementation Checklist

- [x] Create `scripts/jobs/refresh_vote_counts.py` (cross-shard fan-in, merge, broadcast upsert)
- [x] Reuse I-008's `ShardConnectionPool` (`src/worker/db.py`) for parallel per-shard connections rather than creating a second, redundant shard-pool abstraction
- [x] Reuse `_percentage()` from I-009's `src/services/result_aggregator.py` rather than reimplementing it
- [x] Add Kubernetes CronJob manifest (`*/5 * * * *`) in `deploy/cronjobs/refresh-vote-counts.yaml`
- [x] Unit tests: `tests/unit/test_refresh_vote_counts.py` (rounding parity, partial-shard-failure handling)
- [x] Integration test: `tests/integration/test_refresh_vote_counts_db.py` against a real Postgres instance (seeded votes, zero-vote answers, idempotency) — run as a single "shard" since local dev has one Postgres instance standing in for all shards (same constraint as I-008/I-012's integration tests); the cross-shard merge arithmetic itself is covered against fakes in the unit suite
- [ ] Fallback integration test: Redis down, GET /v1/polls still returns correct (if stale) results — **deferred to I-011**, which doesn't exist yet (see Acceptance Criteria note above)
- [x] Document the fallback trigger condition and staleness window in `docs/architecture/caching.md`

---

**Acceptance**: CronJob deployed and running on schedule, `vote_counts` verified correct against seeded data, fallback path validated end-to-end, PR reviewed and merged.
