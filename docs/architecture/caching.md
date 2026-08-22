# Result Aggregation Caching (I-009)

Implements [I-009](../issues/I-009-result-aggregation.md). Covers the two
Redis-backed pieces `src/services/result_aggregator.py` and
`src/cache/answer_cache.py` own: the vote-counter read path and the
answers-metadata cache. The canonical key table for the whole app (every
issue, not just I-009) lives in
[redis-keys.md](redis-keys.md) — this document only goes into the policy
detail that table doesn't have room for.

## Key structure

| Key | TTL | Written by | Read by |
|---|---|---|---|
| `cache:poll:{poll_id}:answer:{answer_id}` | None | I-008 (`INCR` after each durable vote write) | I-009 |
| `cache:poll:{poll_id}:answers` | 3600s (1h) | I-009 (lazy populate on miss) | I-009 |

`cache:poll:{poll_id}:answers` holds a JSON array:

```json
[
  {"answer_id": "...", "text": "...", "order": 0},
  {"answer_id": "...", "text": "...", "order": 1}
]
```

### Why the answers cache has a TTL and the counters don't

A poll's two answers are immutable once the poll is active (DOMAIN_MODEL.md
`Answer` invariants), so staleness isn't a correctness concern the way it
is for the vote counters. The 1h TTL exists purely to bound memory and
let a corrected `answers` row (a pre-activation edit, or manual data
fix) eventually propagate — not because anything invalidates it directly.
The counters, in contrast, are live and read on every result computation,
so they're never given a TTL; they age out only as an operational
decision (spec decision #16), not one this module makes.

## Read path

`compute_poll_results(poll_id, redis)`:

1. `get_cached_answers(poll_id, redis)` — read-through cache; a miss
   queries the replicated `answers` table and populates the cache before
   returning.
2. Build the two counter keys from the answers' `answer_id`s.
3. `mget_pipelined(redis, keys)` — both counters in one pipelined round
   trip (a plain cross-slot `MGET` isn't safe against Redis Cluster, see
   `src/cache/redis_client.py`).
4. Missing/expired counter keys (`None`) are coerced to `0` before
   summing — identical treatment to a counter key that exists and is `0`.

`compute_poll_results_batch(poll_ids, redis)` does the same, but flattens
every requested poll's counter keys into one flat list before the single
`mget_pipelined` call, then regroups the results by `poll_id` — so
rendering N polls costs one counter round trip, not N.

## Rounding rule

Each answer's percentage is computed independently:

```python
def _percentage(count: int, total_votes: int) -> float:
    if total_votes == 0:
        return 0.0
    return round((count / total_votes) * 100, 2)
```

- `total_votes == 0` always yields `0.0` for every answer — never `NaN`
  or a `ZeroDivisionError`. A brand-new active poll with no counter keys
  in Redis yet (never `INCR`ed) is treated identically to one with
  counters present but at `0`.
- Rounding to 2 decimal places matches `vote_counts.percentage`'s
  `DECIMAL(5,2)` column (I-001).
- Because each answer's percentage is rounded independently, the two
  percentages for a poll may sum to something in the 99.98-100.02 range
  rather than exactly 100.00. This is intentional — no largest-remainder
  or other summing correction is applied, consistent with the
  `VoteCount` invariant in DOMAIN_MODEL.md ("Percentages sum to 100% (or
  <=100% if rounding)") and the spec's simplicity-over-strict-consistency
  posture.
- **I-010 must use this exact same rule** when computing
  `vote_counts.percentage` in its materialized-view fallback, so the
  Redis path and the PostgreSQL fallback never visibly disagree when a
  client fails over between them mid-session.

## Failure behavior

Neither `compute_poll_results` nor `compute_poll_results_batch` catches
Redis errors — a connection failure or timeout propagates to the caller.
I-011 is responsible for catching that and falling back to I-010's
`vote_counts` table; this module's job is correct computation when Redis
is healthy, not resilience when it isn't.

## Fallback: `vote_counts` (I-010)

When Redis is unreachable, I-011 falls back to `vote_counts` — a
materialized table refreshed every 5 minutes by the
`refresh-vote-counts` CronJob (`scripts/jobs/refresh_vote_counts.py`).
`votes` is sharded by `user_id` across 8+ Postgres shards, so no single
shard has a poll's full count; the job fans out to every shard in
parallel, sums the partial counts (a shard that's unreachable is logged
and skipped, not fatal to the run), and broadcasts the merged rows —
`(poll_id, answer_id, count, percentage, last_updated_at)` — to every
shard, since `vote_counts` itself is replicated, not sharded.

**Fallback trigger and staleness window**: I-011 treats a `vote_counts`
read as the up-to-5-minutes-stale answer, not the always-fresh one — it
surfaces `last_updated_at` (or a derived `stale: true` flag) whenever it
serves from this table instead of Redis, so that staleness window is
honest to the client rather than silent. `last_updated_at` reflects the
most recent successful job run and is the only staleness signal; there
is no synchronous, per-vote update path into `vote_counts`.

**Percentage rounding is identical to the Redis path above** — the job
imports and reuses `_percentage()` from `src/services/result_aggregator.py`
rather than reimplementing it, specifically so a client that fails over
from Redis to this fallback mid-session never sees percentages jump for
reasons unrelated to new votes.
