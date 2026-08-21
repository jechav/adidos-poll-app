# I-009: Result Aggregation (Redis Cache)

**Status**: Ready for Implementation  
**Epic**: Result Aggregation & Caching  
**Priority**: P0 (Blocker)  
**Estimated Effort**: 3 days

---

## Problem Statement

Once a vote is written to PostgreSQL, the vote processor (I-008) increments a live counter in Redis at `cache:poll:{poll_id}:answer:{answer_id}`. Something has to turn those raw counters into the numbers a client actually wants: total votes cast on a poll, and the percentage breakdown per answer — computed fast enough that GET /v1/polls (I-011) never has to touch PostgreSQL on the hot path.

This issue owns exactly that computation layer. It does **not** own:
- The counters themselves, or how they're incremented (I-008 writes them after each successful DB write)
- The PostgreSQL fallback (I-010 owns the `vote_counts` materialized table, used only if Redis is unreachable)
- The HTTP endpoint that serves results to clients (I-011 owns GET /v1/polls and assembles the final response)

Without a dedicated aggregation layer, every consumer of poll results (I-011 today, potentially admin tooling later) would reimplement counter reads, percentage math, and the zero-vote edge case independently — with a high chance of drifting rounding behavior between them.

---

## Solution

A small, synchronous-feeling async module that, given a `poll_id` (or a batch of them), reads the relevant Redis counters in a single pipelined round trip and returns vote counts + percentages per answer, plus the poll's total. Target: P95 < 50ms per the spec's Result Aggregation decision (#6).

Two supporting pieces are needed beyond the raw vote counters:
1. **Answer metadata cache** — to know *which* two `answer_id`s belong to a poll before we can build the two counter keys to `MGET`.
2. **Deterministic rounding** — a single, documented rule for turning counts into percentages so I-010's materialized view and this module never disagree on how a tie or a zero-vote poll is displayed.

---

## User Stories

1. As the developer implementing I-011, I want a function that returns total votes and per-answer percentages for a poll from Redis, so that GET /v1/polls never queries PostgreSQL on the happy path
2. As a user, I want immediate feedback after voting that shows current results, so that I can see how my answer compares to others (spec story #3)
3. As a user, I want to see vote counts and percentage breakdown per answer, so that I understand overall sentiment (spec story #5)
4. As the system, I want percentage calculations to handle a poll with zero votes without raising a division error, so that newly-activated polls render cleanly
5. As an operator, I want result computation to complete in a single Redis round trip per poll, so that P95 latency stays under 50ms even under load

---

## Implementation Decisions

### Redis Key Structure (Read-Only From This Issue's Perspective)

```
cache:poll:{poll_id}:answer:{answer_id}  →  integer counter
```

This issue only ever reads these keys. They are written exclusively by the vote processor worker (I-008) via `INCR` immediately after a vote is durably written to its shard.

### Answer Metadata Cache

A poll always has exactly two answers, and answers are immutable once the poll is active (DOMAIN_MODEL.md `Answer` invariants). That makes answer metadata cheap to cache aggressively:

```
cache:poll:{poll_id}:answers → JSON [{"answer_id": "...", "text": "...", "order": 0}, {...}]  (TTL 1h)
```

Populated lazily on first miss from the replicated `answers` table (read replica), refreshed on TTL expiry. Because answers never change post-activation, staleness here is not a correctness concern the way it is for vote counts.

### Aggregation Function

```python
async def compute_poll_results(poll_id: UUID, redis: Redis) -> AggregatedResult:
    answers = await get_cached_answers(poll_id, redis)  # [{answer_id, text, order}, ...]
    keys = [f"cache:poll:{poll_id}:answer:{a['answer_id']}" for a in answers]

    raw_counts = await redis.mget(*keys)  # single round trip
    counts = [int(c) if c is not None else 0 for c in raw_counts]
    total_votes = sum(counts)

    results = [
        AnswerResult(
            answer_id=answer["answer_id"],
            text=answer["text"],
            vote_count=count,
            percentage=_percentage(count, total_votes),
        )
        for answer, count in zip(answers, counts)
    ]

    return AggregatedResult(poll_id=poll_id, total_votes=total_votes, answers=results)


def _percentage(count: int, total_votes: int) -> float:
    if total_votes == 0:
        return 0.0
    return round((count / total_votes) * 100, 2)
```

### Batch Variant

GET /v1/polls (I-011) renders a list of polls, not one. A naive per-poll `compute_poll_results` call would mean N Redis round trips for N polls in the response. This issue also provides a batched entry point that flattens all counter keys across all requested polls into a single `MGET`:

```python
async def compute_poll_results_batch(poll_ids: list[UUID], redis: Redis) -> dict[UUID, AggregatedResult]:
    ...  # one MGET across all polls' answer keys, then regroup by poll_id
```

### Zero-Vote Edge Case

- `total_votes == 0` → every answer's `percentage` is `0.0`, never `NaN` or a `ZeroDivisionError`
- A brand-new active poll with no counter keys yet in Redis (never incremented) is treated identically to a poll with counters present but at `0` — `MGET` returns `None` for missing keys, which is coerced to `0` before summing

### Percentage Rounding Consistency

- Each answer's percentage is rounded **independently** to 2 decimal places, matching the `DECIMAL(5,2)` type of `vote_counts.percentage` from I-001
- Because rounding is independent per answer, the two percentages may sum to something in the 99.98–100.02 range rather than exactly 100.00 — this is intentional and matches the `VoteCount` invariant in DOMAIN_MODEL.md ("Percentages sum to 100% (or ≤100% if rounding)")
- No largest-remainder or other summing correction is applied — simplicity is preferred over an exact-100 guarantee, consistent with the spec's availability/simplicity-over-strict-consistency posture
- I-010 must use this exact same rounding rule when computing `vote_counts.percentage`, so the Redis path and the PostgreSQL fallback never visibly disagree when a client fails over between them

### Failure Behavior

`compute_poll_results` (and its batch variant) do **not** swallow Redis errors — a connection failure or timeout propagates to the caller. I-011 is responsible for catching that and falling back to I-010's `vote_counts` table. This issue's job is correct computation when Redis is healthy, not resilience when it isn't.

---

## Acceptance Criteria

- [ ] `compute_poll_results(poll_id)` returns total votes and per-answer count + percentage from Redis counters
- [ ] `compute_poll_results_batch(poll_ids)` computes results for N polls in a single `MGET` round trip
- [ ] Answer metadata (`answer_id`, `text`, `order`) is cached with a 1-hour TTL, populated lazily from the replicated `answers` table on miss
- [ ] `total_votes == 0` returns `0.0` percentage for every answer — no exceptions raised
- [ ] Missing/expired counter keys are treated as count `0`, not an error
- [ ] Percentages are rounded to 2 decimal places, matching `vote_counts.percentage` (`DECIMAL(5,2)`)
- [ ] Redis errors propagate to the caller rather than being caught/hidden inside this module
- [ ] P95 latency < 50ms for `compute_poll_results` under concurrent load (measured, not assumed)
- [ ] Results reflect a vote within 5 seconds of the vote being written (spec Result Aggregation test: "Results available within 5 seconds of vote submission")
- [ ] No duplicate aggregation/rounding logic exists elsewhere — I-011 imports and uses this module directly

---

## Testing Strategy

- **Unit Tests**: Percentage rounding (including ties, e.g. 1 vote A / 2 votes B → 33.33% / 66.67%), zero-vote poll, missing counter keys, single-answer-has-all-votes case
- **Integration Tests**: Seed real Redis counters (simulating I-008's writes), verify `compute_poll_results` output matches expected counts/percentages; verify batch variant matches per-poll results for the same input
- **Performance Tests**: Concurrent `compute_poll_results` calls against a populated Redis Cluster, measure P95/P99, target < 50ms / documented ceiling
- **Prior Art**: Spec's "Result Aggregation Tests" module (SPECIFICATION.md) — mirror its scenarios directly (10 votes, 6/4 split → 60%/40%)

---

## Related Issues

- I-001: Database Schema & Migrations (`answers` table sourced for the metadata cache)
- I-002: Redis Cluster Setup (the cluster this module reads from)
- I-008: Vote Processor Workers (writes the `cache:poll:{poll_id}:answer:{answer_id}` counters this module reads)
- I-010: Materialized Views (Fallback) (PostgreSQL fallback used when this module's Redis calls fail; must share this module's rounding rule)
- I-011: GET /v1/polls Endpoint (primary consumer of `compute_poll_results_batch`)
- I-012: GET /v1/user/votes Endpoint (does not use this module — reads directly from `votes`, no Redis involvement)

---

## Implementation Checklist

- [ ] Create `src/services/result_aggregator.py` (`compute_poll_results`, `compute_poll_results_batch`)
- [ ] Create `src/cache/answer_cache.py` (cached answer metadata lookups, lazy population + TTL)
- [ ] Create `src/schemas/results.py` (`AggregatedResult`, `AnswerResult` Pydantic models)
- [ ] Add a pipelined `MGET` helper to `src/cache/redis_client.py`
- [ ] Unit tests: `tests/unit/test_result_aggregator.py` (rounding, zero votes, missing keys)
- [ ] Integration test: seed Redis counters directly, assert `compute_poll_results` output
- [ ] Performance test: benchmark P95 latency under concurrent reads (target < 50ms)
- [ ] Document the Redis key structure and TTL policy in `docs/architecture/caching.md`

---

**Acceptance**: Aggregation module implemented, P95 < 50ms validated under load, all tests pass, PR reviewed and merged.
