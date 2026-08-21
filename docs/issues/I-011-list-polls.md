# I-011: GET /v1/polls Endpoint

**Status**: Ready for Implementation  
**Epic**: Result Aggregation & Caching  
**Priority**: P0 (Blocker)  
**Estimated Effort**: 2 days

---

## Problem Statement

Users need a single endpoint that lists polls with their live results — question, answer options, vote counts, and percentages — so they can discover what's available to vote on and see how opinion is trending (spec user stories #1, #5). Closed and archived polls must also be reachable through the same endpoint so past results remain browsable (spec story #8).

I-003 already reserved the route (`GET /v1/polls`) and defined the response envelope shape. I-009 provides fast result computation from Redis; I-010 provides a PostgreSQL fallback when Redis is unavailable. This issue is the glue: the actual route handler that decides which of those two sources to read from, applies filtering/pagination, and shapes the response.

---

## Solution

An `async def` FastAPI handler on the existing user `APIRouter` (`src/api/routes/user.py`) that:
1. Requires a valid Adidos token (via I-004's `Depends`)
2. Reads poll metadata (question, state, answer text) from a PostgreSQL read replica, filtered by `state` and paginated
3. Overlays live vote counts/percentages by calling I-009's `compute_poll_results_batch` for the returned poll IDs in a single Redis round trip
4. If that Redis call fails (connection error, timeout, circuit open), falls back to reading counts/percentages from I-010's `vote_counts` table for the same poll IDs — never a 503 for this reason alone
5. Wraps everything in the response envelope established by I-003

---

## User Stories

1. As a user, I want to GET /v1/polls and see active polls with vote counts, so that I can see what's available (spec story #1)
2. As a user, I want to see how many votes are cast for each answer and the percentage breakdown, so that I can understand overall sentiment (spec story #5)
3. As a user, I want to see archived poll results, so that I can review past opinions and trends (spec story #8)

---

## Implementation Decisions

### Query Parameters

| Param | Type | Default | Notes |
|-------|------|---------|-------|
| `state` | string | `active` | One of `active`, `closed`, `archived`. Single value, not a list — matches the poll lifecycle's one-way state machine. |
| `limit` | int | `20` | Max `100`. |
| `offset` | int | `0` | |

Defaulting `state` to `active` preserves the simple "what can I vote on right now" experience from story #1, while `state=closed` / `state=archived` serve story #8 without a separate endpoint.

### Poll Metadata Source

Poll question, state, and answer text are read from PostgreSQL read replicas (`idx_polls_state` from I-001 makes the state-filtered scan fast), not from Redis. This data is small, relatively static, and not the expensive part of serving this endpoint — the expensive part is vote counts at scale, which is exactly what I-009/I-010 exist to make cheap. Mixing "cheap, rarely-changing metadata from Postgres" with "hot, frequently-changing counts from Redis" keeps each data source doing the job it's good at.

### Primary Path: Redis via I-009

```python
@router.get("/v1/polls")
async def list_polls(
    state: PollState = PollState.active,
    limit: int = Query(default=20, le=100),
    offset: int = Query(default=0, ge=0),
    user: AuthenticatedUser = Depends(get_current_user),
    db: ReadReplicaPool = Depends(get_read_replica),
    redis: Redis = Depends(get_redis),
) -> SuccessResponse[PollListData]:
    polls = await fetch_polls_page(db, state=state, limit=limit, offset=offset)
    poll_ids = [p.poll_id for p in polls]

    try:
        results_by_poll = await compute_poll_results_batch(poll_ids, redis)
        stale = False
    except RedisError:
        results_by_poll = await fetch_vote_counts_fallback(db, poll_ids)
        stale = True

    return build_poll_list_response(polls, results_by_poll, stale=stale)
```

### Fallback Path: `vote_counts` via I-010

`fetch_vote_counts_fallback` issues a single query against the `vote_counts` table (read replica, same shard-agnostic replicated table from I-001) for the requested `poll_ids`, and reuses the same `_percentage()` rounding already computed and stored there by I-010's refresh job — no recomputation needed, since I-010 stores `percentage` directly.

### Response Shape

```json
{
  "success": true,
  "data": {
    "polls": [
      {
        "poll_id": "3fa8...",
        "question": "Is pineapple on pizza acceptable?",
        "state": "active",
        "answers": [
          { "answer_id": "a1...", "text": "Yes", "vote_count": 120, "percentage": 60.0 },
          { "answer_id": "a2...", "text": "No", "vote_count": 80, "percentage": 40.0 }
        ],
        "total_votes": 200,
        "created_at": "2026-08-10T09:00:00Z",
        "activated_at": "2026-08-10T09:05:00Z"
      }
    ],
    "pagination": { "limit": 20, "offset": 0, "total": 47 }
  },
  "meta": {
    "timestamp": "2026-08-21T11:00:00Z",
    "request_id": "req-abc123",
    "stale": false
  }
}
```

`meta.stale` is `true` whenever the response was served from I-010's fallback rather than I-009's live Redis path, so clients can optionally surface "results may be a few minutes old" rather than presenting fallback data as if it were real-time.

### Per-User Vote Status Is Explicitly Not Included Here

Each answer entry does **not** include whether the requesting user voted for it. Surfacing "which polls has this user voted on and for what" is a separate, purpose-built query — see I-012 — rather than an extra per-poll lookup bolted onto this listing endpoint. Keeping them separate avoids forcing every list-polls call to also pay for a per-user join or Redis lookup it usually doesn't need.

---

## Acceptance Criteria

- [ ] `GET /v1/polls` returns 200 with the response envelope shape from I-003
- [ ] `state` query param filters correctly (`active` default, `closed`, `archived` supported)
- [ ] `limit` (default 20, max 100) and `offset` (default 0) pagination work correctly; `pagination.total` reflects the full matching count, not just the returned page
- [ ] Each poll entry includes `question`, `state`, per-answer `vote_count` and `percentage`, and `total_votes` (spec story #5)
- [ ] Primary path uses I-009's `compute_poll_results_batch` — a single Redis round trip for the whole page, not one call per poll
- [ ] When Redis is unavailable, the endpoint falls back to I-010's `vote_counts` table and still returns 200 (never a 503 purely because Redis is down)
- [ ] Fallback responses set `meta.stale: true`; primary-path responses set `meta.stale: false`
- [ ] Closed and archived polls are reachable via `state=closed` / `state=archived` and still include full results (spec story #8)
- [ ] An empty result set (no polls match the filter) returns 200 with `polls: []`, not 404
- [ ] Unauthenticated requests return 401 via I-004's `Depends`
- [ ] P95 latency < 50ms on the primary (Redis) path for a typical page size (20 polls)

---

## Testing Strategy

- **Integration Tests**: Seed polls in each state with known vote distributions, verify correct filtering, counts, and percentages match spec's Result Aggregation Tests module (e.g., "After 10 votes (6 answer A, 4 answer B) → GET /v1/polls returns 60% and 40%")
- **Fallback Test**: Simulate Redis being down (mock/kill connection), verify the endpoint still returns 200 with correct data sourced from `vote_counts` and `meta.stale: true`
- **Pagination Tests**: `limit`/`offset` edge cases (page past the end, `limit=100` boundary, `limit>100` rejected or clamped)
- **Auth Tests**: Missing/invalid token → 401
- **Prior Art**: Spec's Result Aggregation Tests and Admin Poll Management Tests modules — mirror the "closed poll results still queryable" and "archived poll no longer in active list, but visible in history" scenarios

---

## Out of Scope

- Per-user vote status on this listing (which answer the requesting user chose) — see I-012: GET /v1/user/votes
- Poll creation, activation, closing, archiving — see I-013: Admin Endpoints
- Admin-only fields (anomaly counts, geo breakdowns) — see I-013/I-014
- Sorting options other than the implicit default (creation order) — not called for by any user story in scope

---

## Related Issues

- I-003: API Framework & Routing (response envelope, route registration, auth dependency pattern)
- I-004: Authentication Middleware (token validation used by this endpoint)
- I-009: Result Aggregation (Redis Cache) (primary results source, `compute_poll_results_batch`)
- I-010: Materialized Views (Fallback) (`vote_counts` fallback source when Redis is down)
- I-012: GET /v1/user/votes Endpoint (sibling endpoint for per-user vote history, deliberately not merged into this one)
- I-001: Database Schema & Migrations (`polls`/`answers` tables read for metadata)

---

## Implementation Checklist

- [ ] Add `GET /v1/polls` handler to `src/api/routes/user.py`
- [ ] Create `src/db/queries/polls.py` (`fetch_polls_page` — state-filtered, paginated read-replica query)
- [ ] Create `src/db/queries/vote_counts.py` (`fetch_vote_counts_fallback` — batched `vote_counts` read)
- [ ] Create `src/schemas/polls.py` (`PollListData`, `PollSummary` Pydantic response models)
- [ ] Wire the Redis-then-fallback branching logic into the handler, with `meta.stale` set correctly
- [ ] Integration tests: `tests/integration/test_list_polls.py` (state filtering, pagination, counts/percentages)
- [ ] Fallback integration test: `tests/integration/test_list_polls_fallback.py`
- [ ] Manual test with curl/Postman across all three `state` values

---

**Acceptance**: Endpoint returns correct results on both primary and fallback paths, all tests pass, PR reviewed and merged.
