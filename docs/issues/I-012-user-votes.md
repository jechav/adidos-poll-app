# I-012: GET /v1/user/votes Endpoint

**Status**: Ready for Implementation  
**Epic**: Result Aggregation & Caching  
**Priority**: P1  
**Estimated Effort**: 2 days

---

## Problem Statement

Users need to see which polls they've voted on and what they chose (spec user story #4). Unlike GET /v1/polls (I-011), this is not a hot aggregate path — it's a low-volume, per-user lookup that only ever needs one user's own data. Routing it through Redis or a cross-shard fan-in would be solving a problem this endpoint doesn't have.

Privacy is a hard constraint here, not an implementation detail: per spec Implementation Decision (data privacy user story #38), admins must never see raw per-user vote logs — only aggregate counts and anomaly alerts (owned by I-013/I-014). This endpoint must be structurally incapable of returning another user's votes; there is no admin bypass, no `user_id` query parameter, nothing to misconfigure.

---

## Solution

An `async def` FastAPI handler on the user `APIRouter` (`src/api/routes/user.py`) that:
1. Requires a valid Adidos token (I-004), which is the *only* source of `user_id` — never a query/path parameter
2. Routes the query to the single PostgreSQL shard that owns this `user_id`, using the same shard-routing function I-008's vote processor uses (`user_id % num_shards`)
3. Joins `votes` against `polls` and `answers` on that shard to return question text, poll state, and chosen answer text alongside each vote
4. Does not filter by poll state — closed and archived polls the user voted on remain in their history, even though they can no longer be voted on

---

## User Stories

1. As a user, I want to see my previous votes on all polls I've participated in, so that I can track my voting history (spec story #4)

---

## Implementation Decisions

### No Physical `UserVoteState` Table

DOMAIN_MODEL.md describes `UserVoteState` as a conceptual entity ("a denormalized view of which polls a user has voted on"), but I-001 did not create a physical `user_vote_state` table — only `votes`, `polls`, `answers`, `vote_counts`, and `anomalies` exist. This endpoint derives the equivalent view directly with a join, rather than introducing a second write path that could drift from `votes` (the actual source of truth per DOMAIN_MODEL.md's `VoteAggregate` invariants). One source of truth, one less thing to keep in sync.

### Shard Routing (Reused, Not Reimplemented)

```python
async def get_user_votes(
    user: AuthenticatedUser,  # user_id comes only from the validated token
    limit: int = 20,
    offset: int = 0,
) -> UserVotesData:
    shard = shard_for_user(user.user_id)  # same function I-008 uses to route writes
    rows = await shard.fetch(USER_VOTES_QUERY, user.user_id, limit, offset)
    ...
```

`shard_for_user` is imported from wherever I-008 defines it (e.g. `src/db/sharding.py`), not duplicated — a user's votes always live on exactly the shard their `user_id` hashes to, so this lookup never fans out across shards the way I-010's aggregation job does.

### Query

```sql
SELECT
  v.poll_id,
  p.question,
  p.state AS poll_state,
  v.answer_id,
  a.answer_text,
  v.created_at AS voted_at
FROM votes v
JOIN polls p ON p.poll_id = v.poll_id
JOIN answers a ON a.answer_id = v.answer_id
WHERE v.user_id = $1
ORDER BY v.created_at DESC
LIMIT $2 OFFSET $3;
```

Uses the `idx_votes_user_poll (user_id, poll_id)` index from I-001 — an indexed, single-shard, single-user lookup, cheap regardless of overall vote volume.

### No State Filtering

The query intentionally does not filter on `p.state`. Per DOMAIN_MODEL.md's `Poll` business rules ("Closed/archived polls are visible to all authenticated users") and the spec's Integration Tests module ("Closed polls visible in user voting history, but can't be voted on"), a poll moving to `closed` or `archived` must not cause it to vanish from the user's own history — only from being votable (enforced elsewhere, by I-005/I-006, not here).

### Anonymization Interaction (Documented Behavior, Not a Bug)

I-001's 90-day anonymization migration nulls `votes.user_id` for old votes. Once nulled, `WHERE v.user_id = $1` no longer matches that row, so the vote silently disappears from the user's visible history at that point — consistent with the spec's retention policy (Implementation Decision #14: individual vote records anonymized after 90 days). This endpoint doesn't need special-case logic for it; it falls out naturally from querying by `user_id`. Worth calling out explicitly so it isn't mistaken for a data-loss bug during testing.

### Response Shape

```json
{
  "success": true,
  "data": {
    "votes": [
      {
        "poll_id": "3fa8...",
        "question": "Is pineapple on pizza acceptable?",
        "poll_state": "closed",
        "answer_id": "a1...",
        "answer_text": "Yes",
        "voted_at": "2026-07-15T14:22:00Z"
      }
    ],
    "pagination": { "limit": 20, "offset": 0, "total": 12 }
  },
  "meta": {
    "timestamp": "2026-08-21T11:00:00Z",
    "request_id": "req-abc123"
  }
}
```

Matches the response envelope established in I-003.

### No Redis, No Aggregate Counts

This endpoint never touches Redis and never returns `vote_count`/`percentage` fields — those describe the whole poll's results (I-009/I-011's job), not what one user personally chose. Mixing the two would blur a clean privacy boundary: this endpoint answers "what did I vote?", not "how is the poll doing?".

### Privacy Enforcement

- `user_id` is derived exclusively from the validated Adidos token (I-004); the route signature has no `user_id` path or query parameter for it to be spoofed through
- No admin variant of this endpoint exists or is planned — admins get aggregate counts and anomaly alerts only (I-013/I-014), never raw per-user vote logs, per spec story #38
- Nothing in this response includes other users' data, even in aggregate form (no "X% of people who voted like you also chose..." — out of scope, not a privacy leak risk to design around further, just not built)

---

## Acceptance Criteria

- [ ] `GET /v1/user/votes` returns 200 with the response envelope shape from I-003
- [ ] Returns only the authenticated caller's own votes — `user_id` is taken exclusively from the validated token, never from a request parameter
- [ ] Query is routed to the single shard owning the user's `user_id` (reuses I-008's shard-routing function; no cross-shard fan-out)
- [ ] Each entry includes `poll_id`, `question`, `poll_state`, `answer_id`, `answer_text`, `voted_at`
- [ ] Closed and archived polls the user voted on appear in the results (not filtered by poll state)
- [ ] Pagination (`limit` default 20, max 100; `offset` default 0) supported, `pagination.total` correct
- [ ] Anonymized votes (`user_id` nulled after 90 days per I-001's migration) no longer appear — verified as expected behavior, not treated as a bug
- [ ] Query uses `idx_votes_user_poll`; confirmed via `EXPLAIN ANALYZE` that it's an index lookup, not a sequential scan
- [ ] Unauthenticated requests return 401 via I-004's `Depends`
- [ ] A user with zero votes returns 200 with `votes: []`
- [ ] Endpoint has no dependency on Redis and is unaffected by a Redis outage (still fully functional during I-009's degraded state)

---

## Testing Strategy

- **Integration Tests**: Seed a user's votes spanning active, closed, and archived polls; verify all appear with correct `poll_state` and answer text
- **Pagination Tests**: `limit`/`offset` edge cases, total count correctness
- **Anonymization Behavior Test**: Mark a seeded vote's `user_id` as null (simulating the 90-day job), verify it no longer appears in that user's results
- **Shard Routing Test**: Verify votes for users hashing to different shards are correctly retrieved from their respective shard only
- **Privacy Test**: Verify a request with User A's token never returns User B's votes regardless of any manipulated input; verify there is no code path that accepts a `user_id` override
- **Prior Art**: Spec's Integration Tests module — "Closed polls visible in user voting history, but can't be voted on"

---

## Out of Scope

- Aggregate vote counts/percentages for a poll — see I-009/I-011
- Admin visibility into any user's raw vote log — explicitly prohibited by spec story #38; admins only ever see counts and anomaly alerts (I-013/I-014)
- Data export tooling (spec admin story #19) — a separate, admin-facing concern
- Any cross-user comparison or social feature — out of scope per SPECIFICATION.md's global Out of Scope section (#7, Social Features)

---

## Related Issues

- I-001: Database Schema & Migrations (`votes`, `polls`, `answers` tables; the 90-day anonymization migration this endpoint's behavior depends on)
- I-003: API Framework & Routing (response envelope, route registration pattern)
- I-004: Authentication Middleware (sole source of `user_id` for this endpoint)
- I-008: Vote Processor Workers (defines the shard-routing function this endpoint reuses)
- I-009: Result Aggregation (Redis Cache) (contrast: that path is the hot aggregate path; this endpoint deliberately bypasses it entirely)
- I-011: GET /v1/polls Endpoint (sibling endpoint; aggregate poll results rather than personal vote history)

---

## Implementation Checklist

- [ ] Add `GET /v1/user/votes` handler to `src/api/routes/user.py`
- [ ] Create `src/db/queries/user_votes.py` (`get_user_votes` — single-shard join query)
- [ ] Import (not reimplement) the shard-routing function from I-008's module
- [ ] Create `src/schemas/user_votes.py` (`UserVotesData`, `UserVoteEntry` Pydantic response models)
- [ ] Integration tests: `tests/integration/test_user_votes.py` (state visibility, pagination, anonymization behavior)
- [ ] Shard-routing test: `tests/unit/test_user_votes_sharding.py`
- [ ] Manual test with curl/Postman using tokens for two different users, confirm isolation

---

**Acceptance**: Endpoint returns only the authenticated user's own votes across all poll states, shard routing verified correct, all tests pass, PR reviewed and merged.
