# I-005: Vote Acceptance & Queueing

**Status**: Ready for Implementation  
**Epic**: Voting Infrastructure  
**Priority**: P0 (Blocker)  
**Estimated Effort**: 4 days

---

## Problem Statement

The poll app needs an endpoint that accepts a user's vote and returns instantly, even under bursts of 50K-100K votes/sec, without blocking on a database write. This endpoint must:
- Validate the poll is active and the answer belongs to that poll before accepting anything
- Reject a second vote from the same user on the same poll (409, not silently ignored)
- Reject requests that exceed rate limits (429) before they ever reach the queue
- Enqueue accepted votes for asynchronous processing and return 202 immediately
- Behave predictably on retry: a client that resends a request for a vote that already succeeded must get 409, not a duplicate queue entry

This is the orchestration layer described in SPECIFICATION.md's Vote Acceptance Strategy (decision #3) and Idempotency & Retry Semantics (decision #11). It composes the rate limiter (I-007) and the uniqueness check (I-006) but does not own either's internals — this issue is strictly about the `POST /v1/vote` handler and the queue hand-off.

---

## Solution

Implement `POST /v1/vote` as an `APIRouter` route in `src/api/routes/user.py` (stubbed in I-003) that:

1. Validates the request body (Pydantic model: `poll_id`, `answer_id`) and extracts `user_id` from the authenticated token (via I-004's `Depends(get_current_user)`)
2. Loads the poll and answer, confirms `state == 'active'` and `answer_id` belongs to `poll_id` — 400 otherwise
3. Calls the rate limiter dependency (I-007) — 429 if exceeded
4. Calls the uniqueness check (I-006) — 409 if the user already voted
5. `LPUSH`es a vote payload onto the `queue:votes` Redis list
6. Returns `202 Accepted` with a `request_id` and no further blocking work

The handler is intentionally thin: it sequences four already-defined operations (poll/answer lookup, rate limit, uniqueness, enqueue) and does not itself implement rate limiting or uniqueness logic — those live in I-007 and I-006 respectively, and are pulled in as FastAPI dependencies or plain async functions.

---

## User Stories

(from SPECIFICATION.md)

2. As a user, I want to vote on a poll by selecting one of two answers, so that I can express my opinion
3. As a user, I want to receive immediate feedback after voting, showing poll results, so that I can see how my answer compares to others
6. As a user, I want to be blocked from voting twice on the same poll, so that my opinion counts only once (enforced without confusion)
7. As a user, I want to receive a clear error if I try to vote on a closed poll, so that I understand the poll is no longer accepting votes
9. As a user, I want my vote to be processed in less than 200ms (P99), so that the experience feels instant
10. As a user, I want my vote to succeed even during traffic spikes, so that I don't miss the opportunity to vote during a viral moment
25. As the system, I want to verify that a vote is only cast once per user per question, enforced at the database level, so that I guarantee data integrity even if the async queue fails

---

## Implementation Decisions

### Request Schema (Pydantic)
```python
class VoteRequest(BaseModel):
    poll_id: UUID
    answer_id: UUID
```
`user_id` is never accepted from the request body — it comes exclusively from the validated Adidos token via `Depends(get_current_user)` (I-004). This prevents a client from voting on another user's behalf.

### Response (202 Accepted)
```json
{
  "success": true,
  "data": {
    "status": "queued",
    "poll_id": "8f14e...",
    "answer_id": "3ab21..."
  },
  "meta": {
    "timestamp": "2026-08-10T11:00:00Z",
    "request_id": "req-abc123"
  }
}
```
Uses the standard success envelope from I-003. No vote result/count is returned here — clients fetch results via `GET /v1/polls` (I-011) after the fact; this endpoint's only job is to confirm the vote was accepted for processing.

### Handler Flow
```python
@router.post("/vote", status_code=202)
async def cast_vote(
    body: VoteRequest,
    user_id: str = Depends(get_current_user),
    request: Request = None,
):
    poll, answer = await load_poll_and_answer(body.poll_id, body.answer_id)
    if poll is None or answer is None:
        raise NotFoundError("Poll or answer not found")
    if poll.state != "active":
        raise PollClosedError("Poll is not accepting votes")
    if answer.poll_id != poll.poll_id:
        raise InvalidRequestError("answer_id does not belong to poll_id")

    await check_rate_limit(user_id=user_id, ip=request.client.host)   # I-007
    await check_and_reserve_uniqueness(user_id, body.poll_id)          # I-006, raises 409 on duplicate

    await redis.lpush("queue:votes", VotePayload(
        vote_id=uuid4(),
        user_id=user_id,
        poll_id=body.poll_id,
        answer_id=body.answer_id,
        requested_at=utcnow(),
    ).json())

    return success_envelope({"status": "queued", "poll_id": body.poll_id, "answer_id": body.answer_id})
```
Ordering matters: poll/answer validation is cheapest (in-memory or cached lookup) and runs first, so malformed requests never touch Redis. Rate limiting runs before uniqueness so a user who is already rate-limited doesn't also consume a uniqueness check. Uniqueness runs immediately before enqueue — per I-006, this is the atomic `SET NX` that reserves the vote — so the queue never receives a vote that will be rejected as a duplicate.

### Non-Idempotency (Retry Semantics)
Per spec decision #11, `POST /v1/vote` is **not** idempotent. If a client retries a request for a vote that already succeeded (the Redis `SET NX` key from I-006 already exists), the retry is rejected with `409 DUPLICATE_VOTE` — identical to a genuine second vote attempt. There is no request-level idempotency key; the uniqueness key (`vote:user:{user_id}:poll:{poll_id}`) *is* the idempotency mechanism. This is deliberate: it doubles as duplicate-attempt detection feeding the 3-strike bot defense (I-015).

### Error Cases

| Condition | Code | HTTP Status |
|---|---|---|
| Missing/invalid `poll_id` or `answer_id` | `INVALID_REQUEST` | 400 |
| `answer_id` does not belong to `poll_id` | `INVALID_REQUEST` | 400 |
| Poll not found | `NOT_FOUND` | 404 |
| Poll not in `active` state | `POLL_CLOSED` | 400 |
| User already voted (Redis `SET NX` hit) | `DUPLICATE_VOTE` | 409 |
| Rate limit exceeded (user or IP) | `RATE_LIMIT_EXCEEDED` | 429 |
| Redis queue unreachable | `SERVICE_UNAVAILABLE` | 503 |

These map directly to the error codes table in I-003 — no new codes are introduced here.

### What This Issue Does NOT Own
- The mechanics of the Redis `SET NX` uniqueness reservation — see I-006
- The rate limit window algorithm and keys — see I-007
- Draining `queue:votes` and writing to PostgreSQL — see I-008
- Computing/serving vote results — see I-009, I-011

This handler is a composition point, not an implementation of any of the above.

---

## Acceptance Criteria

- [ ] `POST /v1/vote` returns 202 with `request_id` for a valid vote on an active poll
- [ ] Voting on a closed/draft/archived poll returns 400 `POLL_CLOSED`
- [ ] Voting with an `answer_id` that doesn't belong to `poll_id` returns 400 `INVALID_REQUEST`
- [ ] Voting on a nonexistent poll or answer returns 404 `NOT_FOUND`
- [ ] A second vote by the same user on the same poll returns 409 `DUPLICATE_VOTE` (no queue entry created)
- [ ] Retrying an already-successful vote request returns 409, not 202
- [ ] Exceeding rate limits returns 429 before any uniqueness check or enqueue happens
- [ ] Vote payload pushed to `queue:votes` contains `vote_id`, `user_id`, `poll_id`, `answer_id`, `requested_at`
- [ ] Missing/malformed request body returns 400 with field-level detail
- [ ] Handler is `async def` and does not block on any database write
- [ ] P99 handler latency < 200ms under load (excludes downstream queue processing)

---

## Testing Strategy

- **Unit Tests**: Validation logic (poll/answer state checks), error mapping for each rejection path
- **Integration Tests**: Full request/response cycle against a running FastAPI app with test Redis — vote → 202 → verify `queue:votes` has one entry; duplicate vote → 409; closed poll → 400
- **Contract Tests**: Verify this endpoint calls I-006 and I-007 as dependencies rather than reimplementing their logic (mock both, assert they were invoked with correct args)
- **Load Tests**: 1000 votes/sec sustained, P99 < 200ms, zero 503s (feeds into I-023)
- **Prior Art**: Mirror I-003's route testing structure; reuse I-001's seeded polls/answers for fixtures

---

## Related Issues

- I-001: Database Schema (poll/answer lookups read from this schema)
- I-002: Redis Cluster Setup (queue and uniqueness keys live here)
- I-003: API Framework & Routing (this issue implements the `POST /v1/vote` route it defines)
- I-004: Authentication Middleware (supplies `user_id` via `Depends`)
- I-006: Uniqueness Enforcement (called by this handler, not reimplemented)
- I-007: Rate Limiting (called by this handler, not reimplemented)
- I-008: Vote Processor Workers (consumes `queue:votes` populated by this handler)
- I-009: Result Aggregation (serves the results clients check after voting)

---

## Implementation Checklist

- [ ] Add `VoteRequest` Pydantic model to `src/schemas/votes.py`
- [ ] Implement `POST /v1/vote` handler in `src/api/routes/user.py`
- [ ] Implement `load_poll_and_answer()` helper in `src/services/polls.py` (cached lookup)
- [ ] Wire `check_rate_limit()` (I-007) and `check_and_reserve_uniqueness()` (I-006) as calls/dependencies
- [ ] Implement `VotePayload` schema and `LPUSH` to `queue:votes` in `src/services/vote_queue.py`
- [ ] Add error classes (`PollClosedError`, `InvalidRequestError`) mapping to I-003's exception handlers
- [ ] Write integration tests covering all rows in the Error Cases table
- [ ] Load test at 1000 votes/sec, confirm P99 < 200ms

---

**Acceptance**: All acceptance criteria met, integration and load tests pass, PR reviewed and merged.
