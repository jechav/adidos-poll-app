# I-013: Admin Poll Management Endpoints

**Status**: Ready for Implementation  
**Epic**: Admin & Security  
**Priority**: P0 (Blocker)  
**Estimated Effort**: 3 days

---

## Problem Statement

Admins need to manage the poll lifecycle and monitor bot defense activity, without ever touching raw vote logs. Three capabilities are missing:
- A way to create a poll with a question and exactly two answers, landing in `draft` state until an admin explicitly activates it
- A way to move a poll through its lifecycle (`draft` → `active` → `closed` → `archived`) that rejects any attempt to skip a step or go backward
- A read-only view into detected anomalies (rate limit violations, duplicate-attempt blocks, bot patterns) so admins can see attack activity as counts and alert summaries — never individual vote records

All three routes are stubbed as placeholders in I-003's route table (`POST /v1/admin/polls`, `PUT /v1/admin/polls/:id/state`, `GET /v1/admin/anomalies`) and gated by I-004's admin authorization dependency. This issue implements the handlers.

---

## Solution

Implement three routes in `src/api/routes/admin.py` as an `APIRouter`, each behind `Depends(require_admin)` (I-004) so a non-admin token gets `403 FORBIDDEN` before any handler logic runs:

1. `POST /v1/admin/polls` — creates a poll + 2 answers, always landing in `draft` state (I-001's `polls`/`answers` tables)
2. `PUT /v1/admin/polls/:id/state` — transitions poll state, enforcing the one-way `draft → active → closed → archived` sequence from the domain model
3. `GET /v1/admin/anomalies` — reads I-001's `anomalies` table (populated by I-014 and I-015), returns alert summaries and counts, never raw vote data

State transition validation and the create-poll shape are owned entirely by this issue. The anomaly *data* is produced elsewhere (I-014, I-015) — this issue is strictly a read-only consumer of that table, per spec decision #2/#38 (admins see aggregates and alerts, not raw logs).

---

## User Stories

(from SPECIFICATION.md)

11. As an admin, I want to create a poll by specifying a question and two answer options, so that I can launch a new voting initiative
12. As an admin, I want to activate a draft poll, so that users can start voting
13. As an admin, I want to close an active poll, so that I can stop accepting new votes at a specific time
14. As an admin, I want to archive closed polls, so that I can manage the poll lifecycle and keep historical records
15. As an admin, I want to see real-time vote counts and percentages for active polls, so that I can monitor engagement
16. As an admin, I want to view bot detection alerts showing suspicious voting patterns, so that I can identify and respond to attacks
17. As an admin, I want to see rate limit violations (IPs and users exceeding quotas), so that I can understand attack vectors
19. As an admin, I want to export poll data (question, answers, vote counts, timestamp), so that I can analyze trends in external tools
38. As a privacy officer, I want admins to see only vote counts and anomaly alerts, never raw vote logs, so that user privacy is protected by default

Story 15 (real-time vote counts) is already served by `GET /v1/polls` (I-011) — admins hit the same cached-results endpoint as regular users, so no separate admin route is introduced for it. Story 19 (export) is partially satisfied by combining `GET /v1/polls` output with the poll metadata this issue's create endpoint returns; a dedicated bulk-export/CSV route is out of scope here (see Out of Scope).

---

## Implementation Decisions

### Request Schema: Create Poll
```python
class AnswerInput(BaseModel):
    text: str = Field(..., min_length=1, max_length=255)

class CreatePollRequest(BaseModel):
    question: str = Field(..., min_length=1, max_length=255)
    answers: list[AnswerInput] = Field(..., min_length=2, max_length=2)
```
Exactly 2 answers is enforced at the schema level (`min_length=2, max_length=2`), not just as a business rule checked later — an invalid payload never reaches the handler. `order` is not accepted from the client; it's derived from list position (`answers[0]` → order 0, `answers[1]` → order 1), matching I-001's `UNIQUE(poll_id, "order")` constraint.

### Handler: Create Poll
```python
@router.post("/admin/polls", status_code=201, dependencies=[Depends(require_admin)])
async def create_poll(body: CreatePollRequest):
    poll_id = uuid4()
    answers = [
        Answer(answer_id=uuid4(), poll_id=poll_id, answer_text=a.text, order=i)
        for i, a in enumerate(body.answers)
    ]
    poll = await polls_repo.create(poll_id=poll_id, question=body.question, state="draft", answers=answers)
    return success_envelope(poll_response(poll, answers), status_code=201)
```
Poll is created directly in `draft` state (I-001 default) — there is no way to create an already-active poll via this endpoint. Activation is a separate, explicit transition, matching the domain model's requirement that admins can review a draft before it's user-visible.

### State Transition Rules

Per the domain model's `PollState` value object, transitions are strictly one-way and single-step: `draft → active → closed → archived`. This issue enforces three rules:

1. **Same-state request is idempotent** — per DOMAIN_MODEL.md's invariant ("closing an already-closed poll is safe"), requesting the poll's current state is a no-op that returns 200 with the poll unchanged, not an error.
2. **Next-state request succeeds** — moving from `draft`→`active`, `active`→`closed`, or `closed`→`archived` transitions the poll and stamps the corresponding timestamp (`activated_at`/`closed_at`/`archived_at`).
3. **Any other request is rejected with 400** — this covers both backward transitions (`closed`→`active`, the spec's explicit test case) and forward *skips* (`draft`→`closed`), since neither is a state any admin action in the user stories (12/13/14) can legitimately request in one step.

```python
POLL_STATE_ORDER = {"draft": 0, "active": 1, "closed": 2, "archived": 3}

class UpdatePollStateRequest(BaseModel):
    state: Literal["active", "closed", "archived"]

def validate_transition(current: str, requested: str) -> None:
    if POLL_STATE_ORDER[requested] != POLL_STATE_ORDER[current] + 1:
        raise InvalidRequestError(
            f"Cannot transition poll from '{current}' to '{requested}'; "
            "states move forward one step at a time (draft -> active -> closed -> archived)"
        )

@router.put("/admin/polls/{poll_id}/state", dependencies=[Depends(require_admin)])
async def update_poll_state(poll_id: UUID, body: UpdatePollStateRequest):
    poll = await polls_repo.get(poll_id)
    if poll is None:
        raise NotFoundError("Poll not found")
    if body.state == poll.state:
        return success_envelope(poll_response(poll))  # idempotent no-op
    validate_transition(poll.state, body.state)
    poll = await polls_repo.transition(poll_id, body.state)  # stamps *_at column, writes state
    return success_envelope(poll_response(poll))
```
`InvalidRequestError` maps to I-003's existing `INVALID_REQUEST` (400) error code — no new error code is introduced for transition failures.

### Handler: List Anomalies
```python
class AnomalySummary(BaseModel):
    alert_id: UUID
    alert_type: Literal["rate_limit_exceeded", "duplicate_attempts_blocked", "bot_pattern_detected"]
    severity: Literal["warning", "critical"]
    user_id: str | None
    ip_address: str | None
    poll_id: UUID | None
    description: str
    created_at: datetime
    acknowledged_at: datetime | None
    action_taken: str | None

@router.get("/admin/anomalies", dependencies=[Depends(require_admin)])
async def list_anomalies(
    severity: Literal["warning", "critical"] | None = None,
    alert_type: str | None = None,
    since: datetime | None = None,
    limit: int = Query(50, le=500),
):
    rows = await anomalies_repo.list(severity=severity, alert_type=alert_type, since=since, limit=limit)
    return success_envelope({
        "anomalies": [AnomalySummary.model_validate(r) for r in rows],
        "counts": await anomalies_repo.counts_by_type(since=since),  # {alert_type: {warning: n, critical: n}}
    })
```
The response includes `user_id` and `ip_address` on each anomaly — this is not a privacy violation of spec decision #38: the anomaly *alert* itself is exactly the aggregate/alert-level signal admins are entitled to see (story #16/#17), distinct from the raw `votes` table, which this endpoint never touches and this router never queries. The `counts` block gives admins the rate-limit-violation and bot-pattern overview from stories #16/#17 without requiring them to page through raw rows.

### Example Response: GET /v1/admin/anomalies
```json
{
  "success": true,
  "data": {
    "anomalies": [
      {
        "alert_id": "9c1e...",
        "alert_type": "duplicate_attempts_blocked",
        "severity": "critical",
        "user_id": "usr_8821",
        "ip_address": "203.0.113.7",
        "poll_id": "8f14e...",
        "description": "User usr_8821 blocked for 1h after 3 duplicate vote attempts on poll 8f14e... from 203.0.113.7",
        "created_at": "2026-08-21T09:12:03Z",
        "acknowledged_at": null,
        "action_taken": "blocked:usr_8821:203.0.113.7 set for 3600s"
      }
    ],
    "counts": {
      "rate_limit_exceeded": { "warning": 42, "critical": 0 },
      "duplicate_attempts_blocked": { "warning": 0, "critical": 3 },
      "bot_pattern_detected": { "warning": 1, "critical": 0 }
    }
  },
  "meta": { "timestamp": "2026-08-21T09:15:00Z", "request_id": "req-def456" }
}
```

### What This Issue Does NOT Own
- Writing anomaly rows — that's I-007 (`rate_limit_exceeded`), I-015 (`duplicate_attempts_blocked`), I-014 (`bot_pattern_detected`); this issue only reads I-001's `anomalies` table
- Reporting anomalies to Adidos — see I-016
- Computing/serving vote results — see I-009, I-011
- Admin credential issuance or admin-role management — Adidos owns this per spec decision #2; I-004's `require_admin` dependency only checks a role claim already on the token

---

## Acceptance Criteria

- [ ] `POST /v1/admin/polls` with a question and exactly 2 answers returns 201 with the poll in `draft` state
- [ ] `POST /v1/admin/polls` with 0, 1, or 3+ answers returns 400 `INVALID_REQUEST` (schema-level rejection)
- [ ] `POST /v1/admin/polls` from a non-admin token returns 403 `FORBIDDEN` before any DB write
- [ ] `PUT /v1/admin/polls/:id/state` with the poll's current state is a no-op, returns 200 unchanged
- [ ] `PUT /v1/admin/polls/:id/state` moving one step forward (`draft`→`active`, `active`→`closed`, `closed`→`archived`) succeeds and stamps the corresponding timestamp
- [ ] `PUT /v1/admin/polls/:id/state` attempting a backward transition (e.g., `closed`→`active`) returns 400 `INVALID_REQUEST`
- [ ] `PUT /v1/admin/polls/:id/state` attempting a forward skip (e.g., `draft`→`closed`) returns 400 `INVALID_REQUEST`
- [ ] `PUT /v1/admin/polls/:id/state` on a nonexistent poll returns 404 `NOT_FOUND`
- [ ] `GET /v1/admin/anomalies` returns alert rows plus a `counts` summary, filterable by `severity`, `alert_type`, and `since`
- [ ] `GET /v1/admin/anomalies` never returns rows from the `votes` table or any per-vote detail
- [ ] All three routes are `async def` and require `Depends(require_admin)`
- [ ] `limit` on `GET /v1/admin/anomalies` is capped at 500 to prevent unbounded scans

---

## Testing Strategy

- **Unit Tests**: `validate_transition()` against every (current, requested) state pair — verify exactly the 3 forward-step transitions succeed, same-state is a no-op, and all 9 remaining combinations (backward + skips) raise
- **Integration Tests**: Full request/response cycle against a running FastAPI app — create poll → draft; activate → active; attempt `closed`→`active` → 400; non-admin token on all 3 routes → 403; anomalies endpoint against seeded `anomalies` rows → correct filtering and counts
- **Contract Tests**: Confirm `GET /v1/admin/anomalies` never issues a query against the `votes` table (mock the repo layer, assert only `anomalies_repo` is called)
- **Prior Art**: Mirror I-003's and I-005's route testing structure; reuse I-001's seeded polls/answers/anomalies for fixtures

---

## Out of Scope

- Bulk export / CSV generation for poll data (story #19 is partially covered by combining existing endpoints; a dedicated export pipeline is a future enhancement)
- Editing a poll's question or answers after creation (per domain model, answers are immutable once the poll is active; even in `draft` this issue doesn't add a PATCH route)
- Hard-deleting polls (immutability principle — spec Out of Scope #12)
- Admin account creation, role assignment, or credential management (Adidos owns this)
- Acknowledging/resolving anomalies (`acknowledged_at`) via this API — tracked as a possible follow-up, not required for launch

---

## Related Issues

- I-001: Database Schema (`polls`, `answers`, `anomalies` tables this issue reads/writes)
- I-003: API Framework & Routing (this issue implements the 3 admin routes it stubs)
- I-004: Authentication Middleware (`require_admin` dependency gates all 3 routes)
- I-007: Rate Limiting (writes `rate_limit_exceeded` anomalies this issue's GET endpoint surfaces)
- I-011: GET /v1/polls Endpoint (serves the real-time vote counts referenced in story #15)
- I-014: Bot Detection & Alerting (writes `bot_pattern_detected` anomalies this issue's GET endpoint surfaces)
- I-015: Duplicate Detection (writes `duplicate_attempts_blocked` anomalies this issue's GET endpoint surfaces)
- I-016: Anomaly Reporting to Adidos (consumes the same `anomalies` table, for a different audience)

---

## Implementation Checklist

- [ ] Create `src/schemas/admin.py` (`CreatePollRequest`, `AnswerInput`, `UpdatePollStateRequest`, `AnomalySummary`)
- [ ] Implement `POST /v1/admin/polls` in `src/api/routes/admin.py`
- [ ] Implement `PUT /v1/admin/polls/{poll_id}/state` with `validate_transition()` in `src/api/routes/admin.py`
- [ ] Implement `GET /v1/admin/anomalies` in `src/api/routes/admin.py`
- [ ] Add `polls_repo.create()`, `polls_repo.transition()` to `src/services/polls.py`
- [ ] Add `anomalies_repo.list()`, `anomalies_repo.counts_by_type()` to `src/services/anomalies.py`
- [ ] Wire `Depends(require_admin)` on all 3 routes
- [ ] Write unit tests for `validate_transition()` covering all 12 state pairs
- [ ] Write integration tests covering all rows in the Acceptance Criteria

---

**Acceptance**: All acceptance criteria met, integration tests pass, PR reviewed and merged.
