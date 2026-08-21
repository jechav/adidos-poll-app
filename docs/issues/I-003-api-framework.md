# I-003: API Framework & Routing

**Status**: Ready for Implementation  
**Epic**: Voting Infrastructure  
**Priority**: P0 (Blocker)  
**Estimated Effort**: 3 days

---

## Problem Statement

The poll app needs a RESTful API framework to handle:
- Vote requests (accept with 202, queue for processing)
- Poll listing (fetch active/closed/archived polls with cached results)
- User voting history (which polls has this user voted on?)
- Admin poll management (create, activate, close, archive polls)

The framework must:
- Support path-based versioning (/v1/, /v2/)
- Return consistent JSON responses
- Handle authentication via Adidos tokens
- Parse and validate request bodies
- Measure request latency (P95, P99)

---

## Solution

Use **FastAPI** (Python) for the API server. Set up:
1. FastAPI app with middleware stack (logging, parsing, error handling)
2. Route handlers for all endpoints (grouped by user vs. admin)
3. Consistent response envelopes (success, error, metadata)
4. Request/response logging (JSON format, 1-in-1000 sampling)
5. Latency instrumentation (record per-endpoint metrics)

---

## User Stories

1. As a client, I want to POST /v1/vote with user token and poll_id + answer_id, so that I can cast my vote
2. As a client, I want to receive 202 Accepted immediately, so that I know the vote is queued
3. As a client, I want to GET /v1/polls and see active polls with vote counts, so that I can see what's available
4. As a client, I want to GET /v1/user/votes and see polls I've voted on, so that I can review my voting history
5. As an admin, I want to POST /v1/admin/polls with question + 2 answers, so that I can create polls
6. As an admin, I want to PUT /v1/admin/polls/:id/state to activate/close/archive, so that I can manage poll lifecycle
7. As an operator, I want latency metrics per endpoint, so that I can monitor P95/P99 performance
8. As a developer, I want consistent error responses (400, 409, 429, 503) with descriptive messages, so that I can debug API issues

---

## Implementation Decisions

### Response Envelope (Success)
```json
{
  "success": true,
  "data": { /* response body */ },
  "meta": {
    "timestamp": "2026-08-10T11:00:00Z",
    "request_id": "req-abc123"
  }
}
```

### Response Envelope (Error)
```json
{
  "success": false,
  "error": {
    "code": "DUPLICATE_VOTE",
    "message": "User already voted on this poll",
    "details": { /* optional */ }
  },
  "meta": {
    "timestamp": "2026-08-10T11:00:00Z",
    "request_id": "req-abc123"
  }
}
```

### Middleware Stack
1. **Request ID Generation**: Assign unique ID per request
2. **Auth Middleware**: Extract and validate Adidos token
3. **Request Logging**: Log incoming request (method, path, token)
4. **Body Parser**: JSON parsing with size limits
5. **Route Handlers**: Business logic
6. **Error Middleware**: Catch exceptions, return consistent error response
7. **Response Logging**: Log response (status, latency, error if any)

### Routes (v1)

#### User Routes
```
GET  /v1/polls                    # List active polls
POST /v1/vote                     # Cast vote (202)
GET  /v1/user/votes               # User's voting history
```

#### Admin Routes
```
POST /v1/admin/polls              # Create poll (admin only)
PUT  /v1/admin/polls/:id/state    # Transition poll state (admin only)
GET  /v1/admin/anomalies          # View bot alerts (admin only)
```

### Error Codes

| Code | HTTP Status | Meaning |
|------|-------------|---------|
| `INVALID_REQUEST` | 400 | Missing/invalid fields |
| `UNAUTHORIZED` | 401 | Invalid or missing token |
| `FORBIDDEN` | 403 | User is not admin |
| `NOT_FOUND` | 404 | Poll/answer not found |
| `DUPLICATE_VOTE` | 409 | User already voted on this poll |
| `POLL_CLOSED` | 400 | Poll is not in active state |
| `RATE_LIMIT_EXCEEDED` | 429 | User or IP exceeded quota |
| `INTERNAL_ERROR` | 500 | Server error |
| `SERVICE_UNAVAILABLE` | 503 | Queue full, shard down, etc. |

### Latency Instrumentation

```python
# Record latency histogram per endpoint
@app.middleware("http")
async def record_latency(request: Request, call_next):
    start_time = time.monotonic()
    response = await call_next(request)
    latency_ms = (time.monotonic() - start_time) * 1000
    route = request.scope.get("route")
    metrics.record_latency(route.path if route else request.url.path, latency_ms)
    return response
```

---

## Acceptance Criteria

- [ ] FastAPI server (uvicorn/gunicorn) initializes on port 3000 (configurable)
- [ ] All 6 routes implemented and return expected HTTP status
- [ ] Request/response logging in JSON format
- [ ] Authentication dependency validates Adidos token (FastAPI `Depends`)
- [ ] Error responses consistent (error envelope with code + message)
- [ ] Latency metrics recorded (P95, P99 per endpoint)
- [ ] All handlers are async (async def, native asyncio)
- [ ] Request size limits enforced (1MB max body)
- [ ] All responses include request_id and timestamp
- [ ] Health check endpoint: GET /health → 200 OK

---

## Testing Strategy

- **Unit Tests**: Middleware testing (auth, error handling)
- **Integration Tests**: HTTP client makes requests, validates response shape
- **Prior Art**: Test Adidos API integration tests (mirror structure)

---

## Related Issues

- I-004: Authentication Middleware (details on token validation)
- I-005: Vote Acceptance & Queueing (implements POST /v1/vote handler)
- I-013: Admin Endpoints (implements POST /v1/admin/polls)

---

## Implementation Checklist

- [ ] Create `src/api/app.py` (FastAPI app setup)
- [ ] Create `src/api/middleware/` (logging, error handling)
- [ ] Create `src/api/routes/user.py` (GET /v1/polls, POST /v1/vote, GET /v1/user/votes) as an `APIRouter`
- [ ] Create `src/api/routes/admin.py` (admin endpoints) as an `APIRouter`
- [ ] Create `src/api/dependencies/auth.py` (token validation via FastAPI `Depends`)
- [ ] Create `src/schemas/responses.py` (response envelope Pydantic models)
- [ ] Create `src/metrics/latency.py` (P95/P99 recording)
- [ ] Add exception handlers in `src/api/middleware/error_handler.py` (FastAPI `exception_handler`)
- [ ] Test with curl/Postman

---

**Acceptance**: Server starts, all routes respond with correct status codes, metrics recorded, PR reviewed and merged.
