# I-018: Structured Logging (JSON + Sampling)

**Status**: Ready for Implementation  
**Epic**: Observability & Launch  
**Priority**: P0 (Blocker)  
**Estimated Effort**: 3 days  
**Depends On**: I-003, I-004

---

## Problem Statement

At 1M+ votes with 50K-100K/sec bursts, logging every request at full detail would be both a cost problem (log volume) and a noise problem (finding the one failed vote in a flood of identical successes). SPECIFICATION.md's Implementation Decision #15 sets the policy: JSON structured logs, 1-in-1000 sampling for successful votes, 100% logging for errors and rejections. This issue implements that policy and answers three questions the policy alone doesn't:

- How does an operator trace **one specific vote** across the API handler, the queue, and the worker (I-008) when only 1-in-1000 successes are logged at all?
- What fields belong on a vote log line, and what must never appear on one, given privacy decision #38 (admins see only vote counts and anomaly alerts, never raw vote logs)?
- Where do these logs go, and what part of that is this issue's responsibility versus deployment/ops scope?

---

## Solution

Bind `structlog` into the FastAPI app to produce single-line JSON logs. I-003's request ID middleware already generates a `request_id` per request; this issue binds that `request_id` into structlog's context for the lifetime of the request, and — as a deliberate design choice — threads the same `request_id` into the `VotePayload` (I-005) that gets pushed onto `queue:votes`, so I-008's worker can re-bind it and continue the same trace when it dequeues and processes the vote. A single `request_id` therefore ties together every log line for one vote's full lifecycle (API accept → enqueue → worker dequeue → DB write outcome), without standing up a distributed tracing backend (Jaeger/Zipkin) for MVP.

Sampling is applied at the vote level, not per log call: whether a given vote's lifecycle is "in the sample" is a deterministic function of its `vote_id`, decided once and consistent across every stage. Errors and rejections bypass sampling entirely — they are always logged.

---

## User Stories

(from SPECIFICATION.md)

38. As a privacy officer, I want admins to see only vote counts and anomaly alerts, never raw vote logs, so that user privacy is protected by default

There is no separately numbered SPECIFICATION.md story for "trace one vote end-to-end," but it's a direct operational consequence of decisions already locked in: async processing (decision #3) means a vote's lifecycle spans two processes (API, worker) that don't share a call stack, and decision #15 caps logging at 1-in-1000 for successes. Without deliberate `request_id` propagation, an on-call engineer investigating a single slow or failed vote would have no way to correlate its API-side and worker-side log lines. That correlation is this issue's core design decision, not an incidental detail — see Implementation Decisions.

---

## Implementation Decisions

### Structlog Configuration

```python
import structlog

structlog.configure(
    processors=[
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.processors.TimeStamper(fmt="iso", utc=True),
        structlog.processors.JSONRenderer(),
    ],
    wrapper_class=structlog.make_filtering_bound_logger(logging.INFO),
    context_class=dict,
    logger_factory=structlog.PrintLoggerFactory(),
)
```

Both the API process and the worker process (I-008) import this same configuration module, so log line shape is identical regardless of which process emitted it.

### Request ID Correlation (API → Queue → Worker)

I-003's request ID middleware generates `request_id` and, in this issue, binds it into structlog's `contextvars` so every log call made during that request automatically carries it:

```python
@app.middleware("http")
async def bind_request_id(request: Request, call_next):
    request_id = request.headers.get("x-request-id") or f"req-{uuid4().hex[:12]}"
    structlog.contextvars.bind_contextvars(request_id=request_id)
    request.state.request_id = request_id
    return await call_next(request)
```

When the vote handler (I-005) enqueues a vote, `request_id` is added to the `VotePayload` — extending I-005's schema beyond `vote_id`, `user_id`, `poll_id`, `answer_id`, `requested_at`:

```python
class VotePayload(BaseModel):
    vote_id: UUID
    user_id: str
    poll_id: UUID
    answer_id: UUID
    requested_at: datetime
    request_id: str   # added by I-018 for cross-process log correlation
```

The worker (I-008), on dequeue, re-binds the same value before logging anything about that vote:

```python
payload = VotePayload.parse_raw(raw)
structlog.contextvars.bind_contextvars(request_id=payload.request_id, vote_id=str(payload.vote_id))
logger.info("vote_dequeued", poll_id=str(payload.poll_id))
# ... write to shard ...
logger.info("vote_written", shard=shard_id, outcome="success")
```

The result: `grep request_id=req-abc123` across the API and worker log streams reconstructs a single vote's complete lifecycle. This is intentionally lightweight — it is a correlation ID threaded through application data, not a tracing library with spans — and is judged sufficient for MVP scale per the same "loose coupling, avoid extra infrastructure" reasoning SPECIFICATION.md applies elsewhere (e.g., decision #8 rejecting Kafka/RabbitMQ).

### Sampling Strategy

Sampling decides whether a **successful** vote's full lifecycle is logged, not whether an individual log call fires independently at each stage. Independent per-call random sampling would almost always produce broken partial traces (API line sampled in, worker line sampled out, or vice versa). Instead, the sampling decision is a pure function of `vote_id`, computed identically wherever it's needed:

```python
def is_sampled(vote_id: UUID) -> bool:
    return vote_id.int % 1000 == 0
```

- If `is_sampled(vote_id)` is `True`, every log line for that vote's lifecycle (API accept, enqueue, worker dequeue, DB write) is emitted.
- If `False`, none of the routine "success" log lines are emitted for that vote.
- **Errors and rejections are exempt from sampling entirely.** Any 4xx/5xx response, any worker-side failure (constraint violation, shard unreachable), and any anomaly/rate-limit rejection is logged at 100%, regardless of what `is_sampled` returns for that vote. This is an unconditional OR, not a higher sampling rate — decision #15 draws a hard line between "successful votes" (sampled) and "errors/rejections" (always).

### Vote Log Line Schema

| Field | On success (sampled) | On error/rejection | Notes |
|---|---|---|---|
| `timestamp` | yes | yes | ISO 8601 UTC |
| `level` | yes | yes | `info` for success, `warning`/`error` for rejections/failures |
| `event` | yes | yes | e.g. `vote_accepted`, `vote_written`, `vote_rejected` |
| `request_id` | yes | yes | correlates API ↔ worker |
| `vote_id` | yes | yes (if assigned) | absent for requests rejected before a `vote_id` exists (e.g. 400 validation) |
| `user_id` | yes | yes | opaque Adidos identifier, never the raw token |
| `poll_id` / `answer_id` | yes | yes (if known) | |
| `route` / `status_code` | yes | yes | |
| `latency_ms` | yes | yes | |
| `shard` | yes (worker only) | yes (worker only) | which PostgreSQL shard handled the write |
| `outcome` | yes | yes | `success`, `duplicate`, `rate_limited`, `constraint_violation`, etc. |

Example line:

```json
{"timestamp":"2026-08-21T16:04:12Z","level":"info","event":"vote_written","request_id":"req-abc123","vote_id":"3ab2...","user_id":"adidos-u-9F2","poll_id":"8f14...","answer_id":"3ab2...","shard":3,"latency_ms":42,"outcome":"success"}
```

### What Must Never Be Logged

- **The raw Adidos token / `Authorization` header value.** I-004 caches only the derived `user_id`; log `user_id`, never the token string. This isn't just hygiene — a leaked token in a log aggregator is a credential leak with a 5-10 minute cache TTL window of live exposure.
- **Full raw request/response bodies at scale.** I-003's request logging middleware logs method/path/status, not the raw JSON body, for both PII sprawl and log volume reasons. Validation error responses already summarize the offending field name (see I-003's error envelope `details`); that summary is what gets logged, not the body itself.
- **IP addresses on ordinary vote log lines.** It's appropriate to log `ip_address` on anomaly/rate-limit events — the domain model's `AnomalyAlert` (DOMAIN_MODEL.md) explicitly models `ip_address` as an attribute for exactly this purpose — but not on routine vote accept/write lines, where it adds no diagnostic value and widens exposure.

Per privacy decision #38, this isn't only about log volume — it's a substantive privacy control. Admins query `vote_counts`/results and the `anomalies` table (I-013's admin endpoints) through the API; they never grep raw logs. Raw logs must not become a shadow channel that exposes individual vote identities beyond what the domain model already permits admins to see.

### Log Aggregation (Related, Not This Issue's Scope)

This issue produces conformant JSON log lines to stdout/stderr (12-factor style), which is what both ELK and CloudWatch expect for collection. Shipping and aggregating those logs — a Fluentd/Filebeat sidecar or CloudWatch Logs agent, index/retention policy, Kibana or CloudWatch Insights dashboards — is deployment checklist item 7 in SPECIFICATION.md and is deployment/ops scope, not this issue's. See Out of Scope.

---

## Acceptance Criteria

- [ ] All log lines emitted as single-line JSON via `structlog`, identical shape across API and worker processes
- [ ] Every log line inside a request or vote lifecycle includes `request_id`
- [ ] `request_id` is threaded into the vote payload (I-005) and re-bound by the worker on dequeue (I-008)
- [ ] Successful vote logs are sampled at 1-in-1000, deterministically by `vote_id` (not independently per log call)
- [ ] All error/rejection responses (4xx/5xx, and worker-side failures) are logged at 100%, independent of the sampling decision
- [ ] Vote log lines never include the raw token/`Authorization` header value
- [ ] Vote log lines never include full raw request/response bodies
- [ ] Vote log line field schema is documented and consistent between API and worker
- [ ] Logs are written to stdout/stderr in a format consumable by ELK/CloudWatch without transformation

---

## Testing Strategy

- **Unit Tests**: `is_sampled()` determinism (same `vote_id` always yields the same result; roughly 1-in-1000 distribution over a large random sample); negative assertion fixture that scans rendered log output for token-like substrings and fails if any appear
- **Integration Tests**: Cast a vote end-to-end, capture both API and worker log output, assert the same `request_id` and `vote_id` appear in both; force a rejection (duplicate vote, rate limit) and assert it's logged regardless of sampling
- **Prior Art**: Mirror Adidos's existing structured logging conventions for field naming consistency across services

---

## Out of Scope

- Log aggregation infrastructure (ELK/CloudWatch shipping, index/retention policy, Kibana dashboards) — deployment checklist item 7, ops/deployment scope
- Distributed tracing (Jaeger/Zipkin) — `request_id` correlation is a deliberate lightweight substitute for MVP scale, not a placeholder for a future tracing rollout described here
- Log-based alerting — alerting reads metrics (I-017) via Alertmanager (I-019), not log queries
- Anomaly detection logic itself (I-014/I-015/I-016) — this issue only ensures anomaly events are logged at 100%, it doesn't detect them

---

## Related Issues

- I-003: API Framework & Routing (origin of `request_id`, and the request logging middleware this issue extends)
- I-004: Authentication Middleware (defines the token this issue must never log; supplies `user_id`)
- I-005: Vote Acceptance & Queueing (`VotePayload` extended here with `request_id`)
- I-008: Vote Processor Workers (re-binds `request_id` on dequeue; worker-side log lines)
- I-017: Metrics & Monitoring (complementary signal — metrics for aggregate health, logs for individual-vote diagnosis)
- I-019: Alerting & Dashboards (does not consume logs directly, but on-call engineers it pages will)
- I-020: Incident Runbooks (diagnosis steps reference these log fields)

---

## Implementation Checklist

- [ ] Add `structlog` dependency
- [ ] Create `src/logging/config.py` (structlog processor pipeline, JSON renderer, shared by API and worker)
- [ ] Extend I-003's request ID middleware to bind `request_id` into structlog contextvars (`src/api/middleware/logging.py`)
- [ ] Extend `VotePayload` (I-005) with a `request_id` field in `src/schemas/votes.py`
- [ ] Re-bind `request_id` (and `vote_id`) in the worker's log context on dequeue (`src/workers/vote_processor.py`, I-008)
- [ ] Implement `is_sampled(vote_id)` in `src/logging/sampling.py`
- [ ] Wire sampled/100%-error logging into the vote accept path (I-005) and worker write path (I-008)
- [ ] Document the vote log line schema in `docs/architecture/logging.md`
- [ ] Write unit tests (sampling determinism, no-token-leak assertion) and an integration test verifying end-to-end `request_id` correlation

---

**Acceptance**: All acceptance criteria met, log lines verified JSON and correctly sampled/correlated across API and worker, PR reviewed and merged.
