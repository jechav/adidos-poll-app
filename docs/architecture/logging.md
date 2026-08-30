# Structured Logging (JSON + Sampling)

Implements [I-018](../issues/I-018-logging.md). Both the API process
(`src.api.app`) and the worker process (`src.worker.main`, I-008) call
the same `configure_logging()` (`src/logging/config.py`) at startup, so
log line shape is identical single-line JSON regardless of which process
emitted it — 12-factor style to stdout, consumable by ELK/CloudWatch
without transformation. Shipping/aggregating those logs is deployment
scope (SPECIFICATION.md checklist item 7), not this document's.

## Correlation: `request_id`

`RequestContextMiddleware` (`src/api/middleware/request_context.py`,
I-003) assigns `request_id` per request and binds it into
`structlog.contextvars` for the lifetime of that request — every log call
made anywhere downstream (route handlers, services, exception handlers)
automatically carries it via the shared `merge_contextvars` processor,
with no need to pass it explicitly at each call site.

When the vote handler enqueues a vote (`src/api/routes/user.py`), the
same `request_id` is threaded into the `VotePayload` pushed onto
`queue:votes` (`src.services.vote_queue.VotePayload`). The worker
(`src/worker/queue_consumer.py`) re-binds `request_id` (and `vote_id`) on
dequeue, and continues logging under that same context through
`src/worker/processor.py`'s write path.

The result: `grep request_id=req-abc123` across the API and worker log
streams reconstructs one vote's complete lifecycle (API accept → enqueue
→ worker dequeue → DB write outcome) — see
`tests/integration/test_logging_correlation.py` for a runnable
demonstration of this.

## Sampling

`src.logging.sampling.is_sampled(vote_id)` decides whether a
**successful** vote's entire lifecycle is logged — a pure, deterministic
function of `vote_id` (`vote_id.int % 1000 == 0`), computed identically
in the API process and the worker process so both reach the same
decision for the same vote without coordinating. It is never consulted
for errors or rejections.

**Errors and rejections are exempt from sampling entirely and are always
logged.** This is enforced at each rejection/error site individually, not
by a single central switch:

| Layer | Where |
|---|---|
| API-level errors (4xx/5xx from `HTTPException` or unhandled exceptions) | `src/api/middleware/error_handler.py` — every registered handler logs unconditionally |
| Generic per-request completion, non-vote routes | `RequestContextMiddleware` — completion line logs on any `status_code >= 400`, independent of its own request-level sample |
| Worker-side DB constraint rejection (Layer 2 duplicate backstop, I-006) | `src/worker/processor.py`'s `_log_constraint_violations` |
| Worker-side shard-unavailable requeue | `src/worker/processor.py`'s `requeue` |

## Vote Log Line Schema

| Field | On success (sampled) | On error/rejection | Notes |
|---|---|---|---|
| `timestamp` | yes | yes | ISO 8601 UTC (`structlog.processors.TimeStamper`) |
| `level` | yes | yes | `info` for success, `warning`/`error` for rejections/failures |
| `event` | yes | yes | `vote_accepted`, `vote_dequeued`, `vote_written`, `vote_rejected`, `vote_write_rejected`, `vote_requeued`, `unhandled_exception`, ... |
| `request_id` | yes | yes | correlates API ↔ worker |
| `vote_id` | yes | yes (if assigned) | absent for requests rejected before a `vote_id` exists (e.g. 400 validation) |
| `user_id` | yes | yes (where known) | opaque Adidos identifier, never the raw token |
| `poll_id` / `answer_id` | yes | yes (if known) | |
| `status_code` | yes (API events) | yes | |
| `shard` | yes (worker only) | yes (worker only) | which PostgreSQL shard handled the write |
| `outcome` | yes | yes | `success`, `duplicate`, `rate_limited`, `constraint_violation`, `shard_unavailable`, etc. |

Example lines (one successful vote's lifecycle, `request_id=req-abc123`):

```json
{"timestamp":"2026-08-21T16:04:12.001Z","level":"info","event":"vote_accepted","request_id":"req-abc123","vote_id":"3ab2...","user_id":"adidos-u-9F2","poll_id":"8f14...","answer_id":"3ab2...","outcome":"success"}
{"timestamp":"2026-08-21T16:04:12.030Z","level":"info","event":"vote_dequeued","request_id":"req-abc123","vote_id":"3ab2...","poll_id":"8f14..."}
{"timestamp":"2026-08-21T16:04:12.042Z","level":"info","event":"vote_written","request_id":"req-abc123","vote_id":"3ab2...","shard":3,"outcome":"success"}
```

## What Must Never Be Logged

- **The raw Adidos token / `Authorization` header value.** Only the
  derived `user_id` is ever logged; `RequestContextMiddleware` logs
  `has_auth_header` (a boolean), never the header's value.
- **Full raw request/response bodies at scale.** Validation errors log
  the structured `details` FastAPI already produces, never the raw body.
- **IP addresses on ordinary vote log lines.** `ip_address` is
  appropriate on anomaly/rate-limit events (I-015/I-016's `AnomalyAlert`
  domain object), not on routine vote accept/write lines.

`tests/unit/test_logging_config.py` includes a negative-assertion
fixture that scans rendered log output for a planted token-like
substring and fails the build if it ever appears.

## Out of Scope

See I-018's own Out of Scope: log aggregation infrastructure, distributed
tracing (Jaeger/Zipkin), log-based alerting, and anomaly *detection*
logic itself (I-014/I-015/I-016) are all separate concerns from making
sure the relevant events are logged.
