# I-016: Anomaly Reporting to Adidos

**Status**: Ready for Implementation  
**Epic**: Admin & Security  
**Priority**: P1  
**Estimated Effort**: 2 days

---

## Problem Statement

Anomaly rows accumulate in I-001's `anomalies` table — written by I-007 (`rate_limit_exceeded`), I-014 (`bot_pattern_detected`), and I-015 (`duplicate_attempts_blocked`) — but nothing forwards them anywhere outside this service. Per spec decision #13 and the loose-coupling principle established in decision #2 ("Adidos issues admin credentials but does not manage poll state directly"), the same separation applies here: this service does local, short-term defense (throttling, blocking, flagging), while Adidos owns the longer-term escalation decision — IP blacklisting, account bans — because only Adidos has visibility across every service a bad actor might be hitting, not just this one.

The service needs a scheduled job that:
- Batches anomaly rows that haven't been sent yet
- POSTs them to an Adidos-provided endpoint on a fixed cadence (every 30 minutes, story #24)
- Marks what it successfully sent, so the next run doesn't resend it
- Fails safe: if Adidos's endpoint is unreachable, nothing is lost — the same rows are simply picked up on the next tick

---

## Solution

A Kubernetes CronJob, running every 30 minutes, that:
1. Acquires a short Redis lock to guard against overlapping runs
2. Queries `anomalies` rows where `reported_at IS NULL` (a new column this issue adds via migration)
3. Batches them (capped per request) into the shape of the domain model's `BotSuspected` event
4. POSTs the batch to `ADIDOS_ANOMALY_WEBHOOK_URL`
5. On a 2xx response, sets `reported_at = now()` for every alert_id in the batch
6. On failure, leaves `reported_at` null — the same rows (plus any new ones) are included in the next run, 30 minutes later

This is a reporting job only. It never blacklists an IP, bans an account, or takes any action beyond what I-007/I-014/I-015 already recorded locally — that decision belongs to Adidos, on the other side of this POST.

---

## User Stories

(from SPECIFICATION.md)

24. As the system, I want to report suspicious voting patterns to Adidos every 30 minutes, so that Adidos can take longer-term action (IP blacklist, user ban)

---

## Implementation Decisions

### Schema Change: `reported_at`

I-001's `anomalies` table has `acknowledged_at` (when *ops* acknowledged an alert — a human, internal action) and `action_taken` (what this service did automatically, e.g. I-015's block). Neither of those means "Adidos has received this." Reusing `acknowledged_at` for that would conflate two different signals — an ops person acknowledging an alert in I-013's admin view is not the same event as this job successfully forwarding it. This issue adds one additive column via a new migration, without touching any existing column:

```sql
-- scripts/migrations/003_anomalies_reported_at.sql
ALTER TABLE anomalies ADD COLUMN reported_at TIMESTAMP;
CREATE INDEX idx_anomalies_unreported ON anomalies(created_at) WHERE reported_at IS NULL;
```
The partial index keeps the "find unreported rows" query cheap even as the table grows, since only the unreported tail is ever scanned — matching I-001's existing pattern of indexing exactly the query shapes this service runs.

### Report Payload (Mirrors Domain Model's `BotSuspected` Event)

DOMAIN_MODEL.md already defines the shape this data should take once it leaves the service boundary:
```
BotSuspected {
  alert_id: UUID,
  user_id: string (or null),
  ip_address: string (or null),
  reason: string,
  action: string,
  timestamp: datetime
}
```
This job maps `anomalies` columns onto that event shape rather than inventing a new wire format:

| `anomalies` column | `BotSuspected` field |
|---|---|
| `alert_id` | `alert_id` |
| `user_id` | `user_id` |
| `ip_address` | `ip_address` |
| `description` | `reason` |
| `action_taken` (falls back to `alert_type` if null) | `action` |
| `created_at` | `timestamp` |

```python
# src/jobs/report_anomalies.py
BATCH_SIZE = 500
LOCK_KEY = "report_anomalies:lock"
LOCK_TTL_SECONDS = 300

async def report_anomalies() -> None:
    if not await redis.set(LOCK_KEY, "1", nx=True, ex=LOCK_TTL_SECONDS):
        return  # another run already in progress; next scheduled tick covers any backlog

    try:
        rows = await anomalies_repo.list_unreported(limit=BATCH_SIZE)
        if not rows:
            return

        payload = [
            {
                "alert_id": str(r.alert_id),
                "user_id": r.user_id,
                "ip_address": r.ip_address,
                "reason": r.description,
                "action": r.action_taken or r.alert_type,
                "timestamp": r.created_at.isoformat(),
            }
            for r in rows
        ]

        response = await adidos_client.post_anomaly_batch(payload)
        if response.status_code // 100 == 2:
            await anomalies_repo.mark_reported([r.alert_id for r in rows], reported_at=utcnow())
        # non-2xx: leave reported_at null, log the failure, next tick retries the same rows
    finally:
        await redis.delete(LOCK_KEY)
```
`BATCH_SIZE` caps a single request; if more than 500 rows are unreported at once (e.g. after an outage), the next tick 30 minutes later picks up the remainder rather than this job trying to drain an unbounded backlog in one call.

### Adidos Client
```python
# src/clients/adidos_anomaly_client.py
async def post_anomaly_batch(payload: list[dict]) -> httpx.Response:
    return await http_client.post(
        settings.adidos_anomaly_webhook_url,
        json={"anomalies": payload},
        headers={"Authorization": f"Bearer {settings.adidos_service_token}"},
        timeout=10.0,
    )
```
A single outbound integration point, consistent with the rest of this service's Adidos relationship (spec decision #1: trust tokens as-is, don't manage token lifecycle) — this issue calls out once per batch and does not retry within the same run; retry is simply "the next scheduled 30-minute tick."

### Scheduling

Run as a Kubernetes CronJob (`*/30 * * * *`) invoking `python -m src.jobs.report_anomalies` as a one-shot process, rather than an in-process asyncio scheduler inside the FastAPI app. This keeps the API process free of long-running background loops, and matches how I-002 already separates vote-processing workers from the API server pods — reporting is its own workload with its own failure mode (a crashed reporting run should never affect vote acceptance).

### Idempotency & Failure Behavior

- **Adidos endpoint down / non-2xx**: rows stay `reported_at IS NULL`; next tick resends them plus anything new. No local escalation is attempted — that would violate the loose-coupling boundary this issue exists to preserve.
- **Job crashes mid-batch, before marking `reported_at`**: same as above — Adidos may receive the same batch twice on the next tick. This is accepted as safe: Adidos's own ingestion is expected to be idempotent on `alert_id` (out of this service's control, but the payload always includes it precisely so the receiving side can dedupe).
- **Two CronJob pods overlap** (rare, K8s doesn't guarantee no-overlap): the Redis lock (`report_anomalies:lock`, TTL 300s) ensures only one run queries/sends at a time; the loser exits immediately and its rows are covered by the winner or the next tick.

---

## Acceptance Criteria

- [ ] CronJob runs every 30 minutes and queries `anomalies WHERE reported_at IS NULL`
- [ ] A batch of unreported rows is POSTed to `ADIDOS_ANOMALY_WEBHOOK_URL` in the `BotSuspected`-shaped payload
- [ ] On a 2xx response, every `alert_id` in the batch gets `reported_at` set; those rows are excluded from the next run's query
- [ ] On a non-2xx response or network failure, `reported_at` stays null and the same rows appear in the next run
- [ ] A run with zero unreported rows completes as a no-op without calling Adidos
- [ ] Batches are capped at 500 rows per request; a backlog larger than that is drained across multiple 30-minute ticks, not one call
- [ ] Two overlapping CronJob executions do not both send the same batch (Redis lock verified)
- [ ] Migration `003_anomalies_reported_at.sql` adds the column without altering any existing column or losing existing data
- [ ] `reported_at` is never conflated with `acknowledged_at` (they can be set independently and mean different things)

---

## Testing Strategy

- **Unit Tests**: Payload mapping from `anomalies` row to `BotSuspected` shape (including `action_taken` fallback to `alert_type` when null); lock acquire/release behavior
- **Integration Tests**: Seed unreported rows, run the job against a mock Adidos endpoint → verify `reported_at` set only on success; mock endpoint returns 500 → verify `reported_at` stays null and rows reappear on a second run; run job with zero unreported rows → verify no HTTP call is made
- **Concurrency Test**: Start two job runs simultaneously against the same Redis/DB → verify only one performs the POST, via the lock
- **Migration Test**: Apply `003_anomalies_reported_at.sql` against a DB already seeded via I-001's schema and I-001's seed script → verify existing rows are unaffected and `reported_at` defaults to null
- **Prior Art**: Mirror I-001's migration-script conventions; mirror I-002's "no business logic beyond what's declared" scoping discipline

---

## Out of Scope

- Any local escalation action (IP blacklist, account ban) — exclusively Adidos's decision, per spec decision #13; this issue only reports
- Webhook authentication/handshake details beyond a static bearer token from config — deeper auth negotiation with Adidos is assumed pre-arranged, not designed here
- Real-time/immediate reporting of critical anomalies outside the 30-minute cadence — I-014 already triggers an immediate internal `notify_admins()` for critical rows; this job's cadence is uniform regardless of severity, matching the spec's stated 30-minute batch decision
- Retrying within a single run (e.g. exponential backoff inside `report_anomalies()`) — the 30-minute tick interval **is** the retry mechanism, deliberately kept simple
- Adidos-side deduplication logic — out of this service's control; this issue only guarantees `alert_id` is always present in the payload so the receiving side can dedupe

---

## Related Issues

- I-001: Database Schema (`anomalies` table; this issue adds an additive `reported_at` migration)
- I-002: Redis Cluster Setup (provides the lock primitive used to guard against overlapping runs)
- I-013: Admin Poll Management Endpoints (a different consumer of the same `anomalies` table — human-facing, not Adidos-facing)
- I-014: Bot Detection & Alerting (writes `bot_pattern_detected` rows this job reports)
- I-015: Duplicate Detection (writes `duplicate_attempts_blocked` rows this job reports)

---

## Implementation Checklist

- [ ] Create `scripts/migrations/003_anomalies_reported_at.sql`
- [ ] Create `src/jobs/report_anomalies.py` (batch query, lock, mark-reported logic)
- [ ] Create `src/clients/adidos_anomaly_client.py` (`post_anomaly_batch`)
- [ ] Add `anomalies_repo.list_unreported()` and `anomalies_repo.mark_reported()` to `src/services/anomalies.py`
- [ ] Add `ADIDOS_ANOMALY_WEBHOOK_URL` and `ADIDOS_SERVICE_TOKEN` to service config
- [ ] Create Kubernetes CronJob manifest (`*/30 * * * *`)
- [ ] Write unit tests for payload mapping and lock behavior
- [ ] Write integration tests covering success, failure, and no-op runs
- [ ] Write a concurrency test for the overlapping-run lock
- [ ] Test the migration against I-001's seeded fixture data

---

**Acceptance**: All acceptance criteria met, integration tests pass, PR reviewed and merged.
