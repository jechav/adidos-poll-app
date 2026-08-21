# I-014: Bot Detection & Alerting

**Status**: Ready for Implementation  
**Epic**: Admin & Security  
**Priority**: P1  
**Estimated Effort**: 4 days

---

## Problem Statement

I-007's rate limiter rejects individual requests that cross a fixed per-user (5/min) or per-IP (50/min) threshold. I-015's 3-strike block mechanically counts duplicate-vote attempts for a single `(user_id, ip)` pair. Neither catches the broader shape of an attack:
- An IP that stays under 50/min but coordinates hundreds of distinct `user_id`s to vote in lockstep (low-and-slow, distributed)
- A single IP hammering one poll fast enough to matter even though no individual request crosses a per-request threshold in isolation (e.g., 100 votes from one IP in 10 seconds — under the per-minute cap, but clearly not organic)
- A traffic spike so large it threatens the whole service (100K votes in 1 second), which needs to page an admin immediately (story #20), not just get logged

This issue is the pattern-detection layer that looks across the vote stream rather than at one request at a time, classifies what it sees by severity, and writes the result to I-001's `anomalies` table so I-013's admin view and I-016's Adidos-reporting job both have something to consume.

---

## Solution

Add a bot-detection component that:
1. Maintains its own short-window counters in Redis, separate from I-007's rate-limit counters, so it can see patterns that never individually trip a per-request rate limit
2. Runs a lightweight periodic evaluator (every 5-10 seconds) that checks accumulated counters against a fixed rule table and writes `anomalies` rows when a rule fires
3. Assigns `severity` per rule (`warning` for suspicious-but-contained, `critical` for attack-scale), and invokes an admin-notification hook for every critical alert (story #20)
4. Owns exactly the `bot_pattern_detected` value of I-001's `alert_type` enum — `rate_limit_exceeded` and `duplicate_attempts_blocked` are written directly by I-007 and I-015 respectively, not by this issue

The counters are cheap (`INCR`/`ZADD` with short TTLs) and are updated inline during vote acceptance (I-005's handler); the rule evaluation itself runs out-of-band so it never adds latency to the vote path.

---

## User Stories

(from SPECIFICATION.md)

16. As an admin, I want to view bot detection alerts showing suspicious voting patterns, so that I can identify and respond to attacks
17. As an admin, I want to see rate limit violations (IPs and users exceeding quotas), so that I can understand attack vectors
18. As an admin, I want to see the top countries or regions voting on a poll (if Adidos provides geo data), so that I can understand demographic insights
20. As an admin, I want to be notified of critical anomalies (e.g., 100K votes in 1 second), so that I can respond to emergencies

Story 17 is served by I-013's read of the `rate_limit_exceeded` rows I-007 writes — this issue does not duplicate that write path (see Problem Statement). Story 18 (geo insight) is out of scope for detection logic itself; if Adidos supplies geo data on the token/request in the future, it could feed a new rule here, but no geo signal exists in the current request shape, so no rule references it in this issue (see Out of Scope).

---

## Implementation Decisions

### Detection Counters (New Redis Keys)

These keys are introduced by this issue and are additive to I-002's key namespace table (to be appended to `docs/architecture/redis-keys.md`):

| Key Pattern | Type | Written By | Read By | TTL | Notes |
|---|---|---|---|---|---|
| `botcheck:ip:{ip}` | Sorted Set (`ZADD` timestamp) | I-005 (inline, on every accepted vote attempt) | I-014 (periodic evaluator) | 10s rolling (members trimmed via `ZREMRANGEBYSCORE`) | Timestamps of recent attempts from one IP, independent of the rate-limit counter |
| `botcheck:ip_users:{ip}` | Set (`SADD user_id`) | I-005 (inline) | I-014 (periodic evaluator) | 300s | Distinct `user_id`s seen from one IP in a 5-minute window (low-and-slow detection) |
| `botcheck:global:{bucket}` | String (`INCR`), `bucket` = current unix second | I-005 (inline) | I-014 (periodic evaluator) | 5s | Total accepted vote attempts service-wide in a 1-second bucket |
| `botcheck:cooldown:{rule}:{key}` | String (flag) | I-014 | I-014 | rule-specific (see below) | Suppresses re-alerting on the same rule/target while a condition is still active |

These are deliberately **not** the same counters I-007 uses for quota enforcement — reusing them would mean a pattern that stays under the per-request quota (e.g., 40 distinct users from one IP, each individually fine) would never accumulate anywhere this issue can see it.

### Rule Table

| Rule | Condition | Severity | Cooldown |
|---|---|---|---|
| Single-IP burst | `ZCARD botcheck:ip:{ip}` ≥ 100 within the trailing 10s | `warning` | 60s per IP |
| Single-IP sustained burst | Single-IP burst condition still true on 3 consecutive evaluator ticks (~30s) for the same IP | `critical` | 300s per IP |
| Global traffic spike | `botcheck:global:{bucket}` ≥ 100,000 for any bucket in the trailing few seconds | `critical` | 30s (global) |
| Distributed low-and-slow | `SCARD botcheck:ip_users:{ip}` ≥ 20 within the trailing 5 minutes | `warning` | 600s per IP |

Thresholds mirror the spec's own test examples directly: "100 votes from same IP within 10 seconds" (single-IP burst) and "100K votes in 1 second" (global traffic spike, critical, story #20). Cooldowns exist so a sustained attack produces one alert per escalation, not one alert per evaluator tick — an admin needs a manageable alert stream, not a flood that itself looks like noise.

### Periodic Evaluator
```python
# src/jobs/bot_detection.py
EVALUATOR_INTERVAL_SECONDS = 5

async def run_bot_detection_loop(redis: RedisCluster) -> None:
    while True:
        await evaluate_single_ip_bursts(redis)
        await evaluate_global_spike(redis)
        await evaluate_low_and_slow(redis)
        await asyncio.sleep(EVALUATOR_INTERVAL_SECONDS)

async def evaluate_single_ip_bursts(redis: RedisCluster) -> None:
    for ip in await recent_active_ips(redis):  # IPs touched since last tick
        now = time.time()
        await redis.zremrangebyscore(f"botcheck:ip:{ip}", 0, now - 10)
        count = await redis.zcard(f"botcheck:ip:{ip}")
        if count >= 100 and not await redis.exists(f"botcheck:cooldown:single_ip_burst:{ip}"):
            await raise_anomaly(
                ip_address=ip,
                alert_type="bot_pattern_detected",
                severity="warning",
                description=f"{count} vote attempts from {ip} in the last 10 seconds",
            )
            await redis.set(f"botcheck:cooldown:single_ip_burst:{ip}", "1", ex=60)
```
This runs as a standalone async loop in a dedicated worker process (not inside an API request handler, and not sharing a process with the vote processor workers from I-008) — evaluation work should never compete with request-serving or DB-write capacity during the exact bursts it's trying to detect.

### Severity → Notification
```python
async def raise_anomaly(*, severity: str, alert_type: str, description: str, **kwargs) -> None:
    alert = await anomalies_repo.create(
        alert_id=uuid4(),
        alert_type=alert_type,
        severity=severity,
        description=description,
        created_at=utcnow(),
        **kwargs,
    )
    if severity == "critical":
        await notify_admins(alert)  # story #20 — hands off to the alerting pipeline
```
`notify_admins()` is a thin interface this issue owns the call site for, not the delivery mechanism — actual paging/Slack/webhook integration is I-019's concern (Phase 5, Alerting & Dashboards). This issue's obligation is that every `critical` row written here reliably triggers that call.

### Why This Differs From I-015

| | I-015 (Duplicate Detection) | I-014 (this issue) |
|---|---|---|
| **Trigger** | A single, specific event: 3rd duplicate vote from one `(user_id, ip)` pair | Accumulated patterns across many requests/IPs/users |
| **Logic** | Deterministic counter + threshold, one rule | Multiple heuristic rules evaluated periodically |
| **Scope** | One `(user_id, ip)` combo at a time | The whole vote stream (per-IP, cross-user, global) |
| **Can catch what the other misses?** | No — I-015 only fires on repeated duplicates from an already-identified pair | Yes — catches attacks where no individual user or IP ever duplicates a vote or crosses I-007's quota (e.g., 100 distinct bot accounts, each voting once, from 100 different IPs, in the same 10 seconds) |
| **`alert_type` written** | `duplicate_attempts_blocked` | `bot_pattern_detected` |

### What This Issue Does NOT Own
- Per-request rate limit enforcement or its `rate_limit_exceeded` anomaly writes — see I-007
- The 3-strike duplicate counter and block — see I-015
- Delivering the actual page/Slack/webhook notification — see I-019 (this issue only calls the hook)
- Reporting anomalies to Adidos — see I-016
- Reading/displaying anomalies for admins — see I-013

---

## Acceptance Criteria

- [ ] `botcheck:*` counters are updated inline during vote acceptance without adding measurable latency to `POST /v1/vote` (single `ZADD`/`SADD`/`INCR`, no blocking reads)
- [ ] 100+ vote attempts from one IP within a rolling 10-second window produces exactly one `warning` anomaly (not one per attempt)
- [ ] The single-IP burst condition persisting for ~30s escalates to a `critical` anomaly
- [ ] 100,000+ accepted vote attempts service-wide within a 1-second bucket produces a `critical` anomaly and triggers `notify_admins()`
- [ ] 20+ distinct `user_id`s voting from the same IP within 5 minutes produces a `warning` anomaly, even if no individual user or the IP itself ever crosses I-007's quota
- [ ] Cooldown keys prevent the same rule/target from re-alerting before its cooldown expires
- [ ] Every written anomaly has `alert_type = 'bot_pattern_detected'` and satisfies I-001's `CHECK (user_id IS NOT NULL OR ip_address IS NOT NULL)`
- [ ] The evaluator runs in a separate process/pod from the API servers and vote processor workers
- [ ] Evaluator loop survives a single Redis node failure (degrades to skipping that tick, retries next interval — no crash loop)

---

## Testing Strategy

- **Unit Tests**: Each rule's threshold/cooldown logic in isolation (mock Redis, assert exact `ZADD`/`SADD`/`INCR` and comparison behavior); severity escalation logic (warning → critical after sustained condition)
- **Integration Tests**: Simulate 100 vote attempts from one IP against a test Redis + evaluator tick → one `warning` row in `anomalies`; simulate the same condition for 3 ticks → escalates to `critical`; simulate 20 distinct users from one IP → `warning` row with no rate-limit or duplicate-block trigger involved
- **Notification Contract Test**: Assert `notify_admins()` is called exactly once per `critical` anomaly, and never for `warning`
- **Load Tests**: Confirm inline counter writes (`ZADD`/`SADD`/`INCR`) add no observable P99 regression to `POST /v1/vote` under the 1000 votes/sec load test from I-005/I-023
- **Prior Art**: Mirror I-002's Redis-key documentation discipline and I-005's error-mapping test structure

---

## Out of Scope

- Geo-based detection (story #18) — no geo signal is present on the current request/token shape; revisit if Adidos adds it
- Machine-learning or statistical anomaly scoring — this issue uses fixed, documented thresholds only, per the spec's "Not Tested Here: internal anomaly scoring algorithm" note in the Bot Detection Tests module
- The actual notification delivery channel (Slack/PagerDuty/email) — see I-019
- IP blacklisting or account bans — Adidos owns long-term action per spec decision #13; see I-016 for how alerts reach them
- Reconfiguring thresholds at runtime (admin-tunable rules) — thresholds are fixed constants for this issue; a future admin-config endpoint could change that

---

## Related Issues

- I-001: Database Schema (`anomalies` table this issue writes to)
- I-002: Redis Cluster Setup (this issue's `botcheck:*` keys extend the documented key namespace)
- I-005: Vote Acceptance & Queueing (inline counter updates happen in this handler's path)
- I-007: Rate Limiting (the per-request layer this issue's pattern detection sits above; owns `rate_limit_exceeded` writes)
- I-013: Admin Poll Management Endpoints (reads the `bot_pattern_detected` rows this issue writes)
- I-015: Duplicate Detection (narrower, deterministic sibling — see comparison table above)
- I-016: Anomaly Reporting to Adidos (reports the rows this issue writes, on a 30-minute batch cadence)
- I-019: Alerting & Dashboards (owns the delivery mechanism behind `notify_admins()`)

---

## Implementation Checklist

- [ ] Create `src/services/bot_detection.py` (counter update helpers called from the vote acceptance path)
- [ ] Create `src/jobs/bot_detection.py` (periodic evaluator loop, rule implementations)
- [ ] Implement `notify_admins()` interface in `src/services/notifications.py` (stub delivery, real integration deferred to I-019)
- [ ] Wire counter updates into I-005's `POST /v1/vote` handler (coordinate with I-005 owner)
- [ ] Add `botcheck:*` key documentation to `docs/architecture/redis-keys.md`
- [ ] Add Kubernetes deployment manifest for the evaluator process (separate pod, not colocated with API or vote processor)
- [ ] Write unit tests for all 4 rules and their cooldowns
- [ ] Write integration tests simulating each rule's trigger condition end-to-end
- [ ] Load test to confirm inline counter writes don't regress vote acceptance latency

---

**Acceptance**: All acceptance criteria met, integration and load tests pass, PR reviewed and merged.
