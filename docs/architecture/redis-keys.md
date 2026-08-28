# Redis Key Namespace

Implements [I-002](../issues/I-002-redis-cluster.md). This is the canonical,
stable contract every downstream issue (I-004 through I-015) implements
against. **Do not introduce a new prefix without updating this table.**

| Key Pattern | Type | Written By | Read By | TTL | Notes |
|---|---|---|---|---|---|
| `queue:votes` | List | I-005 (vote acceptance, `LPUSH`) | I-008 (vote processor, `RPOP`/`BRPOP`) | None (persisted via AOF) | FIFO queue of pending vote payloads (JSON-encoded) |
| `rate_limit:{user_id}` | String (counter) | I-007 | I-007 | 61s | Sliding-window counter, per-user (5/min quota) |
| `rate_limit_ip:{ip}` | String (counter) | I-007 | I-007 | 61s | Sliding-window counter, per-IP (50/min quota) |
| `vote:user:{user_id}:poll:{poll_id}` | String (`SET NX`) | I-005 / I-006 | I-006 | None (persists until poll archived + anonymized) | Fast-path uniqueness check before DB write |
| `blocked:{user_id}:{ip}` | String (flag) | I-015 | I-005 (pre-vote check) | 3600s | 3-strike duplicate block (1-hour lockout) |
| `dup_attempts:{user_id}:{ip}:{poll_id}` | String (`INCR`) | I-015 | I-015 | 3600s (set on first increment) | Per-`(user_id, ip, poll_id)` duplicate-attempt strike counter; 3rd strike sets `blocked:{user_id}:{ip}` |
| `cache:poll:{poll_id}:answer:{answer_id}` | String (counter, `INCR`) | I-008 (worker, on vote write) | I-009 (aggregation), I-011 (result reads) | None (ages naturally, no explicit invalidation per spec decision #16) | Real-time vote count per answer |
| `cache:poll:{poll_id}:answers` | String (JSON array) | I-009 (lazy populate from `answers` table on miss) | I-009 (aggregation, to build the counter keys above) | 3600s (1h) | `[{"answer_id", "text", "order"}, ...]`; safe to cache aggressively — answers are immutable once a poll is active |
| `auth:token:{token_hash}` | Hash (`user_id`, `role`) | I-004 | I-004 | 300-600s (5-10 min) | Cached Adidos token validation result |
| `botcheck:ip:{ip}` | Sorted Set (`ZADD` timestamp) | I-005 (inline, on every vote acceptance attempt) | I-014 (periodic evaluator) | No key-level TTL; members trimmed via `ZREMRANGEBYSCORE` (10s rolling window) at evaluation time | Timestamps of recent attempts from one IP, independent of I-007's rate-limit counters |
| `botcheck:ip_users:{ip}` | Set (`SADD user_id`) | I-005 (inline) | I-014 (periodic evaluator) | 300s | Distinct `user_id`s seen from one IP in a 5-minute window (low-and-slow detection) |
| `botcheck:global:{bucket}` | String (`INCR`), `bucket` = current unix second | I-005 (inline) | I-014 (periodic evaluator) | 5s | Total vote attempts service-wide in a 1-second bucket |
| `botcheck:ip_burst_streak:{ip}` | String (`INCR` counter) | I-014 (periodic evaluator) | I-014 (periodic evaluator) | 3x evaluator interval | Consecutive evaluator ticks the single-IP-burst condition has held, for warning->critical escalation |
| `botcheck:cooldown:{rule}:{key}` | String (flag) | I-014 | I-014 | rule-specific (see I-014's rule table) | Suppresses re-alerting on the same rule/target while a condition is still active |

## Rationale

- **`SET NX` over a Redis Set for uniqueness**: a per-(user, poll) string
  key makes every check O(1) with no key-growth cleanup, versus a shared
  Set per poll that grows unbounded and requires scanning at anonymization
  time.
- **Hashed tokens in `auth:token:{token_hash}`**: avoids storing raw Adidos
  tokens in Redis; only a hash is cached, consistent with "trust but don't
  persist the credential" (spec decision #1).
- **No key stores raw PII** beyond `user_id` (already opaque per Adidos)
  and `ip_address`, both already permitted by the domain model's data
  constraints.

## Ownership

This table lives here so it has one canonical location; each issue's own
ticket also lists the rows it owns, but this file is authoritative if they
ever drift.
