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
| `cache:poll:{poll_id}:answer:{answer_id}` | String (counter, `INCR`) | I-009 (worker updates on vote write) | I-009, I-011 (result reads) | None (ages naturally, no explicit invalidation per spec decision #16) | Real-time vote count per answer |
| `auth:token:{token_hash}` | Hash (`user_id`, `role`) | I-004 | I-004 | 300-600s (5-10 min) | Cached Adidos token validation result |

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
