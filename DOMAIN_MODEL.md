# Poll App Domain Model

**Version**: 1.0  
**Status**: Locked (grilled 49 questions, 10 rounds)  
**Last Updated**: 2026-08-10

---

## Context

The Poll App is a **microservice** operated by Adidos for collecting user opinions on simple binary questions. It serves as a real-time voting engine designed to absorb **1M+ votes** with intense bursts (e.g., influencer-promoted polls) and defend against bot manipulation.

**Core Constraints:**
- One vote per user per question (enforced at DB + cache)
- Two answers only (binary choice)
- Bot attacks must be detected and throttled
- Results must be queryable in real-time without hammering the database
- Cost-optimized for database transactions and API calls

---

## Bounded Contexts

### 1. **Poll Management Context**
**Responsibility**: Lifecycle of polls (creation, activation, closure, archival).  
**Owner**: Admin interface (via Adidos platform).  
**Entities**: Poll, Answer.

### 2. **Voting Context**
**Responsibility**: Accepting votes, enforcing uniqueness, queuing for processing.  
**Owner**: Core microservice.  
**Entities**: Vote, UserVoteState.

### 3. **Result Aggregation Context**
**Responsibility**: Computing and caching poll results.  
**Owner**: Core microservice (Redis + async workers).  
**Entities**: VoteCount (materialized).

### 4. **Bot Detection Context**
**Responsibility**: Rate limiting, anomaly detection, alerting.  
**Owner**: Core microservice (Redis + periodic jobs).  
**Entities**: RateLimitEntry, AnomalyAlert.

---

## Core Entities

### **Poll**
**Definition**: A question with exactly two answer options. Polls transition through states and can be queried by users and admins.

**Attributes:**
- `poll_id` (UUID) – Primary identifier
- `question` (string, max 255 chars) – The question text
- `state` (enum: draft, active, closed, archived) – Current lifecycle state
- `created_at` (timestamp) – When poll was created (admin only)
- `activated_at` (timestamp, nullable) – When poll transitioned to active
- `closed_at` (timestamp, nullable) – When poll transitioned to closed
- `archived_at` (timestamp, nullable) – When poll transitioned to archived

**Business Rules:**
- Admins only can create and transition polls
- Polls flow one-way: draft → active → closed → archived
- Once active, a poll's question is immutable
- Only active polls accept new votes
- Closed/archived polls are visible to all authenticated users (for history)

**Invariants:**
- `state` transitions are idempotent (closing an already-closed poll is safe)
- `activated_at`, `closed_at`, `archived_at` are set only on transition

---

### **Answer**
**Definition**: One of two options for a poll. Answers are immutable after poll activation.

**Attributes:**
- `answer_id` (UUID) – Primary identifier
- `poll_id` (UUID) – Foreign key to Poll
- `answer_text` (string, max 255 chars) – The answer option
- `order` (int, 0 or 1) – Display order (top to bottom)

**Business Rules:**
- Each poll has exactly 2 answers
- Answers are created with the poll (in draft state)
- Once poll is active, answers cannot be edited or deleted
- Order is immutable and enforced: one answer is 0, the other is 1

**Invariants:**
- No two answers in the same poll share the same order value
- `answer_text` is non-empty

---

### **Vote**
**Definition**: A user's choice on a poll. Votes are immutable once recorded and determine both individual user state and aggregate results.

**Attributes:**
- `vote_id` (UUID) – Primary identifier
- `user_id` (string) – From Adidos token (not UUID; opaque identifier)
- `poll_id` (UUID) – Foreign key to Poll
- `answer_id` (UUID) – Foreign key to Answer
- `created_at` (timestamp) – When vote was recorded
- `updated_at` (timestamp) – When vote was processed from queue

**Business Rules:**
- A user can vote on a poll only once (unique constraint: `(user_id, poll_id)`)
- Votes can only be cast on active polls
- Votes are **immutable** (no editing, no deletion)
- Votes determine the source of truth for results

**Invariants:**
- `answer_id` must belong to `poll_id`
- A duplicate vote (same user, same poll) is rejected with 409 Conflict
- Votes are idempotent in the queue: retrying the same vote produces the same result

**Persistence Strategy:**
- Sharded by `user_id` across 8+ PostgreSQL nodes
- Written asynchronously to queue, processed by workers
- Uniqueness enforced in Redis (SET NX) before DB write
- Read replicas used for historical queries (closed/archived polls)

---

### **UserVoteState**
**Definition**: A denormalized view of which polls a user has voted on and what they chose.

**Attributes:**
- `user_id` (string) – From Adidos token
- `poll_id` (UUID) – Foreign key to Poll
- `answer_id` (UUID) – Their chosen answer (or null if they haven't voted yet)
- `voted_at` (timestamp, nullable) – When they voted

**Business Rules:**
- This entity is derived from Votes; it's a convenience view
- Queried when users want to see "which polls have I voted on?"
- Updated whenever a vote is recorded

**Invariants:**
- Only one row per (user_id, poll_id) pair
- If `voted_at` is null, the user hasn't voted on that poll

---

### **VoteCount (Materialized)**
**Definition**: Aggregated vote tallies per answer, computed and stored offline.

**Attributes:**
- `poll_id` (UUID) – Foreign key to Poll
- `answer_id` (UUID) – Foreign key to Answer
- `count` (integer) – Number of votes for this answer
- `percentage` (decimal) – Percentage of total votes for this poll
- `last_updated_at` (timestamp) – When this count was last recomputed

**Business Rules:**
- Computed every 5 minutes from the votes table
- Used as a fallback if Redis is down
- Serves admins and analytics; not sent directly to clients (clients see Redis results)

**Invariants:**
- `count >= 0`
- For any poll, the sum of counts across its two answers = total votes on that poll
- Percentages sum to 100% (or ≤100% if rounding)

---

### **RateLimitEntry**
**Definition**: Tracks vote attempts per user and per IP to detect bot abuse.

**Attributes:**
- `key` (string) – Either `user:{user_id}` or `ip:{ip_address}`
- `window_start` (timestamp) – Start of the current 1-minute window
- `attempt_count` (integer) – Number of attempts in this window
- `last_attempt_at` (timestamp) – Most recent attempt timestamp

**Business Rules:**
- Sliding window: per-user limit is 5 votes/minute; per-IP is 50 votes/minute
- If a user exceeds per-user limit, return 429 Too Many Requests
- If an IP exceeds per-IP limit, return 429 Too Many Requests
- After 3 failed duplicate attempts on the same poll, block that user+IP combo for 1 hour
- Stored in Redis with TTL = 61 seconds (auto-purges)

**Invariants:**
- `attempt_count` never exceeds window threshold before rejection
- `last_attempt_at` is always ≤ current time

---

### **AnomalyAlert**
**Definition**: Signals suspicious voting patterns detected by the service.

**Attributes:**
- `alert_id` (UUID) – Primary identifier
- `user_id` (string) – Suspicious user, or null if IP-based
- `ip_address` (string, nullable) – Suspicious IP
- `alert_type` (enum: rate_limit_exceeded, duplicate_attempts_blocked, bot_pattern_detected) – Type of anomaly
- `poll_id` (UUID, nullable) – Poll involved (if applicable)
- `description` (string) – Human-readable summary
- `severity` (enum: warning, critical) – Alert level
- `created_at` (timestamp) – When alert was generated
- `acknowledged_at` (timestamp, nullable) – When ops acknowledged
- `action_taken` (string, nullable) – What was done (e.g., "user blocked 1h", "IP blacklisted")

**Business Rules:**
- Generated whenever a bot attack pattern is detected
- Rate limit exceeded → severity = warning; 3 duplicates → severity = critical
- Auto-reported to Adidos every 30 minutes (batch)
- Auto-action: 1-hour block on user_id + IP combo after 3 duplicates
- Ops team acknowledges and escalates if needed

**Invariants:**
- At least one of `user_id` or `ip_address` must be set
- `acknowledged_at` is only set if alert status is "acknowledged"
- `action_taken` is populated if action was auto-executed

---

## Value Objects

### **PollState**
An enum representing the lifecycle of a poll.

```
draft → active → closed → archived
```

**Transitions:**
- Only admins can initiate transitions
- Transitions are one-way (no reverting)

---

### **VoteRateLimitQuota**
Encapsulates per-user and per-IP rate limits as a single concept.

```
{
  per_user_per_minute: 5,
  per_ip_per_minute: 50,
  duplicate_block_threshold: 3,
  duplicate_block_duration_seconds: 3600
}
```

---

### **VoteResult**
Data structure returned to clients showing aggregated results.

```
{
  poll_id: UUID,
  question: string,
  answers: [
    {
      answer_id: UUID,
      text: string,
      vote_count: integer,
      percentage: decimal,
      user_answered: boolean (did this user vote for this?)
    }
  ],
  total_votes: integer,
  user_answer_id: UUID or null (what did this user vote?)
}
```

---

## Aggregates

### **PollAggregate**
**Root**: Poll entity  
**Members**: Poll, Answer (list)

**Invariants:**
- Exactly 2 Answer entities per Poll
- Poll state transitions are atomic
- Answers are immutable after poll activation

**Operations:**
- `createPoll(question, answers)` → validates 2 answers
- `activatePoll()` → transitions draft → active, locks answers
- `closePoll()` → transitions active → closed
- `archivePoll()` → transitions closed → archived

---

### **VoteAggregate**
**Root**: Vote entity  
**Members**: Vote (single, immutable)

**Invariants:**
- One Vote per (user_id, poll_id)
- Vote is immutable once persisted
- Vote references a valid Answer (which belongs to the voted-on Poll)

**Operations:**
- `castVote(user_id, poll_id, answer_id)` → validates uniqueness, queues for processing
- `processVoteFromQueue()` → writes to DB, updates Redis cache

---

### **UserVotingHistoryAggregate**
**Root**: UserVoteState (conceptual, derived)  
**Members**: UserVoteState (list of polls user voted on)

**Invariants:**
- One UserVoteState per (user_id, poll_id) pair
- Derived from Votes table (not a separate write)

**Operations:**
- `getUserVotingHistory(user_id)` → returns all polls user voted on + their answers

---

## Domain Events

These events signal state changes that other systems (Adidos, analytics) may subscribe to.

### **PollCreated**
```
{
  poll_id: UUID,
  question: string,
  created_by: admin_id,
  timestamp: datetime
}
```

### **PollActivated**
```
{
  poll_id: UUID,
  activated_at: datetime
}
```

### **VoteCast**
```
{
  vote_id: UUID,
  user_id: string,
  poll_id: UUID,
  answer_id: UUID,
  timestamp: datetime
}
```

### **BotSuspected**
```
{
  alert_id: UUID,
  user_id: string (or null),
  ip_address: string (or null),
  reason: string,
  action: string,
  timestamp: datetime
}
```

---

## Constraints & Validation Rules

### **Data Constraints**
- `poll_id`, `answer_id`, `vote_id`: UUID v4
- `user_id`: non-empty string (max 255 chars, opaque to Poll App)
- `question`, `answer_text`: non-empty, max 255 chars each
- `ip_address`: valid IPv4 or IPv6
- Timestamps: ISO 8601 format, UTC

### **Business Constraints**
- Each poll has exactly 2 answers
- Each user votes at most once per poll (enforced by unique constraint + Redis)
- Only active polls accept new votes
- Rate limits: 5/user/min, 50/IP/min
- Duplicate detection: 3 failed attempts → 1 hour block

### **Persistence Constraints**
- Vote writes are sharded by `user_id` (distributed across 8+ DB nodes)
- Votes are immutable (no updates, deletes only during archive phase)
- Vote records have a unique constraint on `(user_id, poll_id)`
- Uniqueness is double-checked: Redis SET NX + DB constraint

---

## Integration Points

### **With Adidos Platform**
- **Input**: User token (consumed, not stored; cached 5-10 min)
- **Output**: Poll results (on-demand queries)
- **Events**: BotSuspected alerts (batch, every 30 min)

### **With Redis Cache**
- **Vote counts**: Real-time aggregation
- **Rate limits**: Sliding window per user + IP
- **Uniqueness checks**: SET NX before DB write

### **With PostgreSQL Shards**
- **Writes**: Queued and batched by user_id shard (8+ nodes)
- **Reads**: Poll/Answer from read replicas; Votes rarely queried directly
- **Archive**: Votes anonymized after 90 days

---

## Bounded Context Interactions

```
┌─────────────────┐
│ Poll Management │ (Admin creates/activates polls)
└────────┬────────┘
         │ creates Poll + Answer entities
         ↓
┌─────────────────┐
│ Voting Context  │ (Users vote, vote queued)
└────────┬────────┘
         │ emits VoteCast event, queues Vote for processing
         ↓
┌─────────────────┐
│ Result Agg.     │ (Workers process queue, update Redis + DB)
└────────┬────────┘
         │ computes VoteCount, caches in Redis
         ↓
┌─────────────────┐
│ Bot Detection   │ (Monitors rate limits, anomalies)
└─────────────────┘ (emits BotSuspected events to Adidos)
```

---

## Implementation Notes

### **Sharding Strategy**
- Votes are sharded by `user_id` hash
- Each shard is a separate PostgreSQL instance
- Ensures even distribution (hot polls don't bottleneck a single shard)
- Results never queried from shards directly; Redis is the source of truth for aggregates

### **Async Processing**
- Votes enter Redis queue immediately (return 202)
- Workers (1 per shard) drain queue, write to DB
- Results updated in Redis cache incrementally
- Reconciliation job (hourly) detects drift > 1%

### **Failure Modes & Recovery**
- **Redis down**: Fall back to computing results from DB (slower, but accurate)
- **DB shard down**: Queue + retry later; users still see Redis results (stale but available)
- **Worker down**: Queue builds up; Kubernetes autoscales to drain it
- **Token expired**: Trusted from Adidos; cache TTL (5 min) before we notice revocation

### **Monitoring**
- P95/P99 latency per vote (target < 50ms / 200ms)
- Queue depth (alert if > 10,000)
- Error rate (alert if > 1%)
- Redis hit rate (alert if < 95%)
- DB reconciliation drift (alert if > 1%)

---

## Glossary (Ubiquitous Language)

| Term | Definition |
|------|-----------|
| **Poll** | A question with exactly 2 answer options |
| **Answer** | One of two mutually exclusive options for a poll |
| **Vote** | A user's choice on a poll (immutable, one per user per poll) |
| **User ID** | Opaque identifier issued by Adidos platform |
| **Token** | Authentication credential from Adidos; used to identify user, not stored |
| **Shard** | A PostgreSQL instance holding votes for a partition of user_id space |
| **Queue** | Redis-backed work queue holding votes awaiting DB write |
| **Rate Limit** | Threshold on votes per user/IP per minute; enforced in Redis |
| **Bot** | Malicious actor attempting to manipulate results or exhaust quota |
| **Anomaly** | Suspicious voting pattern (rate limit exceeded, duplicates, etc.) |
| **Reconciliation** | Periodic job comparing Redis counts vs. DB counts to catch drift |
| **Materialized View** | Pre-computed vote counts (fallback if Redis down) |

---

## Changelog

- **v1.0** (2026-08-10): Grilled architecture locked. Domain model documented.
