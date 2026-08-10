# Poll App Microservice – Technical Specification

**Version**: 1.0  
**Status**: Ready for Implementation  
**Date**: 2026-08-10  
**Architecture Review**: Grilled through 49 questions (10 rounds)

---

## Problem Statement

Adidos requires a **real-time polling microservice** to collect user opinions on binary questions. This service must:
- Absorb **1M+ concurrent votes** with intense traffic bursts (e.g., influencer-promoted polls)
- Prevent vote manipulation by enforcing "one vote per user per question" at the database level
- Detect and throttle bot attacks attempting to inflate vote counts or exhaust API quota
- Serve results instantly without hammering the database
- Minimize infrastructure cost by optimizing database transactions and API calls

The service will be a **stateless microservice** tightly integrated with Adidos' authentication and user management, but loosely coupled to the main platform.

---

## Solution

A **write-heavy, cache-first voting engine** built on:

1. **Async Queue + Batch Processing**: Votes accepted immediately (202 response), queued, then batched and written to sharded PostgreSQL
2. **Redis Cache Layer**: Real-time results computed from vote cache, not from database queries
3. **User-ID Sharding**: Votes distributed by user_id hash across 8+ PostgreSQL nodes (even distribution, no hot shards even for viral polls)
4. **Redis Rate Limiting**: Per-user (5/min) and per-IP (50/min) sliding windows block bots before they reach the database
5. **Dual Enforcement**: Redis SET NX + PostgreSQL unique constraint prevent duplicate votes across distributed writes
6. **Smart Bot Detection**: 3-strike duplicate detection (1-hour block), auto-reported to Adidos every 30 minutes
7. **Graceful Degradation**: If Redis crashes, fall back to computing results from materialized views (slower but accurate)

**Key Trade-offs:**
- **Availability over Consistency**: Stale cached results > errors (good for user experience, simpler ops)
- **Loose Coupling**: Trust Adidos tokens as-is, no real-time token validation
- **Cost Optimization**: Batch writes (every 1-2 sec) to minimize DB transactions

---

## User Stories

### Voter Experience
1. As a user, I want to see a list of active polls, so that I can discover opinions to vote on
2. As a user, I want to vote on a poll by selecting one of two answers, so that I can express my opinion
3. As a user, I want to receive immediate feedback after voting, showing poll results, so that I can see how my answer compares to others
4. As a user, I want to see my previous votes on all polls I've participated in, so that I can track my voting history
5. As a user, I want to see how many votes are cast for each answer and the percentage breakdown, so that I can understand the overall sentiment
6. As a user, I want to be blocked from voting twice on the same poll, so that my opinion counts only once (enforced without confusion)
7. As a user, I want to receive a clear error if I try to vote on a closed poll, so that I understand the poll is no longer accepting votes
8. As a user, I want to see archived poll results, so that I can review past opinions and trends
9. As a user, I want my vote to be processed in less than 200ms (P99), so that the experience feels instant
10. As a user, I want my vote to succeed even during traffic spikes, so that I don't miss the opportunity to vote during a viral moment

### Admin Experience
11. As an admin, I want to create a poll by specifying a question and two answer options, so that I can launch a new voting initiative
12. As an admin, I want to activate a draft poll, so that users can start voting
13. As an admin, I want to close an active poll, so that I can stop accepting new votes at a specific time
14. As an admin, I want to archive closed polls, so that I can manage the poll lifecycle and keep historical records
15. As an admin, I want to see real-time vote counts and percentages for active polls, so that I can monitor engagement
16. As an admin, I want to view bot detection alerts showing suspicious voting patterns, so that I can identify and respond to attacks
17. As an admin, I want to see rate limit violations (IPs and users exceeding quotas), so that I can understand attack vectors
18. As an admin, I want to see the top countries or regions voting on a poll (if Adidos provides geo data), so that I can understand demographic insights
19. As an admin, I want to export poll data (question, answers, vote counts, timestamp), so that I can analyze trends in external tools
20. As an admin, I want to be notified of critical anomalies (e.g., 100K votes in 1 second), so that I can respond to emergencies

### Bot Defense
21. As the system, I want to reject votes from users exceeding 5 votes per minute, so that I throttle individual bot accounts
22. As the system, I want to reject votes from IPs exceeding 50 votes per minute, so that I throttle botnet swarms
23. As the system, I want to detect duplicate vote attempts (same user, same poll, repeated) and block after 3 attempts for 1 hour, so that I deter systematic attacks
24. As the system, I want to report suspicious voting patterns to Adidos every 30 minutes, so that Adidos can take longer-term action (IP blacklist, user ban)
25. As the system, I want to verify that a vote is only cast once per user per question, enforced at the database level, so that I guarantee data integrity even if the async queue fails

### Infrastructure & Reliability
26. As an operator, I want the service to auto-scale API replicas when queue depth exceeds 1000 votes, so that bursts are absorbed without manual intervention
27. As an operator, I want the service to auto-scale vote processors when queue depth exceeds 1000 votes, so that votes are written to the database quickly
28. As an operator, I want Redis to be clustered (3+ nodes) so that it survives single node failures
29. As an operator, I want votes queued in Redis to survive brief service restarts, so that no votes are lost
30. As an operator, I want hourly reconciliation comparing Redis vote counts vs. database counts, so that I catch drift and alert if it exceeds 1%
31. As an operator, I want to monitor P95 and P99 vote latency (time from vote request to Redis update), so that I detect when bursts degrade performance
32. As an operator, I want to monitor queue depth, error rate, and Redis hit rate, so that I have end-to-end visibility
33. As an operator, I want database backups with 5-minute RPO (recovery point objective), so that data loss is bounded
34. As an operator, I want failed votes (e.g., shard down) to remain in the queue and retry later, so that no votes are abandoned
35. As an operator, I want a big-bang launch with close monitoring (rather than canary), so that I maximize exposure to real traffic and catch edge cases early

### Data Privacy & Retention
36. As a privacy officer, I want individual vote records (user_id + poll_id + answer) anonymized after 90 days, so that we comply with data retention policies
37. As a privacy officer, I want poll questions and results archived forever, so that historical analysis is possible without exposing user identities
38. As a privacy officer, I want admins to see only vote counts and anomaly alerts, never raw vote logs, so that user privacy is protected by default

---

## Implementation Decisions

### 1. **Authentication & Authorization**
- **Decision**: Trust Adidos tokens as-is; cache locally with 5-10 minute TTL
- **Rationale**: Loose coupling, avoids real-time token validation calls, reduces dependencies
- **Implementation**: Token is extracted from request headers, cached in Redis with user_id as key. If cache miss, assume token is valid (fail open). If Adidos revokes a token, cache TTL (5 min) before service notices.

### 2. **Poll Ownership & Isolation**
- **Decision**: Poll microservice owns all Poll, Answer, and Vote entities. Adidos issues admin credentials but does not manage poll state directly.
- **Rationale**: Single source of truth, clear boundaries, simplifies schema and API design
- **Implementation**: Admin creates polls via POST /v1/admin/polls (authenticated); polls transition through states in this service only.

### 3. **Vote Acceptance Strategy**
- **Decision**: Accept votes asynchronously; return 202 Accepted immediately, queue for processing
- **Rationale**: Decouples database writes from API response latency; absorbs bursts without overloading the DB
- **Implementation**: Vote request → Redis queue (atomic check + enqueue) → return 202 → background worker dequeues → write to sharded DB

### 4. **Uniqueness Enforcement (Dual-Layer)**
- **Layer 1**: Redis SET NX (check if "user:{user_id}:poll:{poll_id}" exists; if not, set it atomically)
- **Layer 2**: PostgreSQL unique constraint on (user_id, poll_id) per shard
- **Rationale**: Redis layer catches duplicates fast (before queue); DB layer guarantees correctness even if async fails
- **Decision**: If Redis SET NX fails, return 409 Conflict immediately (strict). If DB constraint violated, vote processor logs and skips (no retry).

### 5. **Rate Limiting**
- **Decision**: Sliding 1-minute window, per-user 5/min, per-IP 50/min; stored in Redis with TTL 61 seconds
- **After 3 failed duplicates on same poll**: Block user+IP combo for 1 hour (key: "blocked:{user_id}:{ip}" with TTL 3600)
- **Implementation**: Checked before vote enters queue. Returns 429 Too Many Requests if exceeded.

### 6. **Result Aggregation**
- **Decision**: Real-time in Redis (increment answer_id counter per vote), materialized views (computed every 5 minutes in PostgreSQL)
- **Rationale**: Redis is fast (P95 < 50ms), materialized views serve as fallback if Redis crashes; admins rarely query DB directly
- **Implementation**: Worker updates Redis cache immediately upon writing vote to DB. Separate cron job (5-min interval) computes materialized view counts.

### 7. **Database Sharding**
- **Decision**: Shard by user_id hash across 8+ PostgreSQL nodes. Poll and Answer tables replicated to all shards (read-only).
- **Rationale**: Distributes writes evenly; a viral poll doesn't bottleneck a single shard. Poll reads distributed to read replicas.
- **Shard Key**: `user_id % num_shards` determines which PostgreSQL instance holds a user's votes
- **Failover**: If a shard is down, votes for that shard's users are queued; worker retries when shard is back online.

### 8. **Async Queue Technology**
- **Decision**: Redis list (LPUSH/RPOP) as queue; workers poll with configurable batch size (100-500 votes per batch)
- **Rationale**: Simple, fast, survives restarts (Redis persistence), integrates with existing cache layer
- **Alternative Rejected**: Kafka (overkill for current scale), RabbitMQ (extra infrastructure)

### 9. **Scaling Strategy**
- **API Servers**: Start 2 replicas, auto-scale to 5 based on queue depth (> 1000 queued votes)
- **Vote Processors**: 1 worker pod per shard; auto-scale total pods based on queue depth (1 pod per 500 queued votes)
- **Database**: Read replicas for poll/answer reads; writes go to primary (user_id shard). Plan for horizontal sharding if single primary becomes bottleneck (> 10K writes/sec sustained).
- **Redis**: Cluster mode (3+ nodes) for HA; sharded by key prefix (e.g., "rate_limit:*", "cache:*")

### 10. **Error Codes & Retries**
- **400 Bad Request**: Missing/invalid fields (non-retryable)
- **409 Conflict**: User already voted on this poll (non-retryable)
- **429 Too Many Requests**: Rate limit exceeded (retryable after delay)
- **503 Service Unavailable**: Queue full or shard unreachable (retryable)
- **Client Retry Logic**: Exponential backoff for 429/503; no retry for 400/409

### 11. **Idempotency & Retry Semantics**
- **Decision**: Strict non-idempotent: POST /v1/vote is NOT idempotent. Retrying a failed vote will be rejected with 409 if it already succeeded.
- **Rationale**: Audit trail for bot detection; allows us to detect duplicate attempts
- **Implementation**: Client must handle 409 gracefully (show "already voted" instead of error)

### 12. **API Versioning**
- **Decision**: Path-based versioning (/v1/, /v2/, etc.)
- **Rationale**: Explicit, easy for clients, survives for years
- **Implementation**: All routes prefixed with /v1/; schema changes go to /v2/ (backward-compatible transitions)

### 13. **Bot Detection & Reporting**
- **Decision**: Detect locally (rate limits, duplicates). Auto-block 1 hour after 3 duplicates. Report all anomalies to Adidos every 30 minutes (batch endpoint).
- **Rationale**: Fast local defense; Adidos decides long-term action (IP blacklist, account ban)
- **Implementation**: Anomaly table stores alerts; cron job POSTs summaries to Adidos every 30 min.

### 14. **Data Retention & Anonymization**
- **Decision**: Keep polls forever. Anonymize individual votes after 90 days (delete user_id, keep poll_id + answer_id + timestamp for aggregate analysis).
- **Rationale**: GDPR-friendly; supports long-term trend analysis without exposing user identities
- **Implementation**: Daily cron job; soft-delete user_id (set to null), keep record for audit trail

### 15. **Monitoring & Observability**
- **Metrics**:
  - P95/P99 vote latency (target: < 50ms / 200ms)
  - Queue depth (alert if > 10,000)
  - Error rate (alert if > 1%)
  - Redis hit rate (alert if < 95%)
  - DB constraint violation count (alert if > 0 anomalies/min)
  - Reconciliation drift (alert if > 1% mismatch)
- **Logging**: JSON format, 1-in-1000 sampling for successful votes, 100% for errors/rejections
- **Alerting**: Different severity (warning vs. critical) based on signal

### 16. **Cache Invalidation & Consistency**
- **Decision**: No explicit invalidation. Redis results age naturally; materialized views refresh every 5 minutes.
- **Eventual Consistency Window**: Up to 5 minutes for results to fully propagate (cache + DB + admin views)
- **Fallback**: If Redis down, compute results from materialized view (potentially 5 min stale, but available)

### 17. **Token Refresh Lifecycle**
- **Decision**: Adidos owns token refresh logic. Poll service does not manage token lifecycle.
- **Rationale**: Loose coupling; Adidos is the source of truth for user sessions
- **If Token Revoked**: Cache TTL (5 min) before service notices; user votes within that window may be accepted then rejected by Adidos

### 18. **Launch Strategy**
- **Decision**: Big bang (not canary). Deploy to production with close monitoring.
- **Rationale**: Canary testing can't catch hot-poll scenarios; better to surface real traffic conditions immediately
- **Monitoring**: Alert on queue depth, latency, error rate, constraint violations; on-call team ready to scale or rollback

---

## Testing Decisions

### Philosophy: External Behavior, Not Implementation Details
- Tests should verify what users and admins **observe** (HTTP responses, result counts, rate limit blocks), not internal mechanics (Redis keys, queue structure)
- Tests should be resilient to refactoring (e.g., if Redis is replaced with another cache, tests still pass)

### Test Modules

#### 1. **Vote Acceptance Tests**
- **Module**: Vote handling, uniqueness enforcement
- **Good Tests**:
  - User votes on active poll → returns 202, Redis shows vote counted
  - User votes twice on same poll → second request returns 409
  - User votes on closed poll → returns 400 Bad Request
  - User votes with invalid token → returns 401 Unauthorized
  - User votes with missing answer_id → returns 400 Bad Request
- **Not Tested Here**: Redis SET NX implementation details, queue structure

#### 2. **Rate Limit Tests**
- **Module**: Rate limit enforcement (per-user, per-IP)
- **Good Tests**:
  - User votes 5 times within 1 minute → 5th succeeds
  - User votes 6 times within 1 minute → 6th returns 429
  - After 60 seconds, rate limit window resets → next vote succeeds
  - Two different IPs each vote 25 times → both succeed (each under 50/IP limit)
  - Two different IPs each vote 26 times → 26th from each returns 429
  - User blocked after 3 duplicate attempts → returns 429 for 1 hour
- **Not Tested Here**: Sliding window algorithm internals

#### 3. **Result Aggregation Tests**
- **Module**: Retrieving and computing poll results
- **Good Tests**:
  - After 10 votes (6 answer A, 4 answer B) → GET /v1/polls returns 60% and 40%
  - Results include total_votes and per-answer vote counts
  - Closed poll results still queryable
  - Results available within 5 seconds of vote submission
  - If Redis down, results computed from materialized view (slower, but accurate)
- **Not Tested Here**: Redis key structure, cache expiration timing

#### 4. **Bot Detection Tests**
- **Module**: Anomaly detection and reporting
- **Good Tests**:
  - 100 votes from same IP within 10 seconds → anomaly alert generated
  - Anomaly alert includes user_id, IP, alert_type, severity
  - User blocked for 1 hour after 3 duplicate attempts
  - Alerts sent to Adidos every 30 minutes (batch endpoint callable)
- **Not Tested Here**: Internal anomaly scoring algorithm

#### 5. **Shard Distribution Tests**
- **Module**: Votes distributed evenly across shards
- **Good Tests**:
  - 1000 votes from random users → distributed within ±10% across shards (no single shard > 55% of votes)
  - Viral poll (100K votes from different users) → results consistent across shards
  - Shard failover: votes queued while shard down, processed after recovery
- **Not Tested Here**: User_id hashing algorithm

#### 6. **Admin Poll Management Tests**
- **Module**: Create, activate, close, archive polls
- **Good Tests**:
  - Admin creates poll with question and 2 answers → poll in draft state
  - Admin activates poll → users can vote, poll is active
  - Admin closes poll → no new votes accepted, returns 400
  - Admin archives poll → poll no longer in active list, but visible in history
  - Non-admin user cannot create polls → returns 403
  - Polls cannot transition backward (e.g., closed → active) → returns 400
- **Not Tested Here**: Admin authentication flow (Adidos manages)

#### 7. **Integration Tests**
- **Module**: End-to-end workflows
- **Good Tests**:
  - User A votes on poll, sees results; User B votes, results update for both
  - Poll gets 1M votes across 10 seconds → P99 latency < 200ms, no data loss
  - Redis cluster loses one node → service stays up, results eventually consistent
  - Reconciliation job detects and alerts on drift > 1%
  - Closed polls visible in user voting history, but can't be voted on
- **Not Tested Here**: Specific infrastructure failures (disk full, network partition)

#### 8. **Load/Stress Tests**
- **Module**: High-volume voting, burst scenarios
- **Good Tests**:
  - 1000 votes/sec for 60 seconds → all succeed, no 503 errors, P99 < 200ms
  - Sudden spike: 0 → 10K votes/sec in 1 second → queue auto-scales, latency stays < 500ms
  - Viral poll: 50K votes/sec on single poll → results accurate, no data loss
  - Sustained 100K votes/sec (5x peak) → API scales to 5 replicas, queue stable
- **Load Testing Tool**: JMeter or Locust scripts; run pre-launch
- **Pass Criteria**: No 503 errors during normal peak, P99 < 200ms sustained

### Test Execution Strategy
- **Unit Tests**: Run on every commit (< 1 min)
- **Integration Tests**: Run on every commit (< 5 min)
- **Load Tests**: Run weekly and before launch (30+ min)
- **Prior Art**: Existing Adidos API tests; mirror structure for consistency

---

## Out of Scope

1. **User Profile Management**: Adidos owns user accounts, authentication, tokens. Poll app does not create or modify users.

2. **Poll Rich Media**: Questions and answers are text-only (no images, links, embeds). Adidos can add these in a future version.

3. **Multi-Language Support**: Polls are not translated. Adidos handles internationalization at the presentation layer.

4. **Custom Answer Types**: Always exactly 2 answers per poll. No open-ended questions, ranked choices, or multiple-select.

5. **Anonymous Voting**: All votes are tied to a Adidos user_id. Anonymous polls are out of scope.

6. **Push Notifications**: No alerts when poll results shift. Adidos handles notifications separately.

7. **Social Features**: No sharing, commenting, or sub-discussions on polls. Poll app is read-only for results.

8. **Analytics Dashboard**: No custom dashboards or trend reports. Admins query results via API; Adidos provides visualization.

9. **A/B Testing**: No testing different question phrasings or answer orders. Poll questions are fixed.

10. **Scheduled Polls**: Polls cannot be scheduled to activate at a future time. Admins activate manually.

11. **User Segmentation**: Polls are not targeted to specific user cohorts. All authenticated users see all active polls.

12. **Deletion / Hard Deletes**: Votes and polls are never hard-deleted (only archived/anonymized). Immutability is a principle.

---

## Further Notes

### Deployment Checklist (Pre-Launch)
1. ✅ Provision 8+ PostgreSQL shards (user_id sharding), configure replication
2. ✅ Provision Redis Cluster (3+ nodes), enable persistence
3. ✅ Deploy 2 initial API server replicas, configure autoscaling (queue depth > 1000)
4. ✅ Deploy vote processor pods (1 per shard), configure autoscaling
5. ✅ Set up monitoring dashboards (Prometheus/Grafana): P95/P99 latency, queue depth, error rate, Redis hit rate
6. ✅ Configure alerting rules: queue > 10K, error rate > 1%, Redis hit < 95%, drift > 1%
7. ✅ Set up logging aggregation (ELK/CloudWatch): JSON format, 1-in-1000 sampling for votes, 100% for errors
8. ✅ Run load tests (5x peak: 100K votes/sec, sustain for 10 min) and validate results
9. ✅ Validate reconciliation job: run hourly, alert on drift
10. ✅ Brief ops team: incident runbook (queue backlog → scale workers, shard down → queue + retry, Redis down → fallback to DB)

### Known Risks & Mitigations
- **Risk**: Single PostgreSQL primary becomes bottleneck if write throughput > 10K/sec sustained
  - **Mitigation**: Monitor DB write latency; plan for read-write splitting or additional sharding if needed
  
- **Risk**: Redis cluster node failure causes cache miss storm
  - **Mitigation**: Cluster mode (gossip protocol) handles failover automatically; materialized views provide fallback
  
- **Risk**: Bot attack exhausts all rate limit quota (50/IP/min × thousands of IPs)
  - **Mitigation**: Report to Adidos; they apply upstream IP blacklist. Local rate limits are not a silver bullet.
  
- **Risk**: Async queue builds up if processors can't keep pace
  - **Mitigation**: Auto-scale processors; monitor queue depth; alert on backlog

### Future Enhancements (Post-MVP)
- Multi-language support (i18n at presentation layer)
- Rich media (images, links in questions/answers)
- Anonymous voting mode
- Scheduled poll activation
- Advanced analytics (demographic breakdowns, trend reports)
- Webhook events to Adidos (real-time vote notifications)
- A/B testing (randomize answer order, question phrasing)

---

## Appendix: Schema Preview

### Polls Table (Replicated to All Shards)
```
polls
  - poll_id: UUID PK
  - question: VARCHAR(255)
  - state: ENUM(draft, active, closed, archived)
  - created_at, activated_at, closed_at, archived_at: TIMESTAMP
```

### Answers Table (Replicated to All Shards)
```
answers
  - answer_id: UUID PK
  - poll_id: UUID FK
  - answer_text: VARCHAR(255)
  - order: INT (0 or 1)
  - UNIQUE(poll_id, order)
```

### Votes Table (Sharded by User_ID)
```
votes
  - vote_id: UUID PK
  - user_id: VARCHAR(255)
  - poll_id: UUID FK
  - answer_id: UUID FK
  - created_at, updated_at: TIMESTAMP
  - UNIQUE(user_id, poll_id) -- enforced per shard
```

### Materialized Vote Counts (Replicated to All Shards)
```
vote_counts
  - poll_id: UUID FK
  - answer_id: UUID FK
  - count: BIGINT
  - percentage: DECIMAL(5,2)
  - last_updated_at: TIMESTAMP
  - PK(poll_id, answer_id)
```

---

**Specification locked and ready for implementation.**
