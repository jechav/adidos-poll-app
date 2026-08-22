# Poll App Project Status

**Date**: 2026-08-10  
**Phase**: 0 - Ready for Implementation  
**Status**: ✅ Architecture Locked, Specification Complete

---

## Project Summary

**Adidos Poll App** is a microservice for collecting user opinions on binary (yes/no, A/B) questions. Designed to handle **1M+ votes** with intense bursts (influencer-driven traffic), bot defense, and real-time result aggregation.

### Key Numbers
- **Expected Load**: 1M votes over service lifetime
- **Peak Bursts**: 50K-100K votes/sec (influencer-promoted poll)
- **Latency Target**: P95 < 50ms, P99 < 200ms
- **Rate Limits**: 5 votes/user/min, 50 votes/IP/min
- **Bot Protection**: 3-strike duplicate block (1-hour lockout)

---

## Architecture (Grilled Through 49 Questions)

### Stack
- **API**: FastAPI (Python)
- **Queue**: Redis list (LPUSH/RPOP)
- **Cache**: Redis Cluster (3+ nodes)
- **Database**: PostgreSQL sharded by user_id (8+ nodes)
- **Container**: Docker + Kubernetes

### Flow
```
Vote Request
    ↓
Rate Limit Check (Redis)
    ↓
Uniqueness Check (Redis SET NX)
    ↓
Enqueue (Redis list) → Return 202 Accepted
    ↓
Background Worker (per-shard)
    ↓
Write to PostgreSQL
    ↓
Update Redis Cache
    ↓
User Sees Results
```

### Design Principles
- **Async-first**: Accept votes immediately, process in background
- **Cache-first**: Results served from Redis, not database
- **Loose coupling**: Trust Adidos tokens, don't call back
- **Availability over Consistency**: Stale cached results > errors
- **Cost-optimized**: Batch database writes, minimize transactions

---

## Documentation

### Required Reading
1. **[DOMAIN_MODEL.md](./DOMAIN_MODEL.md)** (20 min read)
   - Entities: Poll, Answer, Vote, UserVoteState, VoteCount, RateLimitEntry, AnomalyAlert
   - Bounded contexts: Poll Mgmt, Voting, Result Agg, Bot Detection
   - Aggregates & domain events
   - Ubiquitous language glossary

2. **[SPECIFICATION.md](./SPECIFICATION.md)** (45 min read)
   - 38 user stories
   - 18 implementation decisions
   - Testing strategy & modules
   - Monitoring/alerting setup
   - Schema preview

3. **[README.md](./README.md)** (5 min read)
   - Quick start
   - Architecture overview
   - Project structure
   - Key metrics & SLOs

### Issue Tracking
- **[docs/issues/INDEX.md](./docs/issues/INDEX.md)** – Master index of all 24 issues
- Issues organized by phase:
  - Phase 1: Foundation (weeks 1-2)
  - Phase 2: Core Voting (weeks 3-4)
  - Phase 3: Results & Caching (weeks 5-6)
  - Phase 4: Admin & Bot Defense (weeks 7-8)
  - Phase 5: Observability (weeks 9-10)
  - Phase 6: Testing & Launch (weeks 11-12)

---

## Implementation Roadmap

### Phase 1: Foundation (Weeks 1-2)
**Goal**: Get database and API running

- [ ] **I-001: Database Schema & Migrations** (5 days, READY NOW)
  - Polls, answers, votes tables with sharding
  - Indexes for performance
  - Migration scripts
  
- [ ] **I-002: Redis Cluster Setup** (3 days, PARALLEL with I-001)
  - 3+ nodes, clustering enabled
  - Persistence configured
  
- [ ] **I-003: API Framework & Routing** (3 days, DEPENDS on I-001)
  - FastAPI setup
  - Route handlers
  - Response envelopes
  
- [ ] **I-004: Authentication Middleware** (2 days, DEPENDS on I-003)
  - Token validation
  - Cache (5-10 min TTL)

### Phase 2: Core Voting (Weeks 3-4)
**Goal**: Accept and process votes

- [ ] **I-005: Vote Acceptance & Queueing** (4 days, DEPENDS on I-003, I-004)
- [ ] **I-006: Uniqueness Enforcement** (3 days, DEPENDS on I-002)
- [ ] **I-007: Rate Limiting** (3 days, DEPENDS on I-002)
- [ ] **I-008: Vote Processor Workers** (5 days, DEPENDS on I-001)

### Phase 3: Results & Caching (Weeks 5-6)
**Goal**: Serve results without hitting database

- [ ] **I-009: Result Aggregation (Redis)** (3 days)
- [ ] **I-010: Materialized Views (Fallback)** (2 days)
- [ ] **I-011: GET /v1/polls Endpoint** (2 days)
- [ ] **I-012: GET /v1/user/votes Endpoint** (2 days)

### Phase 4: Admin & Bot Defense (Weeks 7-8)
**Goal**: Manage polls and defend against attacks

- [ ] **I-013: Admin Endpoints** (3 days)
- [ ] **I-014: Bot Detection** (4 days)
- [ ] **I-015: Duplicate Detection** (2 days)
- [ ] **I-016: Anomaly Reporting** (2 days)

### Phase 5: Observability (Weeks 9-10)
**Goal**: Monitor and respond to issues

- [ ] **I-017: Metrics & Monitoring** (5 days)
- [ ] **I-018: Structured Logging** (3 days)
- [ ] **I-019: Alerting & Dashboards** (2 days)
- [ ] **I-020: Incident Runbooks** (3 days)

### Phase 6: Testing & Launch (Weeks 11-12)
**Goal**: Validate and ship

- [ ] **I-021: Unit Tests** (5 days)
- [ ] **I-022: Integration Tests** (5 days)
- [ ] **I-023: Load Tests (100K votes/sec)** (5 days)
- [ ] **I-024: Launch Checklist** (3 days)

---

## Critical Path

```
I-001 (DB Schema, 5d)
  ↓
I-003 (API Framework, 3d)
  ↓
I-004 (Auth, 2d)
  ↓
I-005 + I-006 + I-007 (Vote Acceptance + Uniqueness + Rate Limit, 10d parallel)
  ↓
I-008 (Vote Processor, 5d)
  ↓
I-009 + I-010 (Result Aggregation, 5d parallel)
  ↓
I-021 + I-022 + I-023 (Testing, 15d parallel)
  ↓
I-024 (Launch Checklist, 3d)

Total: ~37 days (8.5 weeks)
```

---

## Success Criteria (Pre-Launch)

### Functionality
- ✅ Users can vote on active polls (202 accepted)
- ✅ Results updated in real-time (from Redis cache)
- ✅ Users blocked from voting twice (409 Conflict)
- ✅ Admins can create/activate/close/archive polls
- ✅ Bot attacks detected and reported

### Performance
- ✅ P95 vote latency < 50ms (target)
- ✅ P99 vote latency < 200ms (target)
- ✅ 1000 votes/sec sustained without 503 errors
- ✅ 100K votes/sec peak (5x normal) accepted, queue auto-scales

### Reliability
- ✅ No vote loss (all votes either in queue or DB)
- ✅ Zero duplicate votes (unique constraint enforced)
- ✅ Redis cluster survives single node failure
- ✅ Database shard failover queues votes for retry
- ✅ Hourly reconciliation detects drift < 1%

### Monitoring
- ✅ P95/P99 latency measured per endpoint
- ✅ Queue depth tracked (alert > 10K)
- ✅ Error rate tracked (alert > 1%)
- ✅ Redis hit rate tracked (alert < 95%)
- ✅ Constraint violations tracked (alert > 0 anomalies/min)

## Risk Register

### Risk 1: Single PostgreSQL primary becomes bottleneck
- **Probability**: Medium (if writes exceed 10K/sec sustained)
- **Mitigation**: Plan for read-write splitting or additional sharding

### Risk 2: Redis cluster node failure causes cache miss storm
- **Probability**: Low (cluster protocol handles failover)
- **Mitigation**: Materialized views provide fallback; monitor Redis health

### Risk 3: Bot attack exhausts all rate limit quota
- **Probability**: High (sophisticated attackers could use many IPs/accounts)
- **Mitigation**: Report to Adidos for upstream IP blacklist

### Risk 4: Load test doesn't catch hot-poll bottleneck
- **Probability**: Medium
- **Mitigation**: Load test should simulate influencer scenario (50K/sec on single poll)

---

## Communication

### Issue Tracking
- Link PRs to issues
- Add comments with updates
- Mark done when merged

### Architecture Changes
- Document in DOMAIN_MODEL.md or SPECIFICATION.md
- Get team review before implementing

---

## Glossary (Quick Reference)

| Term | Definition |
|------|-----------|
| **Poll** | A question with 2 answers |
| **Vote** | A user's choice on a poll |
| **Shard** | PostgreSQL instance holding votes for part of user_id space |
| **Queue** | Redis list of votes waiting to be processed |
| **Rate Limit** | Threshold on votes per user/IP per minute |
| **Bot** | Malicious actor trying to manipulate results |
| **Anomaly** | Suspicious voting pattern (rate limit, duplicates) |
| **Reconciliation** | Hourly check: Redis counts vs. DB counts |

---

**Last Updated**: 2026-08-10  
**Next Phase**: Begin Phase 1 implementation (I-001 ready now)
