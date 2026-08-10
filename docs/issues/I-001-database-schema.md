# I-001: Database Schema & Migrations

**Status**: Ready for Implementation  
**Epic**: Voting Infrastructure  
**Priority**: P0 (Blocker)  
**Estimated Effort**: 5 days

---

## Problem Statement

The voting system needs a PostgreSQL schema to store polls, answers, and votes. The schema must:
- Support 8+ user_id shards (each a separate PostgreSQL instance)
- Enforce uniqueness of (user_id, poll_id) per shard
- Support efficient querying of:
  - Polls by state (active, closed, archived)
  - Votes by poll_id for result aggregation
  - Votes by user_id for voting history
  - Materialized vote counts (fallback if Redis down)
- Support anonymization after 90 days (null out user_id)
- Include proper indexes for query performance

---

## Solution

Create a PostgreSQL schema with 4 core tables (replicated across all shards):

1. **polls** – Poll metadata (question, state, timestamps)
2. **answers** – Answer options (2 per poll)
3. **votes** – Individual votes (sharded by user_id)
4. **vote_counts** – Materialized aggregates (updated every 5 minutes)

Plus supporting tables:
- **user_vote_state** – Denormalized view (user voted on this poll?)
- **anomalies** – Bot detection alerts

---

## User Stories

1. As a database admin, I want to provision shard 0 through 7 (8 total) with identical schema, so that votes are distributed evenly by user_id
2. As a query optimizer, I want indexes on (user_id, poll_id) on votes table, so that uniqueness checks are fast
3. As a result aggregator, I want indexes on (poll_id, answer_id) on votes table, so that computing result counts is fast
4. As a privacy officer, I want a migration script to anonymize votes after 90 days (set user_id to null), so that we comply with data retention
5. As a developer, I want schema versioning in version control, so that migrations are tracked and reviewable
6. As an operator, I want to seed test data (100K votes across multiple polls), so that I can test performance locally

---

## Implementation Decisions

### Table: polls (Replicated to All Shards)
```sql
CREATE TABLE polls (
  poll_id UUID PRIMARY KEY,
  question VARCHAR(255) NOT NULL,
  state ENUM('draft', 'active', 'closed', 'archived') DEFAULT 'draft',
  created_at TIMESTAMP DEFAULT now(),
  activated_at TIMESTAMP,
  closed_at TIMESTAMP,
  archived_at TIMESTAMP,
  CHECK (question IS NOT NULL AND length(question) > 0)
);
CREATE INDEX idx_polls_state ON polls(state);
```

### Table: answers (Replicated to All Shards)
```sql
CREATE TABLE answers (
  answer_id UUID PRIMARY KEY,
  poll_id UUID NOT NULL REFERENCES polls(poll_id),
  answer_text VARCHAR(255) NOT NULL,
  "order" INT NOT NULL CHECK ("order" IN (0, 1)),
  UNIQUE(poll_id, "order"),
  CHECK (answer_text IS NOT NULL AND length(answer_text) > 0)
);
CREATE INDEX idx_answers_poll_id ON answers(poll_id);
```

### Table: votes (Sharded by user_id)
```sql
CREATE TABLE votes (
  vote_id UUID PRIMARY KEY,
  user_id VARCHAR(255) NOT NULL,
  poll_id UUID NOT NULL,
  answer_id UUID NOT NULL,
  created_at TIMESTAMP DEFAULT now(),
  updated_at TIMESTAMP DEFAULT now(),
  UNIQUE(user_id, poll_id),
  FOREIGN KEY (poll_id) REFERENCES polls(poll_id),
  FOREIGN KEY (answer_id) REFERENCES answers(answer_id)
);
CREATE INDEX idx_votes_user_poll ON votes(user_id, poll_id);
CREATE INDEX idx_votes_poll_answer ON votes(poll_id, answer_id);
CREATE INDEX idx_votes_created ON votes(created_at);
```

### Table: vote_counts (Materialized View, Replicated)
```sql
CREATE TABLE vote_counts (
  poll_id UUID NOT NULL,
  answer_id UUID NOT NULL,
  "count" BIGINT DEFAULT 0,
  percentage DECIMAL(5, 2),
  last_updated_at TIMESTAMP DEFAULT now(),
  PRIMARY KEY (poll_id, answer_id),
  FOREIGN KEY (poll_id) REFERENCES polls(poll_id),
  FOREIGN KEY (answer_id) REFERENCES answers(answer_id)
);
```

### Table: anomalies (Replicated)
```sql
CREATE TABLE anomalies (
  alert_id UUID PRIMARY KEY,
  user_id VARCHAR(255),
  ip_address VARCHAR(45),
  alert_type ENUM('rate_limit_exceeded', 'duplicate_attempts_blocked', 'bot_pattern_detected'),
  poll_id UUID,
  description TEXT,
  severity ENUM('warning', 'critical'),
  created_at TIMESTAMP DEFAULT now(),
  acknowledged_at TIMESTAMP,
  action_taken TEXT,
  CHECK (user_id IS NOT NULL OR ip_address IS NOT NULL)
);
CREATE INDEX idx_anomalies_created ON anomalies(created_at DESC);
```

---

## Acceptance Criteria

- [ ] Schema created for 8 PostgreSQL shards
- [ ] All tables have proper indexes (checked with `EXPLAIN ANALYZE`)
- [ ] Foreign key constraints prevent orphaned votes
- [ ] Uniqueness constraint on (user_id, poll_id) enforced per shard
- [ ] Anonymization migration script created (clears user_id after 90 days)
- [ ] Test data seed script creates 100K votes
- [ ] All migrations stored in `scripts/migrations/` directory
- [ ] README updated with schema diagram and ER model
- [ ] Performance validated: 1000 votes/sec write throughput on single shard

---

## Testing Strategy

- **Unit Tests**: Schema validation (test that constraints work as expected)
- **Integration Tests**: Insert 100K votes, verify uniqueness constraint rejects duplicates
- **Performance Tests**: Measure insert latency (target: < 5ms per vote on single shard)
- **Prior Art**: Existing Adidos schema tests (mirror structure)

---

## Out of Scope

- Sharding at the application layer (schema assumes all shards exist)
- Query rewriting for cross-shard joins
- Multi-version concurrency control (MVCC tuning)

---

## Related Issues

- I-002: Redis Cluster Setup (cache layer, doesn't depend on schema)
- I-008: Vote Processor Workers (depends on this schema)
- I-010: Materialized Views (uses vote_counts table from this issue)

---

## Implementation Checklist

- [ ] Create `scripts/schema/polls.sql` (core tables)
- [ ] Create `scripts/schema/indexes.sql` (indexes)
- [ ] Create `scripts/migrations/001_initial_schema.sql`
- [ ] Create `scripts/migrations/002_anonymization_90days.sql`
- [ ] Create `scripts/seed/polls_and_votes.sql` (test data)
- [ ] Add schema diagram to `docs/architecture/schema.md`
- [ ] Document shard provisioning in `docs/setup/sharding.md`
- [ ] Test locally with Docker PostgreSQL container

---

**Acceptance**: Schema validated, all tests pass, PR reviewed and merged.
