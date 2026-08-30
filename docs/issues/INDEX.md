# Poll App Issues & Tickets

**Format**: Markdown-based issue tracking (sync'd with git)  
**Labels**: `epic`, `feature`, `bug`, `task`, `ready-for-implementation`, `blocked`, `in-progress`, `done`

## Active Issues

### Phase 1: Foundation (Weeks 1-2)
- [x] [I-001: Database Schema & Migrations](./I-001-database-schema.md)
- [x] [I-002: Redis Cluster Setup](./I-002-redis-cluster.md)
- [x] [I-003: API Framework & Routing](./I-003-api-framework.md)
- [x] [I-004: Authentication Middleware](./I-004-auth-middleware.md)

### Phase 2: Core Voting (Weeks 3-4)
- [x] [I-005: Vote Acceptance & Queueing](./I-005-vote-acceptance.md)
- [x] [I-006: Uniqueness Enforcement (Redis + DB)](./I-006-uniqueness.md)
- [x] [I-007: Rate Limiting (Per-User, Per-IP)](./I-007-rate-limiting.md)
- [x] [I-008: Vote Processor Workers](./I-008-vote-processor.md)

### Phase 3: Results & Caching (Weeks 5-6)
- [x] [I-009: Result Aggregation (Redis Cache)](./I-009-result-aggregation.md)
- [x] [I-010: Materialized Views (Fallback)](./I-010-materialized-views.md)
- [x] [I-011: GET /v1/polls Endpoint](./I-011-list-polls.md)
- [x] [I-012: GET /v1/user/votes Endpoint](./I-012-user-votes.md)

### Phase 4: Admin & Bot Defense (Weeks 7-8)
- [x] [I-013: Admin Poll Management Endpoints](./I-013-admin-endpoints.md)
- [x] [I-014: Bot Detection & Alerting](./I-014-bot-detection.md)
- [x] [I-015: Duplicate Detection (3-Strike Block)](./I-015-duplicate-detection.md)
- [x] [I-016: Anomaly Reporting to Adidos](./I-016-anomaly-reporting.md)

### Phase 5: Observability (Weeks 9-10)
- [x] [I-017: Metrics & Monitoring (Prometheus)](./I-017-monitoring.md)
- [x] [I-018: Structured Logging (JSON + Sampling)](./I-018-logging.md)
- [x] [I-019: Alerting & Dashboards (Grafana)](./I-019-alerting.md)
- [x] [I-020: Incident Runbooks](./I-020-runbooks.md)

### Phase 6: Testing & Launch (Weeks 11-12)
- [x] [I-021: Unit Tests (Vote, Rate Limit, Uniqueness)](./I-021-unit-tests.md)
- [x] [I-022: Integration Tests (End-to-End Workflows)](./I-022-integration-tests.md)
- [x] [I-023: Load Tests (5x Peak: 100K votes/sec)](./I-023-load-tests.md)
- [ ] [I-024: Pre-Launch Checklist](./I-024-launch-checklist.md)

### Bugs / Infra
- [x] [I-025: App Wasn't Actually Running Inside the Compose Network Redis Needs](./I-025-redis-cluster-announce-address.md)

## Epics

### Epic 1: Voting Infrastructure
**Goal**: Accept and process votes asynchronously with 1M+ capacity

**Issues**: I-001, I-002, I-003, I-004, I-005, I-006, I-007, I-008, I-009

---

### Epic 2: Result Aggregation & Caching
**Goal**: Serve real-time results without hitting database

**Issues**: I-009, I-010, I-011, I-012

---

### Epic 3: Admin & Security
**Goal**: Admin poll management + bot defense

**Issues**: I-013, I-014, I-015, I-016

---

### Epic 4: Observability & Launch
**Goal**: Monitoring, alerting, testing, launch readiness

**Issues**: I-017, I-018, I-019, I-020, I-021, I-022, I-023, I-024

---

## Backlog (Post-MVP)

- [ ] Multi-language support (i18n)
- [ ] Rich media (images, links)
- [ ] Anonymous voting mode
- [ ] Scheduled poll activation
- [ ] Advanced analytics dashboards
- [ ] Webhook events to Adidos
- [ ] A/B testing (randomize questions/answers)

---

## Legend

- **ready-for-implementation**: Spec complete, waiting for development
- **in-progress**: Someone is actively working on it
- **blocked**: Can't proceed (waiting for another issue)
- **done**: Complete and merged

---

## How to Use This Tracker

1. **Pick an Issue**: Start with Phase 1 issues (foundation)
2. **Read the Issue**: Each issue has acceptance criteria, user stories, implementation notes
3. **Implement**: Create a branch, write tests, implement feature
4. **Submit PR**: Link the issue, request review
5. **Mark Done**: Close issue when PR is merged

---

**Last Updated**: 2026-08-22
