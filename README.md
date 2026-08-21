# Adidos Poll App Microservice

A **real-time binary polling microservice** designed to absorb 1M+ votes with intense traffic bursts, bot defense, and zero data loss.

## Quick Start

- **Domain Model**: See [DOMAIN_MODEL.md](./DOMAIN_MODEL.md)
- **Technical Specification**: See [SPECIFICATION.md](./SPECIFICATION.md)
- **Issues & Tickets**: See [.github/issues/](./docs/issues/)

## Architecture at a Glance

```
User Vote
    ↓
Rate Limit Check (Redis)
    ↓
Uniqueness Check (Redis SET NX)
    ↓
Return 202 Accepted
    ↓
Queue in Redis
    ↓
Worker Processor (1 per DB shard)
    ↓
Write to PostgreSQL (sharded by user_id)
    ↓
Update Redis Cache
    ↓
User sees results in real-time
```

### Key Design Decisions

- **Async Queue**: Accept votes immediately (202), process asynchronously
- **User-ID Sharding**: Distributes writes evenly across 8+ PostgreSQL nodes
- **Redis Cache**: Results served from cache, not database (avoids aggregation bottleneck)
- **Dual Uniqueness**: Redis SET NX + PostgreSQL unique constraint
- **Bot Defense**: Per-user (5/min), per-IP (50/min) rate limits + 3-strike duplicate block
- **Big Bang Launch**: Deploy to production with close monitoring (not canary)

## Project Structure

```
adidos-poll-app/
├── DOMAIN_MODEL.md          # Ubiquitous language, entities, aggregates
├── SPECIFICATION.md         # Technical spec, user stories, implementation decisions
├── README.md                # This file
├── docs/
│   ├── issues/              # Issue/ticket tracking (markdown format)
│   ├── architecture/        # Diagrams, decision records
│   └── runbooks/            # Incident response guides
├── src/
│   ├── api/                 # FastAPI REST endpoints
│   ├── queue/               # Redis queue producer/consumer
│   ├── db/                  # PostgreSQL connections, shard routing
│   ├── cache/               # Redis cache layer
│   ├── bot-detection/       # Rate limiting, anomaly detection
│   └── worker/              # Background vote processor
├── tests/
│   ├── unit/
│   ├── integration/
│   ├── load/                # JMeter/Locust scripts
│   └── fixtures/            # Test data
├── k8s/                     # Kubernetes manifests
├── docker/                  # Dockerfiles
└── scripts/
    ├── schema/              # PostgreSQL schema setup
    ├── migration/           # Database migrations
    └── seed/                # Test data seeding
```

## Development Workflow

### 1. Read the Spec
Start with [SPECIFICATION.md](./SPECIFICATION.md) to understand problem, solution, user stories, and implementation decisions.

### 2. Set Up Local Environment
```bash
docker-compose up -d  # PostgreSQL, Redis, etc.
pip install -r requirements.txt
alembic upgrade head   # Apply schema
python scripts/seed/seed.py  # Populate test data
```

### 3. Implement Feature by Feature
Issues are tracked in [docs/issues/](./docs/issues/). Pick an issue, implement, test, PR.

### 4. Test Before Pushing
```bash
pytest tests/unit             # Unit tests
pytest tests/integration      # Integration tests
locust -f tests/load/locustfile.py  # Load tests (optional, pre-launch)
```

## Key Metrics & Monitoring

### SLOs (Service Level Objectives)
- **Availability**: 99.9% uptime
- **Latency**: P95 < 50ms, P99 < 200ms for vote requests
- **Accuracy**: 100% vote uniqueness (no duplicates)
- **Recovery**: RTO 1 hour, RPO 5 minutes

### Alerts
- Queue depth > 10,000
- Error rate > 1%
- Redis hit rate < 95%
- Database reconciliation drift > 1%
- P99 latency > 500ms

## Deployment

### Pre-Launch Checklist
See [SPECIFICATION.md → Deployment Checklist](./SPECIFICATION.md#deployment-checklist-pre-launch)

### Big Bang Launch
1. Deploy API servers (2 initial replicas, autoscale to 5)
2. Deploy vote processors (1 per shard, autoscale)
3. Enable monitoring and alerting
4. On-call team ready to respond to incidents

## API Endpoints

### User Endpoints
- `GET /v1/polls` – List active polls with results
- `POST /v1/vote` – Cast a vote (202 Accepted)
- `GET /v1/user/votes` – List polls user voted on + their answers

### Admin Endpoints
- `POST /v1/admin/polls` – Create poll (admin only)
- `PUT /v1/admin/polls/:id/state` – Transition poll state (draft → active → closed → archived)
- `GET /v1/admin/anomalies` – View bot detection alerts

## Contributing

### Code Style
- Python: Ruff + Black
- Database: Migrations tracked in version control
- Tests: 100% coverage for business logic (external behavior, not implementation details)

### Commit Message Format
```
<type>: <subject>

<body>

Co-authored-by: Copilot <223556219+Copilot@users.noreply.github.com>
```

Types: `feat`, `fix`, `refactor`, `test`, `docs`, `chore`

## Troubleshooting

### Queue Building Up
- Check if vote processors are running: `kubectl get pods -l app=poll-worker`
- Check database shard latency: `SELECT p99_latency FROM db_metrics`
- Auto-scale should kick in; if not, page on-call

### Redis Hit Rate Dropping
- Check if Redis memory is full: `REDIS INFO memory`
- Check if network latency is high
- If cluster node down, cluster should auto-failover

### High Error Rate
- Check application logs: `kubectl logs -l app=poll-api --tail=100`
- Run reconciliation job to catch data drift

## References

- [Domain Model](./DOMAIN_MODEL.md) – Entities, aggregates, bounded contexts
- [Specification](./SPECIFICATION.md) – Full technical spec, user stories, testing strategy
- [Issues](./docs/issues/) – Feature requests, bugs, tasks
- [Architecture Decisions](./docs/architecture/) – ADRs, design rationale

## Support

For questions or issues:
1. Check [SPECIFICATION.md](./SPECIFICATION.md) for context
2. File an issue in [docs/issues/](./docs/issues/)
3. Contact on-call team for production incidents

---

**Status**: Ready for implementation (specification locked, 2026-08-10)
