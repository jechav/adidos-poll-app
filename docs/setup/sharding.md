# Shard Provisioning

Implements [I-001](../issues/I-001-database-schema.md). Covers provisioning
the 8 PostgreSQL shards; application-layer shard routing is out of scope
for this issue (tracked separately, needed by I-008's vote processors).

## Local development

A single Postgres instance is enough for local dev — the seed/migration
scripts don't assume multiple databases. Bring one up with:

```bash
docker-compose up -d postgres
```

Then apply the schema:

```bash
psql "$DATABASE_URL" -f scripts/migrations/001_initial_schema.sql
psql "$DATABASE_URL" -f scripts/migrations/002_anonymization_90days.sql
psql "$DATABASE_URL" -f scripts/seed/polls_and_votes.sql
```

## Provisioning all 8 shards

Each shard is a separate PostgreSQL instance/database with the identical
schema. Apply the same two migrations to each:

```bash
for shard in 0 1 2 3 4 5 6 7; do
  psql "postgres://poll-app@shard-${shard}.internal:5432/poll_app" \
    -f scripts/migrations/001_initial_schema.sql
  psql "postgres://poll-app@shard-${shard}.internal:5432/poll_app" \
    -f scripts/migrations/002_anonymization_90days.sql
done
```

`scripts/migrations/001_initial_schema.sql` is guarded by a
`schema_migrations` table, so re-running it against an already-migrated
shard raises an error instead of silently re-applying (and possibly
failing halfway through) DDL that already exists.

## Verifying a shard

```sql
-- Confirm cluster_state / migration state
SELECT version, applied_at FROM schema_migrations ORDER BY version;

-- Confirm indexes exist
\di

-- Sanity-check the uniqueness constraint
EXPLAIN ANALYZE
SELECT 1 FROM votes WHERE user_id = 'seed-user-1' AND poll_id = '00000000-0000-0000-0000-000000000001';
```

## Anonymization sweep

Schedule `SELECT anonymize_votes_older_than_90_days();` as a daily job
(k8s CronJob or equivalent) against every shard once I-001 is deployed.
This is intentionally not wired into a migration or startup hook — it's an
operational job, not schema.
