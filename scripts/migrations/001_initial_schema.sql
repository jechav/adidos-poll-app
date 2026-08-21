-- Migration 001: initial schema.
-- Run against every shard (shard_0 .. shard_7). Idempotent guard via
-- schema_migrations so re-running is a no-op.

BEGIN;

CREATE TABLE IF NOT EXISTS schema_migrations (
  version VARCHAR(255) PRIMARY KEY,
  applied_at TIMESTAMP NOT NULL DEFAULT now()
);

DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM schema_migrations WHERE version = '001_initial_schema') THEN
    RAISE EXCEPTION 'migration 001_initial_schema already applied';
  END IF;
END $$;

\ir ../schema/polls.sql
\ir ../schema/indexes.sql

INSERT INTO schema_migrations (version) VALUES ('001_initial_schema');

COMMIT;
