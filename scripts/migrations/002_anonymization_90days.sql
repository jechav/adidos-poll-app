-- Migration 002: 90-day vote anonymization.
--
-- Adds a function that nulls out votes.user_id for votes older than 90
-- days, for data retention compliance. The function is idempotent (already
-- anonymized rows have user_id IS NULL and are excluded by the WHERE
-- clause) and is intended to be invoked on a recurring schedule (e.g. a
-- daily cron job / k8s CronJob calling
-- `SELECT anonymize_votes_older_than_90_days();`), not automatically by
-- this migration.
--
-- votes.user_id is nullable at the column-constraint level (no NOT NULL
-- was declared in scripts/schema/polls.sql) precisely so this anonymization
-- step can run without a schema change.

BEGIN;

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM schema_migrations WHERE version = '001_initial_schema') THEN
    RAISE EXCEPTION 'migration 001_initial_schema must be applied first';
  END IF;
  IF EXISTS (SELECT 1 FROM schema_migrations WHERE version = '002_anonymization_90days') THEN
    RAISE EXCEPTION 'migration 002_anonymization_90days already applied';
  END IF;
END $$;

CREATE OR REPLACE FUNCTION anonymize_votes_older_than_90_days()
RETURNS BIGINT AS $$
DECLARE
  rows_updated BIGINT;
BEGIN
  UPDATE votes
  SET user_id = NULL
  WHERE user_id IS NOT NULL
    AND created_at < now() - INTERVAL '90 days';

  GET DIAGNOSTICS rows_updated = ROW_COUNT;
  RETURN rows_updated;
END;
$$ LANGUAGE plpgsql;

INSERT INTO schema_migrations (version) VALUES ('002_anonymization_90days');

COMMIT;
