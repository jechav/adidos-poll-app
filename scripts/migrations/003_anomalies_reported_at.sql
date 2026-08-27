-- Migration 003: adds `anomalies.reported_at` for I-016's Adidos
-- anomaly-reporting job.
--
-- Distinct from `acknowledged_at` (an ops person acknowledging an alert
-- in I-013's admin view -- an internal, human action) and from
-- `action_taken` (what this service did automatically, e.g. I-015's
-- block) -- neither of those means "Adidos has received this batch."
-- Purely additive: no existing column is touched, no existing row's
-- data changes; `reported_at` defaults to NULL for every row, including
-- ones that already existed before this migration ran.
--
-- The partial index only covers the unreported tail (`reported_at IS
-- NULL`), matching I-016's "find rows to report" query shape exactly --
-- it stays cheap as the table grows since reported rows, which will be
-- the overwhelming majority over time, are never scanned by it.

BEGIN;

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM schema_migrations WHERE version = '001_initial_schema') THEN
    RAISE EXCEPTION 'migration 001_initial_schema must be applied first';
  END IF;
  IF EXISTS (SELECT 1 FROM schema_migrations WHERE version = '003_anomalies_reported_at') THEN
    RAISE EXCEPTION 'migration 003_anomalies_reported_at already applied';
  END IF;
END $$;

ALTER TABLE anomalies ADD COLUMN reported_at TIMESTAMP;

CREATE INDEX idx_anomalies_unreported ON anomalies(created_at) WHERE reported_at IS NULL;

INSERT INTO schema_migrations (version) VALUES ('003_anomalies_reported_at');

COMMIT;
