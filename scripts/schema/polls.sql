-- Core schema for the poll voting service.
-- Applied identically to every shard (shard_0 .. shard_7); votes are the only
-- sharded table (by user_id), but polls/answers/vote_counts/anomalies are
-- replicated to all shards so a single shard can serve reads without
-- cross-shard joins.

CREATE TYPE poll_state AS ENUM ('draft', 'active', 'closed', 'archived');
CREATE TYPE anomaly_alert_type AS ENUM ('rate_limit_exceeded', 'duplicate_attempts_blocked', 'bot_pattern_detected');
CREATE TYPE anomaly_severity AS ENUM ('warning', 'critical');

CREATE TABLE polls (
  poll_id UUID PRIMARY KEY,
  question VARCHAR(255) NOT NULL,
  state poll_state NOT NULL DEFAULT 'draft',
  created_at TIMESTAMP NOT NULL DEFAULT now(),
  activated_at TIMESTAMP,
  closed_at TIMESTAMP,
  archived_at TIMESTAMP,
  CHECK (question IS NOT NULL AND length(question) > 0)
);

CREATE TABLE answers (
  answer_id UUID PRIMARY KEY,
  poll_id UUID NOT NULL REFERENCES polls(poll_id),
  answer_text VARCHAR(255) NOT NULL,
  "order" INT NOT NULL CHECK ("order" IN (0, 1)),
  UNIQUE(poll_id, "order"),
  CHECK (answer_text IS NOT NULL AND length(answer_text) > 0)
);

-- Sharded by user_id: only the votes cast by users hashed to this shard
-- live here. poll_id/answer_id foreign keys are valid because polls and
-- answers are replicated to every shard.
CREATE TABLE votes (
  vote_id UUID PRIMARY KEY,
  -- Nullable (not NOT NULL): the 90-day anonymization migration nulls this
  -- out for retention compliance while keeping the vote/answer counts.
  user_id VARCHAR(255),
  poll_id UUID NOT NULL,
  answer_id UUID NOT NULL,
  created_at TIMESTAMP NOT NULL DEFAULT now(),
  updated_at TIMESTAMP NOT NULL DEFAULT now(),
  UNIQUE(user_id, poll_id),
  FOREIGN KEY (poll_id) REFERENCES polls(poll_id),
  FOREIGN KEY (answer_id) REFERENCES answers(answer_id)
);

-- Materialized aggregate counts, refreshed every 5 minutes as a fallback
-- for when the Redis result cache (I-009) is unavailable.
CREATE TABLE vote_counts (
  poll_id UUID NOT NULL,
  answer_id UUID NOT NULL,
  "count" BIGINT NOT NULL DEFAULT 0,
  percentage DECIMAL(5, 2),
  last_updated_at TIMESTAMP NOT NULL DEFAULT now(),
  PRIMARY KEY (poll_id, answer_id),
  FOREIGN KEY (poll_id) REFERENCES polls(poll_id),
  FOREIGN KEY (answer_id) REFERENCES answers(answer_id)
);

CREATE TABLE anomalies (
  alert_id UUID PRIMARY KEY,
  user_id VARCHAR(255),
  ip_address VARCHAR(45),
  alert_type anomaly_alert_type NOT NULL,
  poll_id UUID,
  description TEXT,
  severity anomaly_severity NOT NULL,
  created_at TIMESTAMP NOT NULL DEFAULT now(),
  acknowledged_at TIMESTAMP,
  action_taken TEXT,
  CHECK (user_id IS NOT NULL OR ip_address IS NOT NULL)
);
