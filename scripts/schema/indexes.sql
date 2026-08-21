-- Indexes for the poll voting service. Applied after polls.sql, on every
-- shard. Kept separate from table definitions so index changes can be
-- reviewed/rolled out independently of schema changes.

CREATE INDEX idx_polls_state ON polls(state);

CREATE INDEX idx_answers_poll_id ON answers(poll_id);

-- votes(user_id, poll_id) is already indexed by the UNIQUE constraint in
-- polls.sql, so uniqueness checks are fast without a redundant index here.
CREATE INDEX idx_votes_poll_answer ON votes(poll_id, answer_id);
CREATE INDEX idx_votes_created ON votes(created_at);

CREATE INDEX idx_anomalies_created ON anomalies(created_at DESC);
