-- Seed script: a handful of polls/answers plus 100K votes spread across
-- them, for local performance testing. Safe to re-run against an empty
-- database (uses fixed UUIDs for polls/answers so re-running is idempotent
-- via ON CONFLICT DO NOTHING; votes are regenerated with random UUIDs each
-- run, so truncate votes first if you want an exact re-seed).

BEGIN;

CREATE EXTENSION IF NOT EXISTS pgcrypto;

INSERT INTO polls (poll_id, question, state, activated_at) VALUES
  ('00000000-0000-0000-0000-000000000001', 'Should pineapple go on pizza?', 'active', now()),
  ('00000000-0000-0000-0000-000000000002', 'Is a hot dog a sandwich?', 'active', now()),
  ('00000000-0000-0000-0000-000000000003', 'Best season: summer or winter?', 'active', now())
ON CONFLICT (poll_id) DO NOTHING;

INSERT INTO answers (answer_id, poll_id, answer_text, "order") VALUES
  ('10000000-0000-0000-0000-000000000001', '00000000-0000-0000-0000-000000000001', 'Yes', 0),
  ('10000000-0000-0000-0000-000000000002', '00000000-0000-0000-0000-000000000001', 'No', 1),
  ('10000000-0000-0000-0000-000000000003', '00000000-0000-0000-0000-000000000002', 'Yes', 0),
  ('10000000-0000-0000-0000-000000000004', '00000000-0000-0000-0000-000000000002', 'No', 1),
  ('10000000-0000-0000-0000-000000000005', '00000000-0000-0000-0000-000000000003', 'Summer', 0),
  ('10000000-0000-0000-0000-000000000006', '00000000-0000-0000-0000-000000000003', 'Winter', 1)
ON CONFLICT (poll_id, "order") DO NOTHING;

-- 100,000 votes: each simulated user (identified by a generated user_id)
-- votes on one of the three polls, on a random answer for that poll.
INSERT INTO votes (vote_id, user_id, poll_id, answer_id, created_at)
SELECT
  gen_random_uuid(),
  'seed-user-' || g,
  poll.poll_id,
  (SELECT answer_id FROM answers WHERE poll_id = poll.poll_id ORDER BY random() LIMIT 1),
  now() - (random() * INTERVAL '30 days')
FROM generate_series(1, 100000) AS g
CROSS JOIN LATERAL (
  SELECT poll_id FROM polls ORDER BY random() LIMIT 1
) AS poll
ON CONFLICT (user_id, poll_id) DO NOTHING;

COMMIT;
