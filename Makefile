.PHONY: test-unit test-integration test

# Fast, dependency-free unit suite (I-021) — fakeredis + an in-memory
# poll/answer fake, no Postgres/Redis cluster required. Meant to run on
# every commit; see docs/setup/testing.md.
test-unit:
	pytest tests/unit

# Needs a real PostgreSQL + Redis cluster reachable from the host (see
# docs/setup/redis-cluster.md); tests skip themselves when unreachable.
test-integration:
	pytest tests/integration

test:
	pytest
