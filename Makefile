.PHONY: test-unit test-integration test

# I-021: hermetic, mocked-infrastructure suite -- never touches Docker.
test-unit:
	pytest tests/unit

# I-022: real dockerized PostgreSQL + Redis. Assumes
# `docker compose up -d postgres redis-node-1 redis-node-2 redis-node-3 redis-cluster-init`
# has already been run (and migrations applied via `scripts/db/run-sql.sh
# migrations`) -- see docs/setup/testing.md for the full prerequisites and
# for why some Redis-backed tests only exercise for real when run inside
# the compose network rather than directly on the host.
test-integration:
	pytest tests/integration

test: test-unit test-integration
