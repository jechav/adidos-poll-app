#!/usr/bin/env bash
# Run a .sql file (or all pending migrations) against the app's Postgres
# container, from the host — no local psql/postgres install required.
#
# Why this exists: piping a file into `psql -f /dev/stdin` breaks any
# script that uses `\ir`/`\include_relative` (e.g. migrations/001 pulls in
# ../schema/polls.sql) because psql has no on-disk path to resolve the
# include against, and it fails *silently* (psql doesn't stop on error by
# default) rather than with an obvious error. This script instead copies
# the whole scripts/ tree into the container and runs the file with
# ON_ERROR_STOP=1, so real failures actually surface and relative
# includes resolve correctly.
#
# Usage:
#   scripts/db/run-sql.sh migrations/001_initial_schema.sql
#   scripts/db/run-sql.sh seed/polls_and_votes.sql
#   scripts/db/run-sql.sh migrations          # run every *.sql in a directory, in sorted order
#   scripts/db/run-sql.sh --shell             # open an interactive psql session

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DB_USER="poll_app"
DB_NAME="poll_app"
CONTAINER_PATH="/tmp/scripts"

container_id() {
  docker compose ps -q postgres
}

CID="$(container_id)"
if [[ -z "$CID" ]]; then
  echo "error: postgres service isn't running (docker compose ps -q postgres returned nothing)." >&2
  echo "Start it with: docker compose up -d postgres" >&2
  exit 1
fi

# Always refresh the copy so edits to any .sql file are picked up.
docker cp "$SCRIPT_DIR" "$CID:$CONTAINER_PATH"

run_file() {
  local rel="$1"
  echo "==> running $rel"
  docker exec "$CID" psql -U "$DB_USER" -d "$DB_NAME" -v ON_ERROR_STOP=1 \
    -f "$CONTAINER_PATH/$rel"
}

if [[ "${1:-}" == "--shell" ]]; then
  exec docker exec -it "$CID" psql -U "$DB_USER" -d "$DB_NAME"
fi

if [[ $# -ne 1 ]]; then
  echo "usage: $0 <relative/path.sql | relative/dir | --shell>" >&2
  exit 1
fi

target="$1"

if [[ -d "$SCRIPT_DIR/$target" ]]; then
  while IFS= read -r -d '' f; do
    run_file "${f#"$SCRIPT_DIR"/}"
  done < <(find "$SCRIPT_DIR/$target" -maxdepth 1 -name '*.sql' -print0 | sort -z)
else
  run_file "$target"
fi
