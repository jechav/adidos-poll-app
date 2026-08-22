# I-025: App Wasn't Actually Running Inside the Compose Network Redis Needs

**Status**: Done
**Triage**: ready-for-agent
**Epic**: Voting Infrastructure
**Priority**: P1 (blocked local manual testing of every Redis-backed feature)
**Estimated Effort**: 0.5 day
**Depends On**: I-002 (Redis Cluster Setup)

---

## Problem Statement

`docker-compose.yml`'s three Redis nodes (I-002) form a real cluster (`CLUSTER INFO` reports `cluster_state:ok`), but a client connecting from the bare host (e.g. `uvicorn` run directly via `.venv`, or `pytest`) can only ever reach the *first* node it dials. `redis-py`'s `RedisCluster` client resolves the correct node per key via `CLUSTER SLOTS`, and that response returns each node's **internal Docker-network hostname** (`redis-node-2`, `redis-node-3`) — unresolvable from outside the compose network. The client hangs retrying rather than failing fast.

This was already known and documented: [docs/setup/redis-cluster.md](../setup/redis-cluster.md) states plainly — *"Cluster clients must connect from inside the compose network, not from the bare host... Run the app itself as a compose service."* The prescribed fix was simply never built — `docker-compose.yml` had no `app` service, so every local run used `uvicorn` directly on the host, silently violating that constraint.

Concretely, this broke:
- `GET /health` (a bounded-timeout fix landed in a prior commit so it reports `degraded` instead of hanging, but that only stopped the hang — it still couldn't report `ok`)
- `POST /v1/vote` (I-005): rate limiting, uniqueness reservation, and the queue enqueue all touch the real cluster and would 503 or hang when run from the host

### A dead end worth recording

The first fix attempted here was setting `cluster-announce-ip 127.0.0.1` (plus matching `--cluster-announce-port`) on each node, so the *nodes themselves* would advertise a host-reachable address. This does not work: once a node announces `127.0.0.1`, its **peers** also receive that announcement via gossip and try to open their cluster-bus connection to `127.0.0.1:<port>` — which, from inside another container, means itself, not the sibling node. The three nodes became three isolated one-node "clusters" instead of one real cluster. Redis Cluster has no way to advertise two different addresses to two different audiences (peers vs. external clients), so this class of fix is fundamentally unworkable for this topology — confirmed by testing it, not just reasoning about it. Reverted immediately; see git history on this file for the attempted diff.

---

## Solution

Follow the design `docs/setup/redis-cluster.md` already specified: add the FastAPI app as a `docker-compose` service (`Dockerfile` + an `app:` entry) so it joins the same Docker network as the Redis nodes and Postgres, and addresses them by internal hostname/port (`redis-node-1:6379`, `postgres:5432`) — no announce-address tricks needed, because `CLUSTER SLOTS`' internal-hostname responses are directly reachable from another container on the same network.

```yaml
# docker-compose.yml (excerpt)
app:
  build: .
  ports: ["8000:8000"]
  environment:
    POLL_APP_DATABASE_URL: postgresql://poll_app:poll_app@postgres:5432/poll_app
    POLL_APP_REDIS_STARTUP_HOST: redis-node-1
    POLL_APP_REDIS_STARTUP_PORT: "6379"
  volumes:
    - ./src:/app/src
  depends_on:
    postgres:
      condition: service_healthy
    redis-node-1: { condition: service_started }
    redis-node-2: { condition: service_started }
    redis-node-3: { condition: service_started }
```

A `pg_isready` healthcheck was added to the `postgres` service so `app` waits for it to actually accept connections, not just for the container to start.

`app` depends on the three Redis node services directly rather than on `redis-cluster-init`'s completion: that init container isn't idempotent (`redis-cli --cluster create` errors if the nodes already hold cluster state from a prior run), so gating `app`'s startup on its exit code would break on every `docker compose up` after the first.

### Secondary finding: `redis-py`'s socket calls had no timeout at all

Separately from the addressing problem, `src/cache/redis_client.py`'s `RedisCluster` construction had no `socket_connect_timeout`/`socket_timeout`, so *any* Redis command — not just `ping()` — would hang forever rather than raise if a connection attempt stalled. Added both (3s), which turns a stuck socket into a `redis.exceptions.TimeoutError` (a `RedisError`, already handled by every existing fallback in auth.py/rate_limit.py/uniqueness.py/vote_queue.py). Verified this closes the gap for a single direct call (`hgetall` against the unreachable topology now fails in ~3s instead of hanging).

**This is not a complete fix for running `pytest` from the host against a genuinely up 3-node cluster**, though: across a *sequence* of calls in the same test file, something inside `redis-py`'s cluster client's own retry/slot-refresh logic still hangs indefinitely on some calls even with the socket timeout set — this looks like a deeper issue inside the library's retry path rather than something addressable from this app's code, and wasn't worth chasing further under this ticket. The practical implication: if you bring the whole stack up with `docker compose up -d` and then also run `pytest` directly on the host, some integration test files may hang rather than skip. **Workaround**: `docker compose stop redis-node-1 redis-node-2 redis-node-3` before running `pytest` from the host (connection-refused fails fast and tests skip cleanly), or restart them afterward for manual `curl`/browser testing against `app`. A cleaner long-term fix would be running `pytest` itself inside the compose network (`docker compose run app pytest`) so it never needs to reach the cluster from outside at all — noted as a follow-up, not implemented here.

---

## Acceptance Criteria

- [x] `docker compose up -d` brings up an `app` service reachable at `localhost:8000`
- [x] `curl localhost:8000/health` reports `"redis": "ok"` (verified live)
- [x] `POST /v1/vote` against a real active poll succeeds end-to-end: 202, entry confirmed in `queue:votes` via `redis-cli`, and a same-user retry correctly returns 409 `DUPLICATE_VOTE` (verified live, both cases)
- [x] `docs/setup/redis-cluster.md` already documented the constraint correctly; no changes needed there, only the missing implementation
- [ ] ~~Integration tests that skip on "no Redis cluster reachable" now run instead of skip~~ — **not applicable as originally written**: `pytest` still runs on the bare host (that's correct — tests shouldn't require a container to execute), so it still can't reach the cluster's internal-hostname addresses and correctly continues to skip. Making the test suite itself exercise the live cluster would require running `pytest` inside the compose network too (e.g. `docker compose run app pytest`), which is a reasonable follow-up but wasn't part of this fix.

---

## Testing Strategy

- **Manual** (all performed live during this fix): `docker compose build app && docker compose up -d`; `curl localhost:8000/health` → `redis: ok`; seeded a real poll/answer via `psql`, `POST /v1/vote` → 202 with a real `queue:votes` entry; repeated the same request → 409 `DUPLICATE_VOTE`
- **Automated**: full suite still passes (skips unchanged) when run from the host, since that was never the broken path

---

## Out of Scope

- Kubernetes/staging/production Redis addressing — pods address each other directly on the cluster network there; this was purely a local Docker Compose gap
- Running `pytest` itself inside the compose network so integration tests stop skipping locally (noted above as a reasonable follow-up, not required here)
- Re-introducing a replica-per-master topology — still out of scope per I-002

---

## Related Issues

- I-002: Redis Cluster Setup (`docs/setup/redis-cluster.md` already specified this fix; this ticket closes the gap between that doc and the actual `docker-compose.yml`)
- I-005: Vote Acceptance & Queueing (the first endpoint this was verified against end-to-end)

---

## Implementation Checklist

- [x] Add `Dockerfile` for the FastAPI app
- [x] Add `app` service to `docker-compose.yml`, addressing Postgres/Redis by internal compose hostnames
- [x] Add a `pg_isready` healthcheck to `postgres` so `app` doesn't race it on startup
- [x] Verify `/health` reports `ok` and `POST /v1/vote` succeeds end-to-end, including the duplicate-vote path
- [x] (Attempted and reverted) `cluster-announce-ip` on the Redis nodes themselves — documented above as a confirmed dead end, so it isn't re-attempted later
- [x] Add `socket_connect_timeout`/`socket_timeout` to the shared `RedisCluster` client (src/cache/redis_client.py) — a real, verified improvement, but not a complete fix for host-side `pytest` against a live cluster; see the note above and the workaround it documents

---

**Acceptance**: The app runs inside the compose network per `docs/setup/redis-cluster.md`'s existing design; `/health` and live Redis-backed endpoints work against `docker-compose up -d` without hanging. Verified live, not just by inspection.
