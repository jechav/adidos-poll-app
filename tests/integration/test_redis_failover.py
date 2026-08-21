"""Failover and persistence tests for I-002's acceptance criteria:

- "Killing one node (docker stop) does not take the whole cluster down;
  other slots remain writable"
- "AOF and RDB persistence both enabled and verified (restart a node,
  confirm data present)"

Drives the real `docker compose` cluster via subprocess (mirroring
docs/setup/redis-cluster.md's manual steps) rather than redis-py, since
redis-py's cluster client can't resolve the nodes' internal Docker
hostnames from outside the compose network (see that doc's "Cluster
clients must connect from inside the compose network" note). Skips
automatically if docker compose / the redis-node-* services aren't up.
"""

import shutil
import subprocess
import time
import uuid

import pytest

COMPOSE_SERVICES = ["redis-node-1", "redis-node-2", "redis-node-3"]
# cluster-node-timeout is 15000ms (scripts/redis/redis.conf); gossip needs
# a few rounds beyond that to converge, so poll generously rather than
# sleeping a fixed guess.
POLL_TIMEOUT_S = 45


def _compose(*args: str, timeout: float = 30) -> subprocess.CompletedProcess:
    try:
        return subprocess.run(
            ["docker", "compose", *args],
            capture_output=True,
            text=True,
            timeout=timeout,
        )
    except subprocess.TimeoutExpired as exc:
        return subprocess.CompletedProcess(exc.cmd, returncode=124, stdout="", stderr="timeout")


def _redis_cli(
    service: str, *args: str, follow_redirects: bool = True, timeout: float = 30
) -> subprocess.CompletedProcess:
    # -c: follow MOVED/ASK redirects, the same way a real cluster client
    # (e.g. redis-py's RedisCluster) would. Skipped for the down-node probe
    # below, where following a redirect into a stopped node would hang on
    # a dead connection instead of failing fast.
    flags = ["-c"] if follow_redirects else []
    return _compose(
        "exec", "-T", service, "redis-cli", *flags, "-p", "6379", *args, timeout=timeout
    )


def _cluster_info(service: str = "redis-node-1") -> dict:
    result = _redis_cli(service, "cluster", "info")
    info = {}
    for line in result.stdout.splitlines():
        if ":" in line:
            key, _, value = line.partition(":")
            info[key.strip()] = value.strip()
    return info


def _wait_until(predicate, timeout_s: float = POLL_TIMEOUT_S, interval_s: float = 1.0):
    deadline = time.monotonic() + timeout_s
    last = None
    while time.monotonic() < deadline:
        last = predicate()
        if last:
            return last
        time.sleep(interval_s)
    return last


@pytest.fixture(scope="module", autouse=True)
def require_cluster_running():
    if shutil.which("docker") is None:
        pytest.skip("docker not available")

    for service in COMPOSE_SERVICES:
        result = _compose("ps", "-q", service)
        if result.returncode != 0 or not result.stdout.strip():
            pytest.skip(
                f"{service} isn't running; start it with "
                "`docker compose up -d redis-node-1 redis-node-2 redis-node-3 redis-cluster-init`"
            )

    if _cluster_info().get("cluster_state") != "ok":
        pytest.skip("redis cluster isn't formed (cluster_state != ok)")


@pytest.fixture
def restart_node_2():
    yield
    _compose("start", "redis-node-2")
    recovered = _wait_until(
        lambda: _cluster_info().get("cluster_slots_ok") == "16384"
    )
    assert recovered, "cluster did not fully recover after restarting redis-node-2"


def test_other_slots_stay_writable_when_one_node_is_down(restart_node_2):
    _compose("stop", "redis-node-2")

    degraded = _wait_until(
        lambda: (info := _cluster_info()).get("cluster_slots_ok") != "16384" and info
    )
    assert degraded, "redis-node-2's slots were never marked unavailable after stopping it"
    assert degraded["cluster_state"] == "ok", (
        "cluster_state flipped to non-ok on a single node loss — "
        "cluster-require-full-coverage should keep it 'ok' with other slots serving"
    )

    successes = 0
    failures = 0
    for i in range(20):
        result = _redis_cli(
            "redis-node-1",
            "set",
            f"failover-probe-{i}",
            "value",
            follow_redirects=False,
            timeout=5,
        )
        if "OK" in result.stdout:
            successes += 1
        else:
            failures += 1

    assert successes > 0, "no writes succeeded — the whole cluster went down, not just one node's slots"
    assert failures > 0, "expected writes to redis-node-2's slots to fail while it's down"


def test_aof_persists_data_across_node_restart():
    key = f"aof-test-{uuid.uuid4()}"
    write_result = _redis_cli("redis-node-1", "set", key, "persisted")
    assert "OK" in write_result.stdout

    _compose("restart", "redis-node-1")
    recovered = _wait_until(
        lambda: _cluster_info().get("cluster_slots_ok") == "16384"
    )
    assert recovered, "cluster did not report full slot coverage after restarting redis-node-1"

    # cluster_slots_ok can briefly report full coverage from stale gossip
    # state just before the restarted node finishes rejoining the cluster
    # bus, so retry the read rather than trusting a single snapshot.
    read_result = _wait_until(
        lambda: (r := _redis_cli("redis-node-1", "get", key)) and "persisted" in r.stdout and r
    )
    assert read_result and "persisted" in read_result.stdout, (
        f"expected the pre-restart key to survive via AOF, got: {read_result.stdout if read_result else None!r}"
    )
