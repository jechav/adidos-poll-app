# Load Testing

Implements [I-023](../issues/I-023-load-tests.md) (Test Module 8 from
SPECIFICATION.md). See that ticket for the full scenario table and
rationale; this doc covers how to actually run each scenario.

## Install

```bash
pip install -r loadtest/requirements.txt
```

Kept out of the app's own `requirements.txt` deliberately: importing
`locust` monkey-patches `socket`/`ssl`/threading at import time (gevent),
which is incompatible with the app's asyncio-based test suite if both
ever share a process. Never add `locust` to the app image.

## Seed target polls first

Every scenario needs real `poll_id`/`answer_id` values that exist in the
target environment — this suite never creates them. `sustained_peak` and
`sudden_spike` spread load across a pool of them; `viral_single_poll`
needs exactly one.

```bash
# pool for sustained_peak / sudden_spike (comma-separated poll:answer pairs)
export POLL_APP_LOADTEST_POLL_IDS="<poll-id-1>:<answer-id-1>,<poll-id-2>:<answer-id-2>"

# single target for viral_single_poll
export POLL_APP_LOADTEST_TARGET_POLL_ID="<poll-id>"
export POLL_APP_LOADTEST_TARGET_ANSWER_ID="<answer-id>"
```

## Running against staging

**Load tests only ever run against a staging environment provisioned
identically to production** (same shard count, Redis cluster size,
replica/autoscaling config) — never a shared dev environment, and never
production itself except the mandatory pre-launch run (Deployment
Checklist item 8). No such environment exists inside this repository or
its CI — the commands below are what an operator with access to that
environment runs; they are not something a checked-in test suite can
execute on its own.

### Sustained normal peak (single process — this is the one scenario a
laptop/CI runner can generate on its own)

```bash
python -m scripts.loadtest.run_scenario sustained_peak \
    --host https://staging.example.internal \
    --reports-dir reports/
```

Or drive Locust directly for the interactive web UI:

```bash
locust -f loadtest/scenarios/sustained_peak.py \
    --host https://staging.example.internal
# open http://localhost:8089
```

### Sudden spike / viral single poll / sustained 5x peak (need a worker
fleet — see `k8s/loadtest/`)

```bash
kubectl apply -f k8s/loadtest/master-job.yaml
kubectl apply -f k8s/loadtest/worker-job.yaml
```

Edit each manifest's `LOCUSTFILE`/`SCENARIO_NAME`/`EXPECTED_WORKERS` env
vars (and `worker-job.yaml`'s matching `parallelism`/`completions`) for
the scenario you're running — `master-job.yaml`'s header comment has the
full explanation. Start worker count at roughly 1 pod per 1K req/sec of
target load and tune from the run's own observed throughput (per I-023's
Distributed Execution section — there's no substitute for measuring this
against the real environment).

## Reading a result

Each run archives `reports/<scenario>-<timestamp>.json`
(`loadtest/utils/report.py`) with the request stats, the pass/fail
verdict, and *why* it failed if it did:

```json
{
  "scenario": "sustained_peak",
  "timestamp": "2026-08-29T12-00-00Z",
  "stats": { "num_requests": 60000, "num_503": 0, "response_time_p99_ms": 145, ... },
  "monitoring_snapshot": {},
  "passed": true,
  "failure_reasons": []
}
```

`monitoring_snapshot` (recorded vote counts for `viral_single_poll`,
queue-depth samples and max replica count for `sustained_5x_peak`) is
pulled from I-017's dashboards during the run and is not fetched
automatically by `run_scenario.py` — there is no environment-agnostic
way to query an arbitrary Prometheus/Grafana deployment, and this repo
has no live one to validate that integration against. Assemble it by
hand (or export it from Grafana's API) into a JSON file matching the
shape documented in `loadtest/utils/report.py`'s module docstring, and
pass `--monitoring-snapshot-json <file>`.

## If a scenario fails

**Do not launch.** A failing scenario means the deploy-checklist gate
(I-024) is not met. Escalate to on-call/infra using the runbook that
matches the failure symptom (I-020):

| Failure reason contains... | Runbook |
|---|---|
| `503` | [`docs/runbooks/queue-backlog.md`](../runbooks/queue-backlog.md) |
| `p99 latency` | [`docs/runbooks/db-bottleneck.md`](../runbooks/db-bottleneck.md) |
| `data loss` | [`docs/runbooks/shard-down.md`](../runbooks/shard-down.md) |
| `queue depth` | [`docs/runbooks/queue-backlog.md`](../runbooks/queue-backlog.md) |
| `replicas` | escalate directly — this is an autoscaler/infra config issue, not one of I-020's five incident scenarios |

Re-run the scenario only after the runbook's mitigation has been applied
and the underlying cause (not just the symptom) is understood — a
load-test failure is exactly the case I-023 exists to catch before real
traffic does.

## Weekly scheduled run

`deploy/cronjobs/loadtest-weekly.yaml` runs `sustained_peak` every Monday
06:00 UTC — the one scenario cheap enough to run unattended and often
(see that file's header comment for why the three higher-throughput
scenarios are triggered manually instead).

## Baseline reports

`reports/baseline/` is where the pre-launch baseline run's archived
reports for all four scenarios live, per the Implementation Checklist.
**No staging environment exists in this repository/sandbox**, so no
baseline reports are checked in yet — see `reports/baseline/README.md`
for what running the baseline consists of once one does.
