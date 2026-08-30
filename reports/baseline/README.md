# Baseline Load Test Reports

This directory should hold the archived, timestamped JSON reports
(`loadtest/utils/report.py`) from the mandatory pre-launch run of all
four I-023 scenarios (Deployment Checklist item 8), so future weekly
runs have something to compare against.

**No reports are checked in here yet.** Generating them requires a
staging environment provisioned identically to production (I-023's
Target Environment section) — same shard count, Redis cluster size, and
replica/autoscaling config — which does not exist in this repository or
in the sandbox this suite was built in. Fabricating placeholder "passing"
reports here would misrepresent load test results that were never
actually produced, so none are included.

## To produce the real baseline

Once a production-topology staging environment exists:

```bash
python -m scripts.loadtest.run_scenario sustained_peak \
    --host <staging-host> --reports-dir reports/baseline/

kubectl apply -f k8s/loadtest/master-job.yaml   # sudden_spike config
kubectl apply -f k8s/loadtest/worker-job.yaml
# then viral_single_poll and sustained_5x_peak configs in turn --
# see docs/setup/load-testing.md for the full walkthrough.
```

This is a hard gate: per I-023's Acceptance and I-024's launch checklist,
launch does not proceed until all four scenarios pass here.
