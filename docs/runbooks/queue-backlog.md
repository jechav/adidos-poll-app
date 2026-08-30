# Runbook: Queue Backlog

**Pages from**: `PollQueueDepthWarning` (`poll_queue_depth` > 5,000, warning) /
`PollQueueDepthCritical` (`poll_queue_depth` > 10,000, critical) — see
[I-019](../issues/I-019-alerting.md).

---

## Symptom

The `queue:votes` list (I-005/I-008) is growing faster than the vote
processor workers can drain it. `poll_queue_depth` (I-017) crosses 5,000
(warning) or 10,000 (critical, the spec-defined threshold at which votes
are at risk of delayed processing).

## Diagnosis

1. Open the **Infra Health** Grafana dashboard's **Queue Depth** panel
   and check the trend: is depth climbing steadily, spiking, or flat but
   above threshold?
2. Compare the running vote-processor pod count against the autoscaling
   target of **1 pod per 500 queued votes** (SPECIFICATION.md decision
   #9). If pod count is well below `depth / 500`, autoscaling hasn't
   caught up yet (or is misconfigured).
3. Determine whether the dequeue rate is **zero** (a stalled worker —
   pods are up but not making progress) or **positive but insufficient**
   (a genuine traffic burst outpacing capacity). Worker logs (I-018)
   show `vote_dequeued` / `vote_written` events per pod; a pod with no
   recent `vote_dequeued` lines is stalled.

## Mitigation

- **Autoscaling lagging, not stalled**: manually scale the vote
  processor deployment ahead of the autoscaler —
  `kubectl scale deployment vote-processor --replicas=N` — using
  `N = ceil(current_depth / 500)` per decision #9's target ratio.
- **A specific worker is stalled**: check that pod's logs (I-018) for
  repeated DB errors or exceptions around its last `vote_dequeued`
  line, then restart it (`kubectl delete pod <pod>` to let the
  Deployment recreate it, or `kubectl rollout restart deployment
  vote-processor` if several pods look affected).
- After either action, confirm `poll_queue_depth` on the **Queue
  Depth** panel is trending back down and returns below the 5,000
  warning threshold.

## Escalation

If depth keeps growing after manual scale-out (more workers, no
improvement), the bottleneck is very likely DB write capacity, not
worker count — move to
[`db-bottleneck.md`](db-bottleneck.md) rather than continuing to add
workers, since more workers hammering a saturated primary can make
things worse.
