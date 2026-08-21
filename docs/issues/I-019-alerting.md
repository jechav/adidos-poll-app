# I-019: Alerting & Dashboards (Grafana)

**Status**: Ready for Implementation  
**Epic**: Observability & Launch  
**Priority**: P0 (Blocker)  
**Estimated Effort**: 2 days  
**Depends On**: I-017

---

## Problem Statement

I-017 makes the six observability signals scrapeable, but a metric nobody is watching doesn't prevent an incident. This issue turns those metrics into something humans act on:

- Alertmanager rules that fire on each of the six signals, with the exact thresholds SPECIFICATION.md's Implementation Decision #15 and Deployment Checklist item 6 specify: queue depth > 10,000, error rate > 1%, Redis hit rate < 95%, reconciliation drift > 1%, and P99 latency > 500ms
- Severity routing — decision #15 says alerting uses "different severity (warning vs. critical) based on signal," which this issue must translate into concrete bands and destinations, not just a single threshold per metric
- Two Grafana dashboards: one for API-level SLOs, one for infrastructure health

One threshold in this list needs a careful distinction: **P99 > 500ms is an alerting trigger, not the SLO target.** The target this whole system is designed against is P99 < 200ms (SPECIFICATION.md's Performance success criteria, user story #9, and I-017's histogram buckets). 500ms is the point at which SPECIFICATION.md's deployment checklist says to page — well past "missing the target" and into "something is actually broken." Conflating the two in a dashboard or alert name would either page on every minor SLO miss or fail to page on a real degradation; this issue treats them as two separate numbers throughout.

---

## Solution

Build Prometheus Alertmanager rules on top of I-017's metrics, with two severity bands per signal (`warning`, `critical`), each routed to a different destination. Provision two Grafana dashboards from the same metric catalog: **API SLOs** (latency, error rate) and **Infra Health** (queue depth, Redis hit rate, shard/cluster status). Alert rules and dashboards are defined as code (YAML/JSON, version-controlled) rather than clicked together in the Grafana UI, so they survive redeploys and are reviewable in PRs like any other change.

---

## User Stories

(from SPECIFICATION.md)

20. As an admin, I want to be notified of critical anomalies (e.g., 100K votes in 1 second), so that I can respond to emergencies
31. As an operator, I want to monitor P95 and P99 vote latency (time from vote request to Redis update), so that I detect when bursts degrade performance
32. As an operator, I want to monitor queue depth, error rate, and Redis hit rate, so that I have end-to-end visibility

---

## Implementation Decisions

### Alert Rules: Warning vs. Critical Bands

Per decision #15, each signal escalates through a warning band before reaching the critical threshold named in the spec — the critical threshold is always the exact number SPECIFICATION.md lists; the warning band is a tighter margin chosen so on-call gets advance notice before a metric crosses into "spec-defined incident" territory.

| Signal | PromQL | Warning | Critical (spec-defined) | `for` |
|---|---|---|---|---|
| Queue depth | `poll_queue_depth` | > 5,000 | > 10,000 | warning 2m, critical 1m |
| Error rate | `sum(rate(poll_requests_total{status_code=~"5.."}[5m])) / sum(rate(poll_requests_total[5m]))` | > 0.5% | > 1% | warning 5m, critical 2m |
| Redis hit rate | `sum(rate(poll_redis_cache_hits_total[5m])) / sum(rate(poll_redis_cache_hits_total[5m]) + rate(poll_redis_cache_misses_total[5m]))` | < 97% | < 95% | warning 10m, critical 5m |
| Reconciliation drift | `poll_reconciliation_drift_max_ratio` | > 0.5% | > 1% | fires per hourly run (no `for` — job runs once/hour) |
| P99 latency | `histogram_quantile(0.99, sum(rate(poll_vote_latency_seconds_bucket[5m])) by (le))` | > 200ms *(SLO target miss)* | > 500ms *(spec alerting trigger)* | warning 5m, critical 2m |
| DB constraint violations | `increase(poll_db_constraint_violations_total[1m])` | > 0 (any occurrence) | > 5/min sustained for 5m | — |

Notes on judgment calls:
- **Queue depth** is deliberately alerted well above the 1,000-vote autoscaling trigger (stories #26/#27) — autoscaling should absorb bursts before 5,000/10,000; these thresholds mean "autoscaling isn't keeping up," which is the actual incident.
- **P99 latency** carries both bands explicitly labeled by *meaning*, not just severity — the warning band's annotation says "SLO target missed," the critical band's says "spec alerting threshold breached," so nobody reading a fired alert confuses the two.
- **DB constraint violations**: per I-006/I-008's design, a small number of races (client retries hitting an already-processed vote) are expected and non-incident. Spec's literal "> 0 anomalies/min" is treated as the *warning* band (visibility, not paging); sustained volume (> 5/min for 5m) is promoted to critical, since that pattern suggests the Redis `SET NX` uniqueness layer (I-006) is failing to catch duplicates before they reach the queue.

### Severity Routing

- **warning** → posted to the `#poll-app-alerts` Slack channel; no page. Reviewed at business hours or the next stand-up.
- **critical** → routed to the PagerDuty on-call rotation; pages immediately; 5-minute acknowledgment SLA.

```yaml
# deploy/alertmanager/config.yml (excerpt)
route:
  group_by: ["alertname"]
  routes:
    - match:
        severity: warning
      receiver: slack-poll-app-alerts
    - match:
        severity: critical
      receiver: pagerduty-oncall
receivers:
  - name: slack-poll-app-alerts
    slack_configs:
      - channel: "#poll-app-alerts"
  - name: pagerduty-oncall
    pagerduty_configs:
      - service_key: "${PAGERDUTY_SERVICE_KEY}"
```

Example rule (queue depth, both bands):

```yaml
# deploy/prometheus/alerts.rules.yml (excerpt)
groups:
  - name: poll-app-queue
    rules:
      - alert: PollQueueDepthWarning
        expr: poll_queue_depth > 5000
        for: 2m
        labels: { severity: warning }
        annotations:
          summary: "Queue depth above 5,000 — autoscaling may not be keeping up"
          runbook_url: "docs/runbooks/queue-backlog.md"
      - alert: PollQueueDepthCritical
        expr: poll_queue_depth > 10000
        for: 1m
        labels: { severity: critical }
        annotations:
          summary: "Queue depth above 10,000 (spec threshold) — votes at risk of delayed processing"
          runbook_url: "docs/runbooks/queue-backlog.md"
```

Every alert's `annotations.runbook_url` points at the corresponding I-020 runbook, so a paged engineer's first action is one click, not a search.

### Grafana Dashboards

**Dashboard 1 — API SLOs**: P50/P95/P99 latency line chart (with 50ms and 200ms target lines annotated, and a separate 500ms alert-threshold line so the SLO target and the alert trigger are visually distinct per the framing above), request rate, error rate %, error breakdown by status code.

**Dashboard 2 — Infra Health**: queue depth gauge (with the 5,000/10,000 threshold lines), Redis hit rate %, Redis cluster node status (up/down per node, from I-002), PostgreSQL shard status (up/down per shard, write latency, from I-001/I-008), reconciliation drift (last run value + trend), DB constraint violation rate.

Both dashboards are provisioned from JSON definitions checked into the repo (Grafana's provisioning-via-config mechanism), not built by hand in the UI, so they deploy consistently across environments.

---

## Acceptance Criteria

- [ ] Alertmanager rules defined for all six signals, each with warning and critical bands
- [ ] Alert routing: warning → Slack, critical → PagerDuty (or the team's equivalent), configured as code
- [ ] The P99 > 500ms alert threshold is explicitly and visibly distinguished from the P99 < 200ms SLO target, in both rule annotations and dashboard labeling
- [ ] Grafana dashboard "API SLOs" provisioned (latency percentiles, error rate)
- [ ] Grafana dashboard "Infra Health" provisioned (queue depth, Redis hit rate, shard/cluster status, reconciliation drift)
- [ ] Every alert rule's annotations include a `runbook_url` pointing at its corresponding I-020 runbook
- [ ] Alert rules and dashboards are version-controlled (YAML/JSON in the repo), not manually configured in the Grafana/Alertmanager UI
- [ ] Each alert threshold validated by synthetically breaching it in staging and confirming the correct destination fires

---

## Testing Strategy

- **Unit Tests**: Validate PromQL rule syntax and expected fire/no-fire behavior against known metric samples using `promtool test rules`
- **Integration Tests**: Deploy the rule set and dashboards against a running I-017 `/metrics` endpoint; confirm dashboards render with live data
- **Staging Fire Drill**: For each of the six signals, synthetically drive the underlying metric past both the warning and critical thresholds (e.g., artificially inflate `poll_queue_depth`) and confirm Slack/PagerDuty receive the correctly routed alert
- **Prior Art**: Mirror Adidos's existing on-call alerting conventions (channel naming, PagerDuty escalation policy structure) for consistency across services

---

## Out of Scope

- Producing the underlying metrics themselves (I-017) — this issue only reads them
- On-call rotation and PagerDuty schedule setup — people/process configuration, not part of this issue
- Writing the runbooks alert annotations link to (I-020)
- Auto-remediation triggered by alerts — no alert in this rule set triggers autoscaling or any automated action; Kubernetes HPA reacts to `poll_queue_depth` directly and independently of Alertmanager (see I-017's Out of Scope)

---

## Related Issues

- I-017: Metrics & Monitoring (produces every metric these rules and dashboards read)
- I-002: Redis Cluster Setup (node status feeds the Infra Health dashboard)
- I-008: Vote Processor Workers (source of queue depth and DB constraint violations shown here)
- I-018: Structured Logging (referenced during incident response, not consumed by alerts directly)
- I-020: Incident Runbooks (the destination of every alert's `runbook_url` annotation)

---

## Implementation Checklist

- [ ] Create `deploy/prometheus/alerts.rules.yml` (all six signals, warning + critical bands)
- [ ] Create `deploy/alertmanager/config.yml` (routing: warning → Slack, critical → PagerDuty)
- [ ] Create `deploy/grafana/dashboards/api-slos.json`
- [ ] Create `deploy/grafana/dashboards/infra-health.json`
- [ ] Add `runbook_url` annotations linking each alert to its I-020 runbook
- [ ] Validate rules with `promtool check rules` and `promtool test rules`
- [ ] Provision dashboards via Grafana's config-based provisioning (not manual UI import)
- [ ] Run a staging fire-drill for each of the six alert thresholds and document results

---

**Acceptance**: All alert rules and dashboards deployed as code, staging fire-drill passes for all six signals, PR reviewed and merged.
