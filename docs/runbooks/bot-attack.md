# Runbook: Bot Attack Exhausting Rate Limit Quota

**Pages from**: `PollDbConstraintViolationsWarning` (any occurrence,
visibility only) / `PollDbConstraintViolationsCritical` (sustained
> 5/min for 5m, possible uniqueness-layer failure) — see
[I-019](../issues/I-019-alerting.md). Also watch for a spike in
rate-limit 429 responses on the **API SLOs** dashboard's **Error
Breakdown by Status Code** panel, which doesn't page on its own but
corroborates this scenario.

---

## Symptom

`poll_db_constraint_violations_total` (I-017) spikes, or duplicate-attempt
anomalies accumulate; rate-limit 429 responses spike across many
distinct IPs; `anomalies` rows (I-014/I-015/I-016) accumulate with
`alert_type = rate_limit_exceeded` or `bot_pattern_detected`.

## Diagnosis

Check `GET /v1/admin/anomalies` (I-013/I-016) for volume and pattern.
The critical distinction: a single hot IP behaves very differently from
load spread thinly across many distinct IPs. The latter pattern — many
IPs, each individually staying under the per-IP limit — is a
coordinated botnet deliberately distributing load to avoid tripping any
one IP's threshold.

## Mitigation

Local defenses apply automatically and need no manual action for that
layer: the 5/user/min and 50/IP/min rate limits and the 3-strike
duplicate block (SPECIFICATION.md decisions #5/#13) are already
enforced by I-007/I-015 regardless of whether an on-call engineer does
anything.

**For a distributed attack spread across many IPs specifically to stay
under per-IP limits, local rate limiting is not sufficient** (Known
Risk #3) — per-IP and per-user limits, by construction, cannot detect
or stop load that's deliberately kept under each individual limit. Local
rate limiting is a **delay tactic**, buying time, not a standalone
solution to a coordinated distributed attack. The real mitigation is
upstream: the 30-minute batch anomaly report to Adidos (I-016) is what
enables Adidos-side IP blacklisting across every service Adidos sees,
which this service alone cannot do.

If urgent, don't wait for the next scheduled tick — manually trigger
the anomaly report job early:

```
kubectl create job --from=cronjob/report-anomalies report-anomalies-manual-$(date +%s)
```

and contact the Adidos platform team directly with the anomaly summary
from `GET /v1/admin/anomalies` rather than waiting for them to notice.

## Escalation

Escalate to the Adidos platform team for upstream IP blacklist /
account-ban action. This service's local defenses are a delay tactic
against a sophisticated distributed attack, not a solution on their
own — resolution requires action outside this service's own rate
limiting.
