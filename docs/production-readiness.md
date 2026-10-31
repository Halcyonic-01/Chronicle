# Production readiness

What was tested, what held, what was fixed, and what still needs infrastructure
this repository does not have. Everything below ran on local kind clusters on one
laptop; the last section says what that cannot show. How to deploy to a real
cluster is in [deployment.md](deployment.md).

## Service-level objectives

Measured on kind, with the thresholds in `deploy/monitoring/prometheusrule.yaml`
chosen with room above the measurements. They are starting points to re-fit on a
real cluster.

| Objective | Target | Measured on kind | Metric |
|---|---|---|---|
| Event pipeline lag (emitted to stored) | p99 ≤ 5 s | 99.4 % ≤ 1 s, 100 % ≤ 2 s at 6,000 pods, about 20 events/s | `chronicle_event_lag_seconds` |
| Analysis latency | p99 ≤ 5 s | 2.1 s at 6,000 pods | `chronicle_analyze_duration_seconds` |
| Approval to finished execution | p95 ≤ 5 min | about 60 s live (a restore or rollback, verification included) | `chronicle_heal_decision_to_execution_seconds` |
| Leader present | exactly one, always | 3 s (force delete) to 18 s (frozen, no lease release) to hand over | `chronicle_leader` |
| No event lost | 0 | 0 in every drill below | `chronicle_events_stored_total` and the drill checks |

`chronicle_event_lag_seconds` is the pipeline's own delay. It is *not* an event's
age: a collector reports old facts (pods that existed at start-up carry their
creation time), and an earlier version of the metric that used the event's own
timestamp made 10 % of events look minutes late when they were hours old.

## Scale (KWOK)

KWOK simulates nodes and pods on a disposable kind cluster (`scripts/scale-kwok.sh`);
a local collector then watches it against a scratch PostgreSQL
(`scripts/scale-measure.sh`). Each run rolls every Deployment to generate events,
then analyses 100 synthetic symptoms on random services.

| Simulated | Graph edges | Graph sync | Collector RSS (after churn) | Heap | Analysis p50 / p95 / p99 | Pipeline lag |
|---|---|---|---|---|---|---|
| 300 pods, 50 nodes | 1,372 | 0.05 s | 94 MiB | 34 MiB | 0.04 / 0.07 / 0.07 s | 96 % ≤ 1 s |
| 3,000 pods, 100 nodes | 12,322 | 0.57 s | 322 MiB | 239 MiB | 0.47 / 0.51 / 0.56 s | see note |
| 6,000 pods, 100 nodes | 24,322 | 1.5 s | 566 MiB | 433 MiB | 0.89 / 1.43 / 2.12 s | 99.4 % ≤ 1 s, 100 % ≤ 2 s |

* Memory, sync time and analysis time grow roughly linearly with pods (about
  80 MiB per 1,000 pods). Extrapolated, the Deployment's 1 GiB limit is reached
  near 11,000 pods; that has **not** been tried (the laptop's Docker has 7.6 GiB,
  shared with the e2e cluster), so raise the limit before a cluster that size.
* The collector was run locally on macOS for these numbers; on Linux the process
  figures will differ somewhat. Memory is peak after a full rolling restart of
  every Deployment, which is more churn than steady state.
* The 3,000-pod row's lag was measured with the earlier definition (event age), so
  its tail is the pods that existed at start-up, not pipeline delay.
* This is what a *simulated* cluster shows: real pods make real traffic, real
  metrics and real Kubernetes events. Analysis cost on real incident data will be
  higher than on synthetic symptoms with little else in the window.

## Failure modes

Drills are `scripts/drill-leader-kill.sh` and `scripts/drill-dependency.sh`. Each
makes a steady stream of changes to a zero-replica Deployment, disrupts something,
and then checks that every change is accounted for.

| Failure | What happens | Verified | Fixed in P2 |
|---|---|---|---|
| Leader pod force-deleted | Hand-over in 3 s; no change lost | yes | |
| Leader killed (SIGKILL) | The pod restarts and reclaims its own lease; no change lost | yes | |
| Leader frozen past its lease (SIGSTOP 40 s, then resumed) | Another replica leads after 18 s; the stale one steps down on its first renewal; changes made meanwhile are all recorded; no duplicates | yes | catch-up events now carry template hashes, and a catch-up for a move already recorded is dropped |
| PostgreSQL unreachable 60 s | Events queue in Kafka and are written after; healing and rules endpoints keep answering; event-reading requests fail in 5 s | yes | a request hung until the client gave up (no connect timeout); now 5 s, and every API request has a 60 s budget |
| Kafka down 60 s | Collectors block on a full 10,000-event buffer rather than drop; nothing lost | yes (60 s, light load) | |
| Redis (event cache) down 60 s | Console event list falls back to PostgreSQL | yes | the list hung 15 s per request; now one 1 s wait then 10 s hold-off |
| Prometheus unreachable 60 s | API unaffected; no new metric alerts while it is down | yes | |
| Loki down 60 s | No effect on the rest | yes | |
| Kubernetes API unavailable | not tested | no | |
| OIDC provider down | existing sessions fine for the token's life; new approvals fail; not tested | no | |
| Slack or pager webhook down | notification is best-effort and logged (code), not tested live | no | |

Why a duplicate cannot be stored: events carry a unique content key
(`idx_events_dedupe`: time, source, entity, type, title), and healing claims are
fenced in PostgreSQL by an advisory lock and conditional updates. The worst a
leader overlap can do (at most the 5 s between the lease expiring and a stale
leader's renewal failing) is an event the other leader also reported, which the
store absorbs, and a catch-up summary beside the individual events it covers.

Found and fixed by these drills:

1. A catch-up after a leader change reported a change without its template
   hashes, so the rollback evidence for that revision was missing, and reported it
   a second time when the old leader had already done so.
2. No database connect timeout and no API request deadline: a database outage held
   every request open.
3. The Redis cache's default timeouts and retries made each failing call take 15 s
   or more.
4. `POSTGRES_URL` fell back to a built-in development password when unset. Now
   Chronicle refuses to start without one in a cluster or in production, and in
   production also without `CHRONICLE_API_TOKEN`, or with live healing but no OIDC
   (`internal/startup`, added in P3).
5. Chronicle exposed no metrics of its own, so none of the objectives above could
   be measured; it now serves `/metrics` on `:9102`, and `/healthz`, `/readyz`.

## Open

| Item | Why it is open |
|---|---|
| Soak (24-72 h) | **not run.** A day-long unattended run was judged out of proportion for a laptop, and the script written for it was removed. Without it, a slow leak or database growth over hours is unmeasured; the 6,000-pod run (about 25 minutes of heavy churn) is the longest observation |
| Kubernetes API, OIDC and notification outages | not drilled |
| Kafka outage under load, longer than 60 s | the 10,000-event buffer is in memory: a leader crash during a long broker outage loses what was buffered (changes to Deployments and pods are recovered by the catch-up; log lines and metric alerts are not) |
| Readiness does not follow the database | by design (`/readyz` exists for monitoring); the kubelet probes still check only the console |
| PostgreSQL on kind has no persistence | a restart empties it: the production database must be a managed or replicated one, which was not tested |
| Kafka replication factor 1, one broker | production needs 3 |
| The 1 GiB memory limit near 11,000 pods | extrapolated, not measured |
| Network partitions between nodes, real node loss, cross-zone latency, API-server load at scale, managed-database failover | need a production-like multi-node cluster |
| AWS Secrets Manager | written, not run: needs an AWS account and an explicit decision on which cluster (`deploy/secrets/README.md`) |
