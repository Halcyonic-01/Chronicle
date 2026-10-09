# Chaos experiments: Chronicle's RCA on real incidents

The benchmarks in `internal/rca/benchdata` are event streams written by the same
person who wrote the analyzer, on a topology assumed to be complete. A chaos
run tests what they cannot: real faults in the running kind cluster, observed by
Chronicle's real collectors (with their real lag, noise and missing edges),
analysed by the deployed Chronicle through the same API the console uses, and
scored with the same code as the benchmarks (`eval.Score`).

## How an experiment runs

1. **Steady state.** Every Deployment in the victim namespace is fully rolled
   out and ready, and no service is above the thresholds Chronicle itself alerts
   on (the same Prometheus expressions, `collect.AlertRules`). Nothing is
   injected otherwise.
2. **Snapshot, then pre-registration.** Every Deployment and Service spec is
   saved, then the experiment's ground truth and expected verdict are written to
   the log *before* the fault goes in, so the expectation cannot be edited after
   seeing the answer.
3. **Inject** one fault (see the catalog).
4. **Detect.** The harness polls Chronicle's signals and picks the one an
   on-call engineer would be paged by: the first metric alert (`error_spike`,
   `latency_spike`), or the first signal if no alert fires within 4 minutes.
5. **Settle, then ask.** It waits 65 s after that symptom, past the analyzer's
   60 s allowance for late events (the answer is not provisional), calls
   `POST /api/analyze`, and scores the answer. Up to four other symptoms of the
   same incident are analysed too, for consistency.
6. **Restore, always**, even after an error or Ctrl-C: specs from the snapshot,
   chaos NetworkPolicies removed, paused nodes and processes resumed. The run
   stops if the steady state does not return within 6 minutes.
7. **Cool down.** Before every fault, the first included, the steady state must
   have held for 2 minutes. Chronicle treats a recovery shorter than its 60 s
   flap window as a flap, so a fault sooner after the last one merges the two
   incidents and is blamed on the first one's cause (seen live, see below).

The order is random from a recorded seed (`-seed` repeats it), so nobody picks
which incident follows which. Only `kind-*` kube contexts are accepted.

## Catalog

| Experiment | How it is injected | Can Chronicle see the cause? | Expected answer |
|---|---|---|---|
| `redis-scaled-to-zero`, `worker-…`, `api-…`, `postgres-…` | `replicas: 0` | yes, `scale` | `scale` on it |
| `api-bad-config` | `REDIS_URL=redis-typo:6379` | yes, `config_change` | `config_change` on api |
| `worker-bad-config` | `POSTGRES_URL` pointing at a missing host | yes, `config_change` | `config_change` on worker |
| `api-bad-release` | Recreate strategy, then an image that does not exist | yes, `deploy` | `deploy` on api |
| `redis-memory-limit-cut` | Recreate, then a 2Mi memory limit | yes, `resource_change` | `resource_change` on redis |
| `redis-selector-broken` | Service selector that matches no pod, then redis drops its clients so they reconnect | yes, `service_change` | `service_change` on redis |
| `redis-node-lost` | `docker pause` of the node running redis | yes, `node_not_ready` | `node_not_ready` on that node |
| `redis-crash` | `redis-cli shutdown` three times | the restarts, not why | `container_restart` on the redis pod |
| `redis-hang` | `CLIENT PAUSE … WRITE` (the api has no timeouts) | no | `no_root_cause` |
| `redis-network-blocked` | a deny-all ingress NetworkPolicy | no (policies are not watched) | `no_root_cause` |

`redis-node-lost` runs only when redis is alone on its node (agents aside):
`scripts/setup.sh` taints `chronicle-worker2` for chaos targets and the victim's
redis prefers it. Pausing a node that also runs Chronicle would blind the thing
being measured.

## Running it

```bash
make setup && make deploy-victim && make deploy-chronicle
kubectl port-forward -n chronicle svc/chronicle 8181:8181 &
make chaos-plan                                    # the order; touches nothing
make chaos-run                                     # every experiment, about 6 minutes each
make chaos-run CHAOS_ARGS="-only redis-hang,api-bad-config -trials 3"
make chaos-report CHAOS_LOG=chaos-results/run-<time>.jsonl
```

The deployed Chronicle is assumed to be built from the checked-out tree; the
log records the commit and the RCA source hash of that tree.

## Reading a run

* **Status**: `scored` (detected and analysed), `not-detected` (the system left
  its steady state but Chronicle raised nothing: a monitoring gap, not an RCA
  error), `no-effect` (the fault disturbed nothing, so there was nothing to
  diagnose), `skipped` (a precondition refused it), `error` (the harness could
  not carry it out).
* **Scored incidents** get exactly the benchmark metrics: Top-1, declared
  no-root-cause, false confident diagnoses (verdict `root_cause` at ≥ 0.7 and
  wrong), dry-run healing.
* **Live only**: seconds from fault to first signal and to the alert analysed,
  and consistency (the share of the incident's other symptoms answered well).

## What it cannot tell you

* One small application on a laptop cluster. A dozen incidents is a smoke test of
  the pipeline end to end, not an accuracy statistic.
* The faults were chosen by the same author as the analyzer; the order is blind
  and the truth pre-registered, but the catalog is not.
* Experiments run back to back, so a recent experiment's restore can sit in the
  next one's lookback. That is realistic (production always has recent changes),
  and each result records which experiment ran before it and how long ago.
* Each analysis is a real `POST /api/analyze`, so Chronicle records a dry-run
  healing decision for it. Healing never executes.

## What the live runs found (2026-10-05 to 10-08)

Logs: `chaos-results/` (local). Each finding below was invisible to the
synthetic benchmarks.

1. **Ingestion fell minutes behind during an incident.** Chronicle published
   one event per synchronous Kafka write, and kafka-go holds a lone message for
   its 1 s `BatchTimeout`: about one event per second. An outage's log flood
   queued alerts minutes deep, so two faults were "not detected" while the alerts
   sat in the queue. Fixed: 10 ms linger and batched publishing
   (`store.DrainBatch`). Alerts are now readable within ~5 s of being raised.
2. **A lost node was declared after its own first alert.** Kubernetes marks a
   node NotReady about 50 s after its last heartbeat; the user-facing alert
   fired at ~10-25 s. Fixed: `node_not_ready` is dated by the node's Lease
   heartbeat, and `rca.Config.AllowedLateness` is 60 s (was 30 s). Verified:
   `redis-node-lost` went from missed to `node_not_ready` at 0.69.
3. **A genuine crash was blamed on a 10-minute-old rollout.** The F2 rule treated
   any failure within 15 minutes of a pod's creation as the rollout's effect.
   Fixed: a pod that served healthily for longer than the flap window is
   established. Rerun: the right pod is now named, but by `became_unready`
   rather than the pre-registered `container_restart`, at 0.46 (still scored
   as a miss).
4. **Topology gap: postgres was invisible.** The worker reaches postgres through
   a default in its code, Prometheus did not scrape the Linkerd proxies, and the
   mesh query used label names Linkerd does not have, so the observed-traffic
   source had never produced an edge. Fixed: a PodMonitor scrapes the proxies
   (`deploy/victim/monitoring.yaml`) and the mesh query reads Linkerd's HTTP
   requests and TCP connections (`tcp_open_total`). Rerun: `postgres-scaled-to-zero`
   went from "no root cause" to `scale:postgres` at 0.82, every symptom agreeing.
5. **Incidents less than a minute apart merge.** Run back to back, a new fault
   was blamed on the previous incident's cause at 0.94. The harness now waits
   two minutes of steady state before every fault; the analyzer limitation
   (FlapWindow) remains.
6. **A broken Service selector breaks nothing until clients reconnect**
   (no effect twice). The experiment now drops redis's clients after the change.

Full run after fixes 1 and 5 (seed 20261005, 13 incidents): 13 scored, 8 of 11
observable causes correct, both unobservable faults declared "no root cause",
0 false confident diagnoses. The three misses were findings 2-4; all three were
then fixed and rerun (2 and 4 correct, 3 names the right pod by another event). A smoke test of one small app, not an accuracy figure.
