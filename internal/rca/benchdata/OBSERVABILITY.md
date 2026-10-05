# What Chronicle's RCA can and cannot see

RCA only reasons over events Chronicle recorded. If a cause is not in the list
below, no amount of ranking will find it, and the analyzer is designed to say so
(`verdict: no_root_cause`) rather than invent one. This list was checked against
the collector code (`internal/collect/`), not written from memory.

## Observed

**Kubernetes, via informers**
* Pods: created, deleted, phase and readiness changes (`resource_status`,
  `became_ready`, `became_unready`), container restarts (`container_restart`),
  OOM kills (`oom_kill`).
* Kubernetes Warning events (`k8s_event`): BackOff, Unhealthy, Failed and so on.
* Deployments: created, deleted, ready-replica status; `deploy` (image of the
  **first** container), `resource_change` (resources of the **first** container),
  `config_change` (env, envFrom, command, args, volumes of every container; names
  and a fingerprint only, never values), `scale` (replicas).
* Services: `service_change` (selector, ports, type; names and fingerprint only),
  deletion.
* Nodes: `node_not_ready`, `node_pressure` and its resolution, `became_ready`.
* ConfigMaps **that a pod uses**: `config_change` (changed key names and a
  fingerprint, never values).
* HorizontalPodAutoscalers: `hpa_change`, recorded on the Deployment it scales.
* Changes made while no collector was leading are recovered from a checkpoint
  ConfigMap and marked `missed`. They are stamped when observed unless Kubernetes
  recorded a timestamp within the last two minutes.

**Metrics (Prometheus, three rules)**: `error_spike` (5xx ratio above 5%),
`latency_spike` (p99 above 1s), `memory_pressure` (working set above 90% of the
limit), each with a `_resolved` event.

**Logs (Loki)**: `log_error`, from the query in `LOKI_QUERY`; the shipped
deployment queries the `default` namespace only.

**Optional**: Argo CD syncs and health, Terraform runs, GitHub commits, if
configured. (`terraform_run` and `commit` have no tuned weight.)

## Not observed

* **Anything outside the cluster**: object stores, CDNs, DNS, payment providers,
  cloud load balancers, regional or availability-zone network faults.
* **Secrets** (deliberately: Chronicle has no read access), and credentials or
  certificates rotating or expiring.
* **Ingress**, NetworkPolicy, RBAC, PersistentVolume and PersistentVolumeClaim
  changes.
* **StatefulSets, DaemonSets, Jobs and CronJobs**: no change events (only
  Deployments are watched for `deploy`, `scale`, `resource_change`).
* **Containers after the first**, and init containers, for image and resource
  changes.
* **Labels and annotations**, and who or what made a change beyond the
  field-manager name Kubernetes records.
* **Node problems that do not surface as node conditions** (a kernel fault, a
  full disk reported late, a hung kubelet before it is marked NotReady).
* **Application-level causes with no log line**: a bad query plan, a slow
  dependency that still answers, data corruption.
* **Logs outside the Loki query**, traces, and any metric other than the three
  rules above.
* **Time**: an event must reach the store within 30 seconds of the symptom's own
  ingestion to be seen, a cause must strictly precede its symptom, and causes are
  searched only within a per-symptom lookback window (5 minutes for
  `became_unready`, 10 for `error_spike`, 15 for `latency_spike` and most others,
  60 for `oom_kill`), widened to the start of a continuing failure.

## What RCA does when evidence is missing

* **No recorded root event** (only logs and metrics): `verdict: no_root_cause`.
  The failure's first appearance is still shown, labelled as not a cause.
* **The answer would lead with something that cannot be a cause** (a
  measurement, an effect, a capacity increase): `verdict: no_root_cause`, even
  when weaker changes sit behind it. Each ruled-out candidate carries `not_root`
  saying why, and the console marks it "not a cause".
* **A rollout whose change was not recorded** (the change ingested too late, or
  missed in a collector gap): the new pod's failures, and the old pod's shutdown,
  are effects of something unobserved, so the answer is `no_root_cause`, not the
  restart. This needs the pod's creation inside the analysis window; a pod
  created earlier looks established, and its failure is still named (asserted in
  `TestF2ThePodsCreationMustBeInsideTheWindow`).
* **Several roots the record cannot separate**: `verdict: ambiguous`, with the
  alternatives listed and named in the explanation. Healing declines to act.
  Corroboration is topological: a change that could reach the observed failures
  stays a rival even if its own component shows nothing (silence is not health).
* **A weak single candidate**: `verdict: inconclusive`.
* For `no_root_cause` the confidence field is the strength of the leading
  evidence, not confidence in a cause; the console shows no percentage for it.
* The LLM, when enabled, only narrates this result. It never ranks or decides.
