# Chronicle

An infrastructure time machine for Kubernetes. Chronicle records what changed
across your cluster, reconstructs the exact state of that cluster at any past
moment, explains why an incident happened, and plans remediation without
executing it behind your back.

## What it does

| Capability | Where it lives |
| --- | --- |
| Normalizes events from Kubernetes, GitHub, Prometheus, Loki, ArgoCD and Terraform into one schema | `internal/collect`, `internal/event` |
| Builds a temporal dependency graph from Kubernetes topology and service-mesh traffic | `internal/graph`, `internal/store/graph.go` |
| Reconstructs cluster state at an arbitrary past instant (snapshot keyframes + event deltas) | `internal/replay` |
| Ranks root-cause candidates deterministically, then has an LLM narrate the ranking | `internal/rca` |
| Plans remediation under confidence floors, rate limits, approval and allowlists | `internal/heal` |
| Serves the operations console and JSON API | `internal/api`, `internal/web` |

Root-cause analysis is deterministic. The LLM never introduces a cause: it is
given the already-ranked evidence and asked to explain it, and every score
carries the multipliers that produced it. If the narrator is unavailable, a
deterministic narrative is used instead.

Failure prediction is intentionally not implemented — there is not enough
observed data to train it honestly.

## Architecture

```
collectors ─┬→ Kafka ─→ writer ─→ PostgreSQL (events) ─┐
            │                  └→ Redis (recent cache) │
            │                                           ├→ replay  → /api/replay
            │  graph sync   ─→ PostgreSQL (graph_edges) ├→ RCA     → /api/analyze
            │  snapshotter  ─→ PostgreSQL (snapshots)   └→ healing → /api/heal/actions
```

Chronicle runs with two replicas. Every replica serves the API and console; a
Kubernetes Lease elects one leader to run the singleton workloads (collectors,
graph sync, snapshotter, database writer). Losing the lease costs a replica
those workloads only — it keeps serving the API and re-enters the election.

## Running it locally

Requires Docker, `kind`, `kubectl`, `helm` and Go 1.26.

```bash
make setup             # kind cluster + Prometheus, Loki, Kafka, PostgreSQL, Redis, Linkerd
make deploy-victim     # the deliberately fragile demo application
make deploy-chronicle  # build the image, apply migrations, roll out Chronicle
```

The console is then reachable by port-forwarding the Service:

```bash
kubectl port-forward -n chronicle svc/chronicle 8181:8181
```

To iterate on the UI without rebuilding the image, use `make deploy-ui`. To
apply migrations against a database you already have, use `make migrate`.

## Configuration

Copy `.env.example` to `.env`. Everything is optional except the PostgreSQL and
Kubernetes connections, which fall back to in-cluster and local-kind defaults.

| Variable | Purpose |
| --- | --- |
| `RCA_API_KEY`, `RCA_API_URL`, `RCA_MODEL` | Narrator, any OpenAI-compatible Chat Completions endpoint. Without a key, RCA uses the deterministic narrative. |
| `KAFKA_BROKERS`, `KAFKA_TOPIC`, `KAFKA_GROUP` | Durable buffer between collectors and the writer. Unset writes straight to PostgreSQL. |
| `REDIS_ADDR`, `REDIS_PASSWORD` | Recent-event cache backing the console's default view. |
| `LOKI_URL`, `LOKI_QUERY` | Log source and its LogQL selector. Defaults to every namespace; scope it (e.g. `{namespace="default"}`) or control-plane chatter drowns out application errors. |
| `GITHUB_TOKEN`, `ARGOCD_*`, `TERRAFORM_*` | Optional collectors; each is skipped when its URL or token is unset. |
| `RCA_DECAY_DIVISOR` | How sharply causal plausibility decays across the window. The time constant is the symptom's own lookback divided by this; default 3 puts three e-folds across the window. |
| `CHRONICLE_API_TOKEN` | When set, every `/api/*` request must present `Authorization: Bearer <token>`. |
| `CHRONICLE_ALLOWED_ORIGIN` | CORS origin. Empty (the default) sends no CORS header; the console is same-origin. |
| `HEAL_*` | Healing safety gates — see below. |

## Safety model for healing

Healing is default-deny at every layer, and all of these must hold before
Chronicle touches Kubernetes:

- `HEAL_LIVE_ENABLED=true` and `HEAL_KILL_SWITCH=false`
- a 30-day dry-run observation period has elapsed since `HEAL_OBSERVATION_STARTED_AT`
- the action type, namespace and exact target are all allowlisted
- the RCA confidence clears the rule's floor and the rule is under its hourly limit
- a reviewer has approved the action with `HEAL_APPROVAL_TOKEN`, sent as
  `X-Chronicle-Heal-Token` (a bearer token in `Authorization` is also accepted)

Only pod restart is executable today. Every decision — including the ones that
were blocked and why — is written to `heal_actions` before anything happens.

Chronicle never reads Secret contents and its ClusterRole does not grant access
to them; Secrets appear in the dependency graph only through the pod specs that
reference them.

## Tests

```bash
go test ./...
```

`make test-phase4` exercises RCA end-to-end against a running cluster.
`make test-phase5-kind` is a destructive healing E2E and is opt-in via
`CHRONICLE_RUN_KIND_E2E=true`.

## License

MIT — see [LICENSE](LICENSE).
