# Deploying Chronicle to a real cluster

This guide takes Chronicle from the local kind setup to a real cluster, managed
with GitOps (Argo CD here; Flux works the same way), in **dry-run** mode: it
watches, diagnoses and proposes, and changes nothing. Turning on live healing is a
later, separate step, described at the end.

> **Not tested here.** The repository's manifests have only run on kind. The
> production overlay below is an example to adapt: it has not been applied to a
> real cluster, and neither has the AWS Secrets Manager setup or a managed
> database. Check each step on a non-critical cluster first.

## What you need

| Need | Why | Notes |
|---|---|---|
| A Kubernetes cluster | to run Chronicle and be watched by it | Chronicle watches every namespace with read-only RBAC |
| A container registry | the base manifests expect a locally loaded image | build from `Dockerfile`, tag with the Git commit |
| PostgreSQL 16, managed or replicated | the record of everything Chronicle saw and decided | the kind setup runs PostgreSQL **without persistence**; never use it for real |
| Prometheus | the alert source (error rate, latency) | Chronicle reads it; the Prometheus Operator is needed only for `deploy/monitoring` |
| Loki (optional) | application error logs | |
| Kafka with 3 brokers (optional) | a durable buffer between collectors and the writer | or leave `KAFKA_BROKERS` empty to write straight to PostgreSQL |
| An OIDC provider | who may approve; required before live healing | Keycloak was used in testing; any OIDC provider works |
| External Secrets Operator and Reloader | secrets from your secret manager, rolled out on change | see [deploy/secrets/README.md](../deploy/secrets/README.md) |
| Argo CD (or Flux) | to sync the manifests from Git | |

Sizing: about 80 MiB of memory per 1,000 pods was measured; the default 1 GiB
limit is enough for roughly 11,000 pods. Raise it for larger clusters.

## What the base manifests assume, and what to change

`deploy/chronicle` is written for the local kind cluster. A production overlay
must change these:

| In `deploy/chronicle` | Problem in production | Change |
|---|---|---|
| `image: chronicle:latest`, `imagePullPolicy: Never` | the image is loaded into kind, not pulled | point `images:` at your registry; pull policy `IfNotPresent` |
| ConfigMaps `chronicle-migrations` and `chronicle-ui` | created by `scripts/deploy-chronicle.sh`, not stored in Git | generate both with `configMapGenerator` from `migrations/` and `internal/web/static/` |
| `POSTGRES_URL` (container and `migrate` init container) | points at the in-cluster kind database | your database's address, `sslmode=require` |
| `PROMETHEUS_URL`, `LOKI_URL` | point at the kind monitoring stack | your services |
| `KAFKA_BROKERS` | points at a one-broker kind Kafka | your 3-broker Kafka, or empty |
| `CHRONICLE_ENVIRONMENT` | empty | `production`: turns on the start-up refusals and paging |

Keep from the base: two replicas, read-only RBAC, `HEAL_LIVE_ENABLED=false`,
`HEAL_KILL_SWITCH=true`. **Do not include `deploy/heal-executor`**: those are the
write permissions, and dry-run does not need them.

## Secrets

Chronicle reads two Kubernetes Secrets: `chronicle-config` (API token, Slack and
pager webhooks, optional collector tokens) and `postgres-postgresql` (the database
password). In production, External Secrets Operator fills them from your secret
manager; nothing secret goes into Git. For AWS, follow
[deploy/secrets/README.md](../deploy/secrets/README.md) and include
`deploy/secrets/aws` in the overlay.

At minimum `chronicle/config` must hold `CHRONICLE_API_TOKEN`: in production
Chronicle refuses to start without it.

## Example production overlay

Create `deploy/production/` (it is not in the repository):

```yaml
# deploy/production/kustomization.yaml
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
resources:
  - ../chronicle          # not ../heal-executor: dry-run needs no write access
  - ../secrets/aws
  - ../monitoring         # needs the Prometheus Operator CRDs; drop it otherwise
images:
  - name: chronicle
    newName: registry.example.com/chronicle
    newTag: "<git-sha>"
generatorOptions:
  disableNameSuffixHash: true
configMapGenerator:
  - name: chronicle-migrations
    namespace: chronicle
    files: [ ]            # every file in ../../migrations, in order (see below)
  - name: chronicle-ui
    namespace: chronicle
    files: [ ]            # every file in ../../internal/web/static
patches:
  - path: env.yaml
```

<details>
<summary>The two file lists</summary>

```yaml
  - name: chronicle-migrations
    namespace: chronicle
    files:
      - ../../migrations/001_events.sql
      - ../../migrations/002_graph.sql
      - ../../migrations/003_snapshots.sql
      - ../../migrations/004_heal_actions.sql
      - ../../migrations/005_heal_approvals_and_payload.sql
      - ../../migrations/006_heal_execution.sql
      - ../../migrations/007_graph_edge_maintenance.sql
      - ../../migrations/008_event_dedupe.sql
      - ../../migrations/009_heal_outcomes.sql
      - ../../migrations/010_heal_cause_identity.sql
      - ../../migrations/011_heal_safety.sql
      - ../../migrations/012_heal_outcome_history.sql
      - ../../migrations/013_heal_outcome_relabel_v3.sql
      - ../../migrations/014_heal_outcome_history_unique.sql
      - ../../migrations/015_heal_approver_identity.sql
      - ../../migrations/016_heal_symptom_check.sql
      - ../../migrations/017_heal_two_approvers.sql
  - name: chronicle-ui
    namespace: chronicle
    files:
      - ../../internal/web/static/index.html
      - ../../internal/web/static/app.js
      - ../../internal/web/static/auth.js
      - ../../internal/web/static/graph.js
      - ../../internal/web/static/incidents.js
      - ../../internal/web/static/replay.js
      - ../../internal/web/static/app.css
      - ../../internal/web/static/graph.css
      - ../../internal/web/static/graph-controls.css
      - ../../internal/web/static/ops.css
      - ../../internal/web/static/theme.css
```

A new migration or console file must be added here too.
</details>

```yaml
# deploy/production/env.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: chronicle
  namespace: chronicle
spec:
  template:
    spec:
      initContainers:
        - name: migrate
          env:
            - name: POSTGRES_URL
              value: postgres://chronicle:$(POSTGRES_PASSWORD)@db.example.internal:5432/chronicle?sslmode=require
      containers:
        - name: chronicle
          imagePullPolicy: IfNotPresent
          env:
            - {name: CHRONICLE_ENVIRONMENT, value: production}
            - name: POSTGRES_URL
              value: postgres://chronicle:$(POSTGRES_PASSWORD)@db.example.internal:5432/chronicle?sslmode=require
            - {name: PROMETHEUS_URL, value: "http://prometheus.monitoring.svc:9090"}
            - {name: LOKI_URL, value: "http://loki.monitoring.svc:3100"}
            - {name: KAFKA_BROKERS, value: ""}   # or your 3-broker Kafka
            - {name: OIDC_ISSUER_URL, value: "https://idp.example.com/realms/ops"}
            - {name: OIDC_CLIENT_ID, value: chronicle}
            - {name: OIDC_ALLOWED_GROUPS, value: chronicle-approvers}
            - {name: CHRONICLE_PUBLIC_URL, value: "https://chronicle.example.com"}
```

The migrations and console files live outside `deploy/production`, which
Kustomize refuses by default. Allow it in Argo CD (`argocd-cm`:
`kustomize.buildOptions: --load-restrictor LoadRestrictionsNone`), or have CI copy
them into the overlay. Check the result locally before committing:

```bash
kubectl kustomize --load-restrictor LoadRestrictionsNone deploy/production
```

## The Argo CD Application

```yaml
apiVersion: argoproj.io/v1alpha1
kind: Application
metadata:
  name: chronicle
  namespace: argocd
spec:
  project: default
  source:
    repoURL: https://github.com/Halcyonic-01/Chronicle
    targetRevision: main
    path: deploy/production
  destination:
    server: https://kubernetes.default.svc
    namespace: chronicle
  syncPolicy:
    automated: {prune: true, selfHeal: true}
    syncOptions: [CreateNamespace=true]
```

Every pod start re-applies all migrations under an advisory lock; they are
written to be safe to re-run.

## After the first sync

1. Both pods are ready, and `kubectl -n chronicle logs deploy/chronicle` shows no
   `refusing to start` line (that names any missing production setting).
2. `GET /readyz` answers `ready` (the database is reachable).
3. `GET /api/heal/rules` shows `"live_enabled": false` and the kill switch on.
4. Events arrive: change something harmless and find it in the console.
5. Prometheus scrapes both replicas (`up{job="chronicle"}`), and the
   `ChronicleNoLeader` alert is not firing.
6. When an alert fires, run the analysis on it in the console and read the
   proposal it records.

## How Chronicle behaves with GitOps

A workload that Argo CD manages is never written to by Chronicle: Argo CD would
undo the change. For those, Chronicle proposes reverting the change in Git
instead. Give Chronicle `ARGOCD_URL` and `ARGOCD_TOKEN` (read-only) so it knows
which Applications exist; without them it recognises Argo CD by the tracking
annotations alone. Workloads run by Argo Rollouts or Flagger are likewise left to
those controllers.

## Before turning on live healing

Run in dry-run for at least 30 days. Live healing stays refused until the record
shows, overall and for each action type separately, at least 20 decisions with a
judged outcome and at least 80% of them right. Watch it build up at
`GET /api/heal/calibration` (`evidence`, `evidence_by_action`, and the
`reliability` table, which says when an action type is `ready_for_floor_review`).

When it is ready, all of these change together, in Git:

* add `../heal-executor` to the overlay (namespaced write permissions);
* set `HEAL_ALLOWED_NAMESPACES`, `HEAL_ALLOWED_ACTIONS` and `HEAL_ALLOWED_TARGETS`
  to the smallest useful set;
* make sure at least two people are in the OIDC approver group (rollbacks and
  memory bumps need two);
* set `HEAL_LIVE_ENABLED=true` and `HEAL_KILL_SWITCH=false`.

`HEAL_KILL_SWITCH=true` stops every execution at once, and reverting the commit
does the same through GitOps. Restarts stay proposal-only until
`HEAL_RESTART_EXECUTION_ENABLED=true`, which should wait until the restart path
has been tested on your cluster.
