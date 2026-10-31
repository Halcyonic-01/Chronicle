# Production overlay (dry-run)

Chronicle on a real cluster, synced by Argo CD, in **dry-run**: it watches,
diagnoses and proposes, and changes nothing. Kafka is not used; events go straight
to PostgreSQL. The full walk-through is [docs/deployment.md](../../docs/deployment.md).

`TestTheProductionOverlayIsDryRunAndComplete` (in `deploy/manifests_test.go`) fails
if this overlay turns live healing on, disables the kill switch, includes the
executor's write permissions, or misses a migration or console file.

## Fill in before the first sync

Every `CHANGE-ME` must be replaced. None of these are secrets.

| Where | Value | Example |
|---|---|---|
| `kustomization.yaml` `images` | your registry path and the image tag (the Git commit) | `123456789012.dkr.ecr.us-west-2.amazonaws.com/chronicle`, `fb74f8c` |
| `env.yaml` `POSTGRES_URL` (both copies) | database host, name and user | `chronicle.abc123.us-west-2.rds.amazonaws.com`, `chronicle`, `chronicle`. **For Supabase** use the session pooler (`aws-0-<region>.pooler.supabase.com`, port 5432, user `postgres.<ref>`), never the direct `db.<ref>.supabase.co` host (IPv6 only) or port 6543. Keep `?sslmode=require`. |
| `env.yaml` `PROMETHEUS_URL` | your Prometheus | `http://prometheus-operated.monitoring.svc:9090` |
| `env.yaml` `LOKI_URL` | your Loki (optional; without it, log errors are not collected) | `http://loki.monitoring.svc:3100` |
| `env.yaml` `OIDC_ISSUER_URL`, `OIDC_CLIENT_ID` | your identity provider and the client registered for Chronicle | `https://idp.example.com/realms/ops`, `chronicle` |
| `env.yaml` `OIDC_ALLOWED_GROUPS` | the group whose members may approve (two or more people) | `chronicle-approvers` |
| `env.yaml` `CHRONICLE_PUBLIC_URL` | where people will open the console | `https://chronicle.example.com` |
| `../secrets/aws/cluster-secret-store.yaml` | the AWS region | `us-west-2` |

## The database schema

`env.yaml` sets `POSTGRES_SCHEMA: chronicle` on both the migrate init container and
the Chronicle container (a test fails if they ever differ). Chronicle's tables live
there, not in `public`. That matters on Supabase, which publishes `public` through
an API and grants its API roles access to every table created there; see
[docs/deployment.md](../../docs/deployment.md), "Using Supabase". Choose the name
before the first start: changing it later starts an empty record.
`POSTGRES_MAX_CONNS: "5"` bounds each replica's pool; raise it if your database
allows more.

## Secrets (in AWS Secrets Manager, never in Git)

| Secret | Keys | Required? |
|---|---|---|
| `chronicle/config` (JSON) | `CHRONICLE_API_TOKEN` (`openssl rand -hex 32`) | **yes**: in production Chronicle will not start without it |
| | `SLACK_WEBHOOK_URL`, `PAGER_WEBHOOK_URL` | optional |
| | `ARGOCD_URL`, `ARGOCD_TOKEN` (read-only) | recommended with GitOps, so Chronicle knows which workloads Argo CD manages |
| | `RCA_API_KEY`, `GITHUB_TOKEN` | optional |
| `chronicle/postgres` (JSON) | `password` | **yes** |

Give External Secrets Operator an IAM role that can read only these two secrets
(`../secrets/aws/iam-policy.json`), through EKS Pod Identity or IRSA.

## Also needed outside this repository

* **The database**: PostgreSQL 16, managed or replicated, reachable from the
  cluster, with the user above able to create tables (the migrations run at every
  pod start).
* **The OIDC client**: a public client with PKCE and the redirect URI
  `<console URL>/*`.
* **Argo CD**: allow the overlay to read `migrations/` and the console files, in
  `argocd-cm`: `kustomize.buildOptions: --load-restrictor LoadRestrictionsNone`.
* **The image**: built from `Dockerfile` and pushed to the registry above.

Check the result before committing:

```bash
kubectl kustomize --load-restrictor LoadRestrictionsNone deploy/production
```
