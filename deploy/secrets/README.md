# External secrets

Chronicle reads Kubernetes Secrets as environment variables and needs no code
change for an external secret manager: [External Secrets Operator][eso] (ESO)
keeps `chronicle-config` and `postgres-postgresql` filled from a backend, and
[Reloader][reloader] rolls Chronicle when either changes, because a running
process never re-reads its environment.

| Environment | Backend | Status |
|---|---|---|
| kind (tests) | ESO `fake` provider, `fake/` | **tested**: delivery and rotation, `scripts/test-secret-rotation.sh` |
| AWS / EKS | AWS Secrets Manager, `aws/` | **written, not tested**: needs an AWS account and an IAM role; see below |
| self-hosted or several clouds | OpenBao or Vault through ESO's `vault` provider | **not written**; same `ExternalSecret`s, a different `ClusterSecretStore` |

The decision (AWS Secrets Manager on AWS, OpenBao for self-hosting or a
multi-cloud estate) and its reasons are in the hardening plan; OpenBao would add
an HA cluster, storage and unsealing to run, which a managed secret store does not.

## What lives where

* `base/external-secrets.yaml`: two `ExternalSecret`s. `chronicle-config` is one
  JSON secret per environment whose keys are Chronicle's variable names
  (`RCA_API_KEY`, `GITHUB_TOKEN`, `SLACK_WEBHOOK_URL`, `PAGER_WEBHOOK_URL`,
  `CHRONICLE_API_TOKEN`, ...). `postgres-postgresql` carries the database
  password. Both use `creationPolicy: Owner` and `deletionPolicy: Retain`: ESO
  adopts nothing that exists, and deleting the `ExternalSecret` does not delete the
  Secret Chronicle is running on.
* `aws/` and `fake/`: the `ClusterSecretStore` and, for `aws/`, the least-privilege
  IAM policy (read two named secrets, nothing else).
* `TestSecretsManifestsHoldNoSecretValues` and
  `TestEverySecretChronicleReadsIsDeliveredAndWatched` (`deploy/manifests_test.go`)
  keep real values out of the repository and keep the Deployment, the
  `ExternalSecret`s and the Reloader annotation in step.

## Rotation

Rotating at the source reaches the running process in the ESO refresh interval
(`1m`) plus about 30 seconds for the rolling restart. Measured on kind with a
forced sync: 0 to 1 s for the Secret, 30 s for ready pods that read the new value.
The Deployment rolls with `maxUnavailable: 0`, and the leader lease moves with it.

Rotate the **database password** in this order, or Chronicle loses its database
for the length of a roll: change it in the database first *as an additional
login* where the engine allows, then in the secret manager, then drop the old one.

## Getting AWS Secrets Manager running (not yet done)

1. Create two secrets in the cluster's region: `chronicle/config` (JSON, keys as
   above) and `chronicle/postgres` (JSON, `{"password": "..."}`).
2. Create an IAM role from `aws/iam-policy.json` (fill in `REGION` and
   `ACCOUNT_ID`) and associate it with the ESO ServiceAccount: EKS Pod Identity
   (an association, no annotation), or IRSA (annotate the ServiceAccount with the
   role ARN and uncomment `auth` in `aws/cluster-secret-store.yaml`).
3. Install ESO and Reloader, then `kubectl apply -k deploy/secrets/aws`.
4. Check `kubectl -n chronicle get externalsecret`: both must read `SecretSynced`.

This needs a real cluster and account, so it waits for an explicit decision about
which one; the existing `retail-store` EKS context in the kubeconfig is not used.

[eso]: https://external-secrets.io
[reloader]: https://github.com/stakater/Reloader
