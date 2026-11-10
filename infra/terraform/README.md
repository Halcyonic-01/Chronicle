# Chronicle on AWS EKS (demo infrastructure)

A small, short-lived EKS cluster running Chronicle (dry-run) and the victim demo
app, deployed by Argo CD from this repository.

| Layer | Where |
|---|---|
| AWS + cluster add-ons | this directory (Terraform) |
| Chronicle manifests | `deploy/eks/chronicle` (builds on `deploy/production`) |
| Victim manifests | `deploy/eks/victim` |
| Images | `scripts/push-images.sh` → ECR |
| Secrets | `scripts/set-aws-secrets.sh` → AWS Secrets Manager → External Secrets |

## What Terraform creates

| Resource | Detail |
|---|---|
| VPC | 2 public subnets in 2 AZs, **no NAT gateway** (saves ~$1/day) |
| EKS | Kubernetes 1.37, API limited to `api_allowed_cidrs`, no CloudWatch control-plane logs |
| Nodes | 2 × `t3.large`, 30 GiB gp3 |
| ECR | `chronicle`, `chronicle-victim` (deleted with their images on destroy) |
| Secrets Manager | `chronicle/config`, `chronicle/postgres`, created **empty** |
| IAM | a role External Secrets uses through Pod Identity, reading only those two secrets |
| Helm | kube-prometheus-stack (`monitoring`), Loki, Linkerd edge, External Secrets, Reloader, Argo CD |
| Argo CD Applications | `chronicle` → `deploy/eks/chronicle`, `victim` → `deploy/eks/victim`, auto-sync from `git_target_revision` |
| Budget | monthly budget with email alerts (optional) |

Rough cost: ~$0.30/hour (~$7/day) on demand. Check current AWS pricing.

## Run order

```bash
# 0. One-time: fill in values
cp terraform.tfvars.example terraform.tfvars        # account, IP, database host/user, email, branch

# 1. Infrastructure (~25 min; billing starts)
terraform init
terraform plan -out=tfplan
terraform apply tfplan
$(terraform output -raw kubeconfig_command)

# 2. Secrets (prompts for the Supabase password)
../../scripts/set-aws-secrets.sh

# 3. Images, then let Argo CD deploy them
../../scripts/push-images.sh
git commit -am "deploy: images <tag>" && git push
```

Argo CD retries until the images and secrets exist, so steps 2 and 3 may run
in either order.

## Look at it

```bash
kubectl -n argocd port-forward svc/argocd-server 8080:80          # Argo CD
kubectl -n argocd get secret argocd-initial-admin-secret -o jsonpath='{.data.password}' | base64 -d
kubectl -n chronicle port-forward svc/chronicle 8181:8181         # Chronicle console
kubectl -n monitoring port-forward svc/monitoring-grafana 3000:80 # Grafana
```

Checks after the first sync are in [docs/deployment.md](../../docs/deployment.md),
"After the first sync".

## What stays out of Git

The repository is public, so two kinds of values live only in the git-ignored
`terraform.tfvars` and reach the cluster through the Argo CD Application spec:

* **The ECR registry** (it names the AWS account). The overlays use the
  placeholder `ecr.invalid/...` with the real tag; Argo CD swaps in the
  registry (`kustomize.images`) and the tag is kept.
* **The database host and user.** Argo CD patches `POSTGRES_URL` in
  (`kustomize.patches`), listing `POSTGRES_PASSWORD` first so Kubernetes
  expands it.

The password and API token are in AWS Secrets Manager only.
`TestTheEKSOverlayStaysDryRun` fails if an ECR address or a Supabase pooler
host appears under `deploy/eks`.

## Branches

Argo CD tracks `git_target_revision`. Test on a feature branch; after merging,
set it to `main` and `terraform apply` (only the Application changes).

## Tear down

```bash
terraform destroy
```

Then check the EC2 (instances, volumes, load balancers) and VPC consoles for
leftovers. A `LoadBalancer` Service created by hand outside Terraform can block
VPC deletion: delete it first.

## Notes

* State is local (`terraform.tfstate`, git-ignored) and holds Linkerd's private
  keys. Don't lose it before running `destroy`.
* Secret values are never in Terraform state: Terraform creates the secrets
  empty and the script fills them.
* Nodes have public IPs (no NAT); their security group admits nothing from the
  internet.
