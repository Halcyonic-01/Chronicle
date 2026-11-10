# Argo CD, and the two Applications it syncs from Git.

resource "helm_release" "argocd" {
  count = local.addons

  name             = "argocd"
  namespace        = "argocd"
  create_namespace = true
  repository       = "https://argoproj.github.io/argo-helm"
  chart            = "argo-cd"
  version          = "10.17.1"
  timeout          = 600

  values = [yamlencode({
    # Reached with kubectl port-forward; no SSO, no notifications.
    dex           = { enabled = false }
    notifications = { enabled = false }
    configs = {
      params = { "server.insecure" = true }
      cm = {
        # The overlays read migrations/, console files and victim manifests
        # from outside their own directory.
        "kustomize.buildOptions" = "--load-restrictor LoadRestrictionsNone"
      }
    }
  })]

  depends_on = [module.eks]
}

# Values kept out of the public repository, injected into the Applications.
locals {
  # <account>.dkr.ecr.<region>.amazonaws.com
  registry = split("/", aws_ecr_repository.this["chronicle"].repository_url)[0]

  # Port 5432: Supabase's session pooler (6543, the transaction pooler, breaks
  # Chronicle's migration lock and prepared statements).
  postgres_url = "postgres://${var.database_user}:$(POSTGRES_PASSWORD)@${var.database_host}:5432/${var.database_name}?sslmode=require"

  # A patch's own env entries go first, and Kubernetes expands $(POSTGRES_PASSWORD)
  # only from variables listed above: the password must precede the URL.
  postgres_env = [
    { name = "POSTGRES_PASSWORD" },
    { name = "POSTGRES_URL", value = local.postgres_url },
  ]
  postgres_patch = yamlencode({
    apiVersion = "apps/v1"
    kind       = "Deployment"
    metadata   = { name = "chronicle", namespace = "chronicle" }
    spec = { template = { spec = {
      initContainers = [{ name = "migrate", env = local.postgres_env }]
      containers     = [{ name = "chronicle", env = local.postgres_env }]
    } } }
  })
}

locals {
  argocd_app = {
    namespace = "argocd"
    project   = "default"
    syncPolicy = {
      automated   = { prune = true, selfHeal = true }
      syncOptions = ["CreateNamespace=true"]
    }
  }
}

resource "helm_release" "argocd_apps" {
  count = local.addons

  name       = "argocd-apps"
  namespace  = "argocd"
  repository = "https://argoproj.github.io/argo-helm"
  chart      = "argocd-apps"
  version    = "2.0.8"

  values = [yamlencode({
    applications = {
      chronicle = merge(local.argocd_app, {
        source = {
          repoURL        = var.git_repo_url
          targetRevision = var.git_target_revision
          path           = "deploy/eks/chronicle"
          kustomize = {
            images = ["ecr.invalid/chronicle=${local.registry}/chronicle"]
            patches = [{
              target = { kind = "Deployment", name = "chronicle" }
              patch  = local.postgres_patch
            }]
          }
        }
        destination = { server = "https://kubernetes.default.svc", namespace = "chronicle" }
      })
      victim = merge(local.argocd_app, {
        source = {
          repoURL        = var.git_repo_url
          targetRevision = var.git_target_revision
          path           = "deploy/eks/victim"
          kustomize = {
            images = ["ecr.invalid/chronicle-victim=${local.registry}/chronicle-victim"]
          }
        }
        destination = { server = "https://kubernetes.default.svc", namespace = "default" }
      })
    }
  })]

  # Everything the Applications rely on: CRDs (ExternalSecret, ServiceMonitor),
  # the secret sync, and the mesh for the victim's pods.
  depends_on = [
    helm_release.argocd,
    helm_release.external_secrets,
    helm_release.reloader,
    helm_release.monitoring,
    helm_release.linkerd_control_plane,
  ]
}
