# Cluster add-ons Chronicle reads from. Mirrors scripts/setup.sh, minus Kafka
# and PostgreSQL (events go straight to the managed database).

locals {
  addons = var.install_addons ? 1 : 0
}

# Release name "monitoring" matters: the repo's ServiceMonitors and PodMonitors
# carry the label release=monitoring, which this chart selects on.
resource "helm_release" "monitoring" {
  count = local.addons

  name             = "monitoring"
  namespace        = "monitoring"
  create_namespace = true
  repository       = "https://prometheus-community.github.io/helm-charts"
  chart            = "kube-prometheus-stack"
  version          = "93.1.1"
  timeout          = 900

  values = [yamlencode({
    prometheus = {
      prometheusSpec = {
        retention = "3d"
      }
    }
  })]

  depends_on = [module.eks]
}

# loki-stack is deprecated upstream but is what the kind setup uses.
resource "helm_release" "loki" {
  count = var.install_addons && var.install_loki ? 1 : 0

  name       = "loki"
  namespace  = "monitoring"
  repository = "https://grafana.github.io/helm-charts"
  chart      = "loki-stack"
  version    = "2.10.3"
  timeout    = 600

  values = [yamlencode({
    loki = { isDefault = false }
  })]

  depends_on = [helm_release.monitoring]
}

# Linkerd identity certificates: a trust anchor and an issuer signed by it.
# The private keys live in Terraform state, acceptable for a demo cluster.
resource "tls_private_key" "linkerd_trust_anchor" {
  algorithm   = "ECDSA"
  ecdsa_curve = "P256"
}

resource "tls_self_signed_cert" "linkerd_trust_anchor" {
  private_key_pem       = tls_private_key.linkerd_trust_anchor.private_key_pem
  is_ca_certificate     = true
  validity_period_hours = 87600
  allowed_uses          = ["cert_signing", "crl_signing"]

  subject {
    common_name = "root.linkerd.cluster.local"
  }
}

resource "tls_private_key" "linkerd_issuer" {
  algorithm   = "ECDSA"
  ecdsa_curve = "P256"
}

resource "tls_cert_request" "linkerd_issuer" {
  private_key_pem = tls_private_key.linkerd_issuer.private_key_pem

  subject {
    common_name = "identity.linkerd.cluster.local"
  }
}

resource "tls_locally_signed_cert" "linkerd_issuer" {
  cert_request_pem      = tls_cert_request.linkerd_issuer.cert_request_pem
  ca_private_key_pem    = tls_private_key.linkerd_trust_anchor.private_key_pem
  ca_cert_pem           = tls_self_signed_cert.linkerd_trust_anchor.cert_pem
  is_ca_certificate     = true
  validity_period_hours = 8760
  allowed_uses          = ["cert_signing", "crl_signing"]
}

resource "helm_release" "linkerd_crds" {
  count = local.addons

  name             = "linkerd-crds"
  namespace        = "linkerd"
  create_namespace = true
  repository       = "https://helm.linkerd.io/edge"
  chart            = "linkerd-crds"
  version          = "2026.11.1"

  # The kind setup installs the Gateway API CRDs separately; here the chart does it.
  values = [yamlencode({
    installGatewayAPI = true
  })]

  depends_on = [module.eks]
}

resource "helm_release" "linkerd_control_plane" {
  count = local.addons

  name       = "linkerd-control-plane"
  namespace  = "linkerd"
  repository = "https://helm.linkerd.io/edge"
  chart      = "linkerd-control-plane"
  version    = "2026.11.1"
  timeout    = 600

  values = [yamlencode({
    identityTrustAnchorsPEM = tls_self_signed_cert.linkerd_trust_anchor.cert_pem
    identity = {
      issuer = {
        tls = {
          crtPEM = tls_locally_signed_cert.linkerd_issuer.cert_pem
          keyPEM = tls_private_key.linkerd_issuer.private_key_pem
        }
      }
    }
  })]

  depends_on = [helm_release.linkerd_crds]
}
