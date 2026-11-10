# Chronicle's two secrets, created empty: scripts/set-aws-secrets.sh puts the
# values in, so they never reach Git or Terraform state.
resource "aws_secretsmanager_secret" "chronicle" {
  for_each = toset(["chronicle/config", "chronicle/postgres"])

  name                    = each.key
  recovery_window_in_days = 0 # delete immediately on destroy, so the name can be reused
}

# External Secrets Operator reads them with its pod's own identity (EKS Pod
# Identity): no AWS key is stored anywhere.
data "aws_iam_policy_document" "external_secrets_trust" {
  statement {
    actions = ["sts:AssumeRole", "sts:TagSession"]
    principals {
      type        = "Service"
      identifiers = ["pods.eks.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "external_secrets" {
  name               = "${var.cluster_name}-external-secrets"
  assume_role_policy = data.aws_iam_policy_document.external_secrets_trust.json
}

# Same scope as deploy/secrets/aws/iam-policy.json: these two secrets only.
resource "aws_iam_role_policy" "external_secrets" {
  name = "read-chronicle-secrets"
  role = aws_iam_role.external_secrets.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["secretsmanager:GetSecretValue", "secretsmanager:DescribeSecret"]
      Resource = [for s in aws_secretsmanager_secret.chronicle : s.arn]
    }]
  })
}

resource "aws_eks_pod_identity_association" "external_secrets" {
  cluster_name    = module.eks.cluster_name
  namespace       = "external-secrets"
  service_account = "external-secrets"
  role_arn        = aws_iam_role.external_secrets.arn
}

resource "helm_release" "external_secrets" {
  count = local.addons

  name             = "external-secrets"
  namespace        = "external-secrets"
  create_namespace = true
  repository       = "https://charts.external-secrets.io"
  chart            = "external-secrets"
  version          = "2.15.0"

  # The association must exist before the pod starts, or it gets no credentials.
  depends_on = [module.eks, aws_eks_pod_identity_association.external_secrets]
}

# Rolls Chronicle when its secrets change (annotations on the Deployment).
resource "helm_release" "reloader" {
  count = local.addons

  name             = "reloader"
  namespace        = "reloader"
  create_namespace = true
  repository       = "https://stakater.github.io/stakater-charts"
  chart            = "reloader"
  version          = "2.2.19"

  depends_on = [module.eks]
}
