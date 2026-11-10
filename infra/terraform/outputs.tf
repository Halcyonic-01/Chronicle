output "cluster_name" {
  value = module.eks.cluster_name
}

output "kubeconfig_command" {
  description = "Point kubectl at the new cluster."
  value       = "aws eks update-kubeconfig --region ${var.region} --name ${module.eks.cluster_name}"
}

output "ecr_repositories" {
  description = "Image repositories for Chronicle and the victim app."
  value       = { for name, repo in aws_ecr_repository.this : name => repo.repository_url }
}

output "ecr_login_command" {
  value = "aws ecr get-login-password --region ${var.region} | docker login --username AWS --password-stdin ${split("/", aws_ecr_repository.this["chronicle"].repository_url)[0]}"
}

output "prometheus_url" {
  description = "PROMETHEUS_URL for deploy/production/env.yaml."
  value       = "http://monitoring-kube-prometheus-prometheus.monitoring.svc:9090"
}

output "loki_url" {
  description = "LOKI_URL for deploy/production/env.yaml."
  value       = var.install_loki ? "http://loki.monitoring.svc:3100" : ""
}
