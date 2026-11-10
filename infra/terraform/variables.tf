variable "aws_account_id" {
  description = "The only AWS account Terraform may act in (the one holding the credits)."
  type        = string
}

variable "region" {
  description = "AWS region."
  type        = string
  default     = "us-east-1"
}

variable "cluster_name" {
  description = "EKS cluster name."
  type        = string
  default     = "chronicle"
}

variable "kubernetes_version" {
  description = "EKS Kubernetes version."
  type        = string
  default     = "1.37"
}

variable "api_allowed_cidrs" {
  description = "CIDRs allowed to reach the Kubernetes API. Set to your own IP (x.x.x.x/32)."
  type        = list(string)
  default     = ["0.0.0.0/0"]
}

variable "node_instance_type" {
  description = "Worker node instance type. t3.large fits ~35 pods per node."
  type        = string
  default     = "t3.large"
}

variable "node_count" {
  description = "Number of worker nodes."
  type        = number
  default     = 2
}

variable "node_capacity_type" {
  description = "ON_DEMAND or SPOT. SPOT is cheaper but nodes can be reclaimed."
  type        = string
  default     = "ON_DEMAND"

  validation {
    condition     = contains(["ON_DEMAND", "SPOT"], var.node_capacity_type)
    error_message = "node_capacity_type must be ON_DEMAND or SPOT."
  }
}

variable "install_addons" {
  description = "Install Prometheus, Loki and Linkerd into the cluster with Helm."
  type        = bool
  default     = true
}

variable "install_loki" {
  description = "Install Loki (optional log source for Chronicle)."
  type        = bool
  default     = true
}

variable "git_repo_url" {
  description = "Git repository Argo CD syncs from."
  type        = string
  default     = "https://github.com/Halcyonic-01/Chronicle"
}

variable "git_target_revision" {
  description = "Branch Argo CD tracks. A feature branch while testing, main once merged."
  type        = string
  default     = "main"
}

variable "database_host" {
  description = "PostgreSQL host. For Supabase, the session pooler (aws-N-<region>.pooler.supabase.com), never db.<ref>.supabase.co."
  type        = string

  validation {
    condition     = !can(regex("^db\\..*\\.supabase\\.co$", var.database_host))
    error_message = "The direct Supabase host is IPv6-only and unreachable from the cluster: use the session pooler host."
  }
}

variable "database_user" {
  description = "PostgreSQL user. For the Supabase pooler: postgres.<project-ref>."
  type        = string
}

variable "database_name" {
  description = "PostgreSQL database name."
  type        = string
  default     = "postgres"
}

variable "budget_limit_usd" {
  description = "Monthly cost budget in USD."
  type        = number
  default     = 50
}

variable "budget_alert_email" {
  description = "Email for budget alerts. Empty skips the budget."
  type        = string
  default     = ""
}
