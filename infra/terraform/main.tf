data "aws_availability_zones" "available" {
  state = "available"
}

locals {
  azs = slice(data.aws_availability_zones.available.names, 0, 2)
}

# Public subnets only, no NAT gateway: cheaper for a demo.
module "vpc" {
  source  = "terraform-aws-modules/vpc/aws"
  version = "~> 6.0"

  name = var.cluster_name
  cidr = "10.0.0.0/16"
  azs  = local.azs

  public_subnets          = ["10.0.0.0/20", "10.0.16.0/20"]
  map_public_ip_on_launch = true
  enable_nat_gateway      = false

  public_subnet_tags = {
    "kubernetes.io/role/elb" = "1"
  }
}

module "eks" {
  source  = "terraform-aws-modules/eks/aws"
  version = "~> 21.0"

  name               = var.cluster_name
  kubernetes_version = var.kubernetes_version

  endpoint_public_access       = true
  endpoint_public_access_cidrs = var.api_allowed_cidrs

  # Whoever runs apply gets cluster admin.
  enable_cluster_creator_admin_permissions = true

  # No control-plane logs to CloudWatch: billed per GB, not needed for a demo.
  enabled_log_types           = []
  create_cloudwatch_log_group = false

  vpc_id     = module.vpc.vpc_id
  subnet_ids = module.vpc.public_subnets

  addons = {
    vpc-cni = {
      before_compute = true
    }
    eks-pod-identity-agent = {
      before_compute = true
    }
    kube-proxy = {}
    coredns    = {}
  }

  # Pod-to-pod traffic across nodes (Linkerd, victim app, Prometheus scrapes).
  node_security_group_additional_rules = {
    ingress_self_all = {
      description = "Node to node all traffic"
      protocol    = "-1"
      from_port   = 0
      to_port     = 0
      type        = "ingress"
      self        = true
    }
  }

  eks_managed_node_groups = {
    default = {
      ami_type       = "AL2023_x86_64_STANDARD"
      instance_types = [var.node_instance_type]
      capacity_type  = var.node_capacity_type

      min_size     = var.node_count
      max_size     = var.node_count + 1
      desired_size = var.node_count

      block_device_mappings = {
        xvda = {
          device_name = "/dev/xvda"
          ebs = {
            volume_size           = 30
            volume_type           = "gp3"
            delete_on_termination = true
          }
        }
      }
    }
  }
}
