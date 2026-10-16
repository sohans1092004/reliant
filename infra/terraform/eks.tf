# The cluster runs k8s/app through the k8s/aws overlay. KEDA (ADR-006) is installed
# into it afterwards with its own manifest, as in the local setup.
module "eks" {
  source  = "terraform-aws-modules/eks/aws"
  version = "~> 21.0"

  name               = var.name
  kubernetes_version = var.kubernetes_version

  vpc_id     = module.vpc.vpc_id
  subnet_ids = module.vpc.private_subnets

  endpoint_public_access       = true
  endpoint_public_access_cidrs = var.cluster_admin_cidrs

  # Whoever runs `terraform apply` can use kubectl straight away.
  enable_cluster_creator_admin_permissions = true

  addons = {
    coredns    = {}
    kube-proxy = {}
    vpc-cni    = { before_compute = true }
    # Gives pods IAM roles through their service account (iam.tf), no static keys.
    eks-pod-identity-agent = { before_compute = true }
  }

  eks_managed_node_groups = {
    default = {
      instance_types = var.node_instance_types
      min_size       = var.node_count.min
      desired_size   = var.node_count.desired
      max_size       = var.node_count.max
      # Looks up the newest EKS node AMI in SSM. LocalStack doesn't publish those parameters.
      use_latest_ami_release_version = !var.localstack
    }
  }
}

# Where CI pushes the one image every Deployment runs.
resource "aws_ecr_repository" "app" {
  name                 = var.name
  image_tag_mutability = "IMMUTABLE" # a tag always means the same image (the local :dev tag taught this)

  image_scanning_configuration {
    scan_on_push = true
  }

  encryption_configuration {
    encryption_type = "KMS"
    kms_key         = aws_kms_key.app.arn
  }
}
