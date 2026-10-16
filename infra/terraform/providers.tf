# With localstack = true, every AWS call goes to LocalStack on localhost:4566 instead of
# a real account. That is how this configuration is planned and partly applied for free:
# LocalStack's free edition serves STS, IAM, EC2 (VPC), KMS and S3, but not EKS, RDS,
# ElastiCache or MSK, so those can be planned there but not created.
provider "aws" {
  region = var.region

  access_key                  = var.localstack ? "test" : null
  secret_key                  = var.localstack ? "test" : null
  skip_credentials_validation = var.localstack
  skip_metadata_api_check     = var.localstack
  skip_requesting_account_id  = var.localstack
  s3_use_path_style           = var.localstack

  dynamic "endpoints" {
    for_each = var.localstack ? [1] : []
    content {
      ec2            = "http://localhost:4566"
      ecr            = "http://localhost:4566"
      eks            = "http://localhost:4566"
      elasticache    = "http://localhost:4566"
      iam            = "http://localhost:4566"
      kafka          = "http://localhost:4566"
      kms            = "http://localhost:4566"
      rds            = "http://localhost:4566"
      s3             = "http://localhost:4566"
      secretsmanager = "http://localhost:4566"
      ssm            = "http://localhost:4566"
      sts            = "http://localhost:4566"
    }
  }

  default_tags {
    tags = {
      Project   = "reliant"
      ManagedBy = "terraform"
    }
  }
}
