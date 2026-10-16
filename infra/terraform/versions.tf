terraform {
  required_version = ">= 1.10"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.0"
    }
  }

  # Remote state for a real account: an S3 bucket created once by hand, with S3-native
  # locking (no DynamoDB table needed since Terraform 1.10). Local state otherwise.
  # backend "s3" {
  #   bucket       = "reliant-terraform-state-<account id>"
  #   key          = "reliant/terraform.tfstate"
  #   region       = "us-east-1"
  #   encrypt      = true
  #   use_lockfile = true
  # }
}
