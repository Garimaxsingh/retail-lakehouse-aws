terraform {
  required_version = ">= 1.5"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.6"
    }
  }

  # Local state for now — this is a solo portfolio account, not a team backend.
  # If this account is ever recreated (Free Plan expiry), state is rebuilt via
  # `terraform apply` from scratch rather than migrated, per the project's own
  # "nothing in the account isn't reproducible from Git" rule.
}

provider "aws" {
  region = var.aws_region
}
