terraform {
  required_version = ">= 1.6"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
    archive = {
      source  = "hashicorp/archive"
      version = "~> 2.0"
    }
  }

  # Uncomment to use S3 remote state
  # backend "s3" {
  #   bucket  = "your-tfstate-bucket"
  #   key     = "ecom-api/terraform.tfstate"
  #   region  = "us-east-1"
  #   encrypt = true
  # }
}

provider "aws" {
  region = var.aws_region

  default_tags {
    tags = {
      Project     = var.project
      Environment = var.environment
      ManagedBy   = "terraform"
    }
  }
}

locals {
  prefix       = "${var.project}-${var.environment}"
  lambda_src   = "${path.module}/../"         # repo root (api/ package lives here)
  zip_output   = "${path.module}/.build/lambda.zip"
}
