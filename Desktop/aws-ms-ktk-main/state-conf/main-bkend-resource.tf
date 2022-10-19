# ###File : main.tf
terraform {
  required_providers {
    databricks = {
      source = "databricks/databricks"
    }
    aws = {
      source  = "hashicorp/aws"
      version = "~> 4.15.0"
    }
  }
}

provider "aws" {
  region = var.region
  access_key = "${var.AWS_ACCESS_KEY_ID}"
  secret_key = "${var.AWS_SECRET_ACCESS_KEY}"
  # assume_role {
  #   role_arn = var.account_role_arn
  # }

}

resource "aws_s3_bucket" "terraform_state" {
  bucket = var.terra_bucket

  versioning {
    enabled = true
  }

    lifecycle {
      prevent_destroy = true
    }

    tags = {
      Name = "${var.terra_bucket}"
    }
}


resource "aws_dynamodb_table" "terraform_locks" {
  name         = var.dynamodb_name
  hash_key     = "LockID"
  read_capacity  = 5
  write_capacity = 5

  attribute {
    name = "LockID"
    type = "S"
  }
  
  tags = {
    Name = "${var.dynamodb_name}"
  }
  
}

output "backend_bucket_name" {
  #value = aws_s3_bucket.terraform_state.bucket_domain_name 
  value = aws_s3_bucket.terraform_state.bucket
}

output "backend_dynamodb_name" {
  value = aws_dynamodb_table.terraform_locks.name
}
