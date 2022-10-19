terraform {
  required_providers {
    databricks = {
      source = "databricks/databricks"
      version = "1.2.0"
    }
    aws = {
      source  = "hashicorp/aws"
      version = "~> 4.30.0"
    }
  }
}


provider "aws" {
  region = var.region
  access_key = "${var.AWS_ACCESS_KEY_ID}"
  secret_key = "${var.AWS_SECRET_ACCESS_KEY}"
  //access_key = "xxxxx"
  //secret_key = "xxxx"
}

// initialize provider in "MWS" mode to provision new workspace1
provider "databricks" {
  alias    = "mws"
  host     = "https://accounts.cloud.databricks.com"
  username = var.databricks_account_username
  password = var.databricks_account_password
}
