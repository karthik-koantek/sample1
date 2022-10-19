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


# provider "aws" {
#   region = var.region
# }

// initialize provider in "MWS" mode to provision new workspace1
# provider "databricks" {
#   alias    = "mws"
#   host     = "https://accounts.cloud.databricks.com"
#   username = var.databricks_account_username
#   password = var.databricks_account_password
# }

provider "databricks" {
  host  = var.host
  token = var.token
}
