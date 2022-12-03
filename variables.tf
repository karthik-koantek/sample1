variable "databricks_account_username" {
  default = ""
}

variable "databricks_account_password" {
  default = ""
}

variable "databricks_account_id" {
  default = ""
}

variable "region" {
  default = "us-west-2"
}

variable "AWS_ACCESS_KEY_ID" {
  
  default = ""
}

variable "AWS_SECRET_ACCESS_KEY" {
 
  default = ""
}


variable "vpc_id" {
  default = "vpc-090c114b72a227dc9"
}


variable "cidr_1" {
  default = "10.159.3.0/24"
}

variable "cidr_2" {
  default = "10.159.4.0/24"
}

variable "security_groups_allowed_cidrs" {
  default = "10.159.0.0/24"
}

variable "tags" {
  default = {}
}

resource "random_string" "naming" {
  special = false
  upper   = false
  length  = 6
}

locals {
  prefix = "databricks-ascendgit"
}


