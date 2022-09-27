variable "databricks_account_username" {
  default = ""
}

variable "databricks_account_password" {
  default = ""
}

variable "workspace" {
  default = ""
}

variable "databricks_account_id" {
  default = ""
}

variable "databricks_workspace_url" {
  default = ""
}

variable "region" {
  default = "us-west-2"
}

variable "AWS_ACCESS_KEY_ID" {
  # default = "AKIASEIZ4MBQPC27FQ64"
  default = ""
}

variable "AWS_SECRET_ACCESS_KEY" {
  # default = "qAx09k6vzovU7lptjzjK6wVlENVRW8yfz/2vE6NA"
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



variable "databricks_workspace_ids" {
  type        = list(string)
}

variable "databricks_users" {
  description = <<EOT
  List of Databricks users to be added at account-level for Unity Catalog.
  Enter with square brackets and double quotes
  e.g ["first.last@domain.com", "second.last@domain.com"]
  EOT
  type        = list(string)
}

variable "databricks_metastore_admins" {
  description = <<EOT
  List of Admins to be added at account-level for Unity Catalog.
  Enter with square brackets and double quotes
  e.g ["first.admin@domain.com", "second.admin@domain.com"]
  EOT
  type        = list(string)
}

variable "unity_admin_group" {
  description = "Name of the admin group. This group will be set as the owner of the Unity Catalog metastore"
  type        = string
  default     = "unity_group"
}
