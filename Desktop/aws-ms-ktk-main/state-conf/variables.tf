variable "region" {
  default = "us-west-2"
}

variable "terra_bucket" {
  default = "ktk-tf-state"
}

variable "dynamodb_name" {
  default = "ktkterraform-lock"
}

variable "account_role_arn" {
  default = "arn:aws:iam::146620047456:user/Terraform"
}

variable "AWS_ACCESS_KEY_ID" {}
variable "AWS_SECRET_ACCESS_KEY" {}
