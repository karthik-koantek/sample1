variable "resource_group_name" {
  description = "Name of the resource group to be imported."
  type        = string
}


variable "adls_name" {
   type        = string
   default = ""
}



variable "account_tier" {
   type        = string
  default = ""
}



variable "account_replication_type" {
   type        = string
  default = ""
}

