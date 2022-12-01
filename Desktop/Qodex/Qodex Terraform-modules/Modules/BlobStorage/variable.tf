variable "resource_group_name" {
  description = "Name of the resource group to be imported."
  type        = string
  default = ""
}


variable "blob_name" {
  type        = string
  default = ""
}

variable "storage_blob" {
  type        = string
  default = ""
}



variable "account_replication_type" {
  type        = string
  default = ""
}