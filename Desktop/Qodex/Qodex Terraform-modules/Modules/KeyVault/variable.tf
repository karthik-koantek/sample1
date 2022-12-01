variable "resource_group_name" {
  description = "Name of the resource group to be imported."
  type        = string
}


variable "transient_name" {
  type        = string
  default     = ""
}

variable "transient_key" {
  default     = ""
}

variable "kv_name" {
  default     = ""
}
