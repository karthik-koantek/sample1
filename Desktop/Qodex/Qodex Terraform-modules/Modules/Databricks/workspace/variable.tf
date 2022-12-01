############################################################ workspace
variable "resource_group_name" {
  description = "Name of the resource group to be imported."
  type        = string
}

variable "vnet_name" {  
  default = ""
}

variable "vnet_ipaddress" {  
  default = ""
}

variable "public_subnet_name" {  
  default = ""
}

variable "public_subnet_ips" {  
  default = ""
}

variable "name" {  
  default = ""
}

variable "private_subnet_name" {  
  default = ""
}

variable "private_subnet_ips" {  
  default = ""
}

variable "databricks_nsg_name" {  
  default = ""
}

variable "databricks_workspace_name" {  
  default = ""
}

