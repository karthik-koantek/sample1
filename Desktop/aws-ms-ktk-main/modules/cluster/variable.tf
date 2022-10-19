variable "spark_version" {
  description = "Spark Runtime Version for databricks clusters"
  default     = "10.4.x-scala2.12"
}

variable "node_type_id" {
  description = "Type of worker nodes for databricks clusters"
  default     = ""
}

variable "node_type_id_jobs" {
  description = "Type of worker nodes for databricks clusters"
  default     = ""
}


variable "notebook_path" {
  description = "Path to a notebook"
  default     = "/python_notebook"
}

variable "min_workers" {
  description = "Minimum workers in a cluster"
  default     = 1
}

variable "max_workers" {
  description = "Maximum workers in a cluster"
  default     = 2
}

variable "defaultpool_id" {  
  default     = ""
}

variable "workspace" {  
  default     = ""
}

variable "Highpool_id" {  
  default     = ""
}

variable "host" {  
  default     = ""
}

variable "token" {  
  default     = ""
}
