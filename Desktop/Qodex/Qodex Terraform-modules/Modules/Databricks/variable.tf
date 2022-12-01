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

############################################################ cluster
variable "cluster_name_shared" {  
  default = ""
}

variable "spark_version_cluster_sh" {  
  default = ""
}

variable "autotermination_minutes_sh" {  
  default = ""
}

variable "min_workers_sh" {  
  default = ""
}

variable "max_workers_sh" {  
  default = ""
}

variable "silver_name" {  
  default = ""
}

variable "bronze_name" {  
  default = ""
}

variable "cluster_name_single" {  
  default = ""
}

variable "spark_version-single" {  
  default = ""
}

variable "node_type_id_single" {  
  default = ""
}

variable "autotermination_minutes_single" {  
  default = ""
}

variable "cluster_name_high" {  
  default = ""
}

variable "spark_version_high" {  
  default = ""
}

variable "autotermination_minutes_high" {  
  default = ""
}

variable "min_workers_high" {  
  default = ""
}

variable "max_workers_high" {  
  default = ""
}

################################################################### instance pool
variable "instance_pool_name" {  
  default = ""
}

variable "min_idle_instances" {  
  default = ""
}

variable "max_capacity" {  
  default = ""
}

variable "node_type_id_pool" {  
  default = ""
}

variable "spark_version_pool" {  
  default = ""
}

variable "min_minute" {  
  default = ""
}

################################################################ jobs
variable "job_name" {  
  default = ""
}

variable "min_worker_job" {  
  default = ""
}

variable "max_worker_job" {  
  default = ""
}

variable "notebook_path_job" {  
  default = ""
}

variable "project_name_jobs" {  
  default = ""
}

##################################################################### repos
variable "git_username" {
  default     = ""
}

variable "git_provider" {
  default     = ""
}

variable "personal_access_token" {
  default     = ""
}

variable "url" {
  default     = ""
}

variable "branch" {
  default     = ""
}

################################################################### secret & scope
variable "scope_name" {
  default     = ""
}

variable "transient_name" {
  default     = ""
}

variable "transient_key" {
  default     = ""
}

variable "bronze_key" {
  default     = ""
}

variable "silver_key" {
  default     = ""
}

################################################################# users & groups
variable "user_name1" {
  default     = ""
}

variable "user_name2" {
  default     = ""
}

variable "user_name3" {
  default     = ""
}

variable "grp_name_contri" {
  default     = ""
}

variable "grp_name_reader" {
  default     = ""
}

################################################################# unity catalog
variable "access_connector_name" {
  default     = ""
}

variable "uc_storage_name" {
  default     = ""
}

variable "unity_cluster_name" {
  default     = ""
}

variable "uc_container_name" {
  default     = ""
}

variable "data_access_key_name" {
  default     = ""
}

variable "uc_catalog_name" {
  default     = ""
}

variable "schema_name" {
  default     = ""
}

variable "adls_container_name" {
  default     = ""
}

variable "ext_access_connector" {
  default     = ""
}

variable "azure_adls_gen2_id" {
  default     = ""
}

variable "adls_gen2" {
  default     = ""
}

variable "adls_gen2_name" {
  default     = ""
}

variable "external_location" {
  default     = ""
}
################################################################