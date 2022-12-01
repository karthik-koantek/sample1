locals {
  module_path        = abspath(path.module)  
}

resource "azurerm_resource_group" "rg" {
  name     = var.rg_name
  location = var.location
}

module "ADLS" {
  source              = "./Modules/ADLS"
  resource_group_name = azurerm_resource_group.rg.name
  adls_name = var.adls_name
  account_tier = var.account_tier
  account_replication_type = var.account_replication_type 
  depends_on = [azurerm_resource_group.rg]
}

module "BlobStorage" {
  source              = "./Modules/BlobStorage"
  resource_group_name = azurerm_resource_group.rg.name
  blob_name = var.blob_name 
  storage_blob = var.storage_blob 
  account_replication_type = var.account_replication_type 
  depends_on = [azurerm_resource_group.rg]
}

module "KeyVault" {
  source              = "./Modules/KeyVault"
  resource_group_name = azurerm_resource_group.rg.name
  transient_name = module.BlobStorage.storage_account_name
  transient_key = module.BlobStorage.storage_account_primary_access_key1
  kv_name = var.kv_name
  depends_on = [azurerm_resource_group.rg]
}

module "Databricks" {
  source              = "./Modules/Databricks"
  databricks_resource_id     = module.Databricks.id
  resource_group_name = azurerm_resource_group.rg.name
  transient_name = module.BlobStorage.storage_account_name
  transient_key = module.BlobStorage.storage_account_primary_access_key1
  bronze_name =  module.ADLS.storage_account_name2
  bronze_key =  module.ADLS.storage_primary_access_key2
  silver_name = module.Databricks.storage_account_name_meta
  silver_key = module.Databricks.storage_key_meta
  vnet_name = var.vnet_name
  vnet_ipaddress = var.vnet_ipaddress
  public_subnet_name = var.public_subnet_name
  public_subnet_ips = var.public_subnet_ips
  name     = var.name
  private_subnet_name = var.private_subnet_name
  private_subnet_ips = var.private_subnet_ips
  databricks_nsg_name = var.databricks_nsg_name
  databricks_workspace_name = var.databricks_workspace_name
  cluster_name_shared = var.cluster_name_shared
  spark_version_cluster_sh = var.spark_version_cluster_sh
  autotermination_minutes_sh = var.autotermination_minutes_sh 
  min_workers_sh = var.min_workers_sh
  max_workers_sh = var.max_workers_sh
  cluster_name_single = var.cluster_name_single
  spark_version-single = var.spark_version-single
  node_type_id_single = var.node_type_id_single
  autotermination_minutes_single = var.autotermination_minutes_single
  cluster_name_high = var.cluster_name_high
  spark_version_high = var.spark_version_high
  autotermination_minutes_high = var.autotermination_minutes_high
  min_workers_high = var.min_workers_high
  max_workers_high = var.max_workers_high
  instance_pool_name = var.instance_pool_name
  min_idle_instances = var.min_idle_instances
  max_capacity = var.max_capacity
  node_type_id_pool = var.node_type_id_pool
  spark_version_pool = var.spark_version_pool
  min_minute = var.min_minute
  job_name = var.job_name
  min_worker_job = var.min_worker_job
  max_worker_job = var.max_worker_job
  notebook_path_job = var.notebook_path_job
  project_name_jobs = var.project_name_jobs
  git_username = var.git_username
  git_provider = var.git_provider 
  personal_access_token =  var.personal_access_token
  url = var.url
  branch = var.branch 
  scope_name = var.scope_name 
  user_name1 = var.user_name1 
  user_name2 = var.user_name2 
  user_name3 = var.user_name3 
  grp_name_contri = var.grp_name_contri 
  grp_name_reader = var.grp_name_reader 
  access_connector_name = var.access_connector_name 
  uc_storage_name = var.uc_storage_name 
  uc_container_name = var.uc_container_name 
  data_access_key_name = var.data_access_key_name 
  uc_catalog_name = var.uc_catalog_name 
  schema_name = var.schema_name 
  ext_access_connector = var.ext_access_connector 
  adls_container_name = module.ADLS.ext_container
  azure_adls_gen2_id = module.ADLS.storage_account_id2
  adls_gen2 = module.ADLS.storage_account_name2
  adls_gen2_name = module.ADLS.storage_account_name2
  external_location = var.external_location
  unity_cluster_name = var.unity_cluster_name
  depends_on = [azurerm_resource_group.rg]
}

