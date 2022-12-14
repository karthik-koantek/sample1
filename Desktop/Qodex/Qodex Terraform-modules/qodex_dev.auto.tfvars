rg_name = "qsa-npd-dbk-eus2-rg01"
location = "East US 2"

#################### ADLS ##########################################################
adls_name = "qsanpddbkeus2adl01" 
account_tier = "Standard"
account_replication_type = "LRS"

######################## Blob_Storage  #############################################################
blob_name = "qsanpddbkeus2blob01" 
storage_blob = "raw"
// account_replication_type = "LRS"
########################### key_valult ####################################################
kv_name = "qsa-npd-dbk-eus2-kv"

################################# databricks ##########################################################
  vnet_name = "qsa-npd-dbk-eus2-vnet01"
  vnet_ipaddress = ["10.0.0.0/18"]
  public_subnet_name = "qsa-npd-dbk-eus2-sn02"
  public_subnet_ips = ["10.0.2.0/24"]
  name     = "qsa"
  private_subnet_name = "qsa-npd-dbk-eus2-sn01"
  private_subnet_ips = ["10.0.1.0/24"]
  databricks_nsg_name = "qsa-npd-dbk-eus2-nsg01"
  databricks_workspace_name = "qsa-npd-dbk-eus2-dbkws01"
  cluster_name_shared = "qsa_shared_default"
  spark_version_cluster_sh = "11.2.x-scala2.12"
  autotermination_minutes_sh = "15"
  min_workers_sh = "1"
  max_workers_sh = "3"
  cluster_name_single = "qsa_single_node"
  spark_version-single = "11.2.x-scala2.12"
  node_type_id_single = "Standard_DS3_v2"
  autotermination_minutes_single = "15"
  cluster_name_high = "qsa_high_concurrency"
  spark_version_high = "11.2.x-scala2.12"
  autotermination_minutes_high = "15"
  min_workers_high = "1"
  max_workers_high = "3"
  instance_pool_name = "qsa_default"
  min_idle_instances = "0"
  max_capacity = "5"
  node_type_id_pool = "Standard_DS3_v2"
  spark_version_pool = ["11.2.x-scala2.12"]
  min_minute = "10"
  job_name = "qsa_UAT_sql"
  min_worker_job = "1"
  max_worker_job = "3"
  notebook_path_job = "/workspace/"
  project_name_jobs = "UAT_SQL"
  git_username = "Momin Umair"
  git_provider ="azureDevOpsServices"
  personal_access_token =  "fytwmddou4ouxuea47zevoitwos36lzaxdw632sicafaentevwoa"
  url = "https://QodexDataServices@dev.azure.com/QodexDataServices/Qodex-DataServices/_git/Qodex-DataServices"
  branch = "Feature" 
  scope_name = "internal" 
  user_name1 = "saptorshe.das@koantek.com" 
  user_name2 = "sayyam.jain@koantek.com"
  user_name3 = "harsh.fadiya@koantek.com" 
  grp_name_contri = "Data Engineer"
  grp_name_reader = "Reader" 
  access_connector_name = "qsa-dbk-mi"
  uc_storage_name = "qsanpddbkeus2adluc01" 
  uc_container_name = "data" 
  data_access_key_name ="qsa-keys" 
  uc_catalog_name = "qsa_uc"
  schema_name = "qsa_db" 
  ext_access_connector = "qsa-ext-dbk-mi" 
  external_location = "qsa_ext_lc"
  unity_cluster_name = "qodex_unity_cluster"
############################################################################################
