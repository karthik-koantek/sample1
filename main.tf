locals {
  module_path        = abspath(path.module)  
}

###backend config 1time run
# module "state-conf" {
#   source = "./state-conf"
#   AWS_ACCESS_KEY_ID = "${var.AWS_ACCESS_KEY_ID}"
#   AWS_SECRET_ACCESS_KEY  = "${var.AWS_SECRET_ACCESS_KEY}"
# }

module "S3" {
  source = "./modules/S3"
  AWS_ACCESS_KEY_ID = "${var.AWS_ACCESS_KEY_ID}"
  AWS_SECRET_ACCESS_KEY  = "${var.AWS_SECRET_ACCESS_KEY}"
}

module "cluster" {
  source = "./modules/cluster"
  host  = databricks_mws_workspaces.this.workspace_url
  token = databricks_mws_workspaces.this.token[0].token_value
  Highpool_id = module.InstancePools.Highpool_id
  defaultpool_id = module.InstancePools.defaultpool_id
  workspace = databricks_mws_workspaces.this
  node_type_id = "m4.large"
  node_type_id_jobs = "m4.large"
  # depends_on = [databricks_mws_workspaces.this]
}

module "InstancePools" {
  source = "./modules/InstancePools"
  host  = databricks_mws_workspaces.this.workspace_url
  token = databricks_mws_workspaces.this.token[0].token_value
  workspace = databricks_mws_workspaces.this
  node_type_id = "m4.large"
  node_type_id_jobs = "m4.large"
  # depends_on = [databricks_mws_workspaces.this]
}

module "Init_Script" {
  source = "./modules/Init_Script"
  host  = databricks_mws_workspaces.this.workspace_url
  token = databricks_mws_workspaces.this.token[0].token_value
}

module "jobs" {
  source = "./modules/Jobs"
  host  = databricks_mws_workspaces.this.workspace_url
  token = databricks_mws_workspaces.this.token[0].token_value
  workspace = databricks_mws_workspaces.this
  node_type_id = "m4.large"
  node_type_id_jobs = "m4.large"
  jobpool = module.InstancePools.jobpool_id
}


module "Repos" {
  source = "./modules/Repos"
  host  = databricks_mws_workspaces.this.workspace_url
  token = databricks_mws_workspaces.this.token[0].token_value
}

module "ScopeSecrets" {
  source = "./modules/ScopeSecrets"
  host  = databricks_mws_workspaces.this.workspace_url
  token = databricks_mws_workspaces.this.token[0].token_value
}

module "UserGroup" {
  source = "./modules/UserGroup"
  host  = databricks_mws_workspaces.this.workspace_url
  token = databricks_mws_workspaces.this.token[0].token_value
  workspace = databricks_mws_workspaces.this
}

module "sql_endpoints" {
  source = "./modules/sql_endpoints"
  host  = databricks_mws_workspaces.this.workspace_url
  token = databricks_mws_workspaces.this.token[0].token_value
  workspace = databricks_mws_workspaces.this
}

module "UnityCatalog" {
  source = "./modules/UnityCatalog"
  databricks_account_username = "${var.databricks_account_username}"
  databricks_account_password = "${var.databricks_account_password}"
  databricks_account_id  = "${var.databricks_account_id}"
  databricks_workspace_url = databricks_mws_workspaces.this.workspace_url
  databricks_workspace_ids = ["2902952775272518"]
  databricks_users   = ["prabhat.kumar@koantek.com"]
  databricks_metastore_admins= ["prabhat.kumar@koantek.com", "karthik@koantek.com"]
  tags            = "uc"
  workspace = databricks_mws_workspaces.this
}

