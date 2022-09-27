data "databricks_spark_version" "latest" {
  provider = databricks.workspace
  depends_on = [var.workspace]
}
data "databricks_node_type" "smallest" {
  provider   = databricks.workspace
  local_disk = true
  depends_on = [var.workspace]
}

resource "databricks_cluster" "unity_sql" {
  provider                = databricks.workspace
  cluster_name            = "Unity_Ascend"
  spark_version           = data.databricks_spark_version.latest.id
  node_type_id            = data.databricks_node_type.smallest.id
  autotermination_minutes = 20
  enable_elastic_disk     = false
  num_workers             = 2
  aws_attributes {
    availability = "SPOT"
  }
  data_security_mode = "USER_ISOLATION"
}
