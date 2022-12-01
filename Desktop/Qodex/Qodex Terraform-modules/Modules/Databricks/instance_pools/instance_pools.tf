########################################################instance_pool

resource "databricks_instance_pool" "defaultpool" {
  instance_pool_name = var.instance_pool_name
  min_idle_instances = var.min_idle_instances
  max_capacity       = var.max_capacity
  node_type_id       = var.node_type_id_pool
  preloaded_spark_versions = var.spark_version_pool
  

  idle_instance_autotermination_minutes = var.min_minute
  depends_on = [azurerm_databricks_workspace.tfadmin-dbwx]
}

output "defaultpool_id" {
  value = databricks_instance_pool.defaultpool.id
}

