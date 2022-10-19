data "databricks_node_type" "smallest" {
  local_disk = true
   depends_on = [var.workspace]
}

data "databricks_spark_version" "latest_lts" {
  long_term_support = true
  depends_on = [var.workspace]
}

data "databricks_current_user" "me" {
  depends_on = [var.workspace]
}

data "databricks_spark_version" "ml" {
  ml= true
  depends_on = [var.workspace]
}




resource "databricks_instance_pool" "defaultpool" {
  instance_pool_name = "Default"
  min_idle_instances = 0
  max_capacity       = 3
  node_type_id       = var.node_type_id
  preloaded_spark_versions = [
    data.databricks_spark_version.latest_lts.id
  ]

  idle_instance_autotermination_minutes = 10
  depends_on = [var.workspace]
}

output "defaultpool_id" {
  value = databricks_instance_pool.defaultpool.id
}

#####################
resource "databricks_instance_pool" "jobmarketo" {
  instance_pool_name = "Job_marketo"
  min_idle_instances = 0
  max_capacity       = 15
  node_type_id       = var.node_type_id_jobs
  preloaded_spark_versions = [
    data.databricks_spark_version.ml.id
  ]

  idle_instance_autotermination_minutes = 10
  depends_on = [var.workspace]
}

output "jobmarketo_id" {
  value = databricks_instance_pool.jobmarketo.id
}

###################

resource "databricks_instance_pool" "jobpool" {
  instance_pool_name = "Job"
  min_idle_instances = 0
  max_capacity       = 6
  node_type_id       = var.node_type_id_jobs
  preloaded_spark_versions = [
    data.databricks_spark_version.ml.id
  ]

  idle_instance_autotermination_minutes = 10
  depends_on = [var.workspace]
}

output "jobpool_id" {
  value = databricks_instance_pool.jobpool.id
}

########################################
resource "databricks_instance_pool" "Highpool" {
  instance_pool_name = "HighConcurrency"
  min_idle_instances = 0
  max_capacity       = 3
  node_type_id       = var.node_type_id
  preloaded_spark_versions = [
    data.databricks_spark_version.latest_lts.id
  ]

  idle_instance_autotermination_minutes = 10
  depends_on = [var.workspace]
}

output "Highpool_id" {
  value = databricks_instance_pool.Highpool.id
}
