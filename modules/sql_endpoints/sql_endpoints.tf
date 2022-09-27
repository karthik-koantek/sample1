data "databricks_current_user" "me" {
  depends_on =[var.workspace]
}

resource "databricks_sql_endpoint" "this" {
  name             = "Endpoint Ascend"
  cluster_size     = "2X-Small"
  max_num_clusters = 1

  # tags {
  #   custom_tags {
  #     key   = "name"
  #     value = "dev"
  #   }
  # }
}
