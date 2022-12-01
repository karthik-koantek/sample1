resource "databricks_sql_endpoint" "this" {
  name             = "qsa_endpoint"
  cluster_size     = "2X-Small"
  channel  {   
    name = "CHANNEL_NAME_PREVIEW"
  }       
  max_num_clusters = 1
}

