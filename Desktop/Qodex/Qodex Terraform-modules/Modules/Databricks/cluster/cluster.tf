############CLUSTER

data "databricks_node_type" "smallest" {
  local_disk = true
   depends_on = [azurerm_databricks_workspace.tfadmin-dbwx]
}

data "databricks_spark_version" "latest_lts" {
  long_term_support = true
  depends_on = [azurerm_databricks_workspace.tfadmin-dbwx]
}

data "databricks_current_user" "me" {
  depends_on = [azurerm_databricks_workspace.tfadmin-dbwx]
}


#####autoscaling cluster

resource "databricks_cluster" "sharedautoscaling" {
  cluster_name              = var.cluster_name_shared
  spark_version             = var.spark_version_cluster_sh
  instance_pool_id          = databricks_instance_pool.defaultpool.id
  driver_instance_pool_id   = databricks_instance_pool.defaultpool.id
  autotermination_minutes = var.autotermination_minutes_sh
  autoscale {
    min_workers = var.min_workers_sh
    max_workers = var.max_workers_sh
  }
  spark_conf = {
    "spark.databricks.io.cache.enabled" : true,
    "spark.databricks.io.cache.maxDiskUsage" : "50g",
    "spark.databricks.io.cache.maxMetaDataCache" : "1g",
	  "spark.databricks.repl.allowedLanguages": "python,sql,scala",
    "spark.hadoop.fs.azure.account.key.${"var.silver_name"}.dfs.core.windows.net" : "{{secrets/internal/SilverGoldStorageAccountKey}}",
    "spark.hadoop.fs.azure.account.key.${"var.bronze_name"}.dfs.core.windows.net" : "{{secrets/internal/BronzeStorageAccountKey}}",
    "spark.databricks.delta.preview.enabled" : true,
    "spark.databricks.cluster.profile": "serverless",
	  // "spark.databricks.acl.dfAclsEnabled" : true,
	  "spark.sql.adaptive.enabled" : true,
	  "spark.databricks.pyspark.enableProcessIsolation" : true
  }
  library {
    pypi {
    package = "pyodbc"
    // repo can also be specified here
    // depends_on = [databricks_cluster.sharedautoscaling]
    }
  }
  depends_on   = [databricks_secret.SilverGoldStorageAccountKey1sec, databricks_secret.BronzeStorageAccountKey1sec]
  // library {
  //   whl =  "dbfs:/FileStore/jars/ktk-0.0.1-py3-none-any.whl" 
  // }
}


#####single node cluster

resource "databricks_cluster" "singlenode" {
   cluster_name            = var.cluster_name_single
   spark_version           = var.spark_version-single
   node_type_id            = var.node_type_id_single
   autotermination_minutes = var.autotermination_minutes_single

   spark_conf = {
     # Single-node
      "spark.hadoop.fs.azure.account.key.${"var.silver_name"}.dfs.core.windows.net" : "{{secrets/internal/SilverGoldStorageAccountKey}}",
      "spark.hadoop.fs.azure.account.key.${"var.bronze_name"}.dfs.core.windows.net" : "{{secrets/internal/BronzeStorageAccountKey}}",
     "spark.databricks.pyspark.enablePy4JSecurity" : false,
     "spark.databricks.cluster.profile" : "singleNode",
     "spark.master" : "local[*]" 
   }

   custom_tags = {
     "ResourceClass" = "SingleNode"
   }

   library {
     pypi {
     package = "pyodbc"
     // repo can also be specified here
     // depends_on = [databricks_cluster.singlenode]
     }
   }
   depends_on   = [databricks_secret.SilverGoldStorageAccountKey1sec, databricks_secret.BronzeStorageAccountKey1sec] 
}
//   library {
//     whl =  "dbfs:/FileStore/jars/ktk-0.0.1-py3-none-any.whl" 
//   }
// }


######high concurrency cluster

resource "databricks_cluster" "high" {
  cluster_name            = var.cluster_name_high
  spark_version           = var.spark_version_high
  instance_pool_id          = databricks_instance_pool.defaultpool.id
  driver_instance_pool_id   = databricks_instance_pool.defaultpool.id
 autotermination_minutes = var.autotermination_minutes_high
  autoscale {
    min_workers = var.min_workers_high
    max_workers = var.max_workers_high
  }
  spark_conf = {
    "spark.databricks.repl.allowedLanguages": "python,sql,scala",
    // "spark.databricks.cluster.profile": "serverless",
    "spark.databricks.delta.preview.enabled" : true,
    "spark.hadoop.fs.azure.account.key.${"var.silver_name"}.dfs.core.windows.net" : "{{secrets/internal/SilverGoldStorageAccountKey}}",
    "spark.hadoop.fs.azure.account.key.${"var.bronze_name"}.dfs.core.windows.net" : "{{secrets/internal/BronzeStorageAccountKey}}",
    "spark.databricks.delta.preview.enabled" : true,
    "spark.databricks.cluster.profile": "serverless",
	  // "spark.databricks.acl.dfAclsEnabled" : true,
	  "spark.sql.adaptive.enabled" : true,
	  "spark.databricks.pyspark.enableProcessIsolation" : true
  }
  library {
    pypi {
    package = "pyodbc"
    // repo can also be specified here
    // depends_on = [databricks_cluster.high]
    }
  }
  // library {
  //   whl =  "dbfs:/FileStore/jars/ktk-0.0.1-py3-none-any.whl" 
  // }

  custom_tags = {
    "ResourceClass" = "Serverless"
  }
  depends_on   = [databricks_secret.SilverGoldStorageAccountKey1sec, databricks_secret.BronzeStorageAccountKey1sec]
}

