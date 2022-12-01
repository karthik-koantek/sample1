data "databricks_spark_version" "ml" {
  ml= true
  depends_on = [azurerm_databricks_workspace.tfadmin-dbwx]
}

########################################################################UAT jobs
resource "databricks_job" "jobsql" {
  name = var.job_name
  new_cluster {
    // cluster_name              = "UAT_Cluster"
    spark_version             = data.databricks_spark_version.ml.id
    instance_pool_id          = databricks_instance_pool.defaultpool.id
    driver_instance_pool_id   = databricks_instance_pool.defaultpool.id   
    autoscale {
      min_workers = var.min_worker_job
      max_workers = var.max_worker_job
    }
    spark_conf = {
      "spark.hadoop.fs.azure.account.key.${"var.silver_name"}.dfs.core.windows.net" : "{{secrets/internal/SilverGoldStorageAccountKey}}",
      "spark.hadoop.fs.azure.account.key.${"var.bronze_name"}.dfs.core.windows.net" : "{{secrets/internal/BronzeStorageAccountKey}}",
      "spark.databricks.delta.preview.enabled" : true,
      "spark.rpc.message.maxSize" : "2024",
      "ReservedCodeCacheSize"      : "4096m",
      "spark.scheduler.mode" : "FAIR"
    }
  }
  // schedule {
  //   quartz_cron_expression = "0 * * * * ?"
  //   timezone_id = "Asia/Kolkata"
  // }
  notebook_task {
    notebook_path = var.notebook_path_job
	  base_parameters = {
	    "timeoutSeconds" : "43200"
	    "projectName"  :  var.project_name_jobs
	    "threadPool" : "1"
	    "whatIf"     : "0"
    }
 }
  library {
    maven {
      coordinates = "com.microsoft.azure:spark-mssql-connector_2.12:1.2.0"
    }
  }
  library {
    pypi {
      package = "great_expectations"
    }
  }
  library {
    pypi {
      package = "pyodbc"
    }
  }
  library {
    pypi {
      package = "databricks-api"
    }
  }
  library {
    pypi {
      package = "loguru"
    }
  }
  library {
    whl =  "dbfs:/FileStore/jars/ktk-0.0.1-py3-none-any.whl" 
  }
}

resource "databricks_permissions" "job" {
  job_id = databricks_job.jobsql.id
//   access_control {
//     user_name        = databricks_user.me4.user_name
//     permission_level = "IS_OWNER"
//   }
  access_control {
    group_name       = databricks_group.spectators.display_name
    permission_level = "CAN_MANAGE_RUN"
  }
}

