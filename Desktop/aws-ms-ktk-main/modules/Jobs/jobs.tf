data "databricks_spark_version" "ml" {
  ml= true
  depends_on = [var.workspace]
}



########################################################################UAT jobs
resource "databricks_job" "jobsql" {
  name = "UAT_SQL"
  new_cluster {
    // cluster_name              = "UAT_Cluster"
    spark_version             = data.databricks_spark_version.ml.id
    instance_pool_id          = var.jobpool
    driver_instance_pool_id   = var.jobpool   
    autoscale {
      min_workers = 1
      max_workers = 3
    }
    spark_conf = {
      "spark.databricks.delta.preview.enabled" : true,
      "spark.rpc.message.maxSize" : "1024",
      "ReservedCodeCacheSize"      : "3072m",
      "spark.scheduler.mode" : "FAIR"
    }
  }
  // schedule {
  //   quartz_cron_expression = "0 * * * * ?"
  //   timezone_id = "Asia/Kolkata"
  // }
  notebook_task {
    notebook_path = "/Repos/karthik@koantek.com/aws-ms-ktk.git/AzureDatabricks/FrameworkNotebooks/Orchestration/Orchestration - Delta"
	  base_parameters = {
      "timeoutSeconds" : "43200"
	    "projectName"  :  "UAT_SQL"
	    "threadPool" : "1"
	    "whatIf"     : "0"
		  "hydrationBehavior"  : "Force"
		  "repoPath"   : "/Workspace/Repos/karthik@koantek.com/aws-ms-ktk.git"
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

# resource "databricks_permissions" "job" {
#   job_id = databricks_job.jobsql.id
# //   access_control {
# //     user_name        = databricks_user.me4.user_name
# //     permission_level = "IS_OWNER"
# //   }
#   access_control {
#     group_name       = databricks_group.spectators.display_name
#     permission_level = "CAN_MANAGE_RUN"
#   }
# }

#################################UAT_ML
resource "databricks_job" "jobsql1" {
  name = "UAT_ML"
  new_cluster {
    // cluster_name              = "UAT_Cluster"
    spark_version             = data.databricks_spark_version.ml.id
    instance_pool_id          = var.jobpool
    driver_instance_pool_id   = var.jobpool   
    autoscale {
      min_workers = 1
      max_workers = 3
    }
    spark_conf = {
      "spark.databricks.delta.preview.enabled" : true,
      "spark.rpc.message.maxSize" : "1024",
      "ReservedCodeCacheSize"      : "3072m",
      "spark.scheduler.mode" : "FAIR"
    }
  }
  // schedule {
  //   quartz_cron_expression = "0 * * * * ?"
  //   timezone_id = "Asia/Kolkata"
  // }
  notebook_task {
    notebook_path = "/Repos/karthik@koantek.com/aws-ms-ktk.git/AzureDatabricks/FrameworkNotebooks/Orchestration/Orchestration - Delta"
	  base_parameters = {
	    "timeoutSeconds" : "43200"
	    "projectName"  :  "UAT_ML"
	    "threadPool" : "1"
	    "whatIf"     : "0"
		  "hydrationBehavior"  : "Force"
		  "repoPath"   : "/Workspace/Repos/karthik@koantek.com/aws-ms-ktk.git"
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

############################################UAT_BigQuery
resource "databricks_job" "jobsql2" {
  name = "UAT_BigQuery"
  new_cluster {
    // cluster_name              = "UAT_Cluster"
    spark_version             = data.databricks_spark_version.ml.id
    instance_pool_id          = var.jobpool
    driver_instance_pool_id   = var.jobpool   
    autoscale {
      min_workers = 1
      max_workers = 3
    }
    spark_conf = {
      "spark.databricks.delta.preview.enabled" : true,
      "spark.rpc.message.maxSize" : "1024",
      "ReservedCodeCacheSize"      : "3072m",
      "spark.scheduler.mode" : "FAIR"
    }
  }
  // schedule {
  //   quartz_cron_expression = "0 * * * * ?"
  //   timezone_id = "Asia/Kolkata"
  // }
  notebook_task {
    notebook_path = "/Repos/karthik@koantek.com/aws-ms-ktk.git/AzureDatabricks/FrameworkNotebooks/Orchestration/Orchestration - Delta"
	  base_parameters = {
	    "timeoutSeconds" : "43200"
	    "projectName"  :  "UAT_BigQuery"
	    "threadPool" : "1"
	    "whatIf"     : "0"
		  "hydrationBehavior"  : "Force"
		  "repoPath"   : "/Workspace/Repos/karthik@koantek.com/aws-ms-ktk.git"
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

#################################################UAAT_Snowflake
resource "databricks_job" "jobsql3" {
  name = "UAT_SNOWFLAKE"
  new_cluster {
    // cluster_name              = "UAT_Cluster"
    spark_version             = data.databricks_spark_version.ml.id
    instance_pool_id          = var.jobpool
    driver_instance_pool_id   = var.jobpool   
    autoscale {
      min_workers = 1
      max_workers = 3
    }
    spark_conf = {
      "spark.databricks.delta.preview.enabled" : true,
      "spark.rpc.message.maxSize" : "1024",
      "ReservedCodeCacheSize"      : "3072m",
      "spark.scheduler.mode" : "FAIR"
    }
  }
  // schedule {
  //   quartz_cron_expression = "0 * * * * ?"
  //   timezone_id = "Asia/Kolkata"
  // }
  notebook_task {
    notebook_path = "/Repos/karthik@koantek.com/aws-ms-ktk.git/AzureDatabricks/FrameworkNotebooks/Orchestration/Orchestration - Delta"
	  base_parameters = {
	    "timeoutSeconds" : "43200"
	    "projectName"  :  "UAT_SNOWFLAKE"
	    "threadPool" : "1"
	    "whatIf"     : "0"
		  "hydrationBehavior"  : "Force"
		  "repoPath"   : "/Workspace/Repos/karthik@koantek.com/aws-ms-ktk.git"
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

##########################################UAT_ExternalFIles
resource "databricks_job" "jobsql4" {
  name = "UAT_ExternalFiles"
  new_cluster {
    // cluster_name              = "UAT_Cluster"
    spark_version             = data.databricks_spark_version.ml.id
    instance_pool_id          = var.jobpool
    driver_instance_pool_id   = var.jobpool   
    autoscale {
      min_workers = 1
      max_workers = 3
    }
    spark_conf = {
      "spark.databricks.delta.preview.enabled" : true,
      "spark.rpc.message.maxSize" : "1024",
      "ReservedCodeCacheSize"      : "3072m",
      "spark.scheduler.mode" : "FAIR"
    }
  }
  // schedule {
  //   quartz_cron_expression = "0 * * * * ?"
  //   timezone_id = "Asia/Kolkata"
  // }
  notebook_task {
    notebook_path = "/Repos/karthik@koantek.com/aws-ms-ktk.git/AzureDatabricks/FrameworkNotebooks/Orchestration/Orchestration - Delta"
	  base_parameters = {
	    "timeoutSeconds" : "43200"
	    "projectName"  :  "UAT_ExternalFiles"
	    "threadPool" : "1"
	    "whatIf"     : "0"
		  "hydrationBehavior"  : "Force"
		  "repoPath"   : "/Workspace/Repos/karthik@koantek.com/aws-ms-ktk.git"
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

###########################Data quality test
resource "databricks_job" "jobsql5" {
  name = "UAT_DataQUality"
  new_cluster {
    // cluster_name              = "UAT_Cluster"
    spark_version             = data.databricks_spark_version.ml.id
    instance_pool_id          = var.jobpool
    driver_instance_pool_id   = var.jobpool   
    autoscale {
      min_workers = 1
      max_workers = 3
    }
    spark_conf = {
      "spark.databricks.delta.preview.enabled" : true,
      "spark.rpc.message.maxSize" : "1024",
      "ReservedCodeCacheSize"      : "3072m",
      "spark.scheduler.mode" : "FAIR"
    }
  }
  // schedule {
  //   quartz_cron_expression = "0 * * * * ?"
  //   timezone_id = "Asia/Kolkata"
  // }
  notebook_task {
    notebook_path = "/Repos/karthik@koantek.com/aws-ms-ktk.git/AzureDatabricks/FrameworkNotebooks/Data Quality Rules Engine/Import Data Quality Rules"
	  base_parameters = {
	    "timeoutSeconds" : "43200"
	    "projectName"  :  "UAT_SQL"
	    "threadPool" : "1"
	    "whatIf"     : "0"
		  "hydrationBehavior"  : "Force"
		  "repoPath"   : "/Workspace/Repos/karthik@koantek.com/aws-ms-ktk.git"
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



