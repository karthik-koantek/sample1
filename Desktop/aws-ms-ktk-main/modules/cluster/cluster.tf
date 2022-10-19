############CLUSTER

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


#####autoscaling cluster

resource "databricks_cluster" "sharedautoscaling" {
  cluster_name              = "default"
  spark_version             = data.databricks_spark_version.latest_lts.id
  instance_pool_id          = var.defaultpool_id
  driver_instance_pool_id   = var.defaultpool_id
  autotermination_minutes = 20
  autoscale {
    min_workers = 1
    max_workers = 2
  }
  spark_conf = {
    "spark.databricks.io.cache.enabled" : true,
    "spark.databricks.io.cache.maxDiskUsage" : "50g",
    "spark.databricks.io.cache.maxMetaDataCache" : "1g",
    "spark.databricks.repl.allowedLanguages": "python,sql,scala",
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
  library {
    whl =  "dbfs:/FileStore/jars/ktk-0.0.1-py3-none-any.whl" 
  }
}

#####single node cluster
resource "databricks_cluster" "singlenode" {
   cluster_name            = "SingleNode"
   spark_version           = data.databricks_spark_version.latest_lts.id
   node_type_id            = var.node_type_id
   autotermination_minutes = 20

   spark_conf = {
     # Single-node   
     // "spark.databricks.pyspark.enablePy4JSecurity" : false,
     "spark.databricks.cluster.profile" : "singleNode",
     "spark.master" : "local[*]" 
   }

   custom_tags = {
     "ResourceClass" = "SingleNode"
   }
   aws_attributes {
    ebs_volume_count        = 1
    ebs_volume_size         = 100
  }

   library {
     pypi {
     package = "pyodbc"
     // repo can also be specified here
     // depends_on = [databricks_cluster.singlenode]
     }
   }
  library {
     whl =  "dbfs:/FileStore/jars/ktk-0.0.1-py3-none-any.whl" 
  } 
  
}

######high concurrency cluster

resource "databricks_cluster" "high" {
  cluster_name            = "HighConcurrency"
  spark_version           = data.databricks_spark_version.latest_lts.id
  instance_pool_id          = var.Highpool_id
  driver_instance_pool_id   = var.Highpool_id
  autotermination_minutes = 20
  autoscale {
    min_workers = 1
    max_workers = 2
  }
  spark_conf = {
    "spark.databricks.repl.allowedLanguages": "python,sql,scala",
    // "spark.databricks.cluster.profile": "serverless",
    // "spark.databricks.delta.preview.enabled" : true
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
  library {
    whl =  "dbfs:/FileStore/jars/ktk-0.0.1-py3-none-any.whl" 
  }

  custom_tags = {
    "ResourceClass" = "Serverless"
  }
}
