# Databricks notebook source
# MAGIC %md # Batch CosmosDB
# MAGIC 
# MAGIC #### Usage
# MAGIC Supply the parameters above and run the notebook.
# MAGIC 
# MAGIC #### Prerequisites
# MAGIC * The cluster must have the latest Azure Cosmos DB Spark Connector loaded as a Shared Library
# MAGIC * azure-cosmosdb-spark Library must be installed and running
# MAGIC * The supplied externalSystem must have an associated secret scope (of the same name) within Databricks with the following secrets added:
# MAGIC   * endpoint
# MAGIC   * masterKey
# MAGIC   * database
# MAGIC   * preferredRegions
# MAGIC 
# MAGIC #### Details
# MAGIC Instructions for the Spark Connector:
# MAGIC https://docs.databricks.com/spark/latest/data-sources/azure/cosmosdb-connector.html

# COMMAND ----------

import ktk
from ktk import utilities as u
import datetime, json
from pyspark.sql.functions import col

# COMMAND ----------

dbutils.widgets.text(name="stepLogGuid", defaultValue="00000000-0000-0000-0000-000000000000", label="stepLogGuid")
dbutils.widgets.text(name="stepKey", defaultValue="-1", label="stepKey")
dbutils.widgets.text(name="externalSystem", defaultValue="", label="External System")
dbutils.widgets.text(name="schemaName", defaultValue="", label="Table Schema Name")
dbutils.widgets.text(name="tableName", defaultValue="", label="Table Name")
dbutils.widgets.text(name="collection", defaultValue="", label="Collection")
dbutils.widgets.text(name="partitionColumn", defaultValue="", label="Partition Column")
dbutils.widgets.text(name="dateToProcess", defaultValue="", label="Date to Process")
dbutils.widgets.text(name="pushdownPredicate", defaultValue="", label="Pushdown Predicate")

widgets = ["stepLogGuid", "stepKey", "externalSystem", "schemaName", "tableName", "collection", "partitionColumn", "dateToProcess", "pushdownPredicate"]
secrets = ["EndPoint","MasterKey","Database","PreferredRegions"]

# COMMAND ----------

snb = ktk.SingleResponsibilityNotebook(widgets, secrets)

p = {}

parameters = json.dumps(snb.mergeAttributes(p))
snb.log_notebook_start(parameters)
print("Parameters:")
snb.displayAttributes()

# COMMAND ----------

# MAGIC %md #### Read Data from Cosmos DB

# COMMAND ----------

try:
  configMap = {
    "spark.cosmos.accountEndpoint" : snb.EndPoint,
    "spark.cosmos.accountKey" : snb.MasterKey,
    "spark.cosmos.database" : snb.Database,
    "spark.cosmos.container" : snb.collection
  }
  dfList = []
  dfList.append(spark \
    .read \
    .format("cosmos.oltp") \
    .option("spark.cosmos.read.inferSchema.enabled", "true")\
    .options(**configMap) \
    .load())
except Exception as e:
  err = {
    "sourceName" : "Batch Cosmos DB: Read Dataframe",
    "errorCode" : 200,
    "errorDescription" : e.__class__.__name__
  }
  error = json.dumps(err)
  snb.log_notebook_error(error)
  raise(e)

# COMMAND ----------

# MAGIC %md #### Cleanse Columns
# MAGIC * remove special characters and spaces from column names

# COMMAND ----------

try:
  dfList.append(u.cleanseColumns(dfList[-1]))
except Exception as e:
  err = {
    "sourceName" : "Batch Cosmos DB: Cleanse Columns",
    "errorCode" : "300",
    "errorDescription" : e.__class__.__name__
  }
  error = json.dumps(err)
  snb.log_notebook_error(error)
  raise(e)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Pushdown Predicate
# MAGIC This connector requires some kind of filtering predicate, even if it evaluates to all rows.  If one is not supplied, generate a bogus one that evaluates to true

# COMMAND ----------

try:
  if snb.pushdownPredicate == "":
    partitionColumn = snb.partitionColumn.replace("/","")
    dfList.append(dfList[-1].filter(col(partitionColumn) == col(partitionColumn)))
  else:
    dfList.append(dfList[-1].filter(snb.pushdownPredicate))
except Exception as e:
  err = {
    "sourceName" : "Batch Cosmos DB: Pushdown Predicate",
    "errorCode" : "400",
    "errorDescription" : e.__class__.__name__
  }
  error = json.dumps(err)
  snb.log_notebook_error(error)
  raise(e)

# COMMAND ----------

# MAGIC %md #### Write Data to Bronze Zone

# COMMAND ----------

try:
  dfList[-1] \
    .write \
    .mode("OVERWRITE") \
    .json(snb.bronzeDataPath)
except Exception as e:
  err = {
    "sourceName" : "Batch Cosmos DB: Write to Bronze Zone",
    "errorCode" : "500",
    "errorDescription" : e.__class__.__name__
  }
  error = json.dumps(err)
  snb.log_notebook_error(error)
  raise(e)

# COMMAND ----------

# MAGIC %md #### Log Completion

# COMMAND ----------

rows = dfList[-1].count()
snb.log_notebook_end(rows)
dbutils.notebook.exit("Succeeded")
