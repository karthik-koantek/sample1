# Databricks notebook source
# MAGIC %md # Cosmos DB
# MAGIC 
# MAGIC 
# MAGIC #### Usage
# MAGIC Supply the parameters above and run the notebook.
# MAGIC 
# MAGIC #### Prerequisites
# MAGIC 1. Table must exist in the Spark Catalog.
# MAGIC 2. The cluster must have the latest Azure Cosmos DB Spark Connector loaded as a Shared Library (azure-cosmosdb-spark Library)
# MAGIC 3. The supplied externalSystem must have an associated secret scope (of the same name) within Databricks with the following secrets added:
# MAGIC   * endpoint
# MAGIC   * masterKey
# MAGIC   * database
# MAGIC   * preferredRegions
# MAGIC 
# MAGIC #### Details
# MAGIC Instructions for the Spark Connector:
# MAGIC 
# MAGIC https://docs.microsoft.com/en-us/azure/cosmos-db/sql/create-sql-api-spark#bk_working_with_connector
# MAGIC 
# MAGIC https://github.com/Azure/azure-sdk-for-java/blob/main/sdk/cosmos/azure-cosmos-spark_3_2-12/docs/configuration-reference.md#write-config

# COMMAND ----------

# MAGIC %md #### Initialize

# COMMAND ----------

import ktk
from ktk import utilities as u
import datetime, json
from pyspark.sql.functions import col
from pyspark.sql.types import StringType

# COMMAND ----------

dbutils.widgets.text(name="stepLogGuid", defaultValue="00000000-0000-0000-0000-000000000000", label="stepLogGuid")
dbutils.widgets.text(name="stepKey", defaultValue="-1", label="stepKey")
dbutils.widgets.text(name="externalSystem", defaultValue="", label="External System")
dbutils.widgets.text(name="collection", defaultValue="", label="Collection")
dbutils.widgets.text(name="tableName", defaultValue="", label="Table Name")
dbutils.widgets.text(name="idColumn", defaultValue="", label="ID Column")

widgets = ["stepLogGuid", "stepKey", "externalSystem", "collection", "tableName", "idColumn"]
secrets = ["EndPoint","MasterKey","Database","PreferredRegions"]

# COMMAND ----------

snb = ktk.SingleResponsibilityNotebook(widgets, secrets)

# COMMAND ----------

p = {}
parameters = json.dumps(snb.mergeAttributes(p))
snb.log_notebook_start(parameters)
print("Parameters:")
snb.displayAttributes()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Refresh Table

# COMMAND ----------

refreshed = u.refreshTable(snb.tableName)
if refreshed == False:
  snb.log_notebook_end( 0)
  dbutils.notebook.exit("Table does not exist")
dfList = []
dfList.append(spark.table(snb.tableName))

# COMMAND ----------

# MAGIC %md
# MAGIC #### ID Column
# MAGIC Cosmos DB Requires a column "id"

# COMMAND ----------

try:
  idColumnExists = [c for c in dfList[-1].columns if c == "id"]
  if len(idColumnExists) == 0:
    dfList.append(dfList[-1].withColumn("id", col(snb.idColumn).cast(StringType())))
except Exception as e:
  err = {
    "sourceName" : "CosmosDB: ID Column",
    "errorCode" : 200,
    "errorDescription" : e.__class__.__name__
  }
  error = json.dumps(err)
  snb.log_notebook_error( error)
  raise(e)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Write to Cosmos DB

# COMMAND ----------

try:
  configMap = {
    "spark.cosmos.accountEndpoint" : snb.EndPoint,
    "spark.cosmos.accountKey" : snb.MasterKey,
    "spark.cosmos.database" : snb.Database,
    "spark.cosmos.container" : snb.collection#,
    #"spark.cosmos.write.strategy": snb.writeStrategy
  }
  dfList[-1] \
    .write \
    .format("cosmos.oltp") \
    .options(**configMap) \
    .mode("APPEND") \
    .save()
except Exception as e:
  err = {
    "sourceName" : "CosmosDB: Write",
    "errorCode" : 300,
    "errorDescription" : e.__class__.__name__
  }
  error = json.dumps(err)
  snb.log_notebook_error( error)
  raise(e)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Log Completion

# COMMAND ----------

rows = dfList[-1].count()
snb.log_notebook_end(rows)
dbutils.notebook.exit("Succeeded")
