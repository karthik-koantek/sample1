# Databricks notebook source
# MAGIC %md # Gold Zone Load
# MAGIC #### Initialize

# COMMAND ----------

import ktk
from ktk import utilities as u
import datetime, json
from pyspark.sql.functions import current_timestamp, lit, unix_timestamp
from pyspark.sql.types import TimestampType, BooleanType

# COMMAND ----------

dbutils.widgets.text(name="stepLogGuid", defaultValue="00000000-0000-0000-0000-000000000000", label="stepLogGuid")
dbutils.widgets.text(name="stepKey", defaultValue="-1", label="stepKey")
dbutils.widgets.text(name="viewName", defaultValue="", label="Source View Name")
dbutils.widgets.text(name="tableName", defaultValue="", label="Destination Table Name")
dbutils.widgets.text(name="purpose", defaultValue="general", label="Purpose")
dbutils.widgets.text(name="primaryKeyColumns", defaultValue="", label="Primary Key Cols")
dbutils.widgets.text(name="partitionCol", defaultValue="", label="Partition By Column")
dbutils.widgets.text(name="clusterCol", defaultValue="pk", label="Cluster By Column")
dbutils.widgets.text(name="clusterBuckets", defaultValue="50", label="Cluster Buckets")
dbutils.widgets.text(name="optimizeWhere", defaultValue="", label="Optimize Where")
dbutils.widgets.text(name="optimizeZOrderBy", defaultValue="", label="Optimize Z Order By")
dbutils.widgets.text(name="vacuumRetentionHours", defaultValue="168", label="Vacuum Retention Hours")
dbutils.widgets.dropdown(name="loadType", defaultValue="Merge", choices=["Append", "Overwrite", "Merge", "MergeType2"], label="Load Type")
dbutils.widgets.text(name="mergeSchema", defaultValue="false", label="Merge or Overwrite Schema")
dbutils.widgets.text(name="deleteNotInSource", defaultValue="false", label="Handle Deletions")
dbutils.widgets.dropdown(name="destination", defaultValue="goldgeneral", choices=["silverprotected", "silvergeneral", "goldprotected", "goldgeneral"], label="Destination")
dbutils.widgets.text(name="concurrentProcessingPartitionLiteral", defaultValue="", label="Partition Literal")

widgets = ["stepLogGuid", "stepKey","viewName", "tableName", "purpose","primaryKeyColumns","partitionCol","clusterCol","clusterBuckets","optimizeWhere","optimizeZOrderBy","optimizeZOrderBy","vacuumRetentionHours","loadType","mergeSchema","deleteNotInSource","destination","concurrentProcessingPartitionLiteral",]
secrets = []

# COMMAND ----------

snb = ktk.SingleResponsibilityNotebook(widgets, secrets)

# COMMAND ----------

pkcols = snb.primaryKeyColumns.replace(" ", "")
upsertTableName = "bronze.goldload_{0}_staging".format(snb.viewName.replace(".","_"))
dataPath = "{0}/{1}/{2}".format(snb.basePath, snb.purpose, snb.tableName.replace(".", "_"))

p = {
  "pkcols": pkcols,
  "dataPath": dataPath,
  "upsertTableName": upsertTableName
}
parameters = json.dumps(snb.mergeAttributes(p))
snb.log_notebook_start(parameters)
print("Parameters:")
snb.displayAttributes()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Validation

# COMMAND ----------

if snb.pkcols == "" and snb.loadType in ["Merge", "MergeType2"]:
  errorDescription = "Primary key column(s) were not supplied and are required for Delta Merge."
  err = {
    "sourceName": "Gold Load: Validation",
    "errorCode": "100",
    "errorDescription": errorDescription
  }
  error = json.dumps(err)
  snb.log_notebook_error(error)
  raise ValueError(errorDescription)

# COMMAND ----------

bronzeDFList = []
viewExists = u.sparkTableExists(snb.viewName)
tableExists = u.sparkTableExists(snb.tableName)
if viewExists == True:
  if tableExists == True and snb.loadType in ["Merge", "MergeType2"] and u.checkDeltaFormat(snb.tableName) != "delta":
    errorDescription = "Table {0} is not in Delta format.".format(snb.tableName)
    err = {
      "sourceName": "Delta Load: Validation",
      "errorCode": "100",
      "errorDescription": errorDescription
    }
    error = json.dumps(err)
    snb.log_notebook_error(error)
    raise ValueError(errorDescription)
  else:
    bronzeDFList.append(spark.table(snb.viewName))
else:
  err = {
    "sourceName": "Delta Load: Validation",
    "errorCode": "150",
    "errorDescription": "View {0} does not exist".format(snb.viewName)
  }
  error = json.dumps(err)
  snb.log_notebook_error(error)
  snb.log_notebook_end(0)
  dbutils.notebook.exit("View does not exist")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Stage data in Bronze Zone

# COMMAND ----------

if snb.pkcols == "*":
  snb.pkcols = ",".join(bronzeDFList[-1].columns)

# COMMAND ----------

try:
  bronzeDFList.append(u.pkColSha(bronzeDFList[-1], snb.pkcols))
  if snb.loadType == "MergeType2":
    bronzeDFList.append(
      bronzeDFList[-1]
        .withColumn("effectiveDate", current_timestamp()))
  spark.sql("DROP TABLE IF EXISTS " + snb.upsertTableName)
  bronzeDFList[-1].write.saveAsTable(snb.upsertTableName)
except Exception as e:
  err = {
    "sourceName": "Delta Load: Stage data in Bronze Zone",
    "errorCode": "200",
    "errorDescription": e.__class__.__name__
  }
  error = json.dumps(err)
  snb.log_notebook_error(error)
  raise(e)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Load Delta Table

# COMMAND ----------

try:
  u.saveDataFrameToDeltaTable(snb.upsertTableName, snb.tableName, snb.loadType, snb.dataPath, snb.partitionCol, snb.deleteNotInSource, mergeSchema=snb.mergeSchema, concurrentProcessingPartitionLiteral=snb.concurrentProcessingPartitionLiteral)
except Exception as e:
  err = {
    "sourceName": "Delta Load: Load Delta Table",
    "errorCode": "300",
    "errorDescription": e.__class__.__name__
  }
  error = json.dumps(err)
  snb.log_notebook_error(error)
  raise(e)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Optimize and Vacuum

# COMMAND ----------

u.optimize(snb.tableName, snb.optimizeWhere, snb.optimizeZOrderBy)
u.vacuum(snb.tableName, snb.vacuumRetentionHours)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Log Completion

# COMMAND ----------

rows = bronzeDFList[-1].count()
snb.log_notebook_end(rows)
dbutils.notebook.exit("Succeeded")
