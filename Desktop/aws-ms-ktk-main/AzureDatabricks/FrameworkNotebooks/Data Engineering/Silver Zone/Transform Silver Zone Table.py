# Databricks notebook source
# MAGIC %md # Transform Silver Zone Table

# COMMAND ----------

# MAGIC %sql
# MAGIC --SHOW DATABASES;
# MAGIC --USE silverunencrypted;
# MAGIC --SHOW TABLES;
# MAGIC
# MAGIC --SELECT * FROM adventureworkslt_saleslt_address
# MAGIC --columns to encrypt: AddressLine1,AddressLine2,City
# MAGIC --timestamp columns: ModifiedDate
# MAGIC --format "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"
# MAGIC
# MAGIC --SELECT * FROM silverunencrypted.adventureworkslt_saleslt_salesorderheader
# MAGIC --timestamp Columns: DueDate,ModifiedDate,OrderDate,ShipDate
# MAGIC --format "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"
# MAGIC
# MAGIC --SELECT * FROM silverunencrypted.externalblobstore_youtube_youtube
# MAGIC --explodeAndFlatten: True
# MAGIC
# MAGIC --SELECT * FROM silverencrypted.onpremfilestorage_complex_databricksreadme
# MAGIC --explodeAndFlatten: True
# MAGIC --cleanseColumns: True
# MAGIC
# MAGIC --SELECT * FROM silverunencrypted.externalblobstore_userdata_userdata1
# MAGIC --columns to encrypt: cc,birthdate,email,first_name,ip_address,last_name,salary
# MAGIC --columns to convert to timestamp: registration_dttm
# MAGIC --timestamp format: ? 2016-02-03T02:19:07.000Z
# MAGIC --                    "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"

# COMMAND ----------

# MAGIC %md #### Initialize

# COMMAND ----------

# MAGIC %run "../../Orchestration/Notebook Functions"

# COMMAND ----------

# MAGIC %run ../../Development/Utilities

# COMMAND ----------

import ktk
from ktk import utilities as u
from pyspark.sql.functions import explode, col, unix_timestamp, coalesce, lit
from pyspark.sql.types import TimestampType
from datetime import datetime, timedelta
import json
import hashlib

# COMMAND ----------

dbutils.widgets.text(name="stepLogGuid", defaultValue="00000000-0000-0000-0000-000000000000", label="stepLogGuid")
dbutils.widgets.text(name="stepKey", defaultValue="-1", label="stepKey")
dbutils.widgets.text(name="externalSystem", defaultValue="", label="External System")
dbutils.widgets.text(name="schemaName", defaultValue="", label="Table Schema Name")
dbutils.widgets.text(name="tableName", defaultValue="", label="Table")
dbutils.widgets.text(name="deltaHistoryHours", defaultValue="-1", label="History Hours")
dbutils.widgets.text(name="destinationTableName", defaultValue="", label="Destination Table")
dbutils.widgets.dropdown(name="explodeAndFlatten", defaultValue="True", choices=["True", "False"], label="Explode and Flatten")
dbutils.widgets.dropdown(name="cleanseColumnNames", defaultValue="True", choices=["True", "False"], label="Cleanse Columns")
dbutils.widgets.text(name="encryptColumns", defaultValue="", label="Encrypt Columns")
dbutils.widgets.text(name="timestampColumns", defaultValue="", label="Timestamp Columns")
dbutils.widgets.text(name="timestampFormat", defaultValue="yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", label="Timestamp Format")
dbutils.widgets.text(name="partitions", defaultValue="8", label="Number of Partitions")
dbutils.widgets.text(name="primaryKeyColumns", defaultValue="", label="Primary Key Cols")
dbutils.widgets.text(name="partitionCol", defaultValue="", label="Partitioned By Column")
dbutils.widgets.text(name="clusterCol", defaultValue="pk", label="Clustered By Column")
dbutils.widgets.text(name="clusterBuckets", defaultValue="50", label="Cluster Buckets")
dbutils.widgets.text(name="optimizeWhere", defaultValue="", label="Optimize Where")
dbutils.widgets.text(name="optimizeZOrderBy", defaultValue="", label="Optimize Z Order By")
dbutils.widgets.text(name="vacuumRetentionHours", defaultValue="168", label="Vacuum Retention Hours")
dbutils.widgets.dropdown(name="loadType", defaultValue="Merge", choices=["Append", "Overwrite", "Merge"], label="Load Type")
dbutils.widgets.dropdown(name="destination", defaultValue="silvergeneral", choices=["silverprotected", "silvergeneral"], label="Destination")

widgets = ["stepLogGuid","stepKey","externalSystem","schemaName","tableName","deltaHistoryHours","destinationTableName","explodeAndFlatten",
"cleanseColumnNames","encryptColumns","timestampColumns","timestampFormat","partitions","primaryKeyColumns","partitionCol","clusterCol",
"clusterBuckets","optimizeWhere","optimizeZOrderBy","vacuumRetentionHours","loadType","destination"]
secrets = []

# COMMAND ----------

snb = ktk.SingleResponsibilityNotebook(widgets, secrets)

# COMMAND ----------

deltaHistoryHours = int(snb.deltaHistoryHours)
upsertTableName = "bronze.{0}_upsert".format(snb.destinationTableName)
pkcols = snb.pkcols.replace(" ", "")

p = {
  "deltaHistoryHours": deltaHistoryHours,
  "pkcols": pkcols,
  "upsertTableName": upsertTableName
}
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
  snb.log_notebook_end(0)
  dbutils.notebook.exit("Table does not exist")
dfList = []
dfList.append(spark.table(snb.tableName))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Obtain Change Delta

# COMMAND ----------

if snb.deltaHistoryHours != -1:
  try:
    print("Obtaining time travel delta")
    dfList.append(u.getTableChangeDelta(dfList[-1], snb.tableName, snb.deltaHistoryMinutes))
    if dfList[-1].count == 0:
      snb.log_notebook_end(0)
      dbutils.notebook.exit("No new or modified rows to process.")
  except Exception as e:
    err = {
      "sourceName" : "Transform Silver Zone Table: Obtain Change Delta",
      "errorCode" : "100",
      "errorDescription" : e.__class__.__name__
    }
    error = json.dumps(err)
    snb.log_notebook_error(error)
    raise(e)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Initial Schema

# COMMAND ----------

dfList[-1].printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Flatten and Explode

# COMMAND ----------

if snb.explodeAndFlatten == "True":
  try:
    print("Exploding and Flattening JSON Arrays/Structs")
    dfList.append(u.flattenAndExplodeRecursive(dfList[-1], 10))
    dfList[-1].printSchema()
  except Exception as e:
    err = {
      "sourceName" : "Transform Silver Zone Table: Flatten and Explode",
      "errorCode" : "200",
      "errorDescription" : e.__class__.__name__
    }
    error = json.dumps(err)
    snb.log_notebook_error(error)
    raise(e)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Cleanse Columns

# COMMAND ----------

if snb.cleanseColumnNames == "True":
  try:
    print("Cleansing Table Column Names")
    dfList.append(u.cleanseColumns(dfList[-1]))
    dfList[-1].printSchema()
  except Exception as e:
    err = {
      "sourceName" : "Transform Silver Zone Table: Cleanse Columns",
      "errorCode" : "300",
      "errorDescription" : e.__class__.__name__
    }
    error = json.dumps(err)
    snb.log_notebook_error(error)
    raise(e)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Timestamp Columns

# COMMAND ----------

if snb.timestampColumns != "":
  try:
    timestampColumnList = snb.timestampColumns.split(",")
    for ts in timestampColumnList:
      tsColumnName = "{0}_ts".format(ts)
      dfList.append(dfList[-1].withColumn(tsColumnName, unix_timestamp(ts, snb.timestampFormat).cast(TimestampType())))
    dfList[-1].printSchema()
  except Exception as e:
    err = {
      "sourceName" : "Transform Silver Zone Table: Timestamp Columns",
      "errorCode" : "400",
      "errorDescription" : e.__class__.__name__
    }
    error = json.dumps(err)
    snb.log_notebook_error(error)
    raise(e)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Encrypt Columns

# COMMAND ----------

if snb.encryptColumns != "":
  try:
    for e in snb.encryptColumns.split(","):
      encryptionColumnName = "{0}_encrypted".format(e.strip())
      dfList.append(dfList[-1].withColumn(e, coalesce(e,lit('null'))).withColumn(encryptionColumnName, u.encryptColumn(e)))
    dfList[-1].printSchema()
  except Exception as e:
    err = {
      "sourceName" : "Transform Silver Zone Table: Encrypt Columns",
      "errorCode" : "500",
      "errorDescription" : e.__class__.__name__
    }
    error = json.dumps(err)
    snb.log_notebook_error(error)
    raise(e)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Stage data in Bronze Zone

# COMMAND ----------

try:
  pkdf = u.pkCol(dfList[-1], snb.pkcols)
  spark.sql("DROP TABLE IF EXISTS " + snb.upsertTableName)
  pkdf.write.saveAsTable(snb.upsertTableName)
except Exception as e:
  err = {
    "sourceName": "Transform Silver Zone Table: Stage data in Bronze Zone",
    "errorCode": "600",
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
  u.saveDataFrameToDeltaTable(snb.upsertTableName, snb.destinationTableName, snb.loadType, snb.silverDataPath, snb.partitionCol)
except Exception as e:
  err = {
    "sourceName": "Transform Silver Zone Table: Load Delta Table",
    "errorCode": "700",
    "errorDescription": e.__class__.__name__
  }
  error = json.dumps(err)
  snb.log_notebook_error(error)
  raise(e)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Optimize and Vacuum

# COMMAND ----------

u.optimize(snb.destinationTableName, snb.optimizeWhere, snb.optimizeZOrderBy)
u.vacuum(snb.destinationTableName, snb.vacuumRetentionHours)

# COMMAND ----------

# MAGIC %md #### Log Completion

# COMMAND ----------

rows = pkdf.count()
snb.log_notebook_end(rows)
dbutils.notebook.exit("Succeeded")