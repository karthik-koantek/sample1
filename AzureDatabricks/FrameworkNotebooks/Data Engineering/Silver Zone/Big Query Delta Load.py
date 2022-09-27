# Databricks notebook source
# MAGIC %md # Delta Load

# COMMAND ----------

# MAGIC %md
# MAGIC #### Initialize

# COMMAND ----------

import ktk
from ktk import utilities as u
from pyspark.sql.functions import explode, col, unix_timestamp, coalesce, lit, explode_outer
from pyspark.sql.types import TimestampType
from datetime import datetime, timedelta
import json
import hashlib
spark.conf.set("spark.databricks.delta.merge.joinBasedMerge.enabled", True)
spark.conf.set("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation","true")

dbutils.widgets.text(name="stepLogGuid", defaultValue="00000000-0000-0000-0000-000000000000", label="stepLogGuid")
dbutils.widgets.text(name="stepKey", defaultValue="-1", label="stepKey")
dbutils.widgets.text(name="dateToProcess", defaultValue="", label="Date to Process")
dbutils.widgets.text(name="externalSystem", defaultValue="", label="External System")
dbutils.widgets.text(name="partitions", defaultValue="760", label="Number of Partitions")
dbutils.widgets.text(name="primaryKeyColumns", defaultValue="", label="Primary Key Cols")
dbutils.widgets.text(name="schemaName", defaultValue="", label="Table Schema Name")
dbutils.widgets.text(name="tableName", defaultValue="", label="Table Name")
dbutils.widgets.text(name="partitionCol", defaultValue="", label="Partitioned By Column")
dbutils.widgets.text(name="clusterCol", defaultValue="pk", label="Clustered By Column")
dbutils.widgets.text(name="clusterBuckets", defaultValue="50", label="Cluster Buckets")
dbutils.widgets.text(name="optimizeWhere", defaultValue="", label="Optimize Where")
dbutils.widgets.text(name="optimizeZOrderBy", defaultValue="", label="Optimize Z Order By")
dbutils.widgets.text(name="vacuumRetentionHours", defaultValue="168", label="Vacuum Retention Hours")
dbutils.widgets.dropdown(name="loadType", defaultValue="Merge", choices=["Append", "Overwrite", "Merge"], label="Load Type")
dbutils.widgets.dropdown(name="destination", defaultValue="silvergeneral", choices=["silverprotected", "silvergeneral"], label="Destination")
dbutils.widgets.dropdown(name="explodeAndFlatten", defaultValue="True", choices=["True", "False"], label="Explode and Flatten")
dbutils.widgets.dropdown(name="cleanseColumnNames", defaultValue="True", choices=["True", "False"], label="Cleanse Columns")
dbutils.widgets.text(name="encryptColumns", defaultValue="", label="Encrypt Columns")
dbutils.widgets.text(name="timestampColumns", defaultValue="", label="Timestamp Columns")
dbutils.widgets.text(name="timestampFormat", defaultValue="yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", label="Timestamp Format")
dbutils.widgets.text(name="adswerveTableName", defaultValue="", label="Adswerve Table Name")
dbutils.widgets.text(name="silverTransformationsPath", defaultValue="", label="Silver Transformations Notebook Path")

widgets = ["stepLogGuid", "stepKey", "dateToProcess", "externalSystem", "partitions", "primaryKeyColumns", "schemaName", "tableName", "partitionCol", "clusterCol", "clusterBuckets", "optimizeWhere", "optimizeZOrderBy", "vacuumRetentionHours", "loadType", "destination", "explodeAndFlatten", "cleanseColumnNames", "encryptColumns", "timestampColumns", "timestampFormat", "adswerveTableName", "silverTransformationsPath"]
secrets = []

# COMMAND ----------

snb = ktk.SingleResponsibilityNotebook(widgets, secrets)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Update Silver Data Path

# COMMAND ----------

snb.silverDataPath = snb.silverDataPath + snb.adswerveTableName + '/'
print(snb.silverDataPath)

# COMMAND ----------

# silverZoneTableName = "{0}.`{1}_{2}_{3}`".format(snb.destination, snb.externalSystem, snb.schemaName, snb.tableName)
# e.g. arena_big_query_1234566_ga_sessions_20222_sessions
# tableName = snb.tableName.replace('_ga_sessions', '')
silverZoneTableName = "{0}.`{1}_{2}_{3}_{4}`".format(snb.destination, snb.externalSystem, snb.schemaName, snb.adswerveTableName, snb.tableName)
# silverZoneTableName = "{0}.`{1}_{2}_{3}`".format(snb.destination, snb.externalSystem, snb.schemaName, snb.tableName)
upsertTableName = "bronze.`{0}_{1}_{2}_{3}_staging`".format(snb.externalSystem, snb.schemaName, snb.adswerveTableName, snb.tableName)
pkcols = snb.primaryKeyColumns.replace(" ", "")\

p = {
  "silverZoneTableName": silverZoneTableName,
  "upsertTableName": upsertTableName,
  "pkcols": pkcols
}
parameters = json.dumps(snb.mergeAttributes(p))
snb.log_notebook_start(parameters)
print("Parameters:")
snb.displayAttributes()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Validation

# COMMAND ----------

bronzeDataPathFiles = u.pathHasData(snb.bronzeDataPath, "json")
if bronzeDataPathFiles == "":
  snb.log_notebook_end(0)
  dbutils.notebook.exit("No files found to process")

# COMMAND ----------

if pkcols == "" and snb.loadType == "Merge":
  errorDescription = "Primary key column(s) were not supplied and are required for Delta Merge."
  err = {
    "sourceName": "Delta Load: Validation",
    "errorCode": "200",
    "errorDescription": errorDescription
  }
  error = json.dumps(err)
  snb.log_notebook_error(error)
  raise ValueError(errorDescription)

# COMMAND ----------

silverTableExists = u.sparkTableExists(snb.silverZoneTableName)
if silverTableExists == True and snb.loadType == "Merge":
  if u.checkDeltaFormat(snb.silverZoneTableName) != "delta":
    errorDescription = "Table {0} is not in Delta format.".format(snb.silverZoneTableName)
    err = {
      "sourceName": "Delta Load: Validation",
      "errorCode": "200",
      "errorDescription": errorDescription
    }
    error = json.dumps(err)
    snb.log_notebook_error(error)
    raise ValueError(errorDescription)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Get Schema

# COMMAND ----------

schema = u.getSchema(snb.bronzeDataPath, snb.externalSystem, snb.schemaName, snb.tableName, snb.stepLogGuid, snb.BronzeBasePath)
schema

# COMMAND ----------

# MAGIC %md
# MAGIC #### Read Data from Bronze Zone

# COMMAND ----------

try:
  dfList = []
  dfList.append(spark.read \
      .schema(schema) \
      .option("badRecordsPath", snb.badRecordsPath) \
      .json(snb.bronzeDataPath) \
      .dropDuplicates())
except Exception as e:
  err = {
    "sourceName" : "Delta Load: Read Data from Bronze Zone",
    "errorCode" : "300",
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
# MAGIC #### Transformation

# COMMAND ----------

# MAGIC %run snb.silverTransformationsPath

# COMMAND ----------

if snb.adswerveTableName == "sessions":
    dfList.append(sessions_transformation(dfList[-1]))
elif snb.adswerveTableName == "hitsPage":
    dfList.append(hitsPage_transformation(dfList[-1]))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Flatten and Explode

# COMMAND ----------

if snb.explodeAndFlatten == "True":
  try:
    print("Exploding and Flattening JSON Arrays/Structs")
    dfList.append(u.flattenAndExplodeRecursive(dfList[-1], 20))
    dfList[-1].printSchema()
  except Exception as e:
    err = {
      "sourceName" : "Delta Load: Flatten and Explode",
      "errorCode" : "400",
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
      "sourceName" : "Delta Load: Cleanse Columns",
      "errorCode" : "500",
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
      "sourceName" : "Delta Load: Timestamp Columns",
      "errorCode" : "600",
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
      "sourceName" : "Delta Load: Encrypt Columns",
      "errorCode" : "700",
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
    "sourceName": "Delta Load: Stage data in Bronze Zone",
    "errorCode": "800",
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
  u.saveDataFrameToDeltaTable(snb.upsertTableName, snb.silverZoneTableName, snb.loadType, snb.silverDataPath, snb.partitionCol)
except Exception as e:
  err = {
    "sourceName": "Delta Load: Load Delta Table",
    "errorCode": "900",
    "errorDescription": e.__class__.__name__
  }
  error = json.dumps(err)
  snb.log_notebook_error(error)
  raise(e)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Optimize and Vacuum

# COMMAND ----------

# u.optimize(snb.silverZoneTableName, snb.optimizeWhere, snb.optimizeZOrderBy)
# u.vacuum(snb.silverZoneTableName, snb.vacuumRetentionHours)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Validate and Log Completion

# COMMAND ----------

try:
  dbutils.fs.ls(snb.badRecordsPath)
  errorDescription = "Rows were written to badRecordsPath: {0} for table {1}.{2}.".format(snb.badRecordsPath, snb.schemaName, snb.tableName)
  err = {
    "sourceName": "Delta Load: Bad Records",
    "errorCode": "1000",
    "errorDescription": errorDescription
  }
  error = json.dumps(err)
  snb.log_notebook_error(error)
  raise ValueError(errorDescription)
except:
  print("success")

# COMMAND ----------

rows = pkdf.count()
snb.log_notebook_end(rows)
dbutils.notebook.exit("Succeeded")
