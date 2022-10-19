# Databricks notebook source
# MAGIC %md # Delta Load (AppInsights)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Initialize

# COMMAND ----------
import ktk
from ktk import utilities as u
from pyspark.sql.functions import explode, col, unix_timestamp, coalesce, lit, explode_outer, year, month, dayofmonth
from pyspark.sql.types import TimestampType
from datetime import datetime, timedelta
import json
import hashlib
spark.conf.set("spark.databricks.delta.merge.joinBasedMerge.enabled", True)
spark.conf.set("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation","true")

# COMMAND ----------

dbutils.widgets.text(name="stepLogGuid", defaultValue="00000000-0000-0000-0000-000000000000", label="stepLogGuid")
dbutils.widgets.text(name="stepKey", defaultValue="-1", label="stepKey")
dbutils.widgets.text(name="dateToProcess", defaultValue="", label="Date to Process")
dbutils.widgets.text(name="externalSystem", defaultValue="", label="External System")
dbutils.widgets.text(name="partitions", defaultValue="8", label="Number of Partitions")
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

widgets = ["stepLogGuid","stepKey","dateToProcess","externalSystem","partitions","primaryKeyColumns","schemaName","tableName","partitionCol","clusterCol",
"clusterBuckets","optimizeWhere","optimizeZOrderBy","vacuumRetentionHours","loadType","destination","explodeAndFlatten","cleanseColumnNames",
"encryptColumns","timestampColumns","timestampFormat"]
secrets = []

# COMMAND ----------

snb = ktk.SingleResponsibilityNotebook(widgets, secrets)

# COMMAND ----------

silverZoneTableName = "{0}.{1}_{2}_{3}".format(snb.destination, snb.externalSystem, snb.schemaName, snb.tableName).replace("-","_")
upsertTableName = "bronze.{0}_{1}_{2}_staging".format(snb.externalSystem, snb.schemaName, snb.tableName).replace("-","_")
pkcols = snb.pkcols.replace(" ", "")
schemaPath = "{0}/schemas/appinsights/{1}".format(snb.bronzeBasePath, snb.schemaName)
schemaFile = schemaPath + "/schema.json"

p = {
  "silverZoneTableName": silverZoneTableName,
  "upsertTableName": upsertTableName,
  "primaryKeyColumns": pkcols,
  "schemaFile": schemaFile
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

if snb.pkcols == "" and snb.loadType == "Merge":
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

try:
  head = dbutils.fs.head(snb.schemaFile, 256000)
except Exception as e:
  err = {
    "sourceName": "Stream Processing - App Insights - Event: Read Schema",
    "errorCode": "100",
    "errorDescription": "Schema was not found in directory: {0}".format(snb.schemaFile)
  }
  error = json.dumps(err)
  snb.log_notebook_error(error)
  raise(e)

schema = StructType.fromJson(json.loads(head))
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
      #dfList.append(dfList[-1].withColumn(tsColumnName, unix_timestamp(ts, timestampFormat).cast(TimestampType())))
      dfList.append(dfList[-1].withColumn(tsColumnName, col(ts).cast(TimestampType())))
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

convertedTimestampColumns = [ts + "_ts" for ts in snb.timestampColumnList]
timestampvalidation = dfList[-1].select(*snb.timestampColumnList, *convertedTimestampColumns)
timestampvalidation.createOrReplaceTempView("timestampvalidation")
whereClauseList = []
for timeStampColumn in snb.timestampColumnList:
  whereClauseList = "AND ({0} IS NOT NULL AND {0}_ts IS NULL)".format(timeStampColumn)
whereClauseList
whereClause = "".join(whereClauseList)
sql = """
SELECT *
FROM timestampvalidation
WHERE 1=0
{0}
""".format(whereClause)
failedToConvert = spark.sql(sql).count()
if failedToConvert != 0:
  err = {
    "sourceName" : "Delta Load: Timestamp Columns Validation",
    "errorCode" : "650",
    "errorDescription" : "One or more timestamp columns could not be converted to Timestamp data type.  Review supplied timestamp format against the incoming data."
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
# MAGIC #### Add Date Partition Columns

# COMMAND ----------

dfList.append(dfList[-1] \
  .withColumn("year", year("context_data_eventTime_ts")) \
  .withColumn("month", month("context_data_eventTime_ts")) \
  .withColumn("day", dayofmonth("context_data_eventTime_ts")))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Stage data in Bronze Zone

# COMMAND ----------

if snb.pkcols == "*":
  snb.pkcols = ",".join(dfList[-1].columns)

# COMMAND ----------

try:
  dfList.append(u.pkColSha(dfList[-1], snb.pkcols))
  dfList.append(u.groupByPKMax(dfList[-1]))
  spark.sql("DROP TABLE IF EXISTS " + snb.upsertTableName)
  dfList[-1].write.saveAsTable(snb.upsertTableName)
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

u.optimize(snb.silverZoneTableName, snb.optimizeWhere, snb.optimizeZOrderBy)
u.vacuum(snb.silverZoneTableName, snb.vacuumRetentionHours)

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

rows = dfList[-1].count()
snb.log_notebook_end(rows)
dbutils.notebook.exit("Succeeded")