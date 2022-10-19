# Databricks notebook source
# MAGIC %md # Streaming File JSON

# COMMAND ----------

import ktk
from ktk import utilities as u
import datetime, json
from pyspark.sql.types import StructType
from pyspark.sql.functions import explode, col, unix_timestamp, expr, lit
import time

# COMMAND ----------

dbutils.widgets.text(name="stepLogGuid", defaultValue="00000000-0000-0000-0000-000000000000", label="stepLogGuid")
dbutils.widgets.text(name="stepKey", defaultValue="-1", label="stepKey")
dbutils.widgets.text(name="externalSystem", defaultValue="", label="External System")
dbutils.widgets.text(name="externalDataPath", defaultValue="", label="External Data Path")
dbutils.widgets.text(name="fileExtension", defaultValue="", label="External File Extension")
dbutils.widgets.text(name="numPartitions", defaultValue="8", label="Number of Partitions")
dbutils.widgets.text(name="tableName", defaultValue="", label="Table Name")
dbutils.widgets.text(name="dateToProcess", defaultValue="", label="Date to Process")
dbutils.widgets.dropdown(name="destination", defaultValue="silvergeneral", choices=["silverprotected", "silvergeneral"], label="Destination")

widgets = ["stepLogGuid", "stepKey", "externalSystem", "externalDataPath", "fileExtension", "numPartitions", "tableName", "dateToProcess", "destination"]
secrets = ["StorageAccountName", "StorageAccountKey", "ContainerOrFileSystemName", "BasePath"]

# COMMAND ----------

snb = ktk.SingleResponsibilityNotebook(widgets, secrets)

# COMMAND ----------

rawDataPath = "{0}/{1}/{2}/{3}/".format(snb.BronzeBasePath, snb.externalSystem, snb.tableName, snb.dateToProcess)
queryDataPath = "{0}/{1}/{2}".format(snb.basePath, snb.externalSystem, snb.tableName)
rawCheckpointPath = "{0}/checkpoint/{1}/{2}/{3}/".format(snb.BronzeBasePath, snb.externalSystem, snb.tableName, snb.dateToProcess)
queryCheckpointPath = "{0}/checkpoint/{1}/{2}/".format(snb.basePath, snb.externalSystem, snb.tableName)
fullExternalStorageAccountName = "fs.azure.account.key.{0}.blob.core.windows.net".format(snb.StorageAccountName)
appInsightsBasePath = "wasbs://{0}@{1}.blob.core.windows.net".format(snb.ContainerOrFileSystemName, snb.StorageAccountName)

p = {
  "rawDataPath": rawDataPath,
  "queryDataPath": queryDataPath,
  "rawCheckpointPath": rawCheckpointPath,
  "queryCheckpointPath": queryCheckpointPath,
  "fullExternalStorageAccountName" : fullExternalStorageAccountName,
  "appInsightsBasePath" : appInsightsBasePath
}

parameters = json.dumps(snb.mergeAttributes(p))
snb.log_notebook_start(parameters)
print("Parameters:")
snb.displayAttributes()

# COMMAND ----------

spark.conf.set(snb.StorageAccountName, snb.StorageAccountKey)

# COMMAND ----------

validatedExternalDataPath = u.pathHasData(snb.fullExternalDataPath, snb.fileExtension)
if validatedExternalDataPath == "":
  snb.log_notebook_end(0)
  dbutils.notebook.exit("No {1} files to process for {0}".format(snb.fullExternalDataPath, snb.fileExtension))

# COMMAND ----------

schema = u.getSchema(dataPath=snb.fullExternalDataPath, externalSystem=snb.externalSystem, schema="", table=snb.tableName, stepLogGuid=snb.stepLogGuid, basepath=snb.BronzeBasePath, samplingRatio=1, timeout=6000, zone="bronze", delimiter="", header=False, multiLine=True)
schema

# COMMAND ----------

try:
  df = spark \
    .readStream \
    .schema(schema) \
    .json(snb.fullExternalDataPath)
except Exception as e:
  err = {
    "sourceName" : "Streaming File JSON: Read Stream",
    "errorCode" : "100",
    "errorDescription" : e.__class__.__name__
  }
  error = json.dumps(err)
  snb.log_notebook_error(error)
  raise(e)

# COMMAND ----------

try:
  rawQueryName = "{0} raw".format(snb.tableName)
  dfRaw = (df \
    .writeStream \
    .queryName(rawQueryName) \
    .trigger(once=True) \
    .format("json") \
    .option("checkpointLocation", snb.rawCheckpointPath) \
    .outputMode("append") \
    .start(snb.rawDataPath)
  )
except Exception as e:
  err = {
    "sourceName": "Streaming File JSON: Write Raw Zone Stream",
    "errorCode": "200",
    "errorDescription": e.__class__.__name__
  }
  error = json.dumps(err)
  snb.log_notebook_error(error)
  raise(e)

# COMMAND ----------

try:
  queryQueryName = "{0} query".format(snb.tableName)
  dfQuery = (df \
    .writeStream \
    .queryName(queryQueryName) \
    .trigger(once=True) \
    .format("delta") \
    .option("checkpointLocation", snb.queryCheckpointPath) \
    .outputMode("append") \
    .start(snb.queryDataPath)
  )
except Exception as e:
  err = {
    "sourceName": "Streaming File JSON: Write Query Zone Stream",
    "errorCode": "300",
    "errorDescription": e.__class__.__name__
  }
  error = json.dumps(err)
  snb.log_notebook_error(error)
  raise(e)

# COMMAND ----------

# This is not necessary as long as the stream trigger is set to Once.  Otherwise, uncomment it.

#time.sleep(240)
#while (1==1):
#  if len(spark.streams.active) == 0:
#    break
#  for q in spark.streams.active:
#    try:
#      if q.lastProgress['numInputRows'] == 0:
#        q.stop()
#    except Exception as e:
#      print("query object not instantiated")
#  time.sleep(60)

# COMMAND ----------

snb.log_notebook_end(0)
dbutils.notebook.exit("Succeeded")