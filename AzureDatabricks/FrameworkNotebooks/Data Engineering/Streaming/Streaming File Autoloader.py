# Databricks notebook source
# MAGIC %md # Streaming File Autoloader
# MAGIC
# MAGIC https://docs.microsoft.com/en-us/azure/databricks/spark/latest/structured-streaming/auto-loader
# MAGIC
# MAGIC https://docs.microsoft.com/en-us/azure/databricks/spark/latest/structured-streaming/auto-loader-gen2

# COMMAND ----------

# MAGIC %md
# MAGIC #### Initialize

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Source

# COMMAND ----------

import ktk
from ktk import utilities as u
import datetime, json
from pyspark.sql.types import StructType
from pyspark.sql.functions import explode, col, unix_timestamp, expr, lit
import time

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Widgets

# COMMAND ----------

dbutils.widgets.text(name="stepLogGuid", defaultValue="00000000-0000-0000-0000-000000000000", label="stepLogGuid")
dbutils.widgets.text(name="stepKey", defaultValue="-1", label="stepKey")
dbutils.widgets.text(name="externalSystem", defaultValue="", label="External System")
dbutils.widgets.text(name="externalDataPath", defaultValue="", label="External Data Path")
dbutils.widgets.dropdown(name="fileExtension", defaultValue="json", choices=["json","csv","avro","binaryFile","orc","parquet","text"], label="External File Extension")
dbutils.widgets.text(name="multiLine", defaultValue="False", label="Multiline")
dbutils.widgets.text(name="delimiter", defaultValue=",", label="File Delimiter")
dbutils.widgets.dropdown(name="header", defaultValue="False", choices=["True","False"], label="Header Row")
dbutils.widgets.text(name="numPartitions", defaultValue="8", label="Number of Partitions")
dbutils.widgets.text(name="schemaName", defaultValue="", label="Schema Name")
dbutils.widgets.text(name="tableName", defaultValue="", label="Table Name")
dbutils.widgets.text(name="dateToProcess", defaultValue="", label="Date to Process")
dbutils.widgets.dropdown(name="destination", defaultValue="silvergeneral", choices=["silverprotected", "silvergeneral"], label="Destination")
dbutils.widgets.text(name="inferSchemaSampleBytes", defaultValue="10gb", label="Sample Bytes")
dbutils.widgets.text(name="inferSchemaSampleFiles", defaultValue="1000", label="Sample Files")
dbutils.widgets.dropdown(name="trigger", defaultValue="Once", choices=["Once", "Microbatch"], label="Trigger")
dbutils.widgets.text(name="maxEventsPerTrigger", defaultValue="10000", label="Max Events Per Trigger")
dbutils.widgets.text(name="interval", defaultValue="10", label="Interval Seconds")
dbutils.widgets.text(name="stopStreamsGracePeriodSeconds", defaultValue="240", label="Stream Initialization Seconds")
dbutils.widgets.text(name="stopStreamsCheckInterval", defaultValue="60", label="Stream Check Interval")

widgets = ["stepLogGuid", "stepKey", "externalSystem", "externalDataPath", "fileExtension", "multiLine", "delimiter", "header", "numPartitions", "schemaName", "tableName", "dateToProcess", "destination", "inferSchemaSampleBytes", "inferSchemaSampleFiles", "trigger", "maxEventsPerTrigger", "interval", "stopStreamsGracePeriodSeconds", "stopStreamsCheckInterval"]
secrets = []

# COMMAND ----------

snb = ktk.SingleResponsibilityNotebook(widgets, secrets)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Variables

# COMMAND ----------

rawDataPath = "{0}/{1}/{2}/{3}/".format(snb.BronzeBasePath, snb.externalSystem, snb.schemaName, snb.tableName)
queryDataPath = "{0}/{1}/{2}/{3}/".format(snb.basePath, snb.externalSystem, snb.schemaName, snb.tableName)
rawCheckpointPath = "{0}/checkpoint/{1}/{2}/{3}/".format(snb.BronzeBasePath, snb.externalSystem, snb.schemaName, snb.tableName)
queryCheckpointPath = "{0}/checkpoint/{1}/{2}/{3}/".format(snb.basePath, snb.externalSystem, snb.schemaName, snb.tableName)

spark.conf.set("spark.databricks.cloudFiles.schemaInference.sampleSize.numBytes", snb.inferSchemaSampleBytes)
spark.conf.set("spark.databricks.cloudFiles.schemaInference.sampleSize.numFiles", snb.inferSchemaSampleFiles)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Log Start

# COMMAND ----------

p = {
  "rawDataPath": rawDataPath,
  "queryDataPath": queryDataPath,
  "rawCheckpointPath": rawCheckpointPath,
  "queryCheckpointPath": queryCheckpointPath,
}

parameters = json.dumps(snb.mergeAttributes(p))
snb.log_notebook_start(parameters)
print("Parameters:")
snb.displayAttributes()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Validation

# COMMAND ----------

validatedExternalDataPath = u.pathHasData(snb.fullExternalDataPath, snb.fileExtension)
if validatedExternalDataPath == "":
  snb.log_notebook_end(0)
  dbutils.notebook.exit("No {1} files to process for {0}".format(snb.fullExternalDataPath, snb.fileExtension))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Read Stream

# COMMAND ----------

if snb.fileExtension == "json":
  try:
    df = spark.readStream.format("cloudFiles") \
      .option("cloudFiles.format", snb.fileExtension) \
      .option("multiLine", snb.multiLine) \
      .option("cloudFiles.schemaLocation", snb.rawCheckpointPath) \
      .option("cloudFiles.schemaLocation", snb.rawCheckpointPath) \
      .load()
  except Exception as e:
    err = {
      "sourceName" : "Ingest Autoloader: Read Stream JSON",
      "errorCode" : "100",
      "errorDescription" : e.__class__.__name__
    }
    error = json.dumps(err)
    snb.log_notebook_error(error)
    raise(e)
elif snb.fileExtension == "csv":
  try:
    df = spark.readStream.format("cloudFiles") \
      .option("cloudFiles.format", snb.fileExtension) \
      .option("sep", snb.delimiter) \
      .option("header", snb.header) \
      .option("cloudFiles.schemaLocation", snb.rawCheckpointPath) \
      .option("path", snb.fullExternalDataPath) \
      .load()
  except Exception as e:
    err = {
      "sourceName" : "Ingest Autoloader: Read Stream CSV",
      "errorCode" : "100",
      "errorDescription" : e.__class__.__name__
    }
    error = json.dumps(err)
    snb.log_notebook_error(error)
    raise(e)
else:
  err = {
      "sourceName" : "Ingest Autoloader: Read Stream",
      "errorCode" : "100",
      "errorDescription" : "File Extension Not implemented"
  }
  error = json.dumps(err)
  snb.log_notebook_error(error)
  raise(e)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Write Bronze

# COMMAND ----------

bronzeQueryName = "Bronze {0} - {1} - {2}".format(snb.externalSystem, snb.schemaName, snb.tableName)
if snb.trigger == "Once":
  try:
    dfBronze = (df.writeStream \
      .queryName(bronzeQueryName) \
      .trigger(once=True) \
      .option("mergeSchema", "true") \
      .option("checkpointLocation", snb.rawCheckpointPath) \
      .format("json") \
      .outputMode("append") \
      .start(snb.rawDataPath))
  except Exception as e:
    err = {
      "sourceName": "Streaming File Autoloader: Write Bronze Stream - Trigger Once",
      "errorCode": "200",
      "errorDescription": e.__class__.__name__
    }
    error = json.dumps(err)
    snb.log_notebook_error(error)
    raise(e)
else:
  try:
    intervalSeconds = "{0} seconds".format(snb.interval)
    dfBronze = (df.writeStream \
      .queryName(bronzeQueryName) \
      .trigger(processingTime=intervalSeconds) \
      .option("mergeSchema", "true") \
      .option("checkpointLocation", snb.rawCheckpointPath) \
      .format("json") \
      .outputMode("append") \
      .start(snb.rawDataPath))
  except Exception as e:
    err = {
      "sourceName": "Streaming File Autoloader: Write Bronze Stream - Microbatch",
      "errorCode": "200",
      "errorDescription": e.__class__.__name__
    }
    error = json.dumps(err)
    snb.log_notebook_error(error)
    raise(e)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Write Silver

# COMMAND ----------

queryCheckpointPath

# COMMAND ----------

silverQueryName = "Silver {0} - {1} - {2}".format(snb.externalSystem, snb.schemaName, snb.tableName)
if snb.trigger == "Once":
  try:
    dfSilver = (df.writeStream \
      .queryName(silverQueryName) \
      .trigger(once=True) \
      .option("mergeSchema", "true") \
      .option("checkpointLocation", snb.queryCheckpointPath) \
      .format("json") \
      .outputMode("append") \
      .start(snb.queryDataPath))
  except Exception as e:
    err = {
      "sourceName": "Streaming File Autoloader: Write Silver Stream - Trigger Once",
      "errorCode": "200",
      "errorDescription": e.__class__.__name__
    }
    error = json.dumps(err)
    snb.log_notebook_error(error)
    raise(e)
else:
  try:
    intervalSeconds = "{0} seconds".format(snb.interval)
    dfSilver = (df.writeStream \
      .queryName(silverQueryName) \
      .trigger(processingTime=intervalSeconds) \
      .option("mergeSchema", "true") \
      .option("checkpointLocation", snb.queryCheckpointPath) \
      .format("delta") \
      .outputMode("append") \
      .start(snb.queryDataPath))
  except Exception as e:
    err = {
      "sourceName": "Streaming File Autoloader: Write Silver Stream - Microbatch",
      "errorCode": "200",
      "errorDescription": e.__class__.__name__
    }
    error = json.dumps(err)
    snb.log_notebook_error(error)
    raise(e)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Stop Streams

# COMMAND ----------

if snb.trigger != "Once":
  time.sleep(stopStreamsGracePeriodSeconds)
  while (1==1):
    if len(spark.streams.active) == 0:
      break
    for q in spark.streams.active:
      try:
        if q.lastProgress['numInputRows'] == 0:
          q.stop()
      except Exception as e:
        print("query object not instantiated")
    time.sleep(stopStreamsCheckInterval)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Log Completion

# COMMAND ----------

snb.log_notebook_end(0)
dbutils.notebook.exit("Succeeded")