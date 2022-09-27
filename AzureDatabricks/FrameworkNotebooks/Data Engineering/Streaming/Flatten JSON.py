# Databricks notebook source
# MAGIC %md # Flatten JSON
# MAGIC
# MAGIC Streams a Spark Catalog Table as input and flattens and explodes any JSON/Struct/Array Columns to produce a new (Delta) Table.
# MAGIC
# MAGIC #### Usage
# MAGIC Supply the parameters above and run the notebook.
# MAGIC
# MAGIC #### Prerequisites
# MAGIC * Spark Table must exist
# MAGIC
# MAGIC #### Details

# COMMAND ----------

# MAGIC %md
# MAGIC #### Initialize

# COMMAND ----------

import ktk
from ktk import utilities as u
import datetime, json
from pyspark.sql.types import StructType
from pyspark.sql.functions import explode, col, unix_timestamp, expr, lit, from_json
import time

# COMMAND ----------

dbutils.widgets.text(name="stepLogGuid", defaultValue="00000000-0000-0000-0000-000000000000", label="stepLogGuid")
dbutils.widgets.text(name="stepKey", defaultValue="-1", label="stepKey")
dbutils.widgets.dropdown(name="trigger", defaultValue="Once", choices=["Once", "Microbatch"], label="Trigger")
dbutils.widgets.text(name="maxEventsPerTrigger", defaultValue="10000", label="Max Events Per Trigger")
dbutils.widgets.text(name="interval", defaultValue="10", label="Interval Seconds")
dbutils.widgets.text(name="tableName", defaultValue="", label="Table Name")
dbutils.widgets.text(name="columnName", defaultValue="Body", label="ColumnName")
dbutils.widgets.text(name="outputTableName", defaultValue="", label="Output Table Name")
dbutils.widgets.text(name="stopSeconds", defaultValue="120", label="Stop after Seconds of No Activity")
dbutils.widgets.dropdown(name="destination", defaultValue="silvergeneral", choices=["silverprotected", "silvergeneral"], label="Destination")

widgets = ["stepLogGuid", "stepKey", "trigger", "maxEventsPerTrigger", "interval", "tableName", "columnName", "outputTableName", "stopSeconds", "destination"]
secrets = []

# COMMAND ----------

snb = ktk.SingleResponsibilityNotebook(widgets, secrets)

# COMMAND ----------

outputPath = "{0}/{1}".format(snb.basePath, snb.outputTableName)
stopSeconds = int(dbutils.widgets.get("stopSeconds"))
checkpointPath = "{0}/checkpoint/flatten_{1}_{2}/".format(snb.basePath, snb.tableName, snb.outputTableName)
queryName = "{0}_{1} flatten".format(snb.tableName, snb.outputTableName)

p = {
  "outputPath": outputPath,
  "stopSeconds": stopSeconds,
  "checkpointPath": checkpointPath,
  "queryName": queryName
}

parameters = json.dumps(snb.mergeAttributes(p))
snb.log_notebook_start(parameters)
print("Parameters:")
snb.displayAttributes()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Get Table

# COMMAND ----------

try:
  refreshed = u.refreshTable(snb.tableName)
  if refreshed == False:
    snb.log_notebook_end(0)
    dbutils.notebook.exit("Table does not exist")
  df = spark.readStream.format("delta").table(snb.tableName).select(snb.columnName)
  df.printSchema
except Exception as e:
  err = {
    "sourceName": "Flatten JSON: Get Table",
    "errorCode": "100",
    "errorDescription": e.__class__.__name__
  }
  error = json.dumps(err)
  snb.log_notebook_error(error)
  raise(e)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Get Schema

# COMMAND ----------

try:
  table = spark.table(snb.tableName).select(snb.columnName).alias("Body")
  schema = spark.read.json(table.rdd.map(lambda row: row.Body)).schema
  jsonDF = df.withColumn("json", from_json(col("Body"), schema))
except Exception as e:
  err = {
    "sourceName": "Flatten JSON: Get Schema",
    "errorCode": "200",
    "errorDescription": e.__class__.__name__
  }
  error = json.dumps(err)
  snb.log_notebook_error(error)
  raise(e)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Flatten

# COMMAND ----------

try:
  flattened = u.flatten_df(jsonDF)
except Exception as e:
  err = {
    "sourceName": "Flatten JSON: Flatten",
    "errorCode": "300",
    "errorDescription": e.__class__.__name__
  }
  error = json.dumps(err)
  snb.log_notebook_error(error)
  raise(e)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Write to Delta

# COMMAND ----------

try:
  if(snb.trigger=="Once"):
    dfQuery = (flattened
      .writeStream
      .format("delta")
      .option("checkpointLocation", snb.checkpointPath)
      .outputMode("append")
      .queryName(snb.queryName)
      .trigger(once=True)
      .start(snb.outputPath))
  else:
    if(snb.trigger=="Microbatch"):
      processingTime = "{0} seconds".format(snb.interval)
      dfQuery = (flattened
        .writeStream
        .format("delta")
        .option("checkpointLocation", snb.checkpointPath)
        .outputMode("append")
        .queryName(snb.queryName)
        .trigger(processingTime=processingTime)
        .start(snb.outputPath))
except Exception as e:
  err = {
    "sourceName": "Flatten JSON: Write to Delta",
    "errorCode": "300",
    "errorDescription": e.__class__.__name__
  }
  error = json.dumps(err)
  snb.log_notebook_error(error)
  raise(e)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Stop Inactive Streams

# COMMAND ----------

try:
  if snb.trigger == "Microbatch":
    time.sleep(snb.stopSeconds)
    streams = [s for s in spark.streams.active if s.name in [snb.queryName]]
    while (1==1):
      streams = [s for s in spark.streams.active if s.name in [snb.queryName]]
      if len(streams) == 0:
        break
      for stream in streams:
        if stream.lastProgress['numInputRows'] == 0:
          stream.stop()
      time.sleep(snb.stopSeconds)
except Exception as e:
  err = {
    "sourceName": "Flatten JSON: Stop Inactive Streams",
    "errorCode": "400",
    "errorDescription": e.__class__.__name__
  }
  error = json.dumps(err)
  snb.log_notebook_error(error)
  raise(e)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create Delta Table

# COMMAND ----------

try:
  spark.sql("""
  CREATE TABLE IF NOT EXISTS {0}
  USING DELTA
  LOCATION '{1}'
  """.format(snb.outputTableName, snb.outputPath))
except Exception as e:
  err = {
    "sourceName": "Flatten JSON: Create Delta Table",
    "errorCode": "500",
    "errorDescription": e.__class__.__name__
  }
  error = json.dumps(err)
  snb.log_notebook_error(error)
  raise(e)

# COMMAND ----------

snb.log_notebook_end(0)
dbutils.notebook.exit("Succeeded")