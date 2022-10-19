# Databricks notebook source
# MAGIC %md # Ingest Event Hub
# MAGIC 
# MAGIC Structured Streaming to process an Azure Event Hub.
# MAGIC 
# MAGIC #### Usage
# MAGIC Supply the parameters above and run the notebook.
# MAGIC 
# MAGIC #### Prerequisites
# MAGIC * Assumes a databricks secret scope has been created with the same name as the External System.
# MAGIC * Databricks Cluster must have the Azure Event Hub Maven Library installed: Import Library with Maven coordinates: com.microsoft.azure:azure-eventhubs-spark_2.11:2.3.14.1
# MAGIC 
# MAGIC #### Details
# MAGIC * To create External System secret scope, run Powershell script /ApplicationConfiguration/Powershell/Scripts/ExternalSystemSecrets/CreateExternalSecretsForEventHub.ps1
# MAGIC * https://github.com/Azure/azure-event-hubs-spark/blob/master/docs/structured-streaming-eventhubs-integration.md
# MAGIC * https://docs.microsoft.com/en-us/azure/databricks/spark/latest/structured-streaming/streaming-event-hubs
# MAGIC * https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#triggers

# COMMAND ----------

# MAGIC %md
# MAGIC #### Initialize

# COMMAND ----------

import ktk
from ktk import utilities as u
import datetime, json
from pyspark.sql.types import StructType
from pyspark.sql.functions import explode, col, unix_timestamp, expr, lit
import time

# COMMAND ----------

# MAGIC %run "../../Orchestration/Notebook Functions"

# COMMAND ----------

# MAGIC %scala
# MAGIC import org.apache.spark.eventhubs.{ ConnectionStringBuilder, EventHubsConf, EventPosition }
# MAGIC import org.apache.spark.sql.types._
# MAGIC import org.apache.spark.sql.functions._
# MAGIC import org.apache.spark.sql.streaming.Trigger
# MAGIC import org.apache.spark.sql.{AnalysisException}

# COMMAND ----------

dbutils.widgets.text(name="stepLogGuid", defaultValue="00000000-0000-0000-0000-000000000000", label="stepLogGuid")
dbutils.widgets.text(name="stepKey", defaultValue="-1", label="stepKey")
dbutils.widgets.text(name="externalSystem", defaultValue="", label="External System")
dbutils.widgets.dropdown(name="trigger", defaultValue="Once", choices=["Once", "Microbatch"], label="Trigger")
dbutils.widgets.text(name="maxEventsPerTrigger", defaultValue="10000", label="Max Events Per Trigger")
dbutils.widgets.text(name="offset", defaultValue="0", label="Offset")
dbutils.widgets.text(name="interval", defaultValue="10", label="Interval Seconds")
dbutils.widgets.text(name="tableName", defaultValue="", label="Table Name")
dbutils.widgets.text(name="stopSeconds", defaultValue="120", label="Stop after Seconds of No Activity")
dbutils.widgets.dropdown(name="eventHubStartingPosition", defaultValue="fromStartOfStream", choices=["fromStartOfStream", "fromEndOfStream", "fromLatestSequenceNumber"], label="Starting Position")
dbutils.widgets.dropdown(name="destination", defaultValue="silvergeneral", choices=["silverprotected", "silvergeneral"], label="Destination")

widgets = ["stepLogGuid", "stepKey", "externalSystem", "trigger", "maxEventsPerTrigger", "offset", "interval", "tableName", "stopSeconds", "eventHubStartingPosition", "destination"]
secrets = ["EventHubName", "EventHubConsumerGroup"]

# COMMAND ----------

snb = ktk.SingleResponsibilityNotebook(widgets, secrets)

# COMMAND ----------

rawDataPath = "{0}/{1}/{2}/".format(snb.BronzeBasePath, snb.externalSystem, snb.EventHubName)
queryDataPath = "{0}/{1}/{2}".format(snb.basePath, snb.externalSystem, snb.EventHubName)
rawCheckpointPath = "{0}/checkpoint/{1}/{2}/".format(snb.BronzeBasePath, snb.externalSystem, snb.EventHubName)
queryCheckpointPath = "{0}/checkpoint/{1}/{2}/".format(snb.basePath, snb.externalSystem, snb.EventHubName)
rawQueryName = "{0} raw".format(snb.EventHubName)
queryQueryName = "{0} query".format(snb.EventHubName)

p = {
  "rawDataPath": rawDataPath,
  "queryDataPath": queryDataPath,
  "rawCheckpointPath": rawCheckpointPath,
  "queryCheckpointPath": queryCheckpointPath,
  "rawQueryName": rawQueryName,
  "queryQueryName": queryQueryName
}

parameters = json.dumps(snb.mergeAttributes(p))
snb.log_notebook_start(parameters)
print("Parameters:")
snb.displayAttributes()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Read Event Hub Stream

# COMMAND ----------

# MAGIC %scala
# MAGIC val trigger = dbutils.widgets.get("trigger")
# MAGIC val maxEventsPerTrigger = dbutils.widgets.get("maxEventsPerTrigger")
# MAGIC var offset = dbutils.widgets.get("offset")
# MAGIC val interval = dbutils.widgets.get("interval")
# MAGIC val tableName = dbutils.widgets.get("tableName")
# MAGIC val externalSystem = dbutils.widgets.get("externalSystem")
# MAGIC val eventHubConnectionString = dbutils.secrets.get(scope=externalSystem, key="EventHubConnectionString")
# MAGIC val eventHubName = dbutils.secrets.get(scope=externalSystem, key="EventHubName")
# MAGIC val eventHubConsumerGroup = dbutils.secrets.get(scope=externalSystem, key="EventHubConsumerGroup")
# MAGIC var eventHubStartingPosition = dbutils.widgets.get("eventHubStartingPosition")
# MAGIC val destination = dbutils.widgets.get("destination")
# MAGIC var basePath = ""
# MAGIC 
# MAGIC if (destination == "silverprotected") {
# MAGIC   basePath = silverProtectedBasePath
# MAGIC } else {
# MAGIC   basePath = silverGeneralBasePath
# MAGIC }
# MAGIC val rawDataPath = "%s/raw/%s/%s/".format(bronzeBasePath, externalSystem, eventHubName)
# MAGIC val queryDataPath = "%s/query/%s/%s".format(basePath, externalSystem, eventHubName)
# MAGIC val rawCheckpointPath = "%s/raw/checkpoint/%s/%s/".format(bronzeBasePath, externalSystem, eventHubName)
# MAGIC val queryCheckpointPath = "%s/query/checkpoint/%s/%s/".format(basePath, externalSystem, eventHubName)
# MAGIC val rawQueryName = "%s raw".format(eventHubName)
# MAGIC val queryQueryName = "%s query".format(eventHubName)
# MAGIC var sequenceNumber = 0
# MAGIC 
# MAGIC if (eventHubStartingPosition == "fromLatestSequenceNumber") {
# MAGIC   try {
# MAGIC     sequenceNumber = (spark.sql("SELECT MAX(sequencenumber) AS sequenceNumber FROM %s".format(tableName)).collect()(0)(0)).asInstanceOf[String].toInt
# MAGIC   }
# MAGIC   catch {
# MAGIC     case x: AnalysisException => {
# MAGIC       sequenceNumber = 0
# MAGIC       eventHubStartingPosition = "fromStartOfStream"
# MAGIC       println("Failed to obtain latest Event Hub sequence number from destination table, defaulting to reading fromStartOfStream")
# MAGIC     }
# MAGIC   }
# MAGIC } else {
# MAGIC   sequenceNumber = 0
# MAGIC }
# MAGIC 
# MAGIC val connectionString = ConnectionStringBuilder(eventHubConnectionString)
# MAGIC   .setEventHubName(eventHubName)
# MAGIC   .build
# MAGIC 
# MAGIC var eventHubsConf = EventHubsConf(connectionString.toString())
# MAGIC     .setMaxEventsPerTrigger(maxEventsPerTrigger.toInt)
# MAGIC     .setConsumerGroup(eventHubConsumerGroup)
# MAGIC 
# MAGIC if (eventHubStartingPosition == "fromLatestSequenceNumber") {
# MAGIC   eventHubsConf = EventHubsConf(connectionString.toString())
# MAGIC     .setMaxEventsPerTrigger(maxEventsPerTrigger.toInt)
# MAGIC     .setConsumerGroup(eventHubConsumerGroup)
# MAGIC     .setStartingPosition(EventPosition.fromSequenceNumber(sequenceNumber))
# MAGIC } else {
# MAGIC   if (eventHubStartingPosition == "fromStartOfStream") {
# MAGIC     eventHubsConf = EventHubsConf(connectionString.toString())
# MAGIC       .setMaxEventsPerTrigger(maxEventsPerTrigger.toInt)
# MAGIC       .setConsumerGroup(eventHubConsumerGroup)
# MAGIC       .setStartingPosition(EventPosition.fromStartOfStream)
# MAGIC   } else {
# MAGIC     eventHubsConf = EventHubsConf(connectionString.toString())
# MAGIC       .setMaxEventsPerTrigger(maxEventsPerTrigger.toInt)
# MAGIC       .setConsumerGroup(eventHubConsumerGroup)
# MAGIC       .setStartingPosition(EventPosition.fromEndOfStream)
# MAGIC   }
# MAGIC }
# MAGIC 
# MAGIC val eventHubs = spark.readStream
# MAGIC   .format("eventhubs")
# MAGIC   .options(eventHubsConf.toMap)
# MAGIC   .load()
# MAGIC 
# MAGIC eventHubs.printSchema
# MAGIC eventHubsConf.toMap

# COMMAND ----------

# MAGIC %md
# MAGIC #### Write to Delta Lake

# COMMAND ----------

# MAGIC %scala
# MAGIC if (trigger=="Once") {
# MAGIC   val dfRaw = (eventHubs
# MAGIC     .writeStream
# MAGIC     .queryName(rawQueryName)
# MAGIC     .trigger(Trigger.Once())
# MAGIC     .format("json")
# MAGIC     .option("checkpointLocation", rawCheckpointPath)
# MAGIC     .outputMode("append")
# MAGIC     .start(rawDataPath)
# MAGIC   )
# MAGIC } else {
# MAGIC   if (trigger=="Microbatch") {
# MAGIC     val processingTime = "%s seconds".format(interval)
# MAGIC     val dfRaw = (eventHubs
# MAGIC       .writeStream
# MAGIC       .queryName(rawQueryName)
# MAGIC       .trigger(Trigger.ProcessingTime(processingTime))
# MAGIC       .format("json")
# MAGIC       .option("checkpointLocation", rawCheckpointPath)
# MAGIC       .outputMode("append")
# MAGIC       .start(rawDataPath)
# MAGIC     )
# MAGIC   }
# MAGIC }

# COMMAND ----------

# MAGIC %scala
# MAGIC if(trigger=="Once") {
# MAGIC   val dfQuery = (eventHubs
# MAGIC     .withColumn("Body", $"body".cast(StringType))
# MAGIC     .writeStream
# MAGIC     .queryName(queryQueryName)
# MAGIC     .trigger(Trigger.Once())
# MAGIC     .format("delta")
# MAGIC     .option("checkpointLocation", queryCheckpointPath)
# MAGIC     .outputMode("append")
# MAGIC     .start(queryDataPath)
# MAGIC   )
# MAGIC } else {
# MAGIC   if(trigger=="Microbatch") {
# MAGIC     val processingTime = "%s seconds".format(interval)
# MAGIC     val dfQuery = (eventHubs
# MAGIC       .withColumn("Body", $"body".cast(StringType))
# MAGIC       .writeStream
# MAGIC       .queryName(queryQueryName)
# MAGIC       .trigger(Trigger.ProcessingTime(processingTime))
# MAGIC       .format("delta")
# MAGIC       .option("checkpointLocation", queryCheckpointPath)
# MAGIC       .outputMode("append")
# MAGIC       .start(queryDataPath)
# MAGIC     )
# MAGIC   }
# MAGIC }

# COMMAND ----------

# MAGIC %md
# MAGIC #### Stop Inactive Streams

# COMMAND ----------

if snb.trigger == "Microbatch":
  time.sleep(int(snb.stopSeconds))
  streams = [s for s in spark.streams.active if s.name in [snb.rawQueryName, snb.queryQueryName]]
  while (1==1):
    streams = [s for s in spark.streams.active if s.name in [snb.rawQueryName, snb.queryQueryName]]
    if len(streams) == 0:
      break
    for stream in streams:
      if stream.lastProgress['numInputRows'] == 0:
        stream.stop()
    time.sleep(int(snb.stopSeconds))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create Delta Table

# COMMAND ----------

# MAGIC %scala
# MAGIC try
# MAGIC {
# MAGIC   spark.sql(s"""
# MAGIC     CREATE TABLE IF NOT EXISTS %s
# MAGIC     USING DELTA
# MAGIC     LOCATION "%s"
# MAGIC   """.format(tableName, queryDataPath))
# MAGIC }
# MAGIC catch
# MAGIC {
# MAGIC   case x: AnalysisException =>
# MAGIC   {
# MAGIC     println("No records.")
# MAGIC   }
# MAGIC }

# COMMAND ----------

snb.log_notebook_end(0)
dbutils.notebook.exit("Succeeded")
