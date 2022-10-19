# Databricks notebook source
# MAGIC %md # Output to Event Hub
# MAGIC 
# MAGIC Structured Streaming to load a Delta table to an Azure Event Hub.
# MAGIC 
# MAGIC #### Usage
# MAGIC Supply the parameters above and run the notebook.
# MAGIC 
# MAGIC #### Prerequisites
# MAGIC * Assumes a databricks secret scope has been created with the same name as the External System.
# MAGIC * Databricks Cluster must have the Azure Event Hub Maven Library installed: Import Library with Maven coordinates: com.microsoft.azure:azure-eventhubs-spark_2.11:2.3.14.1
# MAGIC 
# MAGIC #### Details
# MAGIC * https://github.com/Azure/azure-event-hubs-spark/blob/master/docs/structured-streaming-eventhubs-integration.md
# MAGIC * https://docs.microsoft.com/en-us/azure/databricks/spark/latest/structured-streaming/streaming-event-hubs
# MAGIC * https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#triggers

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
dbutils.widgets.text(name="interval", defaultValue="10", label="Interval Seconds")
dbutils.widgets.text(name="tableName", defaultValue="", label="Table Name")
dbutils.widgets.text(name="stopSeconds", defaultValue="120", label="Stop after Seconds of No Activity")
dbutils.widgets.dropdown(name="destination", defaultValue="silvergeneral", choices=["silverprotected", "silvergeneral"], label="Destination")

widgets = ["stepLogGuid", "stepKey", "externalSystem", "trigger", "maxEventsPerTrigger", "interval", "tableName", "stopSeconds", "destination"]
secrets = ["EventHubConnectionString", "EventHubName", "EventHubConsumerGroup"]

# COMMAND ----------

snb = ktk.SingleResponsibilityNotebook(widgets, secrets)

# COMMAND ----------

checkpointPath = "{0}/checkpoint/{1}/{2}/".format(snb.basePath, snb.externalSystem, snb.EventHubName)
queryName = "{0} query".format(snb.EventHubName)

p = {
  "checkpointPath": checkpointPath,
  "queryName": queryName
}

parameters = json.dumps(snb.mergeAttributes(p))
snb.log_notebook_start(parameters)
print("Parameters:")
snb.displayAttributes()

# COMMAND ----------

refreshed = u.refreshTable(snb.tableName)
if refreshed == False:
  snb.log_notebook_end(0)
  dbutils.notebook.exit("Table does not exist")

# COMMAND ----------

# MAGIC %scala
# MAGIC val trigger = dbutils.widgets.get("trigger")
# MAGIC val maxEventsPerTrigger = dbutils.widgets.get("maxEventsPerTrigger")
# MAGIC val interval = dbutils.widgets.get("interval")
# MAGIC val eventHubStartingPosition = 0
# MAGIC val tableName = dbutils.widgets.get("tableName")
# MAGIC val externalSystem = dbutils.widgets.get("externalSystem")
# MAGIC val eventHubConnectionString = dbutils.secrets.get(scope=externalSystem, key="EventHubConnectionString")
# MAGIC val eventHubName = dbutils.secrets.get(scope=externalSystem, key="EventHubName")
# MAGIC val eventHubConsumerGroup = dbutils.secrets.get(scope=externalSystem, key="EventHubConsumerGroup")
# MAGIC var destination = dbutils.widgets.get("destination")
# MAGIC var basePath = ""
# MAGIC 
# MAGIC if (destination == "silverprotected") {
# MAGIC   basePath = silverProtectedBasePath
# MAGIC } else {
# MAGIC   basePath = silverGeneralBasePath
# MAGIC }
# MAGIC 
# MAGIC val checkpointPath = "%s/query/checkpoint/%s/%s/".format(basePath, externalSystem, eventHubName)
# MAGIC val queryName = "%s query".format(eventHubName)
# MAGIC 
# MAGIC val connectionString = ConnectionStringBuilder(eventHubConnectionString)
# MAGIC   .setEventHubName(eventHubName)
# MAGIC   .build
# MAGIC 
# MAGIC val eventHubsConf = EventHubsConf(connectionString.toString())
# MAGIC   .setMaxEventsPerTrigger(maxEventsPerTrigger.toInt)

# COMMAND ----------

# MAGIC %scala
# MAGIC import org.apache.spark.sql.types._
# MAGIC import org.apache.spark.sql.functions._
# MAGIC 
# MAGIC val df = spark.readStream.format("delta").table(tableName)
# MAGIC df.printSchema

# COMMAND ----------

# MAGIC %scala
# MAGIC val eventHubSink = df
# MAGIC   .withColumn("body", to_json(struct(col("*"))))

# COMMAND ----------

# MAGIC %scala
# MAGIC if(trigger=="Once") {
# MAGIC   val dfQuery = (eventHubSink
# MAGIC     .select("body")
# MAGIC     .writeStream
# MAGIC     .format("eventhubs")
# MAGIC     .options(eventHubsConf.toMap)
# MAGIC     .option("checkpointLocation", checkpointPath)
# MAGIC     .outputMode("append")
# MAGIC     .queryName(queryName)
# MAGIC     .trigger(Trigger.Once())
# MAGIC     .start()
# MAGIC   )
# MAGIC } else {
# MAGIC   if(trigger=="Microbatch") {
# MAGIC     val processingTime = "%s seconds".format(interval)
# MAGIC     val dfQuery = (eventHubSink
# MAGIC       .select("body")
# MAGIC       .writeStream
# MAGIC       .format("eventhubs")
# MAGIC       .options(eventHubsConf.toMap)
# MAGIC       .option("checkpointLocation", checkpointPath)
# MAGIC       .outputMode("append")
# MAGIC       .queryName(queryName)
# MAGIC       .trigger(Trigger.ProcessingTime(processingTime))
# MAGIC       .start()
# MAGIC     )
# MAGIC   }
# MAGIC }

# COMMAND ----------

if snb.trigger == "Microbatch":
  time.sleep(int(snb.stopSeconds))
  streams = [s for s in spark.streams.active if s.name in [snb.queryName]]
  while (1==1):
    streams = [s for s in spark.streams.active if s.name in [snb.queryName]]
    if len(streams) == 0:
      break
    for stream in streams:
      if stream.lastProgress['numInputRows'] == 0:
        stream.stop()
    time.sleep(int(snb.stopSeconds))

# COMMAND ----------

snb.log_notebook_end(0)
dbutils.notebook.exit("Succeeded")
