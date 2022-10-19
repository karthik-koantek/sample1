// Databricks notebook source
// MAGIC %md
// MAGIC #### Supply Inputs
// MAGIC * Navigate to your Event Hub in the Azure Portal.
// MAGIC * Create a new Shared access policy with Listen permissions.
// MAGIC * Copy the Connection string-primary key and the Event Hub name and replace below.

// COMMAND ----------

val cs = "<event hub connection string-primary key>"
val eh = "<event hub name>"

// COMMAND ----------

// MAGIC %md
// MAGIC * offset: tells your stream to start from the beginning of the Event Hub queue.  You can alternatively provide an offset and your stream will only return data newer than that offset.
// MAGIC * maxEventsPerTrigger: Since we are returning data to the screen, how long for the stream to wait until it does so.

// COMMAND ----------

val offset = 0
val maxEventsPerTrigger = 5

// COMMAND ----------

// MAGIC %md
// MAGIC #### Create Stream

// COMMAND ----------

import org.apache.spark.eventhubs.{ ConnectionStringBuilder, EventHubsConf, EventPosition }
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

// COMMAND ----------

val connectionString = ConnectionStringBuilder(cs)
  .setEventHubName(eh)
  .build

val eventHubsConf = EventHubsConf(connectionString.toString())
  .setMaxEventsPerTrigger(maxEventsPerTrigger)
  .setStartingPosition(EventPosition.fromSequenceNumber(offset))

val eventHubs = spark.readStream
  .format("eventhubs")
  .options(eventHubsConf.toMap)
  .load()

// COMMAND ----------

// MAGIC %md
// MAGIC #### Output Raw Stream results to console
// MAGIC
// MAGIC Note: This will run until you cancel it

// COMMAND ----------

eventHubs.printSchema

// COMMAND ----------

eventHubs.writeStream.outputMode("append").format("console").option("truncate", false).start().awaitTermination()

// COMMAND ----------

// MAGIC %md
// MAGIC #### Output Formatted Stream results to console
// MAGIC
// MAGIC Note: This will run until you cancel it

// COMMAND ----------

val messages = eventHubs
  .withColumn("Offset", $"offset".cast(LongType))
  .withColumn("Time (readable)", $"enqueuedTime".cast(TimestampType))
  .withColumn("Timestamp", $"enqueuedTime".cast(LongType))
  .withColumn("Body", $"body".cast(StringType))
  .select("Offset", "Time (readable)", "Timestamp", "Body")

// COMMAND ----------

messages.printSchema

// COMMAND ----------

messages.writeStream.outputMode("append").format("console").option("truncate", false).start().awaitTermination()