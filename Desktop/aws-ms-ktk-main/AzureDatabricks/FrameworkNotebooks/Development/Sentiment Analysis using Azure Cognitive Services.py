# Databricks notebook source
# MAGIC %md # Sentiment Analysis using Azure Cognitive Services

# COMMAND ----------

# MAGIC %run "../Orchestration/Accelerator Functions"

# COMMAND ----------

# MAGIC %scala
# MAGIC import org.apache.spark.sql.types._
# MAGIC import org.apache.spark.sql.functions._
# MAGIC import org.apache.spark.sql.streaming.Trigger
# MAGIC import org.apache.spark.sql.{AnalysisException}

# COMMAND ----------

import datetime, json
import time

dbutils.widgets.text(name="stepLogGuid", defaultValue="00000000-0000-0000-0000-000000000000", label="stepLogGuid")
dbutils.widgets.text(name="stepKey", defaultValue="-1", label="stepKey")
dbutils.widgets.text(name="externalSystem", defaultValue="CognitiveServices", label="External System") #for Cognitive Services
dbutils.widgets.dropdown(name="trigger", defaultValue="Once", choices=["Once", "Microbatch"], label="Trigger")
dbutils.widgets.text(name="maxEventsPerTrigger", defaultValue="10000", label="Max Events Per Trigger")
dbutils.widgets.text(name="interval", defaultValue="10", label="Interval Seconds")
dbutils.widgets.text(name="tableName", defaultValue="eventhubtwitteringestion", label="Table Name")
dbutils.widgets.text(name="columnName", defaultValue="Body", label="Sentiment Column")
dbutils.widgets.text(name="scoredTableName", defaultValue="tweetswithsentiment", label="Scored Table Name")

stepLogGuid = dbutils.widgets.get("stepLogGuid")
stepKey = int(dbutils.widgets.get("stepKey"))
externalSystem = dbutils.widgets.get("externalSystem")
trigger = dbutils.widgets.get("trigger")
maxEventsPerTrigger = dbutils.widgets.get("maxEventsPerTrigger")
interval = dbutils.widgets.get("interval")
tableName = dbutils.widgets.get("tableName")
columnName = dbutils.widgets.get("columnName")
scoredTableName = dbutils.widgets.get("scoredTableName")
queryName = "{0} sentiment".format(tableName)
checkpointPath = "{0}/query/checkpoint/{1}/{2}".format(basepath, externalSystem, scoredTableName)
sentimentDataPath = "{0}/query/{1}/{2}".format(basepath, externalSystem, scoredTableName)

context = dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson()
p = {
  "stepLogGuid": stepLogGuid,
  "stepKey": stepKey,
  "externalSystem": externalSystem,
  "trigger": trigger,
  "maxEventsPerTrigger": maxEventsPerTrigger,
  "interval": interval,
  "tableName": tableName,
  "columnName": columnName,
  "scoredTableName": scoredTableName,
  "queryName": queryName,
  "checkpointPath": checkpointPath,
  "sentimentDataPath": sentimentDataPath
}
parameters = json.dumps(p)

notebookLogGuid = str(uuid.uuid4())
log_notebook_start(notebookLogGuid, stepLogGuid, stepKey, parameters, context, server, database, login, pwd)

print("Notebook Log Guid: {0}".format(notebookLogGuid))
print("Step Log Guid: {0}".format(stepLogGuid))
print("Context: {0}".format(context))
print("Parameters: {0}".format(parameters))

# COMMAND ----------

# MAGIC %scala
# MAGIC val externalSystem = dbutils.widgets.get("externalSystem")
# MAGIC val trigger = dbutils.widgets.get("trigger")
# MAGIC val maxEventsPerTrigger = dbutils.widgets.get("maxEventsPerTrigger")
# MAGIC val interval = dbutils.widgets.get("interval")
# MAGIC val tableName = dbutils.widgets.get("tableName")
# MAGIC val columnName = dbutils.widgets.get("columnName")
# MAGIC val scoredTableName = dbutils.widgets.get("scoredTableName")
# MAGIC val cognitiveServicesAccessKey = dbutils.secrets.get(scope=externalSystem, key="AccessKey")
# MAGIC val endpoint = dbutils.secrets.get(scope=externalSystem, key="Endpoint")
# MAGIC val langPath = dbutils.secrets.get(scope=externalSystem, key="LanguagesPath")
# MAGIC val sentPath = dbutils.secrets.get(scope=externalSystem, key="SentimentPath")
# MAGIC val queryName = "%s sentiment".format(tableName)
# MAGIC val checkpointPath = "%s/query/checkpoint/%s/%s".format(basepath, externalSystem, scoredTableName)
# MAGIC val sentimentDataPath = "%s/query/%s/%s".format(basepath, externalSystem, scoredTableName)

# COMMAND ----------

# MAGIC %scala
# MAGIC import org.apache.spark.sql.types._
# MAGIC import org.apache.spark.sql.functions._
# MAGIC
# MAGIC val df = spark.readStream.format("delta").table(tableName)
# MAGIC df.printSchema

# COMMAND ----------

# MAGIC %scala
# MAGIC import java.io._
# MAGIC import java.net._
# MAGIC import java.util._
# MAGIC
# MAGIC case class Language(documents: Array[LanguageDocuments], errors: Array[Any]) extends Serializable
# MAGIC case class LanguageDocuments(id: String, detectedLanguages: Array[DetectedLanguages]) extends Serializable
# MAGIC case class DetectedLanguages(name: String, iso6391Name: String, score: Double) extends Serializable
# MAGIC
# MAGIC case class Sentiment(documents: Array[SentimentDocuments], errors: Array[Any]) extends Serializable
# MAGIC case class SentimentDocuments(id: String, score: Double) extends Serializable
# MAGIC
# MAGIC case class RequestToTextApi(documents: Array[RequestToTextApiDocument]) extends Serializable
# MAGIC case class RequestToTextApiDocument(id: String, text: String, var lanugage: String = "") extends Serializable

# COMMAND ----------

# MAGIC %scala
# MAGIC import javax.net.ssl.HttpsURLConnection
# MAGIC import com.google.gson.Gson
# MAGIC import com.google.gson.GsonBuilder
# MAGIC import com.google.gson.JsonObject
# MAGIC import com.google.gson.JsonParser
# MAGIC import scala.util.parsing.json._
# MAGIC
# MAGIC object SentimentDetector extends Serializable {
# MAGIC   val accessKey = cognitiveServicesAccessKey
# MAGIC   val host = endpoint
# MAGIC   val languagesPath = langPath
# MAGIC   val sentimentPath = sentPath
# MAGIC   val languagesUrl = new URL(host+languagesPath)
# MAGIC   val sentimentUrl = new URL(host+sentimentPath)
# MAGIC   val g = new Gson
# MAGIC
# MAGIC   def getConnection(path: URL): HttpsURLConnection = {
# MAGIC     val connection = path.openConnection().asInstanceOf[HttpsURLConnection]
# MAGIC     connection.setRequestMethod("POST")
# MAGIC     connection.setRequestProperty("Content-Type", "text/json")
# MAGIC     connection.setRequestProperty("Ocp-Apim-Subscription-Key", accessKey)
# MAGIC     connection.setDoOutput(true)
# MAGIC     return connection
# MAGIC   }
# MAGIC
# MAGIC   def prettify (json_text: String): String = {
# MAGIC     val parser = new JsonParser()
# MAGIC     val json = parser.parse(json_text).getAsJsonObject()
# MAGIC     val gson = new GsonBuilder().setPrettyPrinting().create()
# MAGIC     return gson.toJson(json)
# MAGIC   }
# MAGIC
# MAGIC   def processUsingApi(request: RequestToTextApi, path: URL): String = {
# MAGIC         val requestToJson = g.toJson(request)
# MAGIC         val encoded_text = requestToJson.getBytes("UTF-8")
# MAGIC         val connection = getConnection(path)
# MAGIC         val wr = new DataOutputStream(connection.getOutputStream())
# MAGIC         wr.write(encoded_text, 0, encoded_text.length)
# MAGIC         wr.flush()
# MAGIC         wr.close()
# MAGIC
# MAGIC         val response = new StringBuilder()
# MAGIC         val in = new BufferedReader(new InputStreamReader(connection.getInputStream()))
# MAGIC         var line = in.readLine()
# MAGIC         while (line != null) {
# MAGIC             response.append(line)
# MAGIC             line = in.readLine()
# MAGIC         }
# MAGIC         in.close()
# MAGIC         return response.toString()
# MAGIC     }
# MAGIC
# MAGIC   def getLanguage (inputDocs: RequestToTextApi): Option[Language] = {
# MAGIC     try {
# MAGIC       val response = processUsingApi(inputDocs, languagesUrl)
# MAGIC       val niceResponse = prettify(response)
# MAGIC       val language = g.fromJson(niceResponse, classOf[Language])
# MAGIC       if (language.documents(0).detectedLanguages(0).iso6391Name == "(Unknown)")
# MAGIC         return None
# MAGIC       return Some(language)
# MAGIC     } catch {
# MAGIC       case e: Exception => return None
# MAGIC     }
# MAGIC   }
# MAGIC
# MAGIC   def getSentiment(inputDocs: RequestToTextApi): Option[Sentiment] = {
# MAGIC     try {
# MAGIC       val response = processUsingApi(inputDocs, sentimentUrl)
# MAGIC       val niceResponse = prettify(response)
# MAGIC       val sentiment = g.fromJson(niceResponse, classOf[Sentiment])
# MAGIC       return Some(sentiment)
# MAGIC     } catch {
# MAGIC       case e: Exception => return None
# MAGIC     }
# MAGIC   }
# MAGIC }

# COMMAND ----------

# MAGIC %scala
# MAGIC val toSentiment =
# MAGIC     udf((textContent: String) =>
# MAGIC         {
# MAGIC             val inputObject = new RequestToTextApi(Array(new RequestToTextApiDocument(textContent, textContent)))
# MAGIC             val detectedLanguage = SentimentDetector.getLanguage(inputObject)
# MAGIC             detectedLanguage match {
# MAGIC                 case Some(language) =>
# MAGIC                     if(language.documents.size > 0) {
# MAGIC                         //inputObject.documents(0).language = language.documents(0).detectedLanguages(0).iso6391Name
# MAGIC                         val sentimentDetected = SentimentDetector.getSentiment(inputObject)
# MAGIC                         sentimentDetected match {
# MAGIC                             case Some(sentiment) => {
# MAGIC                                 if(sentiment.documents.size > 0) {
# MAGIC                                     sentiment.documents(0).score.toString()
# MAGIC                                 }
# MAGIC                                 else {
# MAGIC                                     "Error happened when getting sentiment: " + sentiment.errors(0).toString
# MAGIC                                 }
# MAGIC                             }
# MAGIC                             case None => "Couldn't detect sentiment"
# MAGIC                         }
# MAGIC                     }
# MAGIC                     else {
# MAGIC                         "Error happened when getting language" + language.errors(0).toString
# MAGIC                     }
# MAGIC                 case None => "Couldn't detect language"
# MAGIC             }
# MAGIC         }
# MAGIC     )

# COMMAND ----------

# MAGIC %scala
# MAGIC if(trigger == "Once") {
# MAGIC   val dfSentiment = (df
# MAGIC    .withColumn("Sentiment", toSentiment(col(columnName:String)))
# MAGIC    .writeStream
# MAGIC    .queryName(queryName)
# MAGIC    .trigger(Trigger.Once())
# MAGIC    .format("delta")
# MAGIC    .option("checkpointLocation", checkpointPath)
# MAGIC    .outputMode("append")
# MAGIC    .start(sentimentDataPath))
# MAGIC } else {
# MAGIC   val processingTime = "%s seconds".format(interval)
# MAGIC   val dfSentiment = (df
# MAGIC    .withColumn("Sentiment", toSentiment(col(columnName:String)))
# MAGIC    .writeStream
# MAGIC    .queryName(queryName)
# MAGIC    .trigger(Trigger.ProcessingTime(processingTime))
# MAGIC    .format("delta")
# MAGIC    .option("checkpointLocation", checkpointPath)
# MAGIC    .outputMode("append")
# MAGIC    .start(sentimentDataPath))
# MAGIC }

# COMMAND ----------

# MAGIC %scala
# MAGIC try
# MAGIC {
# MAGIC   spark.sql(s"""
# MAGIC     CREATE TABLE IF NOT EXISTS %s
# MAGIC     USING DELTA
# MAGIC     LOCATION "%s"
# MAGIC   """.format(scoredTableName, sentimentDataPath))
# MAGIC }
# MAGIC catch
# MAGIC {
# MAGIC   case x: AnalysisException =>
# MAGIC   {
# MAGIC     println("No records.")
# MAGIC   }
# MAGIC }

# COMMAND ----------

if trigger == "Microbatch":
  time.sleep(30)
  streams = [s for s in spark.streams.active if s.name in [queryName]]
  while (1==1):
    streams = [s for s in spark.streams.active if s.name in [queryName]]
    if len(streams) == 0:
      break
    for stream in streams:
      if stream.lastProgress['numInputRows'] == 0:
        stream.stop()
    time.sleep(30)

# COMMAND ----------

log_notebook_end(notebookLogGuid, 0, server, database, login, pwd)
dbutils.notebook.exit("Succeeded")