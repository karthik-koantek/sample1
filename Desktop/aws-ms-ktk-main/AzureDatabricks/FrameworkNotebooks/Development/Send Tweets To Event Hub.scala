// Databricks notebook source
// MAGIC %md # Send Tweets To Event Hub
// MAGIC Uses the Twitter Developer API to obtain tweets containing a hashtag and save them in an Event Hub.
// MAGIC
// MAGIC #### Usage
// MAGIC Supply the parameters and run the notebook.
// MAGIC
// MAGIC Note that setting the timeoutSeconds to 0 will cause this to run until it is stopped.
// MAGIC
// MAGIC #### Prerequisites
// MAGIC * Databricks Cluster must have the Azure Event Hub Maven Library installed: Import Library with Maven coordinates: com.microsoft.azure:azure-eventhubs-spark_2.11:2.3.14.1
// MAGIC * Databricks Cluster must have the Twitter4j Library installed: Import Library with Maven coordinates: org.twitter4j:twitter4j-core:4.0.7
// MAGIC * External System EventHubTwitterIngestion must exist.
// MAGIC * External System TwitterAPI must exist.
// MAGIC
// MAGIC #### Details
// MAGIC * Twitter Developer API: https://developer.twitter.com/en
// MAGIC * To create TwitterAPI, run Powershell script /ApplicationConfiguration/Powershell/Scripts/ExternalSystemSecrets/CreateExternalSecretsForTwitterAPI.ps1
// MAGIC * To create EventHubTwitterIngestion, run Powershell script /ApplicationConfiguration/Powershell/Scripts/ExternalSystemSecrets/CreateExternalSecretsForEventHub.ps1

// COMMAND ----------

// MAGIC %md
// MAGIC #### Initialize

// COMMAND ----------

// MAGIC %run "/Framework/Orchestration/Notebook Functions"

// COMMAND ----------

// MAGIC %python
// MAGIC import datetime, json
// MAGIC import time
// MAGIC
// MAGIC dbutils.widgets.text(name="stepLogGuid", defaultValue="00000000-0000-0000-0000-000000000000", label="stepLogGuid")
// MAGIC dbutils.widgets.text(name="stepKey", defaultValue="-1", label="stepKey")
// MAGIC dbutils.widgets.text("dataSource", "twitter", "Data Source")
// MAGIC dbutils.widgets.text("hashtag", "Databricks", "Hashtag")
// MAGIC dbutils.widgets.text("eventHubExternalSystem", "EventHubTwitterIngestion")
// MAGIC dbutils.widgets.text("twitterExternalSystem", "TwitterAPI")
// MAGIC dbutils.widgets.text("timeoutSeconds", "0", "Timeout Seconds")
// MAGIC
// MAGIC stepLogGuid = dbutils.widgets.get("stepLogGuid")
// MAGIC stepKey = int(dbutils.widgets.get("stepKey"))
// MAGIC dataSource = dbutils.widgets.get("dataSource")
// MAGIC hashtag = dbutils.widgets.get("hashtag")
// MAGIC eventHubExternalSystem = dbutils.widgets.get("eventHubExternalSystem")
// MAGIC twitterExternalSystem = dbutils.widgets.get("twitterExternalSystem")
// MAGIC timeoutSeconds = int(dbutils.widgets.get("timeoutSeconds"))
// MAGIC
// MAGIC context = dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson()
// MAGIC p = {
// MAGIC   "stepLogGuid": stepLogGuid,
// MAGIC   "stepKey": stepKey,
// MAGIC   "dataSource": dataSource,
// MAGIC   "hashtag": hashtag,
// MAGIC   "eventHubExternalSystem": eventHubExternalSystem,
// MAGIC   "twitterExternalSystem": twitterExternalSystem,
// MAGIC   "timeoutSeconds": timeoutSeconds
// MAGIC }
// MAGIC parameters = json.dumps(p)
// MAGIC notebookLogGuid = str(uuid.uuid4())
// MAGIC log_notebook_start(notebookLogGuid, stepLogGuid, stepKey, parameters, context, server, database, login, pwd)
// MAGIC
// MAGIC print("Notebook Log Guid: {0}".format(notebookLogGuid))
// MAGIC print("Step Log Guid: {0}".format(stepLogGuid))
// MAGIC print("Context: {0}".format(context))
// MAGIC print("Parameters: {0}".format(parameters))

// COMMAND ----------

import scala.collection.JavaConverters._
import com.microsoft.azure.eventhubs._
import java.util.concurrent._
import scala.collection.immutable._
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global
import java.time.{OffsetDateTime, LocalDate}
import java.time.temporal.ChronoUnit
import java.time.format.DateTimeFormatter

// COMMAND ----------

val dataSource = dbutils.widgets.get("dataSource")
val hashtag = dbutils.widgets.get("hashtag")
val eventHubExternalSystem = dbutils.widgets.get("eventHubExternalSystem")
val twitterExternalSystem = dbutils.widgets.get("twitterExternalSystem")
val timeoutSeconds = dbutils.widgets.get("timeoutSeconds").toLong
val endTime: OffsetDateTime = OffsetDateTime.now().plus(timeoutSeconds, ChronoUnit.SECONDS)

// COMMAND ----------

val eventHubConnectionString = dbutils.secrets.get(scope=eventHubExternalSystem, key="EventHubConnectionString")
val eventHubName = dbutils.secrets.get(scope=eventHubExternalSystem, key="EventHubName")
val eventHubConsumerGroup = dbutils.secrets.get(scope=eventHubExternalSystem, key="EventHubConsumerGroup")
val twitterAPIKey = dbutils.secrets.get(scope=twitterExternalSystem, key="APIKey")
val twitterSecretKey = dbutils.secrets.get(scope=twitterExternalSystem, key="SecretKey")
val twitterAccessToken = dbutils.secrets.get(scope=twitterExternalSystem, key="AccessToken")
val twitterAccessTokenSecret = dbutils.secrets.get(scope=twitterExternalSystem, key="AccessTokenSecret")

// COMMAND ----------

// MAGIC %md
// MAGIC #### Event Hub Config

// COMMAND ----------

val connStr = new ConnectionStringBuilder(eventHubConnectionString)
            .setEventHubName(eventHubName)
            //.setEventHubConsumerGroup(eventHubConsumerGroup)

val pool = Executors.newScheduledThreadPool(1)
val eventHubClient = EventHubClient.createFromConnectionString(connStr.toString(), pool)

def sleep(time: Long): Unit = Thread.sleep(time)

def sendEvent(message: String, delay: Long) = {
  sleep(delay)
  val messageData = EventData.create(message.getBytes("UTF-8"))
  eventHubClient.get().send(messageData)
  System.out.println("Sent event: " + message + "\n")
}

val testSource = List("the greatest!", "isn't working", "is okay")

// COMMAND ----------

// MAGIC %md
// MAGIC #### Read Tweets from Twitter API and Send to Event Hub

// COMMAND ----------

if (dataSource == "twitter") {
  import twitter4j._
  import twitter4j.TwitterFactory
  import twitter4j.Twitter
  import twitter4j.conf.ConfigurationBuilder

  val cb = new ConfigurationBuilder()
    cb.setDebugEnabled(true)
    .setOAuthConsumerKey(twitterAPIKey)
    .setOAuthConsumerSecret(twitterSecretKey)
    .setOAuthAccessToken(twitterAccessToken)
    .setOAuthAccessTokenSecret(twitterAccessTokenSecret)

  val twitterFactory = new TwitterFactory(cb.build())
  val twitter = twitterFactory.getInstance()

  // Getting tweets matching hashtag and sending them to the Event Hub
  val queryString = " #%s ".format(hashtag)
  val query = new Query(queryString)
  query.setCount(100)
  query.lang("en")
  var break = false
  while (!break) {
    val result = twitter.search(query)
    val statuses = result.getTweets()
    var lowestStatusId = Long.MaxValue
    for (status <- statuses.asScala) {
      if(!status.isRetweet()){
        sendEvent(status.getText(), 5000)
      }
      lowestStatusId = Math.min(status.getId(), lowestStatusId)
    }
    query.setMaxId(lowestStatusId - 1)
    if (timeoutSeconds != 0) {
      if (endTime.isBefore(OffsetDateTime.now())) {
        break = true
      }
    }
  }
} else if (dataSource == "test") {
  // Loop through the list of test input data
  var break = false
  while (!break) {
    testSource.foreach {
      sendEvent(_,5000)
    }
    if (timeoutSeconds != 0) {
      if (endTime.isBefore(OffsetDateTime.now())) {
        break = true
      }
    }
  }
} else {
  System.out.println("Unsupported Data Source. Set 'dataSource' to \"twitter\" or \"test\"")
}

// Closing connection to the Event Hub
eventHubClient.get().close()

// COMMAND ----------

// MAGIC %python
// MAGIC log_notebook_end(notebookLogGuid, 0, server, database, login, pwd)
// MAGIC dbutils.notebook.exit("Succeeded")