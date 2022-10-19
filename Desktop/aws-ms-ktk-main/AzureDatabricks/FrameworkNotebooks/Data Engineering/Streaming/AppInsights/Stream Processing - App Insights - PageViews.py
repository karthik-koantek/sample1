# Databricks notebook source
# MAGIC %md # Stream Processing - App Insights - PageViews
# MAGIC
# MAGIC Structured Streaming to process the App Insights telemetry data for a given App Insights Instance, Data Type and Date.
# MAGIC
# MAGIC #### Usage
# MAGIC
# MAGIC #### Prerequisites
# MAGIC
# MAGIC #### Details

# COMMAND ----------

# MAGIC %md #### Code

# COMMAND ----------

import datetime, json
from pyspark.sql.types import StructType
from pyspark.sql.functions import explode, col, unix_timestamp, expr, lit
import time

# COMMAND ----------

# MAGIC %md #### Initialize

# COMMAND ----------

# MAGIC %run "../../../Orchestration/Notebook Functions"

# COMMAND ----------

# MAGIC %run "../../../Development/Utilities"

# COMMAND ----------

# MAGIC %run "../../../Data Engineering/Streaming/AppInsights/AppInsights_Flattening_Functions"

# COMMAND ----------

dbutils.widgets.text(name="stepLogGuid", defaultValue="00000000-0000-0000-0000-000000000000", label="stepLogGuid")
dbutils.widgets.text(name="stepKey", defaultValue="-1", label="stepKey")
dbutils.widgets.text(name="dateToProcess", defaultValue="", label="Date to Process")
dbutils.widgets.text(name="appInsightsInstanceName", defaultValue="", label="App Insights Instance Name")
dbutils.widgets.text(name="externalSystem", defaultValue="", label="External System")
externalSystem = dbutils.widgets.get("externalSystem")
appInsightsFullStorageAccountName = dbutils.secrets.get(scope=externalSystem, key="StorageAccountName")
appInsightsBasePath = dbutils.secrets.get(scope=externalSystem, key="BasePath")
appInsightsStorageAccountKey = dbutils.secrets.get(scope="external", key="StorageAccountKey")
appInsightsContainerOrFileSystemName = dbutils.secrets.get(scope=externalSystem, key="ContainerOrFileSystemName")
stepLogGuid = dbutils.widgets.get("stepLogGuid")
stepKey = int(dbutils.widgets.get("stepKey"))
dateToProcess = dbutils.widgets.get("dateToProcess")
if dateToProcess == "":
  dateToProcess = datetime.datetime.utcnow().strftime('%Y-%m-%d')
  rawDateToProcess = datetime.datetime.utcnow().strftime('%Y/%m/%d')
else:
  dateToProcess = dateToProcess.replace("/","-")
  rawDateToProcess = dateToProcess.replace("-","/")
appInsightsInstanceName = dbutils.widgets.get("appInsightsInstanceName")
appInsightsDataType = "PageViews"

schemaPath = "{0}/raw/schemas/appinsights/{1}".format(basepath, appInsightsDataType)
schemaFile = schemaPath + "/schema.json"
externalPath = "{0}/{1}/{2}/{3}/*/*".format(appInsightsBasePath, appInsightsInstanceName, appInsightsDataType, dateToProcess)
rawDataPath = "{0}/raw/appinsights/{1}/{2}".format(basepath, appInsightsDataType, appInsightsInstanceName)
queryDataPath = "{0}/query/appinsights/{1}".format(basepath, appInsightsDataType)
rawCheckpointPath = "{0}/raw/checkpoint/{1}/{2}".format(basepath, appInsightsDataType, appInsightsInstanceName)
queryCheckpointPath = "{0}/query/checkpoint/{1}".format(basepath, appInsightsDataType)

context = dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson()
p = {
  "stepLogGuid": stepLogGuid,
  "stepKey": stepKey,
  "dateToProcess": dateToProcess,
  "appInsightsInstanceName": appInsightsInstanceName,
  "appInsightsDataType": appInsightsDataType,
  "appInsightsBasePath": appInsightsBasePath,
  "schemaPath": schemaPath,
  "schemaFile": schemaFile,
  "externalPath": externalPath,
  "rawDataPath": rawDataPath,
  "queryDataPath": queryDataPath,
  "rawCheckPointPath": rawCheckpointPath,
  "queryCheckPointPath": queryCheckpointPath
}
parameters = json.dumps(p)
notebookLogGuid = str(uuid.uuid4())
log_notebook_start(notebookLogGuid, stepLogGuid, stepKey, parameters, context, server, database, login, pwd)

print("Notebook Log Guid: {0}".format(notebookLogGuid))
print("Step Log Guid: {0}".format(stepLogGuid))
print("Context: {0}".format(context))
print("Parameters: {0}".format(parameters))

# COMMAND ----------

spark.conf.set(
  appInsightsFullStorageAccountName,
  appInsightsStorageAccountKey)

# COMMAND ----------

externalStorageAccountName = dbutils.secrets.get(scope="external", key="StorageAccountName")
externalStorageAccountKey = dbutils.secrets.get(scope="external", key="StorageAccountKey")
bizAppsStorageAccountName = dbutils.secrets.get(scope="external_bizapps", key="StorageAccountName")
bizAppsStorageAccountKey = dbutils.secrets.get(scope="external_bizapps", key="StorageAccountKey")
unauthStorageAccountName = dbutils.secrets.get(scope="external_unauth", key="StorageAccountName")
unauthStorageAccountKey = dbutils.secrets.get(scope="external_unauth", key="StorageAccountKey")
bizAppsPartnerStorageAccountName = dbutils.secrets.get(scope="external_bizapps_p", key="StorageAccountName")
bizAppsPartnerStorageAccountKey = dbutils.secrets.get(scope="external_bizapps_p", key="StorageAccountKey")

spark.conf.set(
  externalStorageAccountName,
  externalStorageAccountKey)

spark.conf.set(
  bizAppsStorageAccountName,
  bizAppsStorageAccountKey)

spark.conf.set(
  unauthStorageAccountName,
  unauthStorageAccountKey)

spark.conf.set(
  bizAppsPartnerStorageAccountName,
  bizAppsPartnerStorageAccountKey)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Validate Source Path Exists and Has Data

# COMMAND ----------

#HOTFIX
# try:
#   path = "{0}/{1}/{2}".format(appInsightsInstanceName, appInsightsDataType, dateToProcess)
#   blobs = listBlobsInBlobStorage(appInsightsStorageAccountName, appInsightsStorageAccountKey, appInsightsContainerOrFileSystemName, path)
#   if len(blobs) > 0:
#     print("Found files to process")
#   else:
#     log_notebook_end(notebookLogGuid, 0, server, database, login, pwd)
#     dbutils.notebook.exit("External Path supplied has no valid files to process")
# except Exception as e:
#   log_notebook_end(notebookLogGuid, 0, server, database, login, pwd)
#   dbutils.notebook.exit("External Path supplied has no valid files to process")

# COMMAND ----------

try:
  hours = dbutils.fs.ls("{0}/{1}/{2}/{3}/".format(appInsightsBasePath, appInsightsInstanceName, appInsightsDataType, dateToProcess))
  if len(hours) > 0:
    print("Found files to process")
  else:
    log_notebook_end(notebookLogGuid, 0, server, database, login, pwd)
    dbutils.notebook.exit("External Path supplied has no valid files to process")
except Exception as e:
  log_notebook_end(notebookLogGuid, 0, server, database, login, pwd)
  dbutils.notebook.exit("External Path supplied has no valid files to process")

# COMMAND ----------

# MAGIC %md #### Read Schema

# COMMAND ----------

try:
  head = dbutils.fs.head(schemaFile, 256000)
except Exception as e:
  err = {
    "sourceName": "Stream Processing - App Insights - PageViews: Read Schema",
    "errorCode": "100",
    "errorDescription": "Schema was not found in directory: {0}".format(schemaFile)
  }
  error = json.dumps(err)
  log_notebook_error(notebookLogGuid, error, server, database, login, pwd)
  raise(e)

schema = StructType.fromJson(json.loads(head))

# COMMAND ----------

schema

# COMMAND ----------

# MAGIC %md #### Read Stream

# COMMAND ----------

try:
  df = spark \
    .readStream \
    .schema(schema) \
    .json(externalPath) \
    .withColumn("appInsightsInstancename", lit(appInsightsInstanceName))
except Exception as e:
  err = {
    "sourceName": "Stream Processing - App Insights - PageViews: Read Stream",
    "errorCode": "100",
    "errorDescription": e.__class__.__name__
  }
  error = json.dumps(err)
  log_notebook_error(notebookLogGuid, error, server, database, login, pwd)
  raise(e)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Flatten
# MAGIC * Convert from nested JSON/Array Struct into tabular dataframe

# COMMAND ----------

pageViewsDF, pageViewsViewFlattenedDF = flatten_AppInsights_df_PageViews (df)

# COMMAND ----------

# MAGIC %md #### Write Streams

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Bronze Zone

# COMMAND ----------

try:
  rawDF = (df \
    .writeStream \
    .queryName("pageviews raw") \
    .trigger(once=True) \
    .format("json") \
    .option("checkpointLocation", rawCheckpointPath + "/pageviewsRaw/" + rawDateToProcess) \
    .outputMode("append") \
    .start(rawDataPath + "/pageviews/" + dateToProcess)
)
except Exception as e:
  err = {
    "sourceName": "Stream Processing - App Insights - Event: Write Streams, Bronze Zone",
    "errorCode": "300",
    "errorDescription": e.__class__.__name__
  }
  error = json.dumps(err)
  log_notebook_error(notebookLogGuid, error, server, database, login, pwd)
  raise(e)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Silver Zone

# COMMAND ----------

try:
  pageviewsDFQueryZone = (pageViewsDF \
    .writeStream \
    .queryName("pageviews query") \
    .trigger(once=True) \
    .format("delta") \
    .option("checkpointLocation", queryCheckpointPath + "/pageviewsQuery") \
    .outputMode("append") \
    .start(queryDataPath + "/pagviews")
  )

  pageviewsViewDFQueryZone = (pageViewsViewFlattenedDF \
    .writeStream \
    .queryName("pageviews view query") \
    .trigger(once=True) \
    .format("delta") \
    .option("checkpointLocation", queryCheckpointPath + "/pageviewsViewQuery") \
    .outputMode("append") \
    .start(queryDataPath + "/pageviewsView")
  )
except Exception as e:
  err = {
    "sourceName": "Stream Processing - App Insights - Event: Write Streams, Silver Zone",
    "errorCode": "400",
    "errorDescription": e.__class__.__name__
  }
  error = json.dumps(err)
  log_notebook_error(notebookLogGuid, error, server, database, login, pwd)
  raise(e)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Stop Streams when no more records to process

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

# MAGIC %md #### Log Completion

# COMMAND ----------

log_notebook_end(notebookLogGuid, 0, server, database, login, pwd)
dbutils.notebook.exit("Succeeded")

# COMMAND ----------

