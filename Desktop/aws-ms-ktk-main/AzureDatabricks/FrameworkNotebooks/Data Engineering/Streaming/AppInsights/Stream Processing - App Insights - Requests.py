# Databricks notebook source
# MAGIC %md # Stream Processing - App Insights - Requests
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

# MAGIC %run "../../../Data Engineering/Streaming/AppInsights/AppInsights_Flattening_Functions"

# COMMAND ----------

dbutils.widgets.text(name="stepLogGuid", defaultValue="00000000-0000-0000-0000-000000000000", label="stepLogGuid")
dbutils.widgets.text(name="stepKey", defaultValue="-1", label="stepKey")
dbutils.widgets.text(name="dateToProcess", defaultValue="", label="Date to Process")
dbutils.widgets.text(name="appInsightsInstanceName", defaultValue="appinsightscswapi-qa_ea7fa0dbcb8d44f6b923345ba6462ada", label="App Insights Instance Name")
dbutils.widgets.text(name="externalSystem", defaultValue="", label="External System")
externalSystem = dbutils.widgets.get("externalSystem")
appInsightsFullStorageAccountName = dbutils.secrets.get(scope=externalSystem, key="StorageAccountName")
appInsightsStorageAccountName = appInsightsFullStorageAccountName.replace("fs.azure.account.key.","").replace(".blob.core.windows.net","")
appInsightsStorageAccountKey = dbutils.secrets.get(scope=externalSystem, key="StorageAccountKey")
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
appInsightsDataType = "Requests"

appInsightsBasePath = "wasbs://{1}@{0}.blob.core.windows.net".format(appInsightsStorageAccountName, appInsightsContainerOrFileSystemName)
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
  "externalSystem": externalSystem,
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

spark.conf.set(
  externalStorageAccountName,
  externalStorageAccountKey)

spark.conf.set(
  bizAppsStorageAccountName,
  bizAppsStorageAccountKey)

spark.conf.set(
  unauthStorageAccountName,
  unauthStorageAccountKey)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Validate Source Path Exists and Has Data

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
    "sourceName": "Stream Processing - App Insights - Requests: Read Schema",
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
    "sourceName": "Stream Processing - App Insights - Requests: Read Stream",
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

requestDF, requestRequestFlattenedDF, requestContextCustomDimensionsFlattenedDF = flatten_AppInsights_df_Event (df)

# COMMAND ----------

# MAGIC %md #### Write Streams

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Raw Zone

# COMMAND ----------

rawDF = (df \
  .writeStream \
  .queryName("request raw") \
  .trigger(once=True) \
  .format("json") \
  .option("checkpointLocation", rawCheckpointPath + "/requestRaw/" + dateToProcess) \
  .outputMode("append") \
  .start(rawDataPath + "/request/" + dateToProcess)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Query Zone

# COMMAND ----------

requestDFQueryZone = (requestDF \
  .writeStream \
  .queryName("request query") \
  .trigger(once=True) \
  .format("delta") \
  .option("checkpointLocation", queryCheckpointPath + "/requestQuery") \
  .outputMode("append") \
  .start(queryDataPath + "/request")
)

requestRequestDFQueryZone = (requestRequestFlattenedDF \
  .writeStream \
  .queryName("request request query") \
  .trigger(once=True) \
  .format("delta") \
  .option("checkpointLocation", queryCheckpointPath + "/requestRequestQuery") \
  .outputMode("append") \
  .start(queryDataPath + "/requestRequest")
)

requestCustomDimensionsDFQueryZone = (requestContextCustomDimensionsFlattenedDF \
  .writeStream \
  .queryName("request custom dimensions query") \
  .trigger(once=True) \
  .format("delta") \
  .option("checkpointLocation", queryCheckpointPath + "/requestCustomDimensionsQuery") \
  .outputMode("append") \
  .start(queryDataPath + "/requestCustomDimensions")
)

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