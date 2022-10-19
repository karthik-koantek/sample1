# Databricks notebook source
# MAGIC %md # Stream Processing - App Insights - Event
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
appInsightsDataType = "Event"

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

# MAGIC %md #### Validate Source Path Exists and Has Data

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
    "sourceName": "Stream Processing - App Insights - Event: Read Schema",
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
    "sourceName": "Stream Processing - App Insights - Event: Read Stream",
    "errorCode": "200",
    "errorDescription": e.__class__.__name__
  }
  error = json.dumps(err)
  log_notebook_error(notebookLogGuid, error, server, database, login, pwd)
  raise(e)

# COMMAND ----------

# MAGIC %md #### Flatten
# MAGIC * Convert from nested JSON/Array Struct into tabular dataframe

# COMMAND ----------

def flatten_AppInsights_df_Event(df):
  flattenedDF = flatten_df(df)

  eventDF = flattenedDF \
  .select(
     col("internal_data_documentVersion").alias("internalDataDocumentVersion")
    ,col("internal_data_id")
    ,col("appInsightsInstanceName")
    ,col("context_session_id").alias("contextSessionId")
    ,col("context_session_isFirst").alias("contextSessionIsFirst")
    ,col("context_operation_id").alias("contextOperationId")
    ,col("context_operation_name").alias("contextOperationName")
    ,col("context_operation_parentId").alias("contextOperationParentId")
    ,col("context_location_clientip").alias("contextLocationClientIp")
    ,col("context_location_continent").alias("contextLocationContinent")
    ,col("context_location_country").alias("contextLocationCountry")
    ,col("context_location_province").alias("contextLocationProvince")
    ,col("context_device_roleInstance").alias("contextDeviceRoleInstance")
    ,col("context_device_type").alias("contextDeviceType")
    ,col("context_data_eventTime")
    ,col("context_data_isSynthetic").alias("contextDataIsSynthetic")
    ,col("context_data_samplingRate").alias("contextDataSamplingRate")) \
  .distinct()

  event_eventDF = flattenedDF \
  .select(
     col("internal_data_id")
    ,col("appInsightsInstanceName")
    ,"context_data_eventTime"
    ,explode("event").alias("event")) \
  .distinct()

  event_contextCustomDimensionsDF = flattenedDF \
  .select(
    "internal_data_id"
    ,col("appInsightsInstanceName")
    ,"context_data_eventTime"
    ,explode("context_custom_dimensions").alias("contextCustomDimensions")) \
  .distinct()

  event_contextCustomMetricsDF = flattenedDF \
  .select(
    "internal_data_id"
    ,col("appInsightsInstanceName")
    ,"context_data_eventTime"
    ,explode("context_custom_metrics").alias("contextCustomMetrics")) \
  .distinct()

  event_eventFlattenedDF = event_eventDF \
  .select(
     col("internal_data_id")
    ,col("appInsightsInstanceName")
    ,col("context_data_eventTime")
    ,col("event.count").alias("EventCount")
    ,col("event.name").alias("EventName")
  )

  event_contextCustomDimensionsFlattenedDF = event_contextCustomDimensionsDF \
  .select(
     col("internal_data_id")
    ,col("appInsightsInstanceName")
    ,col("context_data_eventTime")
    ,col("contextCustomDimensions.EnablingExtensionVersion").alias("ContextCustomDimensionsEnablingExtensionVersion")
    ,col("contextCustomDimensions.EventName").alias("ContextCustomDimensionsEventName")
    ,col("contextCustomDimensions.RequestId").alias("ContextCustomDimensionsRequestId")
    ,col("contextCustomDimensions.ServiceProfilerContent").alias("ContextCustomDimensionsServiceProfilerContent")
    ,col("contextCustomDimensions.ServiceProfilerVersion").alias("ContextCustomDimensionsServiceProfilerVersion")
    ,col("contextCustomDimensions.SnapshotCollectorConfiguration").alias("ContextCustomDimensionsSnapshotCollectorConfiguration")
    ,col("contextCustomDimensions.id").alias("ContextCustomDimensionsId")
    ,col("contextCustomDimensions.domainName").alias("ContextCustomDimensionsDomainName")
    ,col("contextCustomDimensions.role").alias("ContextCustomDimensoinsRole")
    ,col("contextCustomDimensions.segment").alias("ContextCustomDimensionsSegment")
    ,col("contextCustomDimensions.tags").alias("ContextCustomDimensionsTags")
    ,col("contextCustomDimensions.ActionType").alias("ContextCustomDimensionsActionType")
    ,col("contextCustomDimensions.linkUrl").alias("ContextCustomDimensionsLinkUrl")
    ,col("contextCustomDimensions.Search Keyword used").alias("ContextCustomDimensionsSearchKeywordUsed")
    ,col("contextCustomDimensions.Language").alias("ContextCustomDimensionsLanguage")
    ,col("contextCustomDimensions.Scenario").alias("ContextCustomDimensionsScenario")
    ,col("contextCustomDimensions.templateName").alias("ContextCustomDimensionsTemplateName")
    ,col("contextCustomDimensions.additionaltopics").alias("ContextCustomDimensionsAdditionalTopics")
    ,col("contextCustomDimensions.assetName").alias("ContextCustomDimensionsAssetName")
    ,col("contextCustomDimensions.pageName").alias("ContextCustomDimensionsPageName")
    ,col("contextCustomDimensions.sourceReferrer").alias("ContextCustomDimensionsSourceReferrer")
    ,col("contextCustomDimensions.windowUrl").alias("ContextCustomDimensionsWindowUrl")
  )

  event_contextCustomMetricsFlattenedDF = event_contextCustomMetricsDF \
  .select(
     col("internal_data_id")
    ,col("appInsightsInstanceName")
    ,col("context_data_eventTime")
    ,col("contextCustomMetrics.CannotSnapshotDueToMemoryUsage.count").alias("ContextCustomMetricsCannotSnapshotDueToMemoryUsageCount")
    ,col("contextCustomMetrics.CannotSnapshotDueToMemoryUsage.max").alias("ContextCustomMetricsCannotSnapshotDueToMemoryUsageMax")
    ,col("contextCustomMetrics.CannotSnapshotDueToMemoryUsage.min").alias("ContextCustomMetricsCannotSnapshotDueToMemoryUsageMin")
    ,col("contextCustomMetrics.CannotSnapshotDueToMemoryUsage.sampledValue").alias("ContextCustomMetricsCannotSnapshotDueToMemoryUsageSampledValue")
    ,col("contextCustomMetrics.CannotSnapshotDueToMemoryUsage.stdDev").alias("ContextCustomMetricsCannotSnapshotDueToMemoryUsageStdDev")
    ,col("contextCustomMetrics.CannotSnapshotDueToMemoryUsage.sum").alias("ContextCustomMetricsCannotSnapshotDueToMemoryUsageSum")
    ,col("contextCustomMetrics.CannotSnapshotDueToMemoryUsage.value").alias("ContextCustomMetricsCannotSnapshotDueToMemoryUsageValue")
    ,col("contextCustomMetrics.CollectionPlanComplete.count").alias("ContextCustomMetricsCollectionPlanCompleteCount")
    ,col("contextCustomMetrics.CollectionPlanComplete.max").alias("ContextCustomMetricsCollectionPlanCompleteMax")
    ,col("contextCustomMetrics.CollectionPlanComplete.min").alias("ContextCustomMetricsCollectionPlanCompleteMin")
    ,col("contextCustomMetrics.CollectionPlanComplete.sampledValue").alias("ContextCustomMetricsCollectionPlanCompleteSampledValue")
    ,col("contextCustomMetrics.CollectionPlanComplete.stdDev").alias("ContextCustomMetricsCollectionPlanCompleteStdDev")
    ,col("contextCustomMetrics.CollectionPlanComplete.sum").alias("ContextCustomMetricsCollectionPlanCompleteSum")
    ,col("contextCustomMetrics.CollectionPlanComplete.value").alias("ContextCustomMetricsCollectionPlanCompleteValue")
    ,col("contextCustomMetrics.FirstChanceExceptions.count").alias("ContextCustomMetricsFirstChanceExceptionsCount")
    ,col("contextCustomMetrics.FirstChanceExceptions.max").alias("ContextCustomMetricsFirstChanceExceptionsMax")
    ,col("contextCustomMetrics.FirstChanceExceptions.min").alias("ContextCustomMetricsFirstChanceExceptionsMin")
    ,col("contextCustomMetrics.FirstChanceExceptions.sampledValue").alias("ContextCustomMetricsFirstChanceExceptionsSampledValue")
    ,col("contextCustomMetrics.FirstChanceExceptions.stdDev").alias("ContextCustomMetricsFirstChanceExceptionsStdDev")
    ,col("contextCustomMetrics.FirstChanceExceptions.sum").alias("ContextCustomMetricsFirstChanceExceptionsSum")
    ,col("contextCustomMetrics.FirstChanceExceptions.value").alias("ContextCustomMetricsFirstChanceExceptionsValue")
    ,col("contextCustomMetrics.SkippedExceptions.count").alias("ContextCustomMetricsSkippedExceptionsCount")
    ,col("contextCustomMetrics.SkippedExceptions.max").alias("ContextCustomMetricsSkippedExceptionsMax")
    ,col("contextCustomMetrics.SkippedExceptions.min").alias("ContextCustomMetricsSkippedExceptionsMin")
    ,col("contextCustomMetrics.SkippedExceptions.sampledValue").alias("ContextCustomMetricsSkippedExceptionsSampledValue")
    ,col("contextCustomMetrics.SkippedExceptions.stdDev").alias("ContextCustomMetricsSkippedExceptionsStdDev")
    ,col("contextCustomMetrics.SkippedExceptions.sum").alias("ContextCustomMetricsSkippedExceptionsSum")
    ,col("contextCustomMetrics.SkippedExceptions.value").alias("ContextCustomMetricsSkippedExceptionsValue")
    ,col("contextCustomMetrics.SnappointMatchExceptions.count").alias("ContextCustomMetricsSnappointMatchExceptionsCount")
    ,col("contextCustomMetrics.SnappointMatchExceptions.max").alias("ContextCustomMetricsSnappointMatchExceptionsMax")
    ,col("contextCustomMetrics.SnappointMatchExceptions.min").alias("ContextCustomMetricsSnappointMatchExceptionsMin")
    ,col("contextCustomMetrics.SnappointMatchExceptions.sampledValue").alias("ContextCustomMetricsSnappointMatchExceptionsSampledValue")
    ,col("contextCustomMetrics.SnappointMatchExceptions.stdDev").alias("ContextCustomMetricsSnappointMatchExceptionsStdDev")
    ,col("contextCustomMetrics.SnappointMatchExceptions.sum").alias("ContextCustomMetricsSnappointMatchExceptionsSum")
    ,col("contextCustomMetrics.SnappointMatchExceptions.value").alias("ContextCustomMetricsSnappointMatchExceptionsValue")
    ,col("contextCustomMetrics.SnapshotDailyRateLimitReached.count").alias("ContextCustomMetricsSnapshotDailyRateLimitReachedCount")
    ,col("contextCustomMetrics.SnapshotDailyRateLimitReached.max").alias("ContextCustomMetricsSnapshotDailyRateLimitReachedMax")
    ,col("contextCustomMetrics.SnapshotDailyRateLimitReached.min").alias("ContextCustomMetricsSnapshotDailyRateLimitReachedMin")
    ,col("contextCustomMetrics.SnapshotDailyRateLimitReached.sampledValue").alias("ContextCustomMetricsSnapshotDailyRateLimitReachedSampledValue")
    ,col("contextCustomMetrics.SnapshotDailyRateLimitReached.stdDev").alias("ContextCustomMetricsSnapshotDailyRateLimitReachedStdDev")
    ,col("contextCustomMetrics.SnapshotDailyRateLimitReached.sum").alias("ContextCustomMetricsSnapshotDailyRateLimitReachedSum")
    ,col("contextCustomMetrics.SnapshotDailyRateLimitReached.value").alias("ContextCustomMetricsSnapshotDailyRateLimitReachedValue")
    ,col("contextCustomMetrics.SnapshotRateLimitExceeded.count").alias("ContextCustomMetricsSnapshotRateLimitExceededCount")
    ,col("contextCustomMetrics.SnapshotRateLimitExceeded.max").alias("ContextCustomMetricsSnapshotRateLimitExceededMax")
    ,col("contextCustomMetrics.SnapshotRateLimitExceeded.min").alias("ContextCustomMetricsSnapshotRateLimitExceededMin")
    ,col("contextCustomMetrics.SnapshotRateLimitExceeded.sampledValue").alias("ContextCustomMetricsSnapshotRateLimitExceededSampledValue")
    ,col("contextCustomMetrics.SnapshotRateLimitExceeded.stdDev").alias("ContextCustomMetricsSnapshotRateLimitExceededStdDev")
    ,col("contextCustomMetrics.SnapshotRateLimitExceeded.sum").alias("ContextCustomMetricsSnapshotRateLimitExceededSum")
    ,col("contextCustomMetrics.SnapshotRateLimitExceeded.value").alias("ContextCustomMetricsSnapshotRateLimitExceededValue")
    ,col("contextCustomMetrics.TrackExceptionCalls.count").alias("ContextCustomMetricsTrackExceptionCallsCount")
    ,col("contextCustomMetrics.TrackExceptionCalls.max").alias("ContextCustomMetricsTrackExceptionCallsMax")
    ,col("contextCustomMetrics.TrackExceptionCalls.min").alias("ContextCustomMetricsTrackExceptionCallsMin")
    ,col("contextCustomMetrics.TrackExceptionCalls.sampledValue").alias("ContextCustomMetricsTrackExceptionCallsSampledValue")
    ,col("contextCustomMetrics.TrackExceptionCalls.stdDev").alias("ContextCustomMetricsTrackExceptionCallsStdDev")
    ,col("contextCustomMetrics.TrackExceptionCalls.sum").alias("ContextCustomMetricsTrackExceptionCallsSum")
    ,col("contextCustomMetrics.TrackExceptionCalls.value").alias("ContextCustomMetricsTrackExceptionCallsValue")
  )

  return eventDF, event_eventFlattenedDF, event_contextCustomDimensionsFlattenedDF, event_contextCustomMetricsFlattenedDF

# COMMAND ----------

eventDF, event_eventFlattenedDF, event_contextCustomDimensionsFlattenedDF, event_contextCustomMetricsFlattenedDF = flatten_AppInsights_df_Event (df)

# COMMAND ----------

# MAGIC %md #### Write Streams

# COMMAND ----------

# MAGIC %md ##### Bronze Zone

# COMMAND ----------

try:
  dfRaw = (df \
    .writeStream \
    .queryName("event raw") \
    .trigger(once=True) \
    .format("json") \
    .option("checkpointLocation", rawCheckpointPath + "/eventRaw/" + dateToProcess) \
    .outputMode("append") \
    .start(rawDataPath + "/event/" + dateToProcess)
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

# MAGIC %md ##### Silver Zone

# COMMAND ----------

try:
  eventDFQueryZone = (eventDF \
    .writeStream \
    .queryName("event query") \
    .trigger(once=True) \
    .format("delta") \
    .option("checkpointLocation", queryCheckpointPath + "/eventQuery") \
    .outputMode("append") \
    .start(queryDataPath + "/event")
  )

  eventEventDFQueryZone = (event_eventFlattenedDF \
    .writeStream \
    .queryName("event event query") \
    .trigger(once=True) \
    .format("delta") \
    .option("checkpointLocation", queryCheckpointPath + "/eventEventQuery") \
    .outputMode("append") \
    .start(queryDataPath + "/eventEvent")
  )

  eventCustomDimensionsDFQueryZone = (event_contextCustomDimensionsFlattenedDF \
    .writeStream \
    .queryName("event custom dimensions query") \
    .trigger(once=True) \
    .format("delta") \
    .option("checkpointLocation", queryCheckpointPath + "/eventCustomDimensionsQueryCCA") \
    .option("mergeSchema", "true") \
    .outputMode("append") \
    .start(queryDataPath + "/eventCustomDimensionsCCA")
  )

  eventCustomMetricsDFQueryZone = (event_contextCustomMetricsFlattenedDF \
    .writeStream \
    .queryName("event custom metrics query") \
    .trigger(once=True) \
    .format("delta") \
    .option("checkpointLocation", queryCheckpointPath + "/eventCustomMetricsQuery") \
    .option("mergeSchema", "true") \
    .outputMode("append") \
    .start(queryDataPath + "/eventCustomMetrics")
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

# MAGIC %md #### Stop Streams when no more records to process

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