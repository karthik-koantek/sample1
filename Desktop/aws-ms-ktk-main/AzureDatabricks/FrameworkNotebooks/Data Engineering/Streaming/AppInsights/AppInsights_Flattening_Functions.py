# Databricks notebook source
# MAGIC %md # AppInsights Flattening Functions
# MAGIC
# MAGIC This notebook contains all the functions used to flatten the data coming from AppInsights events.

# COMMAND ----------

# MAGIC %md #### Initialize

# COMMAND ----------

import json
from pyspark.sql.types import StructType
from pyspark.sql.functions import explode, col, unix_timestamp, expr, lit
import time

# COMMAND ----------

# MAGIC %md #### App Insights Flattening Functions

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

def flatten_AppInsights_df_Exceptions(df):
  flattenedDF = flatten_df(df)

  exceptionsDF = flattenedDF \
    .select(
       col("appInsightsInstanceName")
      ,col("internal_data_id")
      ,col("context_data_eventTime")
      ,col("internal_data_documentVersion")
      ,col("context_session_isFirst")
      ,col("context_operation_id")
      ,col("context_operation_name")
      ,col("context_operation_parentId")
      ,col("context_location_clientip")
      ,col("context_location_continent")
      ,col("context_location_country")
      ,col("context_location_province")
      ,col("context_device_roleInstance")
      ,col("context_device_roleName")
      ,col("context_device_type")
      ,col("context_data_isSynthetic")
      ,col("context_data_samplingRate")
      ,col("context_application_version")
    ) \
    .distinct()

  basicExceptionDFExploded = flattenedDF \
    .select(
       col("appInsightsInstanceName")
      ,col("internal_data_id")
      ,col("context_data_eventTime")
      ,explode(col("basicException")).alias("basicException")
    ) \
    .distinct()

  basicExceptionDFFlattened = basicExceptionDFExploded \
    .select(
       col("appInsightsInstanceName")
      ,col("internal_data_id")
      ,col("context_data_eventTime")
      ,col("basicException.assembly")
      ,col("basicException.count")
      ,col("basicException.exceptionGroup")
      ,col("basicException.exceptionType")
      ,col("basicException.failedUserCodeAssembly")
      ,col("basicException.failedUserCodeMethod")
      ,col("basicException.hasFullStack")
      ,col("basicException.id")
      ,col("basicException.message")
      ,col("basicException.method")
      ,col("basicException.outerExceptionMessage")
      ,col("basicException.outerExceptionThrownAtAssembly")
      ,col("basicException.outerExceptionThrownAtMethod")
      ,col("basicException.outerExceptionType")
      ,col("basicException.outerId")
      ,col("basicException.problemId")
      ,col("basicException.typeName")
    )

  basicExceptionParsedStackExplodedDF = basicExceptionDFExploded \
    .select(
       col("appInsightsInstanceName")
      ,col("internal_data_id")
      ,col("context_data_eventTime")
      ,explode(col("basicException.parsedStack")).alias("BasicExceptionParsedStack")
    )

  basicExceptionParsedStackFlattenedDF = basicExceptionParsedStackExplodedDF \
    .select(
       col("appInsightsInstanceName")
      ,col("internal_data_id")
      ,col("context_data_eventTime")
      ,col("BasicExceptionParsedStack.assembly").alias("basicExceptionParsedStackAssembly")
      ,col("BasicExceptionParsedStack.level").alias("basicExceptionParsedStackLevel")
      ,col("BasicExceptionParsedStack.line").alias("basicExceptionParsedStackLine")
      ,col("BasicExceptionParsedStack.method").alias("basicExceptionParsedStackMethod")
    )

  return exceptionsDF, basicExceptionDFFlattened, basicExceptionParsedStackFlattenedDF

# COMMAND ----------

def flatten_AppInsights_df_Metrics(df):
  flattenedDF = flatten_df(df)

  metricsDF = flattenedDF \
  .select(
     col("internal_data_documentVersion").alias("internalDataDocumentVersion")
    ,col("internal_data_id")
    ,col("appInsightsInstanceName")
    ,col("context_session_isFirst").alias("contextSessionIsFirst")
    ,col("context_location_clientip").alias("contextLocationClientIp")
    ,col("context_location_continent").alias("contextLocationContinent")
    ,col("context_location_country").alias("contextLocationCountry")
    ,col("context_location_province").alias("contextLocationProvince")
    ,col("context_device_roleInstance").alias("contextDeviceRoleInstance")
    ,col("context_device_roleName").alias("contextDeviceRoleName")
    ,col("context_device_type").alias("contextDeviceType")
    ,col("context_data_eventTime")
    ,col("context_data_isSynthetic").alias("contextDataIsSynthetic")
    ,col("context_data_samplingRate").alias("contextDataSamplingRate")
    ,col("context_data_syntheticSource").alias("contextDataSyntheticSource")
    ,col("context_application_version").alias("contextApplicationVersion")) \
  .distinct()

  metrics_metricDF = flattenedDF \
    .select(
       col("internal_data_id")
      ,col("appInsightsInstanceName")
      ,"context_data_eventTime"
      ,explode("metric").alias("metric")) \
    .distinct()

  metrics_contextcustomDimensionsDF = flattenedDF \
    .select(
      "internal_data_id"
      ,col("appInsightsInstanceName")
      ,"context_data_eventTime"
      ,explode("context_custom_dimensions").alias("contextCustomDimensions")) \
    .distinct()

  metrics_contextCustomMetricsDF = flattenedDF \
    .select(
      "internal_data_id"
      ,col("appInsightsInstanceName")
      ,"context_data_eventTime"
      ,explode("context_custom_metrics").alias("contextCustomMetrics")) \
    .distinct()

  metrics_contextcustomDimensionsFlattenedDF = metrics_contextcustomDimensionsDF \
    .select(
       col("internal_data_id")
      ,col("appInsightsInstanceName")
      ,col("context_data_eventTime")
      ,col("ContextCustomDimensions.AspNetCoreEnvironment").alias("AspNetCoreEnvironment")
      ,col("ContextCustomDimensions.`Dependency.Success`").alias("DependencySuccess")
      ,col("ContextCustomDimensions.`Dependency.Type`").alias("DependencyType")
      ,col("ContextCustomDimensions.`Request.Success`").alias("RequestSuccess")
      ,col("ContextCustomDimensions.`_MS.AggregationIntervalMs`").alias("MSAggregationIntervalMs")
      ,col("ContextCustomDimensions.`_MS.IsAutocollected`").alias("MSIsAutoCollected")
      ,col("ContextCustomDimensions.`_MS.MetricId`").alias("MSMetricId")
      ,col("ContextCustomDimensions.appSrv_ResourceGroup").alias("AppSrvResourceGroup")
      ,col("ContextCustomDimensions.appSrv_SiteName").alias("AppSrvSiteName")
      ,col("ContextCustomDimensions.appSrv_wsHost").alias("AppSrvWsHost")
      ,col("ContextCustomDimensions.appSrv_wsOwner").alias("AppSrvWsOwner")
      ,col("ContextCustomDimensions.appSrv_wsStamp").alias("AppSrvWsStamp")
      ,col("ContextCustomDimensions.baseSdkTargetFramework").alias("BaseSDKTargetFramework")
      ,col("ContextCustomDimensions.osType").alias("OSType")
      ,col("ContextCustomDimensions.processSessionId").alias("ProcessSessionId")
      ,col("ContextCustomDimensions.runtimeFramework").alias("RuntimeFramework")
    )

  metrics_contextCustomMetricsFlattenedDF = metrics_contextCustomMetricsDF \
    .select(
       col("internal_data_id")
      ,col("appInsightsInstanceName")
      ,col("context_data_eventTime")
      ,col("contextCustomMetrics.Dependency duration.count").alias("ContextCustomMetricsDependencyDurationCount")
      ,col("contextCustomMetrics.Dependency duration.max").alias("ContextCustomMetricsDependencyDurationMax")
      ,col("contextCustomMetrics.Dependency duration.min").alias("ContextCustomMetricsDependencyDurationMin")
      ,col("contextCustomMetrics.Dependency duration.sampledValue").alias("ContextCustomMetricsDependencyDurationSampledValue")
      ,col("contextCustomMetrics.Dependency duration.stdDev").alias("ContextCustomMetricsDependencyDurationStdDev")
      ,col("contextCustomMetrics.Dependency duration.sum").alias("ContextCustomMetricsDependencyDurationSum")
      ,col("contextCustomMetrics.Dependency duration.value").alias("ContextCustomMetricsDependencyDurationValue")
      ,col("contextCustomMetrics.HeartbeatState.count").alias("ContextCustomMetricsHeartbeatStateCount")
      ,col("contextCustomMetrics.HeartbeatState.max").alias("ContextCustomMetricsHeartbeatStateMax")
      ,col("contextCustomMetrics.HeartbeatState.min").alias("ContextCustomMetricsHeartbeatStateMin")
      ,col("contextCustomMetrics.HeartbeatState.sampledValue").alias("ContextCustomMetricsHeartbeatStateSampledValue")
      ,col("contextCustomMetrics.HeartbeatState.stdDev").alias("ContextCustomMetricsHeartbeatStateStdDev")
      ,col("contextCustomMetrics.HeartbeatState.sum").alias("ContextCustomMetricsHeartbeatStateSum")
      ,col("contextCustomMetrics.HeartbeatState.value").alias("ContextCustomMetricsHeartbeatStateValue")
      ,col("contextCustomMetrics.Server response time.count").alias("ContextCustomMetricsServerResponseTimeCount")
      ,col("contextCustomMetrics.Server response time.max").alias("ContextCustomMetricsServerResponseTimeMax")
      ,col("contextCustomMetrics.Server response time.min").alias("ContextCustomMetricsServerResponseTimeMin")
      ,col("contextCustomMetrics.Server response time.sampledValue").alias("ContextCustomMetricsServerResponseTimeSampledValue")
      ,col("contextCustomMetrics.Server response time.stdDev").alias("ContextCustomMetricsServerResponseTimeStdDev")
      ,col("contextCustomMetrics.Server response time.sum").alias("ContextCustomMetricsServerResponseTimeSum")
      ,col("contextCustomMetrics.Server response time.value").alias("ContextCustomMetricsServerResponseTimeValue")
  )

  return metricsDF, metrics_metricDF, metrics_contextcustomDimensionsFlattenedDF,metrics_contextCustomMetricsFlattenedDF

# COMMAND ----------

def flatten_AppInsights_df_PageViewPerformance(df):
  flattenedDF = flatten_df(df)

  pageViewPerformanceDF = flattenedDF \
    .select(
       col("appInsightsInstanceName")
      ,col("internal_data_id")
      ,col("context_data_eventTime")
      ,col("internal_data_documentVersion").alias("internalDataDocumentVersion")
      ,col("context_user_anonId").alias("contextUserAnonId")
      ,col("context_user_isAuthenticated").alias("contextUserIsAuthenticated")
      ,col("context_session_id").alias("contextSessionId")
      ,col("context_session_isFirst").alias("contextSessionIsFirst")
      ,col("context_operation_id").alias("contextOperationId")
      ,col("context_operation_name").alias("contextOperationName")
      ,col("context_operation_parentId").alias("contextOperationParentId")
      ,col("context_location_city").alias("contextLocationCity")
      ,col("context_location_clientip").alias("contextLocationClientIp")
      ,col("context_location_continent").alias("contextLocationContinent")
      ,col("context_location_country").alias("contextLocationCountry")
      ,col("context_location_province").alias("contextLocationProvince")
      ,col("context_device_browser").alias("contextDeviceBrowser")
      ,col("context_device_browserVersion").alias("contextDeviceBrowserVersion")
      ,col("context_device_deviceModel").alias("contextDeviceDeviceModel")
      ,col("context_device_deviceName").alias("contextDeviceDeviceName")
      ,col("context_device_id").alias("contextDeviceId")
      ,col("context_device_osVersion").alias("contextDeviceOsVersion")
      ,col("context_device_type").alias("contextDeviceType")
      ,col("context_data_isSynthetic").alias("contextDataIsSynthetic")
      ,col("context_data_samplingRate").alias("contextDataSamplingRate")
    ) \
    .distinct()

  pageViewPerformanceClientPerformanceExplodedDF = flattenedDF \
    .select(
       col("appInsightsInstanceName")
      ,col("internal_data_id")
      ,col("context_data_eventTime")
      ,explode(col("clientPerformance")).alias("clientPerformance")
    ) \
    .distinct()

  pageViewPerformanceClientPerformanceFlattenedDF = pageViewPerformanceClientPerformanceExplodedDF \
    .select(
       col("appInsightsInstanceName")
      ,col("internal_data_id")
      ,col("context_data_eventTime")
      ,col("clientPerformance.name").alias("clientPerformanceName")
      ,col("clientPerformance.url").alias("clientPerformanceUrl")
      ,col("clientPerformance.clientProcess.count").alias("clientPerformanceClientProcessCount")
      ,col("clientPerformance.clientProcess.max").alias("clientPerformanceClientProcessMax")
      ,col("clientPerformance.clientProcess.min").alias("clientPerformanceClientProcessMin")
      ,col("clientPerformance.clientProcess.sampledValue").alias("clientPerformanceClientProcessSampledValue")
      ,col("clientPerformance.clientProcess.stdDev").alias("clientPerformanceClientProcessStdDev")
      ,col("clientPerformance.clientProcess.value").alias("clientPerformanceClientProcessValue")
      ,col("clientPerformance.networkConnection.count").alias("clientPerformanceNetworkConnectionCount")
      ,col("clientPerformance.networkConnection.max").alias("clientPerformanceNetworkConnectionMax")
      ,col("clientPerformance.networkConnection.min").alias("clientPerformanceNetworkConnectionMin")
      ,col("clientPerformance.networkConnection.sampledValue").alias("clientPerformanceNetworkConnectionSampledValue")
      ,col("clientPerformance.networkConnection.stdDev").alias("clientPerformanceNetworkConnectionStdDev")
      ,col("clientPerformance.networkConnection.value").alias("clientPerformanceNetworkConnectionValue")
      ,col("clientPerformance.receiveRequest.count").alias("clientPerformanceReceiveRequestCount")
      ,col("clientPerformance.receiveRequest.max").alias("clientPerformanceReceiveRequestMax")
      ,col("clientPerformance.receiveRequest.min").alias("clientPerformanceReceiveRequestMin")
      ,col("clientPerformance.receiveRequest.sampledValue").alias("clientPerformanceReceiveRequestSampledValue")
      ,col("clientPerformance.receiveRequest.stdDev").alias("clientPerformanceReceiveRequestStdDev")
      ,col("clientPerformance.receiveRequest.value").alias("clientPerformanceReceiveRequestValue")
      ,col("clientPerformance.sendRequest.count").alias("clientPerformanceSendRequestCount")
      ,col("clientPerformance.sendRequest.max").alias("clientPerformanceSendRequestMax")
      ,col("clientPerformance.sendRequest.min").alias("clientPerformanceSendRequestMin")
      ,col("clientPerformance.sendRequest.sampledValue").alias("clientPerformanceSendRequestSampledValue")
      ,col("clientPerformance.sendRequest.stdDev").alias("clientPerformanceSendRequestStdDev")
      ,col("clientPerformance.sendRequest.value").alias("clientPerformanceSendRequestValue")
      ,col("clientPerformance.total.count").alias("clientPerformanceTotalCount")
      ,col("clientPerformance.total.max").alias("clientPerformanceTotalMax")
      ,col("clientPerformance.total.min").alias("clientPerformanceTotalMin")
      ,col("clientPerformance.total.sampledValue").alias("clientPerformanceTotalSampledValue")
      ,col("clientPerformance.total.stdDev").alias("clientPerformanceTotalStdDev")
      ,col("clientPerformance.total.value").alias("clientPerformanceTotalValue")
      ,col("clientPerformance.urlData.base").alias("clientPerformanceUrlDataBase")
      ,col("clientPerformance.urlData.hashTag").alias("clientPerformanceUrlDataHashTag")
      ,col("clientPerformance.urlData.host").alias("clientPerformanceUrlDataHost")
      ,col("clientPerformance.urlData.protocol").alias("clientPerformanceUrlDataProtocol")
    )

  return pageViewPerformanceDF, pageViewPerformanceClientPerformanceFlattenedDF

# COMMAND ----------

def flatten_AppInsights_df_PageViews(df):
  flattenedDF = flatten_df(df)

  pageViewsDF = flattenedDF \
    .select(
       col("appInsightsInstancename")
      ,col("internal_data_id")
      ,col("context_data_eventTime")
      ,col("internal_data_documentVersion").alias("internalDataDocumentVersion")
      ,col("context_user_anonId").alias("contextUserAnonId")
      ,col("context_user_isAuthenticated").alias("contextUserIsAuthenticated")
      ,col("context_session_id").alias("contextSessionId")
      ,col("context_session_isFirst").alias("contextSessionIsFirst")
      ,col("context_operation_id").alias("contextOperationId")
      ,col("context_operation_parentId").alias("contextOperationParentId")
      ,col("context_location_city").alias("contextLocationCity")
      ,col("context_location_clientip").alias("contextLocationclientIp")
      ,col("context_location_continent").alias("contextLocationContinent")
      ,col("context_location_country").alias("contextLocationCountry")
      ,col("context_location_province").alias("contextLocationProvince")
      ,col("context_device_browser").alias("contextDeviceBrowser")
      ,col("context_device_browserVersion").alias("contextDeviceBrowserVersion")
      ,col("context_device_deviceModel").alias("contextDeviceDeviceModel")
      ,col("context_device_deviceName").alias("contextDeviceDeviceName")
      ,col("context_device_id").alias("contextDeviceId")
      ,col("context_device_osVersion").alias("contextDeviceOsVersion")
      ,col("context_device_type").alias("contextDeviceType")
      ,col("context_data_isSynthetic").alias("contextDataIsSynthetic")
      ,col("context_data_samplingRate").alias("contextDataSamplingRate")
    ) \
    .distinct()

  pageViewsViewExplodedDF = flattenedDF \
    .select(
       col("appInsightsInstancename")
      ,col("internal_data_id")
      ,col("context_data_eventTime")
      ,explode(col("view")).alias("view")
    ) \
    .distinct()

  pageViewsViewFlattenedDF = pageViewsViewExplodedDF \
    .select(
       col("appInsightsInstancename")
      ,col("internal_data_id")
      ,col("context_data_eventTime")
      ,col("view.count").alias("viewCount")
      ,col("view.name").alias("viewName")
      ,col("view.url").alias("viewUrl")
      ,col("view.durationMetric.count").alias("viewDurationMetricCount")
      ,col("view.durationMetric.max").alias("viewDurationMetricMax")
      ,col("view.durationMetric.min").alias("viewDurationMetricMin")
      ,col("view.durationMetric.sampledValue").alias("viewDurationMetricSampledValue")
      ,col("view.durationMetric.stdDev").alias("viewDurationMetricStdDev")
      ,col("view.durationMetric.value").alias("viewDurationMetricValue")
      ,col("view.urlData.base").alias("viewUrlDataBase")
      ,col("view.urlData.hashTag").alias("viewUrlDataHashTag")
      ,col("view.urlData.host").alias("viewUrlDataHost")
      ,col("view.urlData.protocol").alias("viewUrlDataProtocol")
    )

  return pageViewsDF, pageViewsViewFlattenedDF

# COMMAND ----------

def flatten_AppInsights_df_PerformanceCounters(df):
  flattenedDF = flatten_df(df)

  performanceCountersDF = flattenedDF \
    .select(
       col("appInsightsInstanceName")
      ,col("internal_data_id")
      ,col("context_data_eventTime")
      ,col("internal_data_documentVersion")
      ,col("context_session_isFirst")
      ,col("context_location_clientip")
      ,col("context_location_continent")
      ,col("context_location_country")
      ,col("context_location_province")
      ,col("context_device_roleInstance")
      ,col("context_device_roleName")
      ,col("context_device_type")
      ,col("context_data_isSynthetic")
      ,col("context_data_samplingRate")
      ,col("context_application_version")
    ) \
    .distinct()

  performanceCountersPerformanceCounterExplodedDF = flattenedDF \
    .select(
       col("appInsightsInstanceName")
      ,col("internal_data_id")
      ,col("context_data_eventTime")
      ,explode(col("performanceCounter")).alias("performanceCounter")
    ) \
    .distinct()

  performanceCountersPerformanceCounterFlattenedDF = performanceCountersPerformanceCounterExplodedDF \
    .select(
       col("appInsightsInstanceName")
      ,col("internal_data_id")
      ,col("context_data_eventTime")
      ,col("performanceCounter.`Invalid Perf Counter Name % Processor Time Normalized`.value").alias("performanceCounterPercentProcessorTimeNormalizedValue")
      ,col("performanceCounter.available_bytes.value").alias("performanceCounterAvailableBytesValue")
      ,col("performanceCounter.categoryName").alias("performanceCounterCategoryName")
      ,col("performanceCounter.instanceName").alias("performanceCounterInstanceName")
      ,col("performanceCounter.io_data_bytes_per_sec.value").alias("performanceCounterIODataBytesPerSecValue")
      ,col("performanceCounter.number_of_exceps_thrown_per_sec.value").alias("performanceCounterNumberOfExcepsThrownPerSecValue")
      ,col("performanceCounter.percentage_processor_time.value").alias("performanceCounterPercentageProcessorTimeValue")
      ,col("performanceCounter.process_private_bytes.value").alias("performanceCounterProcessPrivateBytesValue")
      ,col("performanceCounter.request_execution_time.value").alias("performanceCounterRequestExecutionTimeValue")
      ,col("performanceCounter.requests_in_application_queue.value").alias("performanceCounterRequestsInApplicationQueueValue")
      ,col("performanceCounter.requests_per_sec.value").alias("performanceCounterRequestsPerSecValue")
    )

  return performanceCountersDF, performanceCountersPerformanceCounterFlattenedDF

# COMMAND ----------

def flatten_AppInsights_df_Rdd(df):
  flattenedDF = flatten_df(df)

  rddDF = flattenedDF \
    .select(
       col("appInsightsInstanceName")
      ,col("internal_data_id")
      ,col("context_data_eventTime")
      ,col("internal_data_documentVersion").alias("internalDataDocumentVersion")
      ,col("context_user_anonId").alias("contextUserAnonId")
      ,col("context_user_isAuthenticated").alias("contextUserIsAuthenticated")
      ,col("context_session_id").alias("contextSessionId")
      ,col("context_session_isFirst").alias("contextSessionIsFirst")
      ,col("context_operation_id").alias("contextOperationId")
      ,col("context_operation_name").alias("contextOperationName")
      ,col("context_operation_parentId").alias("contextOperationParentId")
      ,col("context_location_city").alias("contextLocationCity")
      ,col("context_location_clientip").alias("contextLocationClientIp")
      ,col("context_location_continent").alias("contextLocationContinent")
      ,col("context_location_country").alias("contextLocationCountry")
      ,col("context_location_province").alias("contextLocationProvince")
      ,col("context_device_browser").alias("contextDeviceBrowser")
      ,col("context_device_browserVersion").alias("contextDeviceBrowserVersion")
      ,col("context_device_deviceModel").alias("contextDeviceDeviceModel")
      ,col("context_device_deviceName").alias("contextDeviceDeviceName")
      ,col("context_device_id").alias("contextDeviceDeviceId")
      ,col("context_device_osVersion").alias("contextDeviceOsVersion")
      ,col("context_device_type").alias("contextDeviceType")
      ,col("context_data_isSynthetic").alias("contextDataIsSynthetic")
      ,col("context_data_samplingRate").alias("contextDataSamplingRate")
    ) \
    .distinct()

  rddRemoteDependencyExplodedDF = flattenedDF \
    .select(
       col("appInsightsInstanceName")
      ,col("internal_data_id")
      ,col("context_data_eventTime")
      ,explode(col("remoteDependency")).alias("remoteDependency")
    ) \
    .distinct()

  rddRemoteDependencyFlattenedDF = rddRemoteDependencyExplodedDF \
    .select(
       col("appInsightsInstanceName")
      ,col("internal_data_id")
      ,col("context_data_eventTime")
      ,col("remoteDependency.async").alias("remoteDependencyAsync")
      ,col("remoteDependency.baseName").alias("remoteDependencyBaseName")
      ,col("remoteDependency.commandName").alias("remoteDependencyCommandName")
      ,col("remoteDependency.count").alias("remoteDependencyCount")
      ,col("remoteDependency.dependencyTypeName").alias("remoteDependencyDependencyTypeName")
      ,col("remoteDependency.id").alias("remoteDependencyId")
      ,col("remoteDependency.name").alias("remoteDependencyName")
      ,col("remoteDependency.resultCode").alias("remoteDependencyResultCode")
      ,col("remoteDependency.success").alias("remoteDependencySuccess")
      ,col("remoteDependency.type").alias("remoteDependencyType")
      ,col("remoteDependency.url").alias("remoteDependencyUrl")
      ,col("remoteDependency.durationMetric.count").alias("remoteDependencyDurationMetricCount")
      ,col("remoteDependency.durationMetric.max").alias("remoteDependencyDurationMetricMax")
      ,col("remoteDependency.durationMetric.min").alias("remoteDependencyDurationMetricMin")
      ,col("remoteDependency.durationMetric.name").alias("remoteDependencyDurationMetricName")
      ,col("remoteDependency.durationMetric.sampledValue").alias("remoteDependencyDurationMetricSampledValue")
      ,col("remoteDependency.durationMetric.stdDev").alias("remoteDependencyDurationMetricStdDev")
      ,col("remoteDependency.durationMetric.type").alias("remoteDependencyDurationMetricType")
      ,col("remoteDependency.durationMetric.value").alias("remoteDependencyDurationMetricValue")
      ,col("remoteDependency.urlData.base").alias("remoteDependencyUrlDataBase")
      ,col("remoteDependency.urlData.hashTag").alias("remoteDependencyUrlDataHashTag")
      ,col("remoteDependency.urlData.host").alias("remoteDependencyUrlDataHost")
      ,col("remoteDependency.urlData.protocol").alias("remoteDependencyUrlDataProtocol")
    )

  return rddDF, rddRemoteDependencyFlattenedDF

# COMMAND ----------

def flatten_AppInsights_df_Event(df):
  flattenedDF = flatten_df(df)

  requestDF = flattenedDF \
    .select(
       col("appInsightsInstanceName")
      ,col("internal_data_id")
      ,col("context_data_eventTime")
      ,col("internal_data_documentVersion").alias("internalDataDocumentVersion")
      ,col("context_session_isFirst").alias("contextSessionIsFirst")
      ,col("context_operation_id").alias("contextOperationId")
      ,col("context_operation_name").alias("contextOperationName")
      ,col("context_operation_parentId").alias("contextOperationParentId")
      ,col("context_location_clientip").alias("contextLocationClientIp")
      ,col("context_location_continent").alias("contextLocationContinent")
      ,col("context_location_country").alias("contextLocationCountry")
      ,col("context_location_province").alias("contextLocationProvince")
      ,col("context_device_roleInstance").alias("contextDeviceRoleInstance")
      ,col("context_device_roleName").alias("contextDeviceRoleName")
      ,col("context_device_type").alias("contextDeviceType")
      ,col("context_data_isSynthetic").alias("contextDataIsSynthetic")
      ,col("context_data_samplingRate").alias("contextDataSamplingRate")
      ,col("context_application_version").alias("contextApplicationVersion")
    ) \
    .distinct()

  requestRequestExplodedDF = flattenedDF \
    .select(
       col("appInsightsInstanceName")
      ,col("internal_data_id")
      ,col("context_data_eventTime")
      ,explode(col("request")).alias("request")
    ) \
    .distinct()

  requestContextCustomDimensionsExplodedDF = flattenedDF \
    .select(
       col("appInsightsInstanceName")
      ,col("internal_data_id")
      ,col("context_data_eventTime")
      ,explode(col("context_custom_dimensions")).alias("contextCustomDimensions")
    ) \
    .distinct()

  requestRequestFlattenedDF = requestRequestExplodedDF \
    .select(
       col("appInsightsInstanceName")
      ,col("internal_data_id")
      ,col("context_data_eventTime")
      ,col("request.count").alias("requestCount")
      ,col("request.id").alias("requestId")
      ,col("request.name").alias("requestName")
      ,col("request.responseCode").alias("requestResponseCode")
      ,col("request.success").alias("requestSuccess")
      ,col("request.url").alias("requestUrl")
      ,col("request.durationMetric.count").alias("requestDurationMetricCount")
      ,col("request.durationMetric.max").alias("requestDurationMetricMax")
      ,col("request.durationMetric.min").alias("requestDurationMetricMin")
      ,col("request.durationMetric.sampledValue").alias("requestDurationMetricSampledValue")
      ,col("request.durationMetric.stdDev").alias("requestDurationMetricStdDev")
      ,col("request.durationMetric.value").alias("requestDurationMetricValue")
      ,col("request.urlData.base").alias("requestUrlDataBase")
      ,col("request.urlData.hashTag").alias("requestUrlDataHashTag")
      ,col("request.urlData.host").alias("requestUrlDataHost")
      ,col("request.urlData.protocol").alias("requestUrlDataProtocol")
    )

  requestContextCustomDimensionsFlattenedDF = requestContextCustomDimensionsExplodedDF \
    .select(
       col("appInsightsInstanceName")
      ,col("internal_data_id")
      ,col("context_data_eventTime")
      ,col("contextCustomDimensions.AspNetCoreEnvironment").alias("contextCustomDimensionsAspNetCoreEnvironment")
      ,col("contextCustomDimensions.`_MS.ProcessedBymetricExtractors`").alias("contextCustomDimensionsMSProcessedByMetricExtractors")
    )

  return requestDF, requestRequestFlattenedDF, requestContextCustomDimensionsFlattenedDF