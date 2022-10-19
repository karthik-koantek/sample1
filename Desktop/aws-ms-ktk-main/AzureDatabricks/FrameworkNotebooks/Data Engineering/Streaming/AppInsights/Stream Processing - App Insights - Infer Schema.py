# Databricks notebook source
# MAGIC %md # Stream Processing - App Insights - Infer Schema
# MAGIC
# MAGIC Infer the schema of blob source files for use in structured streaming.
# MAGIC
# MAGIC #### Usage
# MAGIC The notebook 'Stream Processing - App Insights' will call this notebook to infer and save the schema to the data lake if the schema is not already found.
# MAGIC
# MAGIC #### Prerequisites
# MAGIC
# MAGIC #### Details

# COMMAND ----------

# MAGIC %md #### Initialize

# COMMAND ----------

# MAGIC %run "../../../Orchestration/Notebook Functions"

# COMMAND ----------

dbutils.widgets.text(name="stepLogGuid", defaultValue="00000000-0000-0000-0000-000000000000", label="stepLogGuid")
dbutils.widgets.text(name="stepKey", defaultValue="-1", label="stepKey")
dbutils.widgets.text(name="dateToProcess", defaultValue="", label="Date to Process")
dbutils.widgets.text(name="appInsightsInstanceName", defaultValue="appinsightscswapi-qa_ea7fa0dbcb8d44f6b923345ba6462ada", label="App Insights Instance Name")
dbutils.widgets.text(name="appInsightsDataType", defaultValue="Metrics", label="App Insights Data Type")
dbutils.widgets.text(name="dateToProcess", defaultValue="2019-07-15", label="Date to Process")
dbutils.widgets.text(name="samplingRatio", defaultValue=".2", label="Sampling Ratio")
appInsightsStorageAccountName = dbutils.secrets.get(scope="external", key="appInsightsStorageAccountName")
appInsightsStorageAccountKey = dbutils.secrets.get(scope="external", key="appInsightsStorageAccountKey")

stepLogGuid = dbutils.widgets.get("stepLogGuid")
stepKey = int(dbutils.widgets.get("stepKey"))
dateToProcess = dbutils.widgets.get("dateToProcess")
if dateToProcess == "":
  dateToProcess = datetime.datetime.utcnow().strftime('%Y/%m/%d')
appInsightsInstanceName = dbutils.widgets.get("appInsightsInstanceName")
appInsightsDataType = dbutils.widgets.get("appInsightsDataType")
samplingRatio = dbutils.widgets.get("samplingRatio")
appInsightsFullStorageAccountName = "fs.azure.account.key.{0}.blob.core.windows.net".format(appInsightsStorageAccountName)
schemaPath = "{0}/raw/schemas/appinsights/{1}".format(basepath, appInsightsDataType)
schemaFile = schemaPath + "/schema.json"
externalPath = "{0}/{1}/{2}/{3}/*/*".format(appInsightsBasePath,appInsightsInstanceName, appInsightsDataType, dateToProcess)
appInsightsBasePath = "wasbs://appinsights@{0}.blob.core.windows.net".format(appInsightsStorageAccountName)

context = dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson()
p = {
  "stepLogGuid": stepLogGuid,
  "stepKey": stepKey,
  "dateToProcess": dateToProcess,
  "appInsightsInstanceName": appInsightsInstanceName,
  "appInsightsDataType": appInsightsDataType,
  "samplingRatio": samplingRatio,
  "appInsightsFullStorageAccountName": appInsightsFullStorageAccountName,
  "schemaPath": schemaPath,
  "schemaFile": schemaFile,
  "externalPath": externalPath,
  "appInsightsBasePath": appInsightsBasePath
}
parameters = json.dumps(p)
notebookLogGuid = str(uuid.uuid4())
log_notebook_start(notebookLogGuid, stepLogGuid, stepKey, parameters, context, server, database, login, pwd)

# COMMAND ----------

spark.conf.set(
  appInsightsFullStorageAccountName,
  appInsightsStorageAccountKey)

# COMMAND ----------

# MAGIC %md #### Attempt to Sample Source and Infer Schema

# COMMAND ----------

try:
  df = spark \
    .read \
    .options(samplingRatio=samplingRatio) \
    .json(externalPath)
except Exception as e:
  err = {
    "sourceName": "Stream Processing - App Insights - Infer Schema: Attempt to Sample Source and Infer Schema",
    "errorCode": "200",
    "errorDescription": e.__class__.__name__
  }
  error = json.dumps(err)
  log_notebook_error(notebookLogGuid, error, server, database, login, pwd)
  raise(e)

# COMMAND ----------

df.schema

# COMMAND ----------

# MAGIC %md #### Convert Schema to JSON and Save to Data Lake

# COMMAND ----------

try:
  schema = df.schema
  schema_json = df.schema.json()
  dbutils.fs.mkdirs(schemaPath)
  dbutils.fs.put(schemaFile, schema_json, True)
except Exception as e:
  err = {
  "sourceName": "Stream Processing - App Insights - Infer Schema: Convert Schema to JSON and Save to Data Lake",
  "errorCode": "400",
  "errorDescription": e.__class__.__name__
  }
  error = json.dumps(err)
  log_notebook_error(notebookLogGuid, error, server, database, login, pwd)
  raise(e)

# COMMAND ----------

log_notebook_end(notebookLogGuid, 0, server, database, login, pwd)
dbutils.notebook.exit("Succeeded")