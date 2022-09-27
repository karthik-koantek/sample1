# Databricks notebook source
# MAGIC %run "../ML/MLFunctions"

# COMMAND ----------

# MAGIC %run "../Orchestration/Shared Functions"

# COMMAND ----------

# MAGIC %run "../Development/Utilities"

# COMMAND ----------

# MAGIC %run "../Orchestration/Notebook Functions"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Initialize

# COMMAND ----------

import mlflow
from mlflow import spark as mlflow_spark
import os
import requests
import pandas as pd
import json
import uuid

dbutils.widgets.text(name="stepLogGuid", defaultValue="00000000-0000-0000-0000-000000000000", label="stepLogGuid")
dbutils.widgets.text(name="stepKey", defaultValue="-1", label="stepKey")
dbutils.widgets.text(name="experimentNotebookPath", defaultValue="", label="Experiment Notebook Path")
dbutils.widgets.text(name="experimentId", defaultValue="", label="Experiment Id")
dbutils.widgets.text(name="metricClause", defaultValue="", label="Metric Clause")
dbutils.widgets.text(name="tableName", defaultValue="", label="Table")
dbutils.widgets.text(name="deltaHistoryMinutes", defaultValue="-1", label="Delta History Minutes")
dbutils.widgets.text(name="excludedColumns", defaultValue="", label="Columns to Exclude")
dbutils.widgets.text(name="partitions", defaultValue="8", label="Number of Partitions")
dbutils.widgets.text(name="primaryKeyColumns", defaultValue="", label="Primary Key Cols")
dbutils.widgets.text(name="destinationTableName", defaultValue="", label="Destination Table")
dbutils.widgets.text(name="partitionCol", defaultValue="", label="Partitioned By Column")
dbutils.widgets.text(name="clusterCol", defaultValue="pk", label="Clustered By Column")
dbutils.widgets.text(name="clusterBuckets", defaultValue="50", label="Cluster Buckets")
dbutils.widgets.text(name="optimizeWhere", defaultValue="", label="Optimize Where")
dbutils.widgets.text(name="optimizeZOrderBy", defaultValue="", label="Optimize Z Order By")
dbutils.widgets.text(name="vacuumRetentionHours", defaultValue="168", label="Vacuum Retention Hours")
dbutils.widgets.dropdown(name="loadType", defaultValue="Merge", choices=["Append", "Overwrite", "Merge"], label="Load Type")
dbutils.widgets.dropdown(name="destination", defaultValue="silvergeneral", choices=["silvergeneral", "silverprotected", "goldgeneral", "goldprotected"], label="Destination")

stepLogGuid = dbutils.widgets.get("stepLogGuid")
stepKey = int(dbutils.widgets.get("stepKey"))
experimentNotebookPath = dbutils.widgets.get("experimentNotebookPath")
experimentId = dbutils.widgets.get("experimentId")
metricClause = dbutils.widgets.get("metricClause")
tableName = dbutils.widgets.get("tableName")
deltaHistoryMinutes = dbutils.widgets.get("deltaHistoryMinutes")
excludedColumns = dbutils.widgets.get("excludedColumns")
partitions = dbutils.widgets.get("partitions")
pkcols = dbutils.widgets.get("primaryKeyColumns")
pkcols = pkcols.replace(" ", "")
destinationTableName = dbutils.widgets.get("destinationTableName")
upsertTableName = "bronze.{0}_upsert".format(destinationTableName)
partitionCol = dbutils.widgets.get("partitionCol")
clusterCol = dbutils.widgets.get("clusterCol")
clusterBuckets = dbutils.widgets.get("clusterBuckets")
optimizeWhere = dbutils.widgets.get("optimizeWhere")
optimizeZOrderBy = dbutils.widgets.get("optimizeZOrderBy")
vacuumRetentionHours = dbutils.widgets.get("vacuumRetentionHours")
loadType = dbutils.widgets.get("loadType")
destination = dbutils.widgets.get("destination")
destinationBasePath = getBasePath(destination)
destinationDataPath = "{0}/modelinference/{1}".format(destinationBasePath, destinationTableName)

context = dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson()
p = {
  "stepLogGuid": stepLogGuid,
  "stepKey": stepKey,
  "experimentNotebookPath": experimentNotebookPath,
  "experimentId": experimentId,
  "metricClause": metricClause,
  "tableName": tableName,
  "deltaHistoryMinutes": deltaHistoryMinutes,
  "excludedColumns": excludedColumns,
  "partitions": partitions,
  "pkcols": pkcols,
  "destinationTableName": destinationTableName,
  "partitionCol": partitionCol,
  "clusterCol": clusterCol,
  "clusterBuckets": clusterBuckets,
  "optimizeWhere": optimizeWhere,
  "optimizeZOrderBy": optimizeZOrderBy,
  "vacuumRetentionHours": vacuumRetentionHours,
  "loadType": loadType,
  "destination": destination,
  "destinationDataPath": destinationDataPath
}
parameters = json.dumps(p)

notebookLogGuid = str(uuid.uuid4())
log_notebook_start(notebookLogGuid, stepLogGuid, stepKey, parameters, context, server, database, login, pwd)

print("Notebook Log Guid: {0}".format(notebookLogGuid))
print("Step Log Guid: {0}".format(stepLogGuid))
print("Context: {0}".format(context))
print("Parameters: {0}".format(parameters))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Refresh Table

# COMMAND ----------

refreshed = refreshTable(tableName)
if refreshed == False:
  log_notebook_end(notebookLogGuid, server, database, login, pwd)
  dbutils.notebook.exit("Table does not exist")
dfList = []
dfList.append(spark.table(tableName))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Obtain Change Delta

# COMMAND ----------

if deltaHistoryMinutes != -1:
  try:
    print("Obtaining time travel delta")
    dfList.append(getTableChangeDelta(dfList[-1], tableName, deltaHistoryMinutes))
    if dfList[-1].count == 0:
      log_notebook_end(notebookLogGuid, server, database, login, pwd)
      dbutils.notebook.exit("No new or modified rows to process.")
  except Exception as e:
    err = {
      "sourceName" : "Inference: Obtain Change Delta",
      "errorCode" : "100",
      "errorDescription" : e.__class__.__name__
    }
    error = json.dumps(err)
    log_notebook_error(notebookLogGuid, error, server, database, login, pwd)
    raise(e)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Get Experiment

# COMMAND ----------

experiment = getMLFlowExperiment(experimentId, experimentNotebookPath)
if experiment == None:
  log_notebook_end(notebookLogGuid, 0, server, database, login, pwd)
  dbutils.notebook.exit("Experiment does not exist")
else:
  print("Artifact Location: {0}".format(experiment.artifact_location))
  print("Lifecycle Stage: {0}".format(experiment.lifecycle_stage))
  print("Experiment Id: {0}".format(experiment.experiment_id))
  print("Experiment Name: {0}".format(experiment.name))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Get Model from Experiment Runs

# COMMAND ----------

try:
  selectedModel = selectModel(experiment,metricClause, modelName="model-file")
  selected_experiment_id = selectedModel.first()[0]
  selected_model_id = selectedModel.first()[1]
  selected_model_uri = selectedModel.first()[3]

  print(f"Selected experiment ID: {selected_experiment_id}")
  print(f"Selected model ID: {selected_model_id}")
  print(f"Selected model URI: {selected_model_uri}")
except Exception as e:
  err = {
    "sourceName" : "Inference: Get Model from Experiment Runs",
    "errorCode" : "200",
    "errorDescription" : e.__class__.__name__
  }
  error = json.dumps(err)
  log_notebook_error(notebookLogGuid, error, server, database, login, pwd)
  raise(e)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Load Selected Model

# COMMAND ----------

try:
  selected_model = mlflow_spark.load_model(selected_model_uri)
except Exception as e:
  err = {
    "sourceName" : "Inference: Load Selected Model",
    "errorCode" : "300",
    "errorDescription" : e.__class__.__name__
  }
  error = json.dumps(err)
  log_notebook_error(notebookLogGuid, error, server, database, login, pwd)
  raise(e)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Batch Inference

# COMMAND ----------

try:
  if excludedColumns != "":
    dfList.append(excludeColumns(dfList[-1], excludedColumns))
  batchPredictionsDF = selected_model.transform(dfList[-1])
  #display(batchPredictionsDF)
except Exception as e:
  err = {
    "sourceName" : "Inference: Batch Inference",
    "errorCode" : "400",
    "errorDescription" : e.__class__.__name__
  }
  error = json.dumps(err)
  log_notebook_error(notebookLogGuid, error, server, database, login, pwd)
  raise(e)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Stage data in Bronze Zone

# COMMAND ----------

try:
  pkdf = pkCol(batchPredictionsDF, pkcols)
  spark.sql("DROP TABLE IF EXISTS " + upsertTableName)
  pkdf.write.saveAsTable(upsertTableName)
except Exception as e:
  err = {
    "sourceName": "Batch Inference: Stage data in Bronze Zone",
    "errorCode": "500",
    "errorDescription": e.__class__.__name__
  }
  error = json.dumps(err)
  log_notebook_error(notebookLogGuid, error, server, database, login, pwd)
  raise(e)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Load Delta Table

# COMMAND ----------

try:
  saveDataFrameToDeltaTable(upsertTableName, destinationTableName, loadType, destinationDataPath, partitionCol)
except Exception as e:
  err = {
    "sourceName": "Batch Inference: Load Delta Table",
    "errorCode": "600",
    "errorDescription": e.__class__.__name__
  }
  error = json.dumps(err)
  log_notebook_error(notebookLogGuid, error, server, database, login, pwd)
  raise(e)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Optimize and Vacuum

# COMMAND ----------

optimize(destinationTableName, optimizeWhere, optimizeZOrderBy)
vacuum(destinationTableName, vacuumRetentionHours)

# COMMAND ----------

# MAGIC %md #### Log Completion

# COMMAND ----------

rows = pkdf.count()
log_notebook_end(notebookLogGuid, rows, server, database, login, pwd)
dbutils.notebook.exit("Succeeded")
