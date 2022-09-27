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
dbutils.widgets.text(name="excludedColumns", defaultValue="", label="Columns to Exclude")
dbutils.widgets.text(name="destinationTableName", defaultValue="", label="Destination Table")
dbutils.widgets.dropdown(name="destination", defaultValue="silvergeneral", choices=["silvergeneral", "silverprotected", "goldgeneral", "goldprotected"], label="Destination")

stepLogGuid = dbutils.widgets.get("stepLogGuid")
stepKey = int(dbutils.widgets.get("stepKey"))
experimentNotebookPath = dbutils.widgets.get("experimentNotebookPath")
experimentId = dbutils.widgets.get("experimentId")
metricClause = dbutils.widgets.get("metricClause")
tableName = dbutils.widgets.get("tableName")
excludedColumns = dbutils.widgets.get("excludedColumns")
destinationTableName = dbutils.widgets.get("destinationTableName")
destination = dbutils.widgets.get("destination")
destinationDataPath = "{0}/modelinference/{1}".format(destination, destinationTableName)
destination = dbutils.widgets.get("destination")
destinationBasePath = getBasePath(destination)
destinationDataPath = "{0}/modelinference/{1}".format(destinationBasePath, destinationTableName)
checkpointPath = "{0}/checkpoint/modelinference/{1}/".format(destinationBasePath, destinationTableName)

context = dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson()
p = {
  "stepLogGuid": stepLogGuid,
  "stepKey": stepKey,
  "experimentNotebookPath": experimentNotebookPath,
  "experimentId": experimentId,
  "metricClause": metricClause,
  "tableName": tableName,
  "excludedColumns": excludedColumns,
  "destinationTableName": destinationTableName,
  "destination": destination,
  "destinationDataPath": destinationDataPath,
  "checkpointPath": checkpointPath
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
# MAGIC #### Streaming Inference

# COMMAND ----------

try:
  streamDF = []
  streamDF.append(spark.readStream.table(tableName))
  if excludedColumns != "":
    streamDF.append(excludeColumns(streamDF[-1], excludedColumns))
  streamPredictionsDF = selected_model.transform(streamDF[-1])
  #display(streamPredictionsDF)
except Exception as e:
  err = {
    "sourceName" : "Inference: Streaming Inference",
    "errorCode" : "500",
    "errorDescription" : e.__class__.__name__
  }
  error = json.dumps(err)
  log_notebook_error(notebookLogGuid, error, server, database, login, pwd)
  raise(e)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Write to Delta Lake

# COMMAND ----------

try:
  queryQueryName = "{0} query".format(tableName)
  dfQuery = (streamPredictionsDF \
    .writeStream \
    .queryName(queryQueryName) \
    .trigger(once=True) \
    .format("delta") \
    .option("checkpointLocation", checkpointPath) \
    .outputMode("append") \
    .start(destinationDataPath)
  )
except Exception as e:
  err = {
    "sourceName": "Inference: Write to Delta Lake",
    "errorCode": "600",
    "errorDescription": e.__class__.__name__
  }
  error = json.dumps(err)
  log_notebook_error(notebookLogGuid, error, server, database, login, pwd)
  raise(e)

# COMMAND ----------

# MAGIC %md #### Log Completion

# COMMAND ----------

log_notebook_end(notebookLogGuid, 0, server, database, login, pwd)
dbutils.notebook.exit("Succeeded")
