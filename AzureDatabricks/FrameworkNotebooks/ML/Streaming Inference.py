# Databricks notebook source
# MAGIC %md
# MAGIC #### Initialize

# COMMAND ----------

import ktk
from ktk import utilities as u
import datetime, json
from mlflow import spark as mlflow_spark

# COMMAND ----------

dbutils.widgets.text(name="stepLogGuid", defaultValue="00000000-0000-0000-0000-000000000000", label="stepLogGuid")
dbutils.widgets.text(name="stepKey", defaultValue="-1", label="stepKey")
dbutils.widgets.text(name="experimentNotebookPath", defaultValue="", label="Experiment Notebook Path")
dbutils.widgets.text(name="experimentId", defaultValue="", label="Experiment Id")
dbutils.widgets.text(name="metricClause", defaultValue="", label="Metric Clause")
dbutils.widgets.text(name="tableName", defaultValue="", label="Table")
dbutils.widgets.text(name="excludedColumns", defaultValue="", label="Columns to Exclude")
dbutils.widgets.text(name="destinationTableName", defaultValue="", label="Destination Table")
dbutils.widgets.dropdown(name="destination", defaultValue="silvergeneral", choices=["silvergeneral", "silverprotected", "goldgeneral", "goldprotected"], label="Destination")

widgets = ["stepLogGuid", "stepKey", "experimentNotebookPath", "experimentId", "metricClause", "tableName", "excludedColumns", "destinationTableName", "destination"]
secrets = []

# COMMAND ----------

snb = ktk.SingleResponsibilityNotebook(widgets, secrets)

# COMMAND ----------

fullyQualifiedDestinationTableName = "{0}.{1}".format(snb.destination, snb.destinationTableName)
destinationDataPath = "{0}/modelinference/{1}".format(snb.basePath, snb.destinationTableName)
checkpointPath = "{0}/checkpoint/modelinference/{1}/".format(snb.basePath, snb.destinationTableName)

p = {
  "fullyQualifiedDestinationTableName": fullyQualifiedDestinationTableName,
  "destinationDataPath": destinationDataPath,
  "checkpointPath": checkpointPath
}

parameters = json.dumps(snb.mergeAttributes(p))
snb.log_notebook_start(parameters)
print("Parameters")
snb.displayAttributes()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Validation

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Refresh Table

# COMMAND ----------

refreshed = u.refreshTable(snb.tableName)
if refreshed == False:
  err = {
    "sourceName": "Streaming Inference: Refresh Table",
    "errorCode": "100",
    "errorDescription": "Table {0} does not exist".format(snb.tableName)
  }
  error = json.dumps(err)
  snb.log_notebook_error(error)
  snb.log_notebook_end(0)
  dbutils.notebook.exit("Table does not exist")
dfList = []
dfList.append(spark.table(snb.tableName))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Get Experiment

# COMMAND ----------

e = ktk.aiml.Experiment(snb, snb.experimentNotebookPath)
experiment = e.getMLFlowExperiment(snb.experimentId, snb.experimentNotebookPath)
if experiment == None:
  errorDescription = "Experiment does not exist."
  err = {
    "sourceName": "Streaming Inference: Get Experiment",
    "errorCode": "200",
    "errorDescription": errorDescription
  }
  error = json.dumps(err)
  snb.log_notebook_error(error)
  snb.log_notebook_end(0)
  dbutils.notebook.exit("Experiment does not exist")
else:
  e.displayExperimentDetails(experiment)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Load Model

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Get Model from Experiment Runs

# COMMAND ----------

try:
  selectedModel, selected_experiment_id, selected_model_id, selected_model_uri = e.selectModel(experiment,snb.metricClause, modelName="model-file")
  print(f"Selected experiment ID: {selected_experiment_id}")
  print(f"Selected model ID: {selected_model_id}")
  print(f"Selected model URI: {selected_model_uri}")
except Exception as e:
  err = {
    "sourceName" : "Streaming Inference: Get Model from Experiment Runs",
    "errorCode" : "300",
    "errorDescription" : e.__class__.__name__
  }
  error = json.dumps(err)
  snb.log_notebook_error(error)
  raise(e)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Load Selected Model

# COMMAND ----------

try:
  selected_model = mlflow_spark.load_model(selected_model_uri)
except Exception as e:
  err = {
    "sourceName" : "Streaming Inference: Load Selected Model",
    "errorCode" : "400",
    "errorDescription" : e.__class__.__name__
  }
  error = json.dumps(err)
  snb.log_notebook_error(error)
  raise(e)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Streaming Inference

# COMMAND ----------

try:
  streamDF = []
  streamDF.append(spark.readStream.table(snb.tableName))
  if snb.excludedColumns != "":
    streamDF.append(e.excludeColumns(streamDF[-1], snb.excludedColumns))
  streamPredictionsDF = selected_model.transform(streamDF[-1])
  #display(streamPredictionsDF)
except Exception as e:
  err = {
    "sourceName" : "Streaming Inference: Streaming Inference",
    "errorCode" : "500",
    "errorDescription" : e.__class__.__name__
  }
  error = json.dumps(err)
  log_notebook_error(error)
  raise(e)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Save Results

# COMMAND ----------

try:
  queryQueryName = "{0} query".format(snb.tableName)
  dfQuery = (streamPredictionsDF \
    .writeStream \
    .queryName(queryQueryName) \
    .trigger(once=True) \
    .format("delta") \
    .option("checkpointLocation", snb.checkpointPath) \
    .option("ignoreChanges", True) \
    .outputMode("append") \
    .start(snb.destinationDataPath)
  )
except Exception as e:
  err = {
    "sourceName": "Streaming Inference: Write to Delta Lake",
    "errorCode": "600",
    "errorDescription": e.__class__.__name__
  }
  error = json.dumps(err)
  snb.log_notebook_error(error)
  raise(e)

# COMMAND ----------

sql = "CREATE TABLE IF NOT EXISTS {0} USING delta LOCATION '{1}'".format(snb.fullyQualifiedDestinationTableName, snb.destinationDataPath)
spark.sql(sql)

# COMMAND ----------

# MAGIC %md #### Log Completion

# COMMAND ----------

snb.log_notebook_end(0)
dbutils.notebook.exit("Succeeded")
