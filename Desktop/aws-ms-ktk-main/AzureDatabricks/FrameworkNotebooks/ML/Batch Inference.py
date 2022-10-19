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
dbutils.widgets.dropdown(name="loadType", defaultValue="Merge", choices=["Append", "Overwrite", "Merge", "MergeType2"], label="Load Type")
dbutils.widgets.dropdown(name="destination", defaultValue="silvergeneral", choices=["silvergeneral", "silverprotected", "goldgeneral", "goldprotected"], label="Destination")

widgets = ["stepLogGuid", "stepKey", "experimentNotebookPath", "experimentId", "metricClause", "tableName", "deltaHistoryMinutes","excludedColumns", "partitions", "primaryKeyColumns", "destinationTableName", "partitionCol", "clusterCol", "clusterBuckets", "optimizeWhere", "optimizeZOrderBy", "vacuumRetentionHours", "loadType", "destination"]
secrets = []

# COMMAND ----------

snb = ktk.SingleResponsibilityNotebook(widgets, secrets)

# COMMAND ----------

pkcols = snb.primaryKeyColumns.replace(" ", "")
upsertTableName = "bronze.{0}_upsert".format(snb.destinationTableName)
fullyQualifiedDestinationTableName = "{0}.{1}".format(snb.destination, snb.destinationTableName)
destinationDataPath = "{0}/modelinference/{1}".format(snb.basePath, snb.destinationTableName)

p = {
  "pkcols": pkcols,
  "upsertTableName": upsertTableName,
  "fullyQualifiedDestinationTableName": fullyQualifiedDestinationTableName,
  "destinationDataPath": destinationDataPath
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
# MAGIC ##### Inputs

# COMMAND ----------

if snb.pkcols == "" and snb.loadType in ["Merge", "MergeType2"]:
  errorDescription = "Primary key column(s) were not supplied and are required for Delta Merge."
  err = {
    "sourceName": "Batch Inference: Validation",
    "errorCode": "100",
    "errorDescription": errorDescription
  }
  error = json.dumps(err)
  snb.log_notebook_error(error)
  raise ValueError(errorDescription)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Refresh Table

# COMMAND ----------

refreshed = u.refreshTable(snb.tableName)
if refreshed == False:
  err = {
    "sourceName": "Batch Inference: Refresh Table",
    "errorCode": "150",
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
# MAGIC ##### Obtain Change Delta

# COMMAND ----------

if snb.deltaHistoryMinutes != -1:
  try:
    print("Obtaining time travel delta")
    dfList.append(u.getTableChangeDelta(dfList[-1], snb.tableName, snb.deltaHistoryMinutes))
    if dfList[-1].count == 0:
      snb.log_notebook_end(0)
      dbutils.notebook.exit("No new or modified rows to process.")
  except Exception as e:
    err = {
      "sourceName" : "Batch Inference: Obtain Change Delta",
      "errorCode" : "200",
      "errorDescription" : e.__class__.__name__
    }
    error = json.dumps(err)
    snb.log_notebook_error(error)
    raise(e)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Get Experiment

# COMMAND ----------

e = ktk.aiml.Experiment(snb, snb.experimentNotebookPath)
experiment = e.getMLFlowExperiment(snb.experimentId, snb.experimentNotebookPath)
if experiment == None:
  errorDescription = "Experiment does not exist."
  err = {
    "sourceName": "Batch Inference: Get Experiment",
    "errorCode": "300",
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
    "sourceName" : "Batch Inference: Get Model from Experiment Runs",
    "errorCode" : "400",
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
    "sourceName" : "Batch Inference: Load Selected Model",
    "errorCode" : "500",
    "errorDescription" : e.__class__.__name__
  }
  error = json.dumps(err)
  snb.log_notebook_error(error)
  raise(e)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Batch Inference

# COMMAND ----------

try:
  if snb.excludedColumns != "":
    dfList.append(e.excludeColumns(dfList[-1], snb.excludedColumns))
  batchPredictionsDF = selected_model.transform(dfList[-1])
  #display(batchPredictionsDF)
except Exception as e:
  err = {
    "sourceName" : "Batch Inference: Batch Inference",
    "errorCode" : "600",
    "errorDescription" : e.__class__.__name__
  }
  error = json.dumps(err)
  snb.log_notebook_error(error)
  raise(e)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Save Results

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Stage data in Bronze Zone

# COMMAND ----------

try:
  pkdf = u.pkCol(batchPredictionsDF, snb.pkcols)
  spark.sql("DROP TABLE IF EXISTS " + snb.upsertTableName)
  pkdf.write.saveAsTable(snb.upsertTableName)
except Exception as e:
  err = {
    "sourceName": "Batch Inference: Stage data in Bronze Zone",
    "errorCode": "700",
    "errorDescription": e.__class__.__name__
  }
  error = json.dumps(err)
  snb.log_notebook_error(error)
  raise(e)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Load Delta Table

# COMMAND ----------

try:
  u.saveDataFrameToDeltaTable(snb.upsertTableName, snb.fullyQualifiedDestinationTableName, snb.loadType, snb.destinationDataPath, snb.partitionCol)
except Exception as e:
  err = {
    "sourceName": "Batch Inference: Load Delta Table",
    "errorCode": "800",
    "errorDescription": e.__class__.__name__
  }
  error = json.dumps(err)
  log_notebook_error(error)
  raise(e)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Optimize and Vacuum

# COMMAND ----------

u.optimize(snb.fullyQualifiedDestinationTableName, snb.optimizeWhere, snb.optimizeZOrderBy)
u.vacuum(snb.fullyQualifiedDestinationTableName, snb.vacuumRetentionHours)

# COMMAND ----------

# MAGIC %md #### Log Completion

# COMMAND ----------

rows = pkdf.count()
snb.log_notebook_end(rows)
dbutils.notebook.exit("Succeeded")
