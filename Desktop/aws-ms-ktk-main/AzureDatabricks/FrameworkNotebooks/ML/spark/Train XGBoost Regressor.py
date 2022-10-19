# Databricks notebook source
# MAGIC %md
# MAGIC #### Initialize

# COMMAND ----------

import ktk
import json

# COMMAND ----------

dbutils.widgets.text(name="stepLogGuid", defaultValue="00000000-0000-0000-0000-000000000000", label="stepLogGuid")
dbutils.widgets.text(name="stepKey", defaultValue="-1", label="stepKey")
dbutils.widgets.text(name="tableName", defaultValue="", label="Table Name")
dbutils.widgets.text(name="experimentName", defaultValue="", label="Experiment Name")
dbutils.widgets.text(name="excludedColumns", defaultValue="", label="Columns to Exclude")
dbutils.widgets.text(name="label", defaultValue="", label="Label")
dbutils.widgets.text(name="continuousColumns", defaultValue="", label="Continuous Columns")
dbutils.widgets.text(name="regularizationParameters", defaultValue="0.1, 0.01", label="Regularization Parameters")
dbutils.widgets.text(name="predictionColumn", defaultValue="prediction", label="Prediction Column")
dbutils.widgets.text(name="folds", defaultValue="2", label="Folds")
dbutils.widgets.text(name="trainTestSplit", defaultValue="0.7, 0.3", label="Train/Test Split")

widgets = ["stepLogGuid", "stepKey", "tableName", "experimentName", "excludedColumns", "label", "continuousColumns","regularizationParameters", "predictionColumn", "folds", "trainTestSplit"]
secrets = []

# COMMAND ----------

snb = ktk.SingleResponsibilityNotebook(widgets, secrets)

# COMMAND ----------

experimentNotebookPath = json.loads(snb.context)['extraContext']['notebook_path']
p = {
  "experimentNotebookPath": experimentNotebookPath
}

parameters = json.dumps(snb.mergeAttributes(p))
snb.log_notebook_start(parameters)
print("Parameters:")
snb.displayAttributes()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create Experiment
# MAGIC Create experiment using supplied parameters

# COMMAND ----------

e = ktk.aiml.XGBoostRegressorExperiment(snb, snb.experimentName)
e.displayAttributes()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Refresh Table

# COMMAND ----------

refreshed = e.refreshTable(snb.tableName)
if refreshed == False:
    snb.log_notebook_end(0)
    dbutils.notebook.exit("Table does not exist")
dfList = []
dfList.append(spark.table(snb.tableName))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Validation

# COMMAND ----------

if snb.tableName == "":
  errorDescription = "Table Name is a required parameter."
  err = {
    "sourceName": "Train Linear Regression: Validation",
    "errorCode": "100",
    "errorDescription": errorDescription
  }
  error = json.dumps(err)
  snb.log_notebook_error(error)
  raise ValueError(errorDescription)

if snb.label == "":
  errorDescription = "Label is a required parameter."
  err = {
    "sourceName": "Xgboost Regression: Validation",
    "errorCode": "100",
    "errorDescription": errorDescription
  }
  error = json.dumps(err)
  snb.log_notebook_error(error)
  raise ValueError(errorDescription)

if snb.excludedColumns != "":
  dfList.append(e.excludeColumns(dfList[-1], snb.excludedColumns))

if snb.continuousColumns == "":
  e.continuousColumns = e.getContinuousColumns(dfList[-1])
  if e.continuousColumns == "":
    e.continuousColumns = []

# COMMAND ----------

# MAGIC %md
# MAGIC #### Train

# COMMAND ----------

try:
  runName = "{0}".format(e.experimentName)
  run_uuid, predictions = e.trainingRun(dfList[-1], runName)
  print("Run UUID: {0}".format(run_uuid))
except Exception as e:
  err = {
    "sourceName": "Xgboost Regression: Train",
    "errorCode": "200",
    "errorDescription": e.__class__.__name__
  }
  error = json.dumps(err)
  snb.log_notebook_error(error)
  raise(e)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Log Completion

# COMMAND ----------

rows = dfList[-1].count()
log_notebook_end(notebookLogGuid, rows, server, database, login, pwd)
dbutils.notebook.exit("Succeeded")
