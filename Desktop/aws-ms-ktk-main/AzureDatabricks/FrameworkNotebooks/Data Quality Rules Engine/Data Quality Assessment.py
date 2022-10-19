# Databricks notebook source
# MAGIC %md
# MAGIC # Data Quality Assessment

# COMMAND ----------

# MAGIC %md
# MAGIC #### Initialize

# COMMAND ----------

import ktk
from ktk import utilities as u
import datetime, json
import great_expectations as ge
from pyspark.sql.functions import explode_outer, explode, split, lit
from pyspark.sql.types import LongType
from great_expectations.profile.basic_dataset_profiler import BasicDatasetProfiler
from great_expectations.dataset.sparkdf_dataset import SparkDFDataset
from great_expectations.render.renderer import ProfilingResultsPageRenderer, ExpectationSuitePageRenderer, ValidationResultsPageRenderer
from great_expectations.render.view import DefaultJinjaPageView

# COMMAND ----------

# MAGIC %run "../Data Quality Rules Engine/Create Data Quality Rules Schema"

# COMMAND ----------

dbutils.widgets.text(name="stepLogGuid", defaultValue="00000000-0000-0000-0000-000000000000", label="stepLogGuid")
dbutils.widgets.text(name="stepKey", defaultValue="-1", label="stepKey")
dbutils.widgets.text(name="tableName", defaultValue="", label="Table Name")
dbutils.widgets.text(name="deltaHistoryMinutes", defaultValue="-1", label="History Minutes")

widgets = ["stepLogGuid","stepKey","tableName","deltaHistoryMinutes"]
secrets = []

# COMMAND ----------

snb = ktk.SingleResponsibilityNotebook(widgets, secrets)

# COMMAND ----------

p = {}
parameters = json.dumps(snb.mergeAttributes(p))
snb.log_notebook_start(parameters)
print("Parameters:")
snb.displayAttributes()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Refresh Table

# COMMAND ----------

refreshed = u.refreshTable(snb.tableName)
if refreshed == False:
  err = {
      "sourceName" : "Data Quality Assessment: Refresh Table",
      "errorCode" : "100",
      "errorDescription" : "Table does not exist"}
  error = json.dumps(err)
  snb.log_notebook_error(error)
  snb.log_notebook_end(0)
  dbutils.notebook.exit("Table does not exist")
dfList = []
dfList.append(spark.table(snb.tableName))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Obtain Change Delta

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
      "sourceName" : "Data Quality Assessment: Obtain Change Delta",
      "errorCode" : "100",
      "errorDescription" : e.__class__.__name__
    }
    error = json.dumps(err)
    snb.log_notebook_error(error)
    raise(e)

dfGE = ge.dataset.SparkDFDataset(dfList[-1])

# COMMAND ----------

# MAGIC %md
# MAGIC #### Get Expectations

# COMMAND ----------

try:
  expectation_list = getDataQualityRulesForTable(snb.tableName)
  print(expectation_list)
except Exception as e:
  err = {
    "sourceName" : "Data Quality Assessment: Get Expectations",
    "errorCode" : "200",
    "errorDescription" : e.__class__.__name__
  }
  error = json.dumps(err)
  snb.log_notebook_error(error)
  raise(e)

if len(expectation_list) == 0:
  err = {
      "sourceName" : "Data Quality Assessment: Get Expectations",
      "errorCode" : "200",
      "errorDescription" : "No expectations exist for this table."}
  error = json.dumps(err)
  snb.log_notebook_error(error)
  snb.log_notebook_end(0)
  dbutils.notebook.exit("No expectations exist for this table.")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Validate Expectations

# COMMAND ----------

try:
  validation_results = validateGreatExpectations (dfGE, snb.tableName, expectation_list)
except Exception as e:
  err = {
    "sourceName" : "Data Quality Assessment: Validate Expectations",
    "errorCode" : "300",
    "errorDescription" : e.__class__.__name__
  }
  error = json.dumps(err)
  snb.log_notebook_error(error)
  raise(e)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Save Results

# COMMAND ----------

try:
  validationResultsDF = convertValidationResultsToDataFrame(snb.tableName, validation_results)
  saveDataQualityValidationResultsForTable(snb.tableName, validationResultsDF, goldDataPath, snb.stepLogGuid)
except Exception as e:
  err = {
    "sourceName" : "Data Quality Assessment: Save Results",
    "errorCode" : "400",
    "errorDescription" : e.__class__.__name__
  }
  error = json.dumps(err)
  snb.log_notebook_error(error)
  raise(e)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Log Completion

# COMMAND ----------

rows = 0 #update with rows affected
snb.log_notebook_end(rows)
dbutils.notebook.exit("Succeeded")
