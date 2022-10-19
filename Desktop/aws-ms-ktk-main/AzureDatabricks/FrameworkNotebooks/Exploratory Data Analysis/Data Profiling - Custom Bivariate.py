# Databricks notebook source
# MAGIC %md # Data Profiling - Custom Bivariate

# COMMAND ----------

# MAGIC %md #### Initialize

# COMMAND ----------

import ktk
from ktk import utilities as u
import datetime, json

# COMMAND ----------

dbutils.widgets.text(name="stepLogGuid", defaultValue="00000000-0000-0000-0000-000000000000", label="stepLogGuid")
dbutils.widgets.text(name="stepKey", defaultValue="-1", label="stepKey")
dbutils.widgets.text(name="tableName", defaultValue="", label="Table")
dbutils.widgets.text(name="deltaHistoryMinutes", defaultValue="-1", label="History Minutes")
dbutils.widgets.text(name="samplePercent", defaultValue="-1", label="Sample Percentage")
dbutils.widgets.text(name="columnToAnalyze", defaultValue="", label="Column to Analyze")
dbutils.widgets.text(name="columnToAnalyze2", defaultValue="", label="Column to Analyze 2")


widgets = ["stepLogGuid","stepKey","tableName","deltaHistoryMinutes","samplePercent","columnToAnalyze", "columnToAnalyze2"]
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
# MAGIC #### Prepare Table

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Refresh Table

# COMMAND ----------

refreshed = u.refreshTable(snb.tableName)
if refreshed == False:
  err = {
    "sourceName": "Data Profiling - Univariate Custom: Refresh Table",
    "errorCode": "100",
    "errorDescription": "Table {0} does not exist".format(snb.tableName)
  }
  error = json.dumps(err)
  snb.log_notebook_error(error)
  dbutils.notebook.exit("Table does not exist")
dfList = []
dfList.append(spark.table(snb.tableName))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Obtain Change Delta

# COMMAND ----------

if int(snb.deltaHistoryMinutes) != -1:
  try:
    print("Obtaining time travel delta")
    dfList.append(u.getTableChangeDelta(dfList[-1], snb.tableName, int(snb.deltaHistoryMinutes)))
    if dfList[-1].count == 0:
      snb.log_notebook_end(0)
      dbutils.notebook.exit("No new or modified rows to process.")
  except Exception as e:
    err = {
      "sourceName" : "Data Profiling - Custom Univariate: Obtain Change Delta",
      "errorCode" : "200",
      "errorDescription" : e.__class__.__name__
    }
    error = json.dumps(err)
    snb.log_notebook_error(error)
    raise(e)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Sample Percentage

# COMMAND ----------

if int(snb.samplePercent) != -1:
  try:
    print("Sampling {0} Percent of Dataframe".format(int(snb.samplePercent) * 100))
    dfList.append(dfList[-1].sample(int(snb.samplePercent)))
  except Exception as e:
    err = {
      "sourceName": "Data Profiling - Custom: Sample Percentage",
      "errorCode": "300",
      "errorDescription": e.__class__.__name__
    }
    error = json.dumps(err)
    snb.log_notebook_error(error)
    raise(e)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Bivariate Object

# COMMAND ----------

try:
  eda_bivariate = ktk.Bivariate(dfList[-1], snb.tableName, snb.columnToAnalyze, snb.columnToAnalyze2)
except Exception as e:
  err = {
    "sourceName": "Data Profiling - Custom: Bivariate Object",
    "errorCode": "400",
    "errorDescription": e.__class__.__name__
  }
  error = json.dumps(err)
  snb.log_notebook_error(error)
  raise(e)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Plotting 

# COMMAND ----------

try:
  eda_bivariate.jointPlot()
except Exception as e:
  err = {
    "sourceName": "Data Profiling - Custom Bivariate: Joint Plot",
    "errorCode": "500",
    "errorDescription": e.__class__.__name__
  }
  error = json.dumps(err)
  snb.log_notebook_error(error)
  raise(e)

# COMMAND ----------

try:
  eda_bivariate.disPlot()
except Exception as e:
  err = {
    "sourceName": "Data Profiling - Custom Bivariate: Display Plot",
    "errorCode": "600",
    "errorDescription": e.__class__.__name__
  }
  error = json.dumps(err)
  snb.log_notebook_error(error)
  raise(e)

# COMMAND ----------

try:
  eda_bivariate.relPlot()
except Exception as e:
  err = {
    "sourceName": "Data Profiling - Custom Bivariate: Rel Plot",
    "errorCode": "700",
    "errorDescription": e.__class__.__name__
  }
  error = json.dumps(err)
  snb.log_notebook_error(error)
  raise(e)

# COMMAND ----------

try:
  eda_bivariate.relPlotLine()
except Exception as e:
  err = {
    "sourceName": "Data Profiling - Custom Bivariate: Rel Plot Line",
    "errorCode": "800",
    "errorDescription": e.__class__.__name__
  }
  error = json.dumps(err)
  snb.log_notebook_error(error)
  raise(e)

# COMMAND ----------

try:
  eda_bivariate.relPlotLine()
except Exception as e:
  err = {
    "sourceName": "Data Profiling - Custom Bivariate: Rel Plot Line",
    "errorCode": "900",
    "errorDescription": e.__class__.__name__
  }
  error = json.dumps(err)
  snb.log_notebook_error(error)
  raise(e)

# COMMAND ----------

try:
  eda_bivariate.plotHeatmap()
except Exception as e:
  err = {
    "sourceName": "Data Profiling - Custom Bivariate: Plot Heat Map",
    "errorCode": "1000",
    "errorDescription": e.__class__.__name__
  }
  error = json.dumps(err)
  snb.log_notebook_error(error)
  raise(e)

# COMMAND ----------

# MAGIC %md #### Log Completion

# COMMAND ----------

rows = dfList[-1].count()
snb.log_notebook_end(rows)
dbutils.notebook.exit("Succeeded")
