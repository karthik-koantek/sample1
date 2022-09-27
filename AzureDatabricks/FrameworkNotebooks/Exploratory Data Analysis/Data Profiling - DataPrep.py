# Databricks notebook source
# MAGIC %md # Data Profiling - DataPrep

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

widgets = ["stepLogGuid","stepKey","tableName","deltaHistoryMinutes","samplePercent"]
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
    "sourceName": "Data Profiling - DataPrep: Refresh Table",
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
      "sourceName" : "Data Profiling - DataPrep: Obtain Change Delta",
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
      "sourceName": "Data Profiling - DataPrep: Sample Percentage",
      "errorCode": "300",
      "errorDescription": e.__class__.__name__
    }
    error = json.dumps(err)
    snb.log_notebook_error(error)
    raise(e)

# COMMAND ----------

# MAGIC %md
# MAGIC #### EDA Object

# COMMAND ----------

try:
  eda_general_info = ktk.GeneralInfo(dfList[-1], snb.tableName)
except Exception as e:
  err = {
    "sourceName": "Data Profiling - DataPrep: EDA Object",
    "errorCode": "400",
    "errorDescription": e.__class__.__name__
  }
  error = json.dumps(err)
  snb.log_notebook_error(error)
  raise(e)

# COMMAND ----------

# MAGIC %md
# MAGIC #### DataPrep Profiling

# COMMAND ----------

try:
  rpt = eda_general_info.plotGeneralInfo()
except Exception as e:
  err = {
    "sourceName": "Data Profiling - DataPrep: DataPrep Profiling",
    "errorCode": "500",
    "errorDescription": e.__class__.__name__
  }
  error = json.dumps(err)
  snb.log_notebook_error(error)
  raise(e)

# COMMAND ----------

rpt

# COMMAND ----------

# MAGIC %md
# MAGIC #### Missing Values

# COMMAND ----------

try:
  rpt = eda_general_info.plotMissing()
except Exception as e:
  err = {
    "sourceName": "Data Profiling - DataPrep: Missing Values",
    "errorCode": "600",
    "errorDescription": e.__class__.__name__
  }
  error = json.dumps(err)
  snb.log_notebook_error(error)
  raise(e)

# COMMAND ----------

rpt

# COMMAND ----------

# MAGIC %md #### Log Completion

# COMMAND ----------

rows = dfList[-1].count()
snb.log_notebook_end(rows)
dbutils.notebook.exit("Succeeded")
