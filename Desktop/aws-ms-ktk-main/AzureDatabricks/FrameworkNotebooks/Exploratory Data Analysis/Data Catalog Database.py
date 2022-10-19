# Databricks notebook source
# MAGIC %md # Data Catalog Database

# COMMAND ----------

# MAGIC %md #### Initialize

# COMMAND ----------

# MAGIC %run "../Orchestration/Notebook Functions"

# COMMAND ----------

# MAGIC %run ../Development/Utilities

# COMMAND ----------

import json
import hashlib
import uuid
from multiprocessing.pool import ThreadPool
import sys, traceback
from functools import reduce
from pyspark.sql import DataFrame

dbutils.widgets.text(name="stepLogGuid", defaultValue="00000000-0000-0000-0000-000000000000", label="stepLogGuid")
dbutils.widgets.text(name="stepKey", defaultValue="-1", label="stepKey")
dbutils.widgets.text(name="catalogName", defaultValue="", label="DB Catalog Name")
dbutils.widgets.text(name="doDataProfiling", defaultValue="false", label="Do Data Profiling")
dbutils.widgets.text(name="skipDataProfilingIfExists", defaultValue="true", label="Skip Data Profiling If Exists")
dbutils.widgets.text(name="excludeTables", defaultValue="", label="Tables to Exclude")
dbutils.widgets.text(name="threadPool", defaultValue="1", label="Thread Pool")
dbutils.widgets.text(name="timeoutSeconds", defaultValue="1800", label="Timeout Seconds")
dbutils.widgets.text(name="vacuumRetentionHours", defaultValue="672",label="Vacuum Retention Hours")

stepLogGuid = dbutils.widgets.get("stepLogGuid")
stepKey = int(dbutils.widgets.get("stepKey"))
catalogName = dbutils.widgets.get("catalogName")
doDataProfiling = dbutils.widgets.get("doDataProfiling")
skipDataProfilingIfExists = dbutils.widgets.get("skipDataProfilingIfExists")
excludeTables = dbutils.widgets.get("excludeTables")
threadPool = int(dbutils.widgets.get("threadPool"))
timeout = int(dbutils.widgets.get("timeoutSeconds"))
vacuumRetentionHours = dbutils.widgets.get("vacuumRetentionHours")
silverDataPath = "{0}/{1}".format(silverProtectedBasePath, "datacatalog")

context = dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson()
p = {
  "stepLogGuid": stepLogGuid,
  "stepKey": stepKey,
  "catalogName": catalogName,
  "doDataProfiling": doDataProfiling,
  "excludeTables": excludeTables,
  "threadPool": threadPool,
  "timeout": timeout,
  "vacuumRetentionHours": vacuumRetentionHours,
  "silverDataPath": silverDataPath
}
parameters = json.dumps(p)

notebookLogGuid = str(uuid.uuid4())
log_notebook_start(notebookLogGuid, stepLogGuid, stepKey, parameters, context, server, database, login, pwd)

print("Notebook Log Guid: {0}".format(notebookLogGuid))
print("Step Log Guid: {0}".format(stepLogGuid))
print("Context: {0}".format(context))
print("Parameters: {0}".format(parameters))

# COMMAND ----------

# MAGIC %md #### Validate

# COMMAND ----------

try:
  assert catalogName in [d[0] for d in spark.catalog.listDatabases()], "Database does not exist"
except Exception as e:
    err = {
      "sourceName" : "Data Catalog Database: Initialize",
      "errorCode" : "100",
      "errorDescription" : e.__class__.__name__
    }
    error = json.dumps(err)
    log_notebook_error(notebookLogGuid, error, server, database, login, pwd)
    raise(e)

# COMMAND ----------

# MAGIC %md #### Get Table List

# COMMAND ----------

tableList = [t[0] for t in spark.catalog.listTables(dbName=catalogName) if t[0] not in excludeTables.split(",") and t[3] not in ["TEMPORARY", "VIEW"]]
if skipDataProfilingIfExists == "true":
  sql = """SELECT DISTINCT Table
  FROM silverprotected.dataCatalogSummary
  WHERE Catalog = '{0}'
  """.format(catalogName)
  existingProfilingList = [t[0] for t in spark.sql(sql).collect()]
  dataProfilingTableList = list(set(tableList) - set(existingProfilingList))
else:
  dataProfilingTableList = tableList
if len(tableList) == 0:
  log_notebook_end(notebookLogGuid, 0, server, database, login, pwd)
  dbutils.notebook.exit("Database is empty")
print("Table List: {0}".format(tableList))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Get Table and Column Details

# COMMAND ----------

def mergeTableDetail(catalogName):
  sql = """
  MERGE INTO silverprotected.tableDetail AS tgt
  USING
  (
    SELECT '{0}' AS database, format, id, name, description, location, createdAt, lastModified, partitionColumns, numFiles, sizeInBytes, properties, minReaderVersion, minWriterVersion FROM tablesUnionedStage
  ) AS src
  ON tgt.database = src.database AND tgt.name = src.name AND tgt.database = '{0}'
  WHEN MATCHED THEN UPDATE SET *
  WHEN NOT MATCHED THEN INSERT *
  """.format(catalogName)
  print(sql)
  spark.sql(sql)

# COMMAND ----------

def deletionsTableDetail(catalogName):
  sql = "DELETE FROM silverprotected.tableDetail WHERE database = '{0}' AND name NOT IN (SELECT name FROM tablesUnionedStage)".format(catalogName)
  print(sql)
  spark.sql(sql)

# COMMAND ----------

def mergeColumnDetail(catalogName, tableName):
  sql = """
  MERGE INTO silverprotected.columnDetail AS tgt
  USING
  (
    SELECT '{0}' AS database, tableName, col_name, data_type, comment FROM columnsUnionedStage
  ) AS src
  ON tgt.database = src.database AND tgt.tableName = src.tableName AND tgt.col_name = src.col_name AND tgt.database = '{0}' AND tgt.tableName = '{1}'
  WHEN MATCHED THEN UPDATE SET *
  WHEN NOT MATCHED THEN INSERT *
  """.format(catalogName, tableName)
  print(sql)
  spark.sql(sql)

# COMMAND ----------

def deletionsColumnDetail(catalogName, tableName):
  sql = """
  DELETE FROM silverprotected.ColumnDetail
  WHERE database = '{0}' AND tableName = '{1}'
  AND col_name NOT IN (SELECT col_name FROM columnsUnionedStage)
  """.format(catalogName, tableName)
  print(sql)
  spark.sql(sql)

# COMMAND ----------

tablesAll = []
columnsAll = []

for t in tableList:
  fullyQualifiedTableName = "{0}.{1}".format(catalogName, t)
  try:
    td = describeTable(fullyQualifiedTableName)
    cd = describeTableColumns(fullyQualifiedTableName)
    tablesAll.append(td)
    columnsAll.append(cd)
  except:
    pass

tablesUnioned = reduce(DataFrame.unionAll, tablesAll)
columnsUnioned = reduce(DataFrame.unionAll, columnsAll)
tablesUnioned.createOrReplaceTempView("tablesUnionedStage")
columnsUnioned.createOrReplaceTempView("columnsUnionedStage")

try:
  mergeTableDetail(catalogName)
  deletionsTableDetail(catalogName)
  mergeColumnDetail(catalogName, fullyQualifiedTableName)
  deletionsColumnDetail(catalogName, fullyQualifiedTableName)
except Exception as e:
  err = {
    "sourceName": "Data Catalog Database: Get Table and Column Details",
    "errorCode": "200",
    "errorDescription": e.__class__.__name__
  }
  error = json.dumps(err)
  log_notebook_error(notebookLogGuid, error, server, database, login, pwd)
  raise e

# COMMAND ----------

# MAGIC %md #### Run Data Catalog Tables

# COMMAND ----------

if doDataProfiling != "false":
  try:
    notebook = "../Exploratory Data Analysis/Data Catalog Table"
    pool = ThreadPool(threadPool)
    argsList = []

    for t in dataProfilingTableList:
      args = {
        "catalogName": catalogName,
        "tableName": t
      }
      argsList.append(args)

    pool.map(lambda args: runWithRetry(notebook, timeout, args, max_retries=0), argsList)
  except Exception as e:
    err = {
      "sourceName": "Data Catalog Database: Run Data Catalog Tables",
      "errorCode": "300",
      "errorDescription": e.__class__.__name__
    }
    error = json.dumps(err)
    log_notebook_error(notebookLogGuid, error, server, database, login, pwd)
    pass

# COMMAND ----------

# MAGIC %md
# MAGIC #### Optimize and Vacuum

# COMMAND ----------

try:
  optimizeWhere = "Catalog='{0}'".format(catalogName)
  optimize("silverprotected.datacatalogsummary", optimizeWhere, "Column")
  vacuum("silverprotected.datacatalogsummary", vacuumRetentionHours)
except Exception as e:
  err = {
    "sourceName": "Data Catalog Database: Optimize and Vacuum",
    "errorCode": "400",
    "errorDescription": e.__class__.__name__
  }
  error = json.dumps(err)
  log_notebook_error(notebookLogGuid, error, server, database, login, pwd)
  raise e

# COMMAND ----------

# MAGIC %md #### Log Completion

# COMMAND ----------

log_notebook_end(notebookLogGuid, 0, server, database, login, pwd)
dbutils.notebook.exit("Succeeded")