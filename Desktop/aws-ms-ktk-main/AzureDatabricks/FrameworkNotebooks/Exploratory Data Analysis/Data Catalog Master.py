# Databricks notebook source
# MAGIC %md # Data Catalog Master

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
dbutils.widgets.text(name="excludeDatabases", defaultValue="bronze", label="Databases to Exclude")
dbutils.widgets.text(name="excludeDatabasesFromDataProfiling", defaultValue="bronze,default,archive,sandbox")
dbutils.widgets.text(name="excludeTables", defaultValue="", label="Tables to Exclude")
dbutils.widgets.text(name="threadPool", defaultValue="1", label="Thread Pool")
dbutils.widgets.text(name="timeoutSeconds", defaultValue="180000", label="Timeout Seconds")
dbutils.widgets.text(name="vacuumRetentionHours", defaultValue="672",label="Vacuum Retention Hours")

stepLogGuid = dbutils.widgets.get("stepLogGuid")
stepKey = int(dbutils.widgets.get("stepKey"))
excludeDatabases = dbutils.widgets.get("excludeDatabases")
excludeDatabasesFromDataProfiling = dbutils.widgets.get("excludeDatabasesFromDataProfiling")
excludeTables = dbutils.widgets.get("excludeTables")
threadPool = int(dbutils.widgets.get("threadPool"))
timeout = int(dbutils.widgets.get("timeoutSeconds"))

vacuumRetentionHours = dbutils.widgets.get("vacuumRetentionHours")
silverDataPath = "{0}/{1}".format(silverProtectedBasePath, "datacatalog")

context = dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson()
p = {
  "stepLogGuid": stepLogGuid,
  "stepKey": stepKey,
  "excludeDatabases": excludeDatabases,
  "excludeDatabasesFromDataProfiling": excludeDatabasesFromDataProfiling,
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

# MAGIC %md
# MAGIC #### Clear Silver Zone Data

# COMMAND ----------

#try:
#  silverDataPath = "{0}/{1}".format(silverProtectedBasePath, "datacatalog")
#  dbutils.fs.rm(silverDataPath, True)
#except Exception as e:
#  err = {
#    "sourceName" : "Data Catalog Master: Clear Silver Zone Data",
#    "errorCode" : "100",
#    "errorDescription" : e.__class__.__name__
#  }
#  error = json.dumps(err)
#  log_notebook_error(notebookLogGuid, error, server, database, login, pwd)
#  raise(e)

# COMMAND ----------

#%sql
#DROP TABLE IF EXISTS silverprotected.databaseDetail;
#DROP TABLE IF EXISTS silverprotected.tableDetail;
#DROP TABLE IF EXISTS silverprotected.columnDetail;
#DROP TABLE IF EXISTS silverprotected.dataCatalogValueCounts;
#DROP TABLE IF EXISTS silverprotected.dataCatalogSummary;
#DROP TABLE IF EXISTS silverprotected.fileDetail;

# COMMAND ----------

# MAGIC %md #### Get Database List

# COMMAND ----------

try:
  databases = [d.name for d in spark.catalog.listDatabases() if d.name not in excludeDatabases.split(",")]
  print("Database List: {0}".format(databases))
except Exception as e:
  err = {
    "sourceName" : "Data Catalog Master: Get Database List",
    "errorCode" : "100",
    "errorDescription" : e.__class__.__name__
  }
  error = json.dumps(err)
  log_notebook_error(notebookLogGuid, error, server, database, login, pwd)
  raise(e)

# COMMAND ----------

# MAGIC %md #### Create Catalog Tables and Views

# COMMAND ----------

try:
  notebook = "../Exploratory Data Analysis/Data Catalog Schema"
  runWithRetry(notebook, 120)
except Exception as e:
  err = {
    "sourceName" : "Data Catalog Database: Create Catalog Tables and Views",
    "errorCode" : "200",
    "errorDescription" : e.__class__.__name__
  }
  error = json.dumps(err)
  log_notebook_error(notebookLogGuid, error, server, database, login, pwd)
  raise(e)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Get Database Details

# COMMAND ----------

databasesAll = []
for d in databases:
  dd = describeDatabasePivoted(d)
  databasesAll.append(dd)

databasesUnioned = reduce(DataFrame.unionAll, databasesAll)
databasesUnioned.createOrReplaceTempView("databasesUnionedStage")
databasesql = """
  MERGE INTO silverprotected.databaseDetail AS tgt
  USING databasesUnionedStage AS src
  ON tgt.database = src.`Namespace Name`
  WHEN MATCHED THEN UPDATE SET tgt.location = src.`Location`, tgt.owner = src.`Owner`, tgt.comment = src.`Comment`
  WHEN NOT MATCHED THEN INSERT (database, location, owner, comment) VALUES (src.`Namespace Name`, src.`Location`, src.`Owner`, src.`Comment`)
"""
print(databasesql)
spark.sql(databasesql)

databasedeletesql = """
  DELETE FROM silverprotected.databasedetail
  WHERE database NOT IN (SELECT `Namespace Name` FROM databasesUnionedStage)
"""
spark.sql(databasedeletesql)

# COMMAND ----------

# MAGIC %md #### Run Data Catalog Files

# COMMAND ----------

try:
  fileSystems = []
  notebook = "../Exploratory Data Analysis/Data Catalog File"
  pool = ThreadPool(threadPool)
  fileSystems.append({"stepLogGuid": stepLogGuid,"stepKey": stepKey,"storageAccountSecretName":"BronzeStorageAccountName","storageAccountKeySecretName":"BronzeStorageAccountKey","fileSystemName":"raw","maxResultsPerPage": 5000})
  fileSystems.append({"stepLogGuid": stepLogGuid,"stepKey": stepKey,"storageAccountSecretName":"SilverGoldStorageAccountName","storageAccountKeySecretName":"SilverGoldStorageAccountKey","fileSystemName":"silverprotected","maxResultsPerPage": 5000})
  fileSystems.append({"stepLogGuid": stepLogGuid,"stepKey": stepKey,"storageAccountSecretName":"SilverGoldStorageAccountName","storageAccountKeySecretName":"SilverGoldStorageAccountKey","fileSystemName":"silvergeneral","maxResultsPerPage": 5000})
  fileSystems.append({"stepLogGuid": stepLogGuid,"stepKey": stepKey,"storageAccountSecretName":"SilverGoldStorageAccountName","storageAccountKeySecretName":"SilverGoldStorageAccountKey","fileSystemName":"goldprotected","maxResultsPerPage": 5000})
  fileSystems.append({"stepLogGuid": stepLogGuid,"stepKey": stepKey,"storageAccountSecretName":"SilverGoldStorageAccountName","storageAccountKeySecretName":"SilverGoldStorageAccountKey","fileSystemName":"goldgeneral","maxResultsPerPage": 5000})
  fileSystems.append({"stepLogGuid": stepLogGuid,"stepKey": stepKey,"storageAccountSecretName":"SandboxStorageAccountName","storageAccountKeySecretName":"SandboxStorageAccountKey","fileSystemName":"defaultsandbox","maxResultsPerPage": 5000})
  pool.map(lambda args: runWithRetry(notebook, timeout, args, max_retries=0), fileSystems)
except Exception as e:
  err = {
    "sourceName": "Data Catalog Master: Run Data Catalog Files",
    "errorCode": 300,
    "errorDescription": e.__class__.__name__
  }
  error = json.dumps(err)
  log_notebook_error(notebookLogGuid, error, server, database, login, pwd)
  raise e
  #pass

# COMMAND ----------

# MAGIC %md #### Run Data Catalog Databases

# COMMAND ----------

try:
  notebook = "../Exploratory Data Analysis/Data Catalog Database"
  pool = ThreadPool(threadPool)
  argsList = []

  for db in databases:
    if db in excludeDatabasesFromDataProfiling:
      doDataProfiling = "false"
    else:
      doDataProfiling = "true"
    args = {
      "stepLogGuid": stepLogGuid,
      "stepKey": stepKey,
      "catalogName": db,
      "doDataProfiling": doDataProfiling,
      "excludeTables": excludeTables,
      "threadPool": threadPool,
      "timeoutSeconds": timeout,
      "vacuumRetentionHours": vacuumRetentionHours
    }
    argsList.append(args)
  pool.map(lambda args: runWithRetry(notebook, timeout, args, max_retries=0), argsList)
except Exception as e:
  err = {
    "sourceName": "Data Catalog Master: Run Data Catalog Database",
    "errorCode": 400,
    "errorDescription": e.__class__.__name__
  }
  error = json.dumps(err)
  log_notebook_error(notebookLogGuid, error, server, database, login, pwd)
  raise e
  #pass

# COMMAND ----------

# MAGIC %md
# MAGIC #### Cleanup Staging

# COMMAND ----------

try:
  [spark.sql("DROP TABLE bronze.{0}".format(t[0])) for t in spark.catalog.listTables(dbName="bronze") if t[3] == 'MANAGED' and "datacatalog" in t[0]]
except Exception as e:
  err = {
    "sourceName": "Data Catalog Master: Cleanup Staging",
    "errorCode": 400,
    "errorDescription": e.__class__.__name__
  }
  error = json.dumps(err)
  log_notebook_error(notebookLogGuid, error, server, database, login, pwd)
  raise e
  #pass

# COMMAND ----------

# MAGIC %md #### Log Completion

# COMMAND ----------

log_notebook_end(notebookLogGuid, 0, server, database, login, pwd)
dbutils.notebook.exit("Succeeded")