# Databricks notebook source
# MAGIC %md # Validate Pipeline

# COMMAND ----------

# MAGIC %run "../Orchestration/Orchestration Functions"

# COMMAND ----------

# MAGIC %run "../Orchestration/Notebook Functions"

# COMMAND ----------

# MAGIC %run ../Development/Utilities

# COMMAND ----------

# MAGIC %md
# MAGIC #### Initialize

# COMMAND ----------

import datetime, json
from pyspark.sql.functions import current_timestamp, lit, unix_timestamp
from pyspark.sql.types import TimestampType, BooleanType
from multiprocessing.pool import ThreadPool
import mlflow
import os

dbutils.widgets.text(name="stepLogGuid", defaultValue="00000000-0000-0000-0000-000000000000", label="stepLogGuid")
dbutils.widgets.text(name="stepKey", defaultValue="-1", label="stepKey")
dbutils.widgets.text(name="experimentName", defaultValue="", label="Experiment Name")
dbutils.widgets.text(name="database", defaultValue="", label="Database Catalog")
dbutils.widgets.text(name="threadPool", defaultValue="1", label="Thread Pool")
dbutils.widgets.text(name="timeoutSeconds", defaultValue="1800", label="Timeout Seconds")

stepLogGuid = dbutils.widgets.get("stepLogGuid")
stepKey = int(dbutils.widgets.get("stepKey"))
experimentName = dbutils.widgets.get("experimentName")
databaseCatalog = dbutils.widgets.get("database")
threadPool = int(dbutils.widgets.get("threadPool"))
timeout = int(dbutils.widgets.get("timeoutSeconds"))

context = dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson()
experimentNotebookPath = json.loads(context)['extraContext']['notebook_path']
mlflow.set_experiment(experimentNotebookPath)

p = {

  "stepLogGuid": stepLogGuid,
  "stepKey": stepKey,
  "experimentName": experimentName,
  "databaseCatalog": databaseCatalog,
  "threadPool": threadPool,
  "timeout": timeout
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
# MAGIC #### Get UAT Config

# COMMAND ----------

try:
  tests = getValidationTests(databaseCatalog, server, database, login, pwd).collect()
  if len(tests) == 0:
    errorDescription = "No tests found for database {0}.".format(databaseCatalog)
    err = {
      "sourceName": "Delta Load: Validation",
      "errorCode": "100",
      "errorDescription": errorDescription
    }
    error = json.dumps(err)
    log_notebook_error(notebookLogGuid, error, server, database, login, pwd)
    raise ValueError(errorDescription)
except Exception as e:
  err = {
    "sourceName" : "Validate Pipeline: Get UAT Config",
    "errorCode" : "200",
    "errorDescription" : "getValidationTests failed: {0}".format(e.__class__.__name__)
  }
  error = json.dumps(err)
  log_notebook_error(notebookLogGuid, error)
  raise(e)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Loop through Config Entries
# MAGIC * Multithreaded
# MAGIC * Grab the response from the child notebook (which should report failed tests but not fail; however it will fail on errors)
# MAGIC * Log response to Accelerator DB and to MLFlow experiment

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Spark Tables and Views

# COMMAND ----------

notebook = "../Data Engineering/Gold Zone/Validate Spark Table or View"
pool = ThreadPool(threadPool)
argsList = []
sparkTableOrViewTests = [t for t in tests if t[1] in ['bronze','silver','gold','default'] and t[3] in ['Table','View']]
for test in sparkTableOrViewTests:
  args = {
    "stepLogGuid": stepLogGuid,
    "stepKey": stepKey,
    "validationKey": test[0],
    "dataLakeZone": test[1],
    "databaseCatalog": test[2],
    "objectType": test[3],
    "tableOrViewName": test[4],
    "expectedColumns": test[8],
    "expectedNewOrModifiedRows2Days": test[6],
    "expectedNewOrModifiedRows6Days": test[7]
  }
  if threadPool == 1:
    runWithRetryLogExceptions(notebook, timeout, args, max_retries=0)
  else:
    argsList.append(args)

if threadPool != 1:
  pool.map(lambda args: runWithRetryLogExceptions(notebook, timeout, args, max_retries=0), argsList)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Spark Queries

# COMMAND ----------

notebook = "../Data Engineering/Gold Zone/Validate Query"
pool = ThreadPool(threadPool)
argsList = []
sparkQueryTests = [t for t in tests if t[1] in ['bronze','silver','gold','default','sandbox'] and t[3] in ['Query']]

for test in sparkQueryTests:
  args = {
    "stepLogGuid": stepLogGuid,
    "stepKey": stepKey,
    "validationKey": test[0],
    "dataLakeZone": test[1],
    "expectedColumns": test[8],
    "expectedRowCount": test[9]
  }
  if threadPool == 1:
    runWithRetryLogExceptions(notebook, timeout, args, max_retries=0)
  else:
    argsList.append(args)

if threadPool != 1:
  pool.map(lambda args: runWithRetryLogExceptions(notebook, timeout, args, max_retries=0), argsList)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Platinum Zone Tables and Views

# COMMAND ----------

platinumZoneTableOrViewTests = [t for t in tests if t[1] in ['platinum'] and t[3] in ['Table','View']]

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Platinum Zone Queries

# COMMAND ----------

platinumZoneQueryTests = [t for t in tests if t[1] in ['platinum'] and t[3] in ['Query']]

# COMMAND ----------

# MAGIC %md
# MAGIC #### Gather Results and Log to MLFlow

# COMMAND ----------

testResults = getValidationTestResults (stepLogGuid, server, database, login, pwd)
testResults.createOrReplaceTempView("testResults")

# COMMAND ----------

def validationRun(testResults, p, experimentName):
  totalTestCount = spark.sql("SELECT COUNT(*) AS Total FROM testResults").collect()[0][0]
  validationStatus = spark.sql("SELECT ValidationStatus, COUNT(*) AS Count FROM testResults GROUP BY ValidationStatus").collect()
  failedTests = spark.sql("SELECT * FROM testResults WHERE ValidationStatus <> 'Passed'").collect()
  succeededTests = spark.sql("SELECT TableOrViewName FROM testResults WHERE ValidationStatus == 'Passed'").collect()
  passedTests = len(succeededTests)
  passPercentage = passedTests / totalTestCount
  passedList = []
  succeededTests = spark.sql("SELECT TableOrViewName FROM testResults WHERE ValidationStatus == 'Passed'").collect()
  for succeededTest in succeededTests:
    passedList.append(succeededTest[0])
  passed = ",".join(passedList)
  with mlflow.start_run(run_name=experimentName, nested=True) as run:
    mlflow.log_params(p)
    mlflow.set_tag("Passed Tests", passed)
    for failedTest in failedTests:
      mlflow.set_tag(failedTest[6], failedTest)
    mlflow.log_metric("Test Count", totalTestCount)
    mlflow.log_metric("Passed Percentage", passPercentage)
    for status in validationStatus:
      mlflow.log_metric(status[0], status[1])


# COMMAND ----------

try:
  validationRun(testResults, p, experimentName)
except Exception as e:
  err = {
    "sourceName": "Validate Pipeline: Gather Results and Log to MLFlow",
    "errorCode": "400",
    "errorDescription": e.__class__.__name__
  }
  error = json.dumps(err)
  log_notebook_error(notebookLogGuid, error, server, database, login, pwd)
  raise(e)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Log Completion

# COMMAND ----------

log_notebook_end(notebookLogGuid, 0, server, database, login, pwd)
dbutils.notebook.exit("Completed")