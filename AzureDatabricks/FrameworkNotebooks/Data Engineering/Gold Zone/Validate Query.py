# Databricks notebook source
# MAGIC %md # Validate Query

# COMMAND ----------

# MAGIC %run "../../Orchestration/Notebook Functions"

# COMMAND ----------

# MAGIC %run ../../Development/Utilities

# COMMAND ----------

# MAGIC %md
# MAGIC #### Initialize

# COMMAND ----------

import ktk
from ktk import utilities as u
import datetime, json
from pyspark.sql.functions import current_timestamp, lit, unix_timestamp
from pyspark.sql.types import TimestampType, BooleanType
import uuid

# COMMAND ----------

dbutils.widgets.text(name="stepLogGuid", defaultValue="00000000-0000-0000-0000-000000000000", label="stepLogGuid")
dbutils.widgets.text(name="stepKey", defaultValue="-1", label="stepKey")
dbutils.widgets.text(name="validationKey", defaultValue="-1", label="validationKey")
dbutils.widgets.text(name="dataLakeZone", defaultValue="", label="Data Lake Zone")
dbutils.widgets.text(name="query", defaultValue="", label="Query")
dbutils.widgets.text(name="expectedColumns", defaultValue="", label="Columns")
dbutils.widgets.text(name="expectedRowCount", defaultValue="0", label="Minimum Expected Row Count")

widgets = ["stepLogGuid", "stepKey","validationKey","dataLakeZone","query","expectedColumns","expectedRowCount"]
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
# MAGIC #### Object Exists

# COMMAND ----------

dfList = []
#exists = sparkTableExists(tableOrViewName)
#if exists != True:
#  errorDescription = "Object {0} does not exist.".format(tableOrViewName)
#  err = {
#      "sourceName": "Validate: Object Exists",
#      "errorCode": "100",
#      "errorDescription": errorDescription
#  }
#  validationStatus = "Object not found"
#  error = json.dumps(err)
#  log_notebook_error(notebookLogGuid, error, server, database, login, pwd)
#  log_validationlog(notebookLogGuid, stepLogGuid, validationKey, validationStatus, error, parameters, server, database, login, pwd)
#  log_notebook_end(notebookLogGuid, 0, server, database, login, pwd)
#  dbutils.notebook.exit("Failed: Object does not exist")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Columns Match Expected

# COMMAND ----------

#if expectedColumns != "":
#  expectedColumnsList = expectedColumns.split(",")
#  expectedColumnsList.sort()
#  expectedColumnsSorted = ",".join(expectedColumnsList)
#  print("Expected: " + expectedColumnsSorted)
#  sql = "SELECT * FROM {0} WHERE 1=0".format(tableOrViewName)
#  dfList.append(spark.sql(sql))
#  actualColumnsList = dfList[-1].columns
#  actualColumnsList.sort()
#  actualColumnsSorted = ",".join(actualColumnsList)
#  print("Actual:   " + actualColumnsSorted)
#else:
#  print("No expected columns supplied, skipping columns check.")

# COMMAND ----------

#if expectedColumns != "":
#  if set(actualColumnsList) != set(expectedColumnsList):
#    errorDescription = "Object {0} has missing or unexpected columns.".format(tableOrViewName)
#    err = {
#        "sourceName": "Validate: Columns Match Expected",
#        "errorCode": "200",
#        "errorDescription": errorDescription,
#        "expectedColumns": expectedColumnsSorted,
#        "actualColumns": actualColumnsSorted
#    }
#    validationStatus = "Columns do not match expected"
#    error = json.dumps(err)
#    log_notebook_error(notebookLogGuid, error, server, database, login, pwd)
#    log_validationlog(notebookLogGuid, stepLogGuid, validationKey, validationStatus, error, parameters, server, database, login, pwd)
#    log_notebook_end(notebookLogGuid, 0, server, database, login, pwd)
#    dbutils.notebook.exit("Failed: Columns do not match expected")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Recent Data Validation

# COMMAND ----------

#if objectType == "table" and (expectedNewOrModifiedRows2Days > 0 or expectedNewOrModifiedRows6Days > 0):
#  currentTable = spark.table(tableOrViewName)
#  delta2DaysMinutes = 24 * 60 * 2
#  delta6DaysMinutes = 24 * 60 * 6
#  errorDescription = None
#  try:
#    if expectedNewOrModifiedRows2Days > 0:
#      dfList.append(getTableChangeDelta(currentTable, tableOrViewName, delta2DaysMinutes))
#      actualNewOrModifiedRows2Days = dfList[-1].count()
#      print("Actual New/Modified Rows the past 2 days: {0}".format(actualNewOrModifiedRows2Days))
#      if actualNewOrModifiedRows2Days < expectedNewOrModifiedRows2Days:
#        errorDescription = "Object {0} has {1} new or modified rows in the last 2 days, less than the expected {2}.".format(tableOrViewName, actualNewOrModifiedRows2Days, expectedNewOrModifiedRows2Days)
#        err = {
#          "sourceName": "Validate: Recent Data Validation",
#          "errorCode": "300",
#          "errorDescription": errorDescription
#        }
#        validationStatus = "Recent Data Validation failed"
#        error = json.dumps(err)
#        log_notebook_error(notebookLogGuid, error, server, database, login, pwd)
#    if expectedNewOrModifiedRows6Days > 0:
#      dfList.append(getTableChangeDelta(currentTable, tableOrViewName, delta6DaysMinutes))
#      actualNewOrModifiedRows6Days = dfList[-1].count()
#      print("Actual New/Modified Rows the past 6 days: {0}".format(actualNewOrModifiedRows6Days))
#      if actualNewOrModifiedRows6Days < expectedNewOrModifiedRows6Days:
#        errorDescription = "Object {0} has {1} new or modified rows in the last 6 days, less than the expected {2}.".format(tableOrViewName, actualNewOrModifiedRows6Days, expectedNewOrModifiedRows6Days)
#        err = {
#          "sourceName": "Validate: Recent Data Validation",
#          "errorCode": "300",
#          "errorDescription": errorDescription
#        }
#        validationStatus = "Recent Data Validation failed"
#        error = json.dumps(err)
#        log_notebook_error(notebookLogGuid, error, server, database, login, pwd)
#    if errorDescription != None:
#      print(errorDescription)
#      log_validationlog(notebookLogGuid, stepLogGuid, validationKey, validationStatus, error, parameters, server, database, login, pwd)
#      log_notebook_end(notebookLogGuid, 0, server, database, login, pwd)
#      dbutils.notebook.exit("Failed: Recent Data Validation.")
#  except Exception as e:
#    err = {
#      "sourceName" : "Validate: An error occurred during recent data validation.",
#      "errorCode" : "400",
#      "errorDescription" : e.__class__.__name__
#    }
#    error = json.dumps(err)
#    log_notebook_error(notebookLogGuid, error, server, database, login, pwd)
#    raise(e)
#    dbutils.notebook.exit("Failed: An error occurred during Recent Data Validation.")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Log Completion

# COMMAND ----------

validationStatus = "Passed"
snb.log_validationlog(snb.notebookLogGuid, snb.stepLogGuid, snb.validationKey, validationStatus, '', parameters)
snb.log_notebook_end(0)
dbutils.notebook.exit("Passed")