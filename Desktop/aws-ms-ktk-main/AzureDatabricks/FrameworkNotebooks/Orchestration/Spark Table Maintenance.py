# Databricks notebook source
# MAGIC %md # Spark Table Maintenance

# COMMAND ----------

# MAGIC %md
# MAGIC #### Initialize

# COMMAND ----------

# MAGIC %run "../Orchestration/Notebook Functions"

# COMMAND ----------

# MAGIC %run ../Development/Utilities

# COMMAND ----------

import datetime, json

dbutils.widgets.text(name="stepLogGuid", defaultValue="00000000-0000-0000-0000-000000000000", label="stepLogGuid")
dbutils.widgets.text(name="stepKey", defaultValue="-1", label="stepKey")
dbutils.widgets.text(name="tableName", defaultValue="", label="Table Name")
dbutils.widgets.text(name="runOptimize", defaultValue="1", label="Optimize")
dbutils.widgets.text(name="runVacuum", defaultValue="1", label="Vacuum")
dbutils.widgets.text(name="optimizeWhere", defaultValue="", label="Optimize Where")
dbutils.widgets.text(name="optimizeZOrderBy", defaultValue="", label="Optimize Z Order By")
dbutils.widgets.text(name="vacuumRetentionHours", defaultValue="168", label="Vacuum Retention Hours")

stepLogGuid = dbutils.widgets.get("stepLogGuid")
stepKey = int(dbutils.widgets.get("stepKey"))
tableName = dbutils.widgets.get("tableName")
runOptimize = dbutils.widgets.get("runOptimize")
runVacuum = dbutils.widgets.get("runVacuum")
optimizeWhere = dbutils.widgets.get("optimizeWhere")
optimizeZOrderBy = dbutils.widgets.get("optimizeZOrderBy")
vacuumRetentionHours = dbutils.widgets.get("vacuumRetentionHours")

context = dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson()
p = {
  "stepLogGuid": stepLogGuid,
  "stepKey": stepKey,
  "tableName": tableName,
  "runOptimize": runOptimize,
  "runVacuum": runVacuum,
  "optimizeWhere": optimizeWhere,
  "optimizeZOrderBy": optimizeZOrderBy,
  "vacuumRetentionHours": vacuumRetentionHours
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
# MAGIC #### Get Table

# COMMAND ----------

refreshed = refreshTable(tableName)
if refreshed == False:
  log_notebook_end(notebookLogGuid, 0, server, database, login, pwd)
  dbutils.notebook.exit("Table does not exist")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Optimize

# COMMAND ----------

if runOptimize == "1":
  try:
    optimize(tableName, optimizeWhere, optimizeZOrderBy)
  except Exception as e:
    err = {
      "sourceName": "Spark Table Maintenance: Optimize",
      "errorCode": "100",
      "errorDescription": e.__class__.__name__
    }
    error = json.dumps(err)
    log_notebook_error(notebookLogGuid, error, server, database, login, pwd)
    raise(e)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Vacuum

# COMMAND ----------

if runVacuum == "1":
  try:
    vacuum(tableName, vacuumRetentionHours)
  except Exception as e:
    err = {
      "sourceName": "Spark Table Maintenance: Vacuum",
      "errorCode": "200",
      "errorDescription": e.__class__.__name__
    }
    error = json.dumps(err)
    log_notebook_error(notebookLogGuid, error, server, database, login, pwd)
    raise(e)

# COMMAND ----------

log_notebook_end(notebookLogGuid, 0, server, database, login, pwd)
dbutils.notebook.exit("Succeeded")