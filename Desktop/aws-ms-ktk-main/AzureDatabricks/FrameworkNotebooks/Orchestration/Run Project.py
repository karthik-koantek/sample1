# Databricks notebook source
# MAGIC %md # Run Project

# COMMAND ----------

# MAGIC %md #### Initialize

# COMMAND ----------

# MAGIC %run "../Orchestration/Orchestration Functions"

# COMMAND ----------

import datetime, json

dbutils.widgets.text(name="projectName", defaultValue="", label="Project Name")
dbutils.widgets.text(name="dateToProcess", defaultValue="", label="Date to Process")
dbutils.widgets.text(name="threadPool", defaultValue="1", label="Thread Pool")
dbutils.widgets.text(name="timeoutSeconds", defaultValue="1800", label="Timeout Seconds")

projectName = dbutils.widgets.get("projectName")
dateToProcess = dbutils.widgets.get("dateToProcess")
if dateToProcess == "":
  dateToProcess = datetime.datetime.utcnow().strftime('%Y/%m/%d')
threadPool = int(dbutils.widgets.get("threadPool"))
timeout = int(dbutils.widgets.get("timeoutSeconds"))

context = dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson()
p = {
  "projectName": projectName,
  "dateToProcess": dateToProcess,
  "threadPool": threadPool,
  "timeout": timeout
}
parameters = json.dumps(p)

projectLogGuid = str(uuid.uuid4())
log_project_start(projectLogGuid, parameters, context, server, database, login, pwd)

print("Project Log Guid: {0}".format(projectLogGuid))
print("Context: {0}".format(context))
print("Parameters: {0}".format(parameters))

# COMMAND ----------

# MAGIC %md #### Get External Systems

# COMMAND ----------

try:
  systems = get_project(projectName, server, database, login, pwd).collect()
  print(systems)
except Exception as e:
  err = {
    "sourceName": "Run Project: Get External Systems",
    "errorCode": 101,
    "errorDescription": e.__class__.__name__
  }
  error = json.dumps(err)
  log_project_error(projectLogGuid, error, server, database, login, pwd)
  raise e

# COMMAND ----------

# MAGIC %md
# MAGIC #### Run External Systems

# COMMAND ----------

for row in systems:
  projectKey = row[0]
  args = {
    "projectLogGuid": projectLogGuid,
    "projectKey": projectKey,
    "systemKey": row[2],
    "systemName": row[3],
    "timeoutSeconds": timeout,
    "dateToProcess": dateToProcess
  }
  notebook = "../Orchestration/Run System"
  runWithRetryLogExceptions(notebook, timeout, args, max_retries=0)

# COMMAND ----------

# MAGIC %md #### Log Completed

# COMMAND ----------

log_project_end(projectLogGuid, projectKey, server, database, login, pwd)
reset_project(projectKey, server, database, login, pwd)
dbutils.notebook.exit("Succeeded")