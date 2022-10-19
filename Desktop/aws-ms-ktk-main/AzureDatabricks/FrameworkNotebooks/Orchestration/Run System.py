# Databricks notebook source
# MAGIC %md # Run System

# COMMAND ----------

# MAGIC %md #### Initialize

# COMMAND ----------

# MAGIC %run "../Orchestration/Orchestration Functions"

# COMMAND ----------

import datetime, json

dbutils.widgets.text(name="projectLogGuid", defaultValue="00000000-0000-0000-0000-000000000000", label="projectLogGuid")
dbutils.widgets.text(name="systemName", defaultValue="", label="System Name")
dbutils.widgets.text(name="systemKey", defaultValue="-1", label="System Key")
dbutils.widgets.text(name="dateToProcess", defaultValue="", label="Date to Process")
dbutils.widgets.text(name="threadPool", defaultValue="1", label="Thread Pool")
dbutils.widgets.text(name="timeoutSeconds", defaultValue="1800", label="Timeout Seconds")

projectLogGuid = dbutils.widgets.get("projectLogGuid")
systemName = dbutils.widgets.get("systemName")
systemKey = int(dbutils.widgets.get("systemKey"))
dateToProcess = dbutils.widgets.get("dateToProcess")
if dateToProcess == "":
  dateToProcess = datetime.datetime.utcnow().strftime('%Y/%m/%d')
threadPool = int(dbutils.widgets.get("threadPool"))
timeout = int(dbutils.widgets.get("timeoutSeconds"))

context = dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson()
p = {
  "projectLogGuid": projectLogGuid,
  "systemName": systemName,
  "systemKey": systemKey,
  "dateToProcess": dateToProcess,
  "threadPool": threadPool,
  "timeout": timeout
}
parameters = json.dumps(p)

systemLogGuid = str(uuid.uuid4())
log_system_start(systemLogGuid, projectLogGuid, systemKey, parameters, context, server, database, login, pwd)

print("System Log Guid: {0}".format(systemLogGuid))
print("Project Log Guid: {0}".format(projectLogGuid))
print("Context: {0}".format(context))
print("Parameters: {0}".format(parameters))

# COMMAND ----------

# MAGIC %md #### Get Stages

# COMMAND ----------

try:
  stages = get_system(systemName, server, database, login, pwd).collect()
  print(stages)
except Exception as e:
  err = {
    "sourceName": "Run System: Get Stages",
    "errorCode": 101,
    "errorDescription": e.__class__.__name__
  }
  error = json.dumps(err)
  log_system_error(systemLogGuid, error, systemKey, server, database, login, pwd)
  raise e

# COMMAND ----------

# MAGIC %md
# MAGIC #### Run Stages

# COMMAND ----------

for row in stages:
  systemKey = row[0]
  stageKey = row[3]
  threadPool = row[6]
  args = {
    "systemLogGuid": systemLogGuid,
    "systemKey": systemKey,
    "stageName": row[4],
    "stageKey": stageKey,
    "dateToProcess": dateToProcess,
    "timeoutSeconds": timeout,
    "threadPool": threadPool
  }
  notebook = "../Orchestration/Run Stage"
  runWithRetryLogExceptions(notebook, timeout, args, max_retries=0)

# COMMAND ----------

# MAGIC %md #### Log Completed

# COMMAND ----------

log_system_end(systemLogGuid, systemKey, server, database, login, pwd)
reset_system(systemKey, server, database, login, pwd)
dbutils.notebook.exit("Succeeded")