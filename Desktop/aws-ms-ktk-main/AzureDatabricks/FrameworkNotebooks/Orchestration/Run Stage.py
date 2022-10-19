# Databricks notebook source
# MAGIC %md # Run Stage

# COMMAND ----------

# MAGIC %md #### Initialize

# COMMAND ----------

# MAGIC %run "../Orchestration/Orchestration Functions"

# COMMAND ----------

import datetime, json
from multiprocessing.pool import ThreadPool

dbutils.widgets.text(name="systemLogGuid", defaultValue="00000000-0000-0000-0000-000000000000", label="systemLogGuid")
dbutils.widgets.text(name="stageName", defaultValue="", label="Stage Name")
dbutils.widgets.text(name="stageKey", defaultValue="-1", label="Stage Key")
dbutils.widgets.text(name="dateToProcess", defaultValue="", label="Date to Process")
dbutils.widgets.text(name="threadPool", defaultValue="1", label="Thread Pool")
dbutils.widgets.text(name="timeoutSeconds", defaultValue="1800", label="Timeout Seconds")

systemLogGuid = dbutils.widgets.get("systemLogGuid")
stageName = dbutils.widgets.get("stageName")
stageKey = dbutils.widgets.get("stageKey")
dateToProcess = dbutils.widgets.get("dateToProcess")
if dateToProcess == "":
  dateToProcess = datetime.datetime.utcnow().strftime('%Y/%m/%d')
threadPool = int(dbutils.widgets.get("threadPool"))
timeout = int(dbutils.widgets.get("timeoutSeconds"))

context = dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson()
p = {
  "systemLogGuid": systemLogGuid,
  "stageName": stageName,
  "stageKey": stageKey,
  "dateToProcess": dateToProcess,
  "threadPool": threadPool,
  "timeout": timeout
}
parameters = json.dumps(p)

stageLogGuid = str(uuid.uuid4())
log_stage_start(stageLogGuid, systemLogGuid, stageKey, parameters, context, server, database, login, pwd)

print("Stage Log Guid: {0}".format(stageLogGuid))
print("System Log Guid: {0}".format(systemLogGuid))
print("Context: {0}".format(context))
print("Parameters: {0}".format(parameters))

# COMMAND ----------

# MAGIC %md #### Get Jobs

# COMMAND ----------

try:
  jobs = get_stage(stageName, server, database, login, pwd).collect()
  print(jobs)
except Exception as e:
  err = {
    "sourceName": "Run Stage: Get Jobs",
    "errorCode": 101,
    "errorDescription": e.__class__.__name__
  }
  error = json.dumps(err)
  log_stage_error(stageLogGuid, error, stageKey, server, database, login, pwd)
  raise e

# COMMAND ----------

# MAGIC %md
# MAGIC #### Run Jobs

# COMMAND ----------

notebook = "../Orchestration/Run Job"
pool = ThreadPool(threadPool)
argsList = []

for row in jobs:
  stageKey = row[0]
  jobKey = row[3]
  args = {
    "stageLogGuid": stageLogGuid,
    "stageKey": stageKey,
    "jobName": row[4],
    "jobKey": jobKey,
    "dateToProcess": dateToProcess,
    "timeoutSeconds": timeout
  }
  if threadPool == 1:
    runWithRetryLogExceptions(notebook, timeout, args, max_retries=0)
  else:
    argsList.append(args)

if threadPool != 1:
  pool.map(lambda args: runWithRetryLogExceptions(notebook, timeout, args, max_retries=0), argsList)

# COMMAND ----------

# MAGIC %md #### Log Completed

# COMMAND ----------

log_stage_end(stageLogGuid, stageKey, server, database, login, pwd)
reset_stage(stageKey, server, database, login, pwd)
dbutils.notebook.exit("Succeeded")