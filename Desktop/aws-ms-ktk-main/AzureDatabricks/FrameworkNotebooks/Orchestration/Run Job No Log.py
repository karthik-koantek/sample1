# Databricks notebook source
# MAGIC %md # Run Job

# COMMAND ----------

# MAGIC %md #### Initialize

# COMMAND ----------

# MAGIC %run "../Orchestration/Accelerator Functions"

# COMMAND ----------

import datetime, json

dbutils.widgets.text(name="stageLogGuid", defaultValue="00000000-0000-0000-0000-000000000000", label="stageLogGuid")
dbutils.widgets.text(name="jobName", defaultValue="", label="Job Name")
dbutils.widgets.text(name="jobKey", defaultValue="-1", label="Job Key")
dbutils.widgets.text(name="dateToProcess", defaultValue="", label="Date to Process")
dbutils.widgets.text(name="threadPool", defaultValue="1", label="Thread Pool")
dbutils.widgets.text(name="timeoutSeconds", defaultValue="1800", label="Timeout Seconds")

stageLogGuid = dbutils.widgets.get("stageLogGuid")
jobName = dbutils.widgets.get("jobName")
jobKey = dbutils.widgets.get("jobKey")
dateToProcess = dbutils.widgets.get("dateToProcess")
if dateToProcess == "":
  dateToProcess = datetime.datetime.utcnow().strftime('%Y-%m-%d')
threadPool = int(dbutils.widgets.get("threadPool"))
timeout = int(dbutils.widgets.get("timeoutSeconds"))

context = dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson()
# p = {
#   "stageLogGuid": stageLogGuid,
#   "jobName": jobName,
#   "jobKey": jobKey,
#   "dateToProcess": dateToProcess,
#   "threadPool": threadPool,
#   "timeout": timeout
# }
# parameters = json.dumps(p)

# jobLogGuid = str(uuid.uuid4())
# log_job_start(jobLogGuid, stageLogGuid, jobKey, parameters, context, server, database, login, pwd)

# print("Job Log Guid: {0}".format(jobLogGuid))
# print("Stage Log Guid: {0}".format(stageLogGuid))
# print("Context: {0}".format(context))
# print("Parameters: {0}".format(parameters))

# COMMAND ----------

# MAGIC %md #### Get Steps

# COMMAND ----------

try:
  steps = get_job(jobName, jobKey, server, database, login, pwd).collect()
  print(steps)
except Exception as e:
  err = {
    "sourceName": "Run Job: Get Steps",
    "errorCode": 101,
    "errorDescription": e.__class__.__name__
  }
  error = json.dumps(err)
#   log_job_error(jobLogGuid, error, jobKey, server, database, login, pwd)
  raise e

# COMMAND ----------

# MAGIC %md
# MAGIC #### Run Steps

# COMMAND ----------


for row in steps:
  jobKey = row[0]
  stepKey = row[3]
  args = {
    # "jobLogGuid": jobLogGuid,
    "stepName": row[4],
    "stepKey": stepKey,
    "dateToProcess": dateToProcess,
    "timeoutSeconds": timeout
  }
  notebook = "../Orchestration/Run Step No Log"
  runWithRetry(notebook, timeout, args, max_retries=0)

# COMMAND ----------

# MAGIC %md #### Log Completed

# COMMAND ----------

# log_job_end(jobLogGuid, jobKey, server, database, login, pwd)
# reset_job(jobKey, server, database, login, pwd)
# dbutils.notebook.exit("Succeeded")