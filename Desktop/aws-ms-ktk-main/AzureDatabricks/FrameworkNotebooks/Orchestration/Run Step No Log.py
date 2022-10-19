# Databricks notebook source
# MAGIC %md # Run Step

# COMMAND ----------

# MAGIC %md #### Initialize

# COMMAND ----------

# MAGIC %run "../Orchestration/Accelerator Functions"

# COMMAND ----------

import datetime, json

#dbutils.widgets.text(name="jobLogGuid", defaultValue="00000000-0000-0000-0000-000000000000", label="jobLogGuid")
dbutils.widgets.text(name="stepName", defaultValue="", label="Step Name")
dbutils.widgets.text(name="stepKey", defaultValue="-1", label="Step Key")
dbutils.widgets.text(name="dateToProcess", defaultValue="", label="Date to Process")
dbutils.widgets.text(name="threadPool", defaultValue="1", label="Thread Pool")
dbutils.widgets.text(name="timeoutSeconds", defaultValue="1800", label="Timeout Seconds")

#jobLogGuid = dbutils.widgets.get("jobLogGuid")
stepName = dbutils.widgets.get("stepName")
stepKey = int(dbutils.widgets.get("stepKey"))
dateToProcess = dbutils.widgets.get("dateToProcess")
if dateToProcess == "":
  dateToProcess = datetime.datetime.utcnow().strftime('%Y-%m-%d')
threadPool = int(dbutils.widgets.get("threadPool"))
timeout = int(dbutils.widgets.get("timeoutSeconds"))

context = dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson()
p = {
  #"jobLogGuid": jobLogGuid,
  "stepName": stepName,
  "stepKey": stepKey,
  "dateToProcess": dateToProcess,
  "threadPool": threadPool,
  "timeout": timeout
}
parameters = json.dumps(p)

# stepLogGuid = str(uuid.uuid4())
# log_step_start(stepLogGuid, jobLogGuid, stepKey, parameters, context, server, database, login, pwd)

# print("Step Log Guid: {0}".format(stepLogGuid))
# print("Job Log Guid: {0}".format(jobLogGuid))
# print("Context: {0}".format(context))
# print("Parameters: {0}".format(parameters))

# COMMAND ----------

# MAGIC %md #### Get Parameters

# COMMAND ----------

try:
  parameters = get_parameters(stepKey, server, database, login, pwd).collect()
  print(parameters)
except Exception as e:
  err = {
    "sourceName": "Run Step: Get Parameters",
    "errorCode": 101,
    "errorDescription": e.__class__.__name__
  }
  error = json.dumps(err)
  log_step_error(stepLogGuid, stepKey, error, server, database, login, pwd)
  raise e

# COMMAND ----------

# MAGIC %md
# MAGIC #### Run Notebook

# COMMAND ----------

args = {
  # "stepLogGuid": stepLogGuid,
  "stepKey": stepKey,
  "dateToProcess": dateToProcess
}
for row in parameters:
  args[row[3]] = row[4]

print(args)
runWithRetry(stepName, timeout, args, max_retries=0)

# COMMAND ----------

# MAGIC %md #### Log Completed

# COMMAND ----------

# log_step_end(stepLogGuid, stepKey, server, database, login, pwd)
dbutils.notebook.exit("Succeeded")