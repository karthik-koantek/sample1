# Databricks notebook source
# MAGIC %md # Metadata Review

# COMMAND ----------

# MAGIC %run "../../Orchestration/Orchestration Functions"

# COMMAND ----------

import datetime, json
stepLogGuid = "00000000-0000-0000-0000-000000000000"
stepKey = -1
context = dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson()
p = {
  "stepLogGuid": stepLogGuid,
  "stepKey": stepKey
}
parameters = json.dumps(p)
notebookLogGuid = str(uuid.uuid4())
log_notebook_start(notebookLogGuid, stepLogGuid, stepKey, parameters, context, server, database, login, pwd)

# COMMAND ----------

display(get_all_projects)

# COMMAND ----------

display(get_all_systems)

# COMMAND ----------

display(get_all_stages)

# COMMAND ----------

display(get_all_jobs)

# COMMAND ----------

display(get_all_parameters)

# COMMAND ----------

log_notebook_end(notebookLogGuid, server, database, login, pwd)
dbutils.notebook.exit("Succeeded")