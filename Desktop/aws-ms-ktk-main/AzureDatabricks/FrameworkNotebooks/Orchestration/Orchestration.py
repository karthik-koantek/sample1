# Databricks notebook source
# MAGIC %md # Orchestration

# COMMAND ----------

# MAGIC %md #### Initialize

# COMMAND ----------

import ktk
import datetime, json
from multiprocessing.pool import ThreadPool

# COMMAND ----------

dbutils.widgets.text(name="ADFProjectName", defaultValue="", label="ADF Project Name")
dbutils.widgets.text(name="projectName", defaultValue="", label="Project Name")
dbutils.widgets.text(name="dateToProcess", defaultValue="", label="Date to Process")
dbutils.widgets.text(name="threadPool", defaultValue="1", label="Thread Pool")
dbutils.widgets.text(name="timeoutSeconds", defaultValue="1800", label="Timeout Seconds")
dbutils.widgets.text(name="whatIf", defaultValue="0", label="What If")

# COMMAND ----------

onb = ktk.OrchestrationNotebook()
onb.displayAttributes()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Validate Inputs

# COMMAND ----------

if onb.projectName == "":
  dbutils.notebook.exit("Project Name must be supplied")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Run ADF Pipeline (if exists)

# COMMAND ----------

if onb.ADFProjectName != "":
  args = {
    "ADFProjectName": onb.ADFProjectName,
    "dateToProcess": onb.dateToProcess
  }
  notebook = "../Orchestration/Run ADF Project"
  if onb.whatIf == "0":
    result = onb.runWithRetryLogExceptions(notebook, args, max_retries=0)
    if result != "Succeeded":
      raise ValueError("ADF Pipeline Failed.")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Run Databricks Pipeline

# COMMAND ----------

onb.run_project()
