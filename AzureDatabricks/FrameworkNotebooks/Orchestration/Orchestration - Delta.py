# Databricks notebook source
# MAGIC %md # Orchestration - Delta
# MAGIC 
# MAGIC Uses Delta Tables for configuration (not a configuration database e.g. Azure SQL DB, RDS etc.).  

# COMMAND ----------

# MAGIC %md #### Initialize

# COMMAND ----------

import ktk
import datetime, json
from multiprocessing.pool import ThreadPool
import sys, os

# COMMAND ----------

dbutils.widgets.text(name="projectName", defaultValue="", label="Project Name")
dbutils.widgets.text(name="dateToProcess", defaultValue="", label="Date to Process")
dbutils.widgets.text(name="threadPool", defaultValue="1", label="Thread Pool")
dbutils.widgets.text(name="timeoutSeconds", defaultValue="1800", label="Timeout Seconds")
dbutils.widgets.text(name="whatIf", defaultValue="0", label="What If")

# COMMAND ----------

onb = ktk.OrchestrationDeltaNotebook()
onb.displayAttributes()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Validate Inputs

# COMMAND ----------

if onb.projectName == "":
  dbutils.notebook.exit("Project Name must be supplied")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create Orchestration Schema

# COMMAND ----------

try:
  onb.validate_orchestration_schema()
except Exception as e:
  onb.create_orchestration_schema()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Refresh Hydration
# MAGIC * not sure of the rules yet, but refresh hydration for the supplied project 
# MAGIC * delete existing project (onb.delete_project_from_configuration)
# MAGIC * load project from source control (e.g. /Workspace/Repos/edward.edgeworth@koantek.com/december/ApplicationConfiguration/OrchestrationMetadata/)
# MAGIC * from UAT_SQL import hydrate
# MAGIC * hydrate()

# COMMAND ----------

sys.path.append(os.path.abspath("/Workspace/Repos/edward.edgeworth@koantek.com/december/ApplicationConfiguration/OrchestrationMetadata/"))

# COMMAND ----------

from uat_sql import hydrate

# COMMAND ----------

onb.delete_project_from_configuration("UAT_SQL")

# COMMAND ----------

hydrate("UAT_SQL")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Display Project Configuration

# COMMAND ----------

onb.get_project_configuration(project_name="UAT_SQL", system_is_active=True, system_is_restart=True, stage_is_active=True, stage_is_restart=True, job_is_active=True, job_is_restart=True, step_is_active=True, step_is_restart=True)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Run Databricks Pipeline

# COMMAND ----------

onb.run_project()
