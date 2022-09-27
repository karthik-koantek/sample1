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
dbutils.widgets.text(name="repoPath", defaultValue="/Workspace/Repos/Production/Koantek", label="Repo Path")
dbutils.widgets.dropdown(name="hydrationBehavior", label="Hydration Behavior", defaultValue="None", choices=["Force", "None", "IfSuccessful"])

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

# COMMAND ----------

if onb.hydrationBehavior != "None":
  hydrationPath = "{0}/ApplicationConfiguration/OrchestrationMetadata/".format(onb.repoPath)
  print("Appending hydrationPath: " + hydrationPath + " ...") 
  sys.path.append(os.path.abspath(hydrationPath))
  import_command = "from {0} import *".format(onb.projectName)
  print("Executing Import Hydration (" + import_command + ") ...")
  exec(import_command)
  if onb.hydrationBehavior == "Force":
    print("Hydration Behavior: Force")
    print("Deleting Project " + onb.projectName + " from configuration...")
    onb.delete_project_from_configuration(onb.projectName)
    print("Running hydration for project " + onb.projectName + "...")
    hydrate(onb.projectName)
  elif onb.hydrationBehavior == "IfSuccessful":
    print("Hydration Behavior: IsSuccessful")
    print("Getting last run status for project...")
    status = onb.getLastRunStatusForProject(onb.projectName)
    if status == 2:
      print("Last project run was successful, deleting project from configuration...")
      onb.delete_project_from_configuration(onb.projectName)
      print("Running hydration for project " + onb.projectName + "...")
      hydrate(onb.projectName)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Configuration Review

# COMMAND ----------

configuration_review = onb.get_project_configuration(
  project_name=onb.projectName, 
  system_is_active=True, 
  system_is_restart=True, 
  stage_is_active=True, 
  stage_is_restart=True, 
  job_is_active=True, 
  job_is_restart=True, 
  step_is_active=True, 
  step_is_restart=True)

display(configuration_review)

if configuration_review.count() == 0:
    dbutils.notebook.exit("Project Configuration does not exist")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Run Databricks Pipeline

# COMMAND ----------

onb.run_project()
