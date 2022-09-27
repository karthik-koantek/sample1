# Databricks notebook source
# MAGIC %md
# MAGIC # Export Data Quality Rules
# MAGIC 
# MAGIC Writes active data quality rules to Gold Zone export path as JSON file so that it can be exported for source control.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Initialize

# COMMAND ----------

import ktk
from ktk import utilities as u

# COMMAND ----------

onb = ktk.OrchestrationNotebook()
onb.displayAttributes()

# COMMAND ----------

notebook = "../Data Engineering/Gold Zone/Power BI File"
argsList = []
args = {
  "tableName": "goldprotected.vActiveDataQualityRule",
  "fileFormat": "json",
  "destination": "goldprotected"
}
onb.runWithRetry(notebook, 1800, args, max_retries=0)

# COMMAND ----------

dbutils.fs.head(onb.GoldProtectedBasePath + "/export/goldprotected.vActiveDataQualityRule/goldprotected.vActiveDataQualityRule.json")
