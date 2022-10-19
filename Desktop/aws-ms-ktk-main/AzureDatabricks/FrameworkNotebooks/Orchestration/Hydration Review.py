# Databricks notebook source
# MAGIC %md # Metadata Review

# COMMAND ----------

import ktk
from pyspark.sql.functions import col, create_map, lit, col, crc32, concat
from itertools import chain
import json

onb = ktk.OrchestrationDeltaNotebook()
onb.displayAttributes()

# COMMAND ----------

#display(spark.sql("SELECT * FROM silverprotected.project"))
display(spark.sql("SELECT * FROM bronze.parameter_uat_sql"))

# COMMAND ----------

# MAGIC %sql
# MAGIC USE bronze;
# MAGIC SHOW TABLES

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
