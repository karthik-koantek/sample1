# Databricks notebook source
# MAGIC %md
# MAGIC # Import Data Quality Rules
# MAGIC 
# MAGIC Writes active data quality rules to Gold Zone export path as JSON file so that it can be exported for source control.

# COMMAND ----------

# MAGIC %run "../Data Quality Rules Engine/Create Data Quality Rules Schema"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Initialize

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

# MAGIC %md
# MAGIC #### Validation

# COMMAND ----------

path = "/FileStore/import-stage/goldprotected.vActiveDataQualityRule.json"
try:
  dbutils.fs.ls(path)
except Exception as e:
  dbutils.notebook.exit("Exiting, new data quality rules not found.")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Read Data Quality Rules from Staging
# MAGIC 
# MAGIC Data Quality rules may have been formatted and edited in source control, so attempt to read with both multiLine options.

# COMMAND ----------

rulesStagingDF = []
rulesStagingDF.append(spark.read.option("multiLine", False).json(path))
if "_corrupt_record" in rulesStagingDF[-1].columns:
  rulesStagingDF.append(spark.read.option("multiLine", True).json(path))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Load New Data Quality Tests into Environment

# COMMAND ----------

try:
  rulesStagingDF.append(rulesStagingDF[-1].withColumn("effectiveDate", lit(current_timestamp())))
  dropCols = ["effectiveStartDate", "effectiveEndDate"]
  rulesStagingDF[-1].drop(*dropCols)
  rulesStagingDF[-1].createOrReplaceTempView("rulesStaging")
  mergeDataQualityRuleType2("rulesStaging")
except Exception as e:
  raise(e)
