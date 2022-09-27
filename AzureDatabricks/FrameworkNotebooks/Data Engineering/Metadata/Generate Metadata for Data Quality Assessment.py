# Databricks notebook source
# MAGIC %md # Generate Metadata for Data Quality Assessment
# MAGIC
# MAGIC Generates Framework Hydration Script for Data Quality Assessment Checks.
# MAGIC
# MAGIC #### Usage
# MAGIC
# MAGIC #### Prerequisites
# MAGIC
# MAGIC #### Details

# COMMAND ----------

dbutils.widgets.text(name="projectName", defaultValue="", label="Project")
dbutils.widgets.text(name="systemName", defaultValue="", label="System")
dbutils.widgets.dropdown(name="dataLakeZone", defaultValue="bronze", label="Data Lake Zone", choices=["bronze", "silver", "gold", "platinum"])

project = dbutils.widgets.get("projectName")
system = dbutils.widgets.get("systemName")
dataLakeZone = dbutils.widgets.get("dataLakeZone")
systemSecretScope = "internal"
stageName = "{0}_QC".format(system)
goldZoneNotebookPath = "../Data Quality Rules Engine/Data Quality Assessment"

# COMMAND ----------

tables = [table[0] for table in spark.sql("""
SELECT fullyQualifiedTableName
FROM goldprotected.vactivedataqualityrule
WHERE fullyQualifiedTableName LIKE '%{0}%'
""".format(dataLakeZone)).collect()]

# COMMAND ----------

tables

# COMMAND ----------

for table in tables:
  jobName = "dataqualityassessment_{0}".format(table.replace(".","_"))
  sql = """EXEC [dbo].[HydrateDataQualityAssessment]
   @ProjectName = '{0}'
  ,@SystemName = '{1}'
  ,@SystemSecretScope = '{2}'
  ,@SystemIsActive = 1
  ,@SystemOrder = 10
  ,@StageName = '{3}'
  ,@StageIsActive = 1
  ,@StageOrder = 10
  ,@JobName = '{4}'
  ,@JobIsActive = 1
  ,@JobOrder = 10
  ,@TableName = '{6}'
  ,@DeltaHistoryMinutes = '-1'
  ,@NotebookPath = '{5}';
  """.format(project, system, systemSecretScope, stageName, jobName, goldZoneNotebookPath, table)
  #print(sql)
  print(sql.replace("\r","").replace("\n",""))