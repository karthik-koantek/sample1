# Databricks notebook source
# MAGIC %md # Generate Metadata for Gold Zone
# MAGIC
# MAGIC Generates Framework Hydration Script for Gold Zone.
# MAGIC
# MAGIC #### Usage
# MAGIC
# MAGIC #### Prerequisites
# MAGIC
# MAGIC #### Details

# COMMAND ----------

dbutils.widgets.text(name="projectName", defaultValue="", label="Project")
dbutils.widgets.text(name="systemName", defaultValue="", label="System")

project = dbutils.widgets.get("projectName")
system = dbutils.widgets.get("systemName")
systemSecretScope = "internal"
stageName = "{0}_Daily".format(system)
goldZoneNotebookPath = "../Data Engineering/Gold Zone/Power BI File"

# COMMAND ----------

allTables = spark.catalog.listTables()
tables = [t.name for t in allTables if '_upsert' not in t.name]
tables

# COMMAND ----------

for table in tables:
  sql = """EXEC [dbo].[HydrateGoldZone]
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
  ,@TableName = '{4}'
  ,@CleansePath = 1
  ,@RenameFile = 1
  ,@GoldZoneNotebookPath = '{5}';
  """.format(project, system, systemSecretScope, stageName, table, goldZoneNotebookPath)
  #print(sql)
  print(sql.replace("\r","").replace("\n",""))