# Databricks notebook source
# MAGIC %md # Generate Metadata for Presentation Zone

# COMMAND ----------

dbutils.widgets.text(name="projectName", defaultValue="", label="Project")
dbutils.widgets.text(name="systemName", defaultValue="", label="System")
dbutils.widgets.text(name="destinationSchemaName", defaultValue="dbo", label="Destination Schema Name")

project = dbutils.widgets.get("projectName")
system = dbutils.widgets.get("systemName")
destinationSchemaName = dbutils.widgets.get("destinationSchemaName")

systemSecretScope = "internal"
stageName = "{0}_Daily".format(system)
presentationZoneNotebookPath = "../Data Engineering/Presentation Zone/SQL Spark Connector"

# COMMAND ----------

allTables = spark.catalog.listTables()
upsertTables = [t.name for t in allTables if '_upsert' in t.name]
upsertTables

# COMMAND ----------

for table in upsertTables:
  sql = """EXEC [dbo].[HydratePresentationZone]
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
  ,@DestinationSchemaName = '{5}'
  ,@StoredProcedureName = ''
  ,@SaveMode = 'overwrite'
  ,@BulkCopyBatchSize = '2500'
  ,@BulkCopyTableLock = 'true'
  ,@BulkCopyTimeout = '600'
  ,@PresentationZoneNotebookPath = '{6}';
  """.format(project, system, systemSecretScope, stageName, table, destinationSchemaName, presentationZoneNotebookPath)
  #print(sql)
  print(sql.replace("\r","").replace("\n",""))