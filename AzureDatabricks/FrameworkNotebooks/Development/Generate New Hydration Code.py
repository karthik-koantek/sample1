# Databricks notebook source
dbutils.widgets.text(name="Project", defaultValue="", label="Project Name")
dbutils.widgets.text(name="System", defaultValue="SanctionedZone", label="System Name")
dbutils.widgets.text(name="Stage", defaultValue="SanctionedZone_Daily", label="Stage Name")
dbutils.widgets.text(name="Tables", defaultValue="", label="Tables")
dbutils.widgets.text(name="Schema", defaultValue="staging", label="Schema Name")

project = dbutils.widgets.get("Project")
system = dbutils.widgets.get("System")
stage = dbutils.widgets.get("Stage")
tables = str(dbutils.widgets.get("Tables")).split()
schema = dbutils.widgets.get("Schema")

# COMMAND ----------

sanctioned_query = '''EXEC dbo.HydrateSanctionedZone @ProjectName = '{}', @SystemName = '{}', @SystemSecretScope = 'internal', @SystemIsActive = 1, @SystemOrder = 30, @StageName = '{}', @StageIsActive = 1, @StageOrder = 10,@JobName = '{}',@JobOrder = 10, @JobIsActive = 1, @Tablename = '{}',@DestinationSchemaName = '{}', @StoredProcedureName = '{}', @BulkCopyBatchSize = '2500', @BulkCopyTableLock = 'true', @BulkCopyTimeout = '600', @SanctionedZoneNotebookPath = '../DataEngineering/SanctionedZone/SQL Spark Connector';'''


# COMMAND ----------

from functools import reduce
if system == 'SanctionedZoneProcessing':
  out = reduce((lambda curr, next: curr+'\n'+next),[sanctioned_query.format(project,system,stage,system+'_'+table,'p'+table,schema,schema+'.usp_Load'+table) for table in tables])

# COMMAND ----------

print(out)

# COMMAND ----------

