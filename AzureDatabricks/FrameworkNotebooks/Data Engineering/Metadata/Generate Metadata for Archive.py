# Databricks notebook source
# MAGIC %md # Generate Metadata for Archive
# MAGIC
# MAGIC Generates Framework Hydration Script for Archive.
# MAGIC
# MAGIC #### Usage
# MAGIC
# MAGIC
# MAGIC #### Prerequisites
# MAGIC
# MAGIC #### Details

# COMMAND ----------

# MAGIC %run ../../Development/Utilities

# COMMAND ----------

dbutils.widgets.text(name="projectName", defaultValue="", label="Project")
dbutils.widgets.text(name="systemName", defaultValue="", label="System")
dbutils.widgets.dropdown(name="databaseCatalogName", defaultValue="default", choices=["default", "archive", "silverprotected", "silvergeneral", "goldprotected", "goldgeneral", "sandbox"], label="Database")

project = dbutils.widgets.get("projectName")
system = dbutils.widgets.get("systemName")
database = dbutils.widgets.get("databaseCatalogName")
systemSecretScope = "internal"
stageName = "{0}_Daily".format(system)
NotebookPath = "../Data Engineering/Sandbox Zone/Clone Table"

# COMMAND ----------

allTables

# COMMAND ----------

allTables = spark.catalog.listTables(dbName=database)
tables = [t.name for t in allTables if '_upsert' not in t.name and t.tableType != 'VIEW' and t.isTemporary==False]
tables

# COMMAND ----------

len(tables)

# COMMAND ----------

tables

# COMMAND ----------

for table in tables:
  if table > 'a' and table < 'z':
    sql = """EXEC [dbo].[HydrateClone]
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
    ,@SourceTableName = '{6}.{4}'
    ,@Destination = 'archive'
    ,@DestinationRelativePath = '{4}'
    ,@DestinationTableName = '{4}'
    ,@AppendTodaysDateToTableName = 'True'
    ,@OverwriteTable = 'True'
    ,@DestinationTableComment = 'Archive backup of {4}'
    ,@CloneType = 'DEEP'
    ,@TimeTravelTimestampExpression = ''
    ,@TimeTravelVersionExpression = ''
    ,@LogRetentionDurationDays = '7'
    ,@DeletedFileRetentionDurationDays = '7'
    ,@NotebookPath = '{5}';
    """.format(project, system, systemSecretScope, stageName, table, NotebookPath, database)
    print(table)
    #print(sql)
    #print(sql.replace("\r","").replace("\n",""))