# Databricks notebook source
# MAGIC %md # Generate Metadata for SQL Source
# MAGIC
# MAGIC Generates Framework Hydration Script for External SQL Source.
# MAGIC
# MAGIC #### Usage
# MAGIC Supply the parameter to the External System containing the secrets for the required connection parameters to the Source SQL Database
# MAGIC
# MAGIC #### Prerequisites
# MAGIC * The supplied externalSystem must have an associated secret scope (of the same name) within Databricks with the following secrets added:
# MAGIC   * SQLServerName
# MAGIC   * DatabaseName
# MAGIC   * Login
# MAGIC   * Pwd
# MAGIC
# MAGIC #### Details
# MAGIC * To create External System secret scope, run Powershell script /ApplicationConfiguration/Powershell/Scripts/ExternalSystemSecrets/CreateExternalSecretsForSQL.ps1

# COMMAND ----------

dbutils.widgets.text(name="projectName", defaultValue="", label="Project")
dbutils.widgets.text(name="externalSystem", defaultValue="", label="External System")
dbutils.widgets.text(name="externalSystemSecretScope", defaultValue="", label="Secret Scope")

project = dbutils.widgets.get("projectName")
externalSystem = dbutils.widgets.get("externalSystem")
externalSystemSecretScope = dbutils.widgets.get("externalSystemSecretScope")
stageName = "{0}_Daily".format(externalSystemSecretScope)
bronzeZoneNotebookPath = "../Data Engineering/Bronze Zone/Batch SQL"
silverZoneNotebookPath = "../Data Engineering/Silver Zone/Delta Merge"

# COMMAND ----------

serverName = dbutils.secrets.get(scope=externalSystem, key="SQLServerName")
databaseName = dbutils.secrets.get(scope=externalSystem, key="DatabaseName")
login = dbutils.secrets.get(scope=externalSystem, key="Login")
pwd = dbutils.secrets.get(scope=externalSystem, key="Pwd")
driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
port = 1433
jdbcUrl = "jdbc:sqlserver://{0}:{1};database={2}".format(serverName, port, databaseName)

# COMMAND ----------

def runQuery (jdbcUrl, query, login, pwd, driver):
  df = spark.read \
    .format("jdbc") \
    .option("url", jdbcUrl) \
    .option("query", query) \
    .option("user", login) \
    .option("password", pwd) \
    .option("driver", driver) \
    .load()
  return df

# COMMAND ----------

# MAGIC %md
# MAGIC ### Queries

# COMMAND ----------

rowCounts = """
SELECT TOP 100 PERCENT
     QUOTENAME(SCHEMA_NAME(sOBJ.schema_id)) AS [SchemaName]
    ,QUOTENAME(sOBJ.name) AS [TableName]
    ,QUOTENAME(SCHEMA_NAME(sOBJ.schema_id)) + '.' + QUOTENAME(sOBJ.name) AS [FullyQualifiedTableName]
    ,SUM(sPTN.Rows) AS [RowCount]
FROM sys.objects AS sOBJ
INNER JOIN sys.partitions AS sPTN ON sOBJ.object_id = sPTN.object_id
WHERE sOBJ.type = 'U'
AND sOBJ.is_ms_shipped = 0x0
AND index_id < 2 -- 0:Heap, 1:Clustered
GROUP BY sOBJ.schema_id, sOBJ.name
ORDER BY [FullyQualifiedTableName]
"""

# COMMAND ----------

primaryKeys = """
SELECT TOP 100 PERCENT
     QUOTENAME(SCHEMA_NAME(tab.schema_id)) AS [SchemaName]
    ,QUOTENAME(tab.[name]) AS [TableName]
    ,QUOTENAME(SCHEMA_NAME(tab.schema_id)) + '.' + QUOTENAME(tab.[name]) AS [FullyQualifiedTableName]
    ,pk.[name] AS [PrimaryKeyName]
    ,SUBSTRING(column_names, 1, LEN(column_names)-1) AS [Columns]
FROM sys.tables tab
LEFT OUTER JOIN sys.indexes pk ON tab.object_id = pk.object_id
                                AND pk.is_primary_key = 1
CROSS APPLY
(
    SELECT col.[name] + ', '
    FROM sys.index_columns ic
    JOIN sys.columns col ON ic.object_id = col.object_id
                            AND ic.column_id = col.column_id
    WHERE ic.object_id = tab.object_id
    AND ic.index_id = pk.index_id
    ORDER BY col.column_id
    FOR XML PATH ('')
) D (column_names)
ORDER BY schema_name(tab.schema_id), tab.[name]
"""

# COMMAND ----------

procedures = """
SELECT TOP 100 PERCENT
     QUOTENAME(SCHEMA_NAME(SCHEMA_ID)) AS [SchemaName]
    ,QUOTENAME(SO.name) AS [ObjectName]
    ,SO.Type_Desc AS [ObjectType]
    ,P.parameter_id AS [ParameterID]
    ,P.name AS [ParameterName]
    ,TYPE_NAME(P.user_type_id) AS [ParameterDataType]
    ,P.max_length AS [ParameterMaxBytes]
    ,P.is_output AS [IsOutPutParameter]
FROM sys.objects AS SO
LEFT JOIN sys.parameters AS P ON SO.OBJECT_ID = P.OBJECT_ID
WHERE SO.OBJECT_ID IN
(
    SELECT OBJECT_ID
    FROM sys.objects
    WHERE TYPE IN ('P','FN', 'TF', 'IF')
)
ORDER BY [SchemaName], SO.name, P.parameter_id
"""

# COMMAND ----------

rowCountsDF = runQuery(jdbcUrl, rowCounts, login, pwd, driver)
primaryKeysDF = runQuery(jdbcUrl, primaryKeys, login, pwd, driver)

# COMMAND ----------

tableExtraction = rowCountsDF \
  .select("FullyQualifiedTableName", "RowCount") \
  .join(primaryKeysDF, on="FullyQualifiedTableName", how="inner") \
  .collect()

# COMMAND ----------

display(tableExtraction)

# COMMAND ----------

for row in tableExtraction:
  upperBound = row[1]
  schemaName = row[2].replace('[','').replace(']','')
  tableName = row[3].replace('[','').replace(']','')
  jobName = "{0}_{1}_{2}".format(externalSystem, schemaName, tableName)
  primaryKeyColumns = row[5]
  sql = """EXEC [dbo].[HydrateBatchSQL]
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
  ,@SchemaName = '{5}'
  ,@TableName = '{6}'
  ,@SilverZonePartitionColumn = ''
  ,@SilverZoneClusterColumn = 'pk'
  ,@SilverZoneClusterBuckets = '8'
  ,@OptimizeWhere = ''
  ,@OptimizeZOrderBy = ''
  ,@VacuumRetentionHours = 168
  ,@PartitionColumn = ''
  ,@LowerBound = '0'
  ,@UpperBound = '{7}'
  ,@NumPartitions = '8'
  ,@useWindowedExtraction = 0
  ,@WindowingColumn
  ,@WindowedExtractionBeginDate = NULL
  ,@WindowedExtractionEndDate = NULL
  ,@WindowedExtractionInterval = NULL
  ,@WindowedExtractionProcessLatestWindowFirst = NULL
  ,@pushdownQuery = ''
  ,@PrimaryKeyColumns = '{8}'
  ,@BronzeZoneNotebookPath = '{9}'
  ,@SilverZoneNotebookPath = '{10}';
  """.format(project, externalSystem, externalSystemSecretScope, stageName, jobName, schemaName, tableName, upperBound, primaryKeyColumns, bronzeZoneNotebookPath, silverZoneNotebookPath)
  #print(sql)
  print(sql.replace("\r","").replace("\n",""))