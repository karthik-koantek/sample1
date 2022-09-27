# Databricks notebook source
# MAGIC %md # SQL
# MAGIC
# MAGIC Virtual Data Connector to any Azure SQL, Azure SQL Managed Instance, or Azure SQL Data Warehouse DB.  Should also work for SQL Server database in a VM or on premises provided network connectivity is enabled.
# MAGIC
# MAGIC #### Usage
# MAGIC Supply the parameters to the Source SQL Database you want to explore
# MAGIC
# MAGIC You can use this to test connectivity to a SQL data source
# MAGIC List objects in that source
# MAGIC Query
# MAGIC
# MAGIC #### Prerequisites
# MAGIC List any requirements for using this notebook
# MAGIC
# MAGIC #### Details
# MAGIC Detailed documentation

# COMMAND ----------

serverName = "dbvdc.database.windows.net"
port = 1433
databaseName = "dbvdc"
login = "dbvdc"
pwd = "@armdep10yment@dmin"
driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"

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

columns = """
SELECT
     QUOTENAME(TABLE_SCHEMA) AS [SchemaName]
    ,QUOTENAME(TABLE_NAME) AS [TableName]
    ,QUOTENAME(TABLE_SCHEMA) + '.' + QUOTENAME(TABLE_NAME) AS [FullyQualifiedTableName]
    ,QUOTENAME(COLUMN_NAME) AS [ColumnName]
    ,QUOTENAME(TABLE_SCHEMA) + '.' + QUOTENAME(TABLE_NAME) + '.' + QUOTENAME(COLUMN_NAME) AS [FullyQualifiedColumnName]
    ,ORDINAL_POSITION AS [OrdinalPosition]
    ,COLUMN_DEFAULT AS [ColumnDefault]
    ,IS_NULLABLE AS [IsNullable]
    ,DATA_TYPE AS [DataType]
    ,CHARACTER_MAXIMUM_LENGTH AS [CharacterMaximumLength]
    ,NUMERIC_PRECISION AS [NumericPrecision]
    ,NUMERIC_SCALE AS [NumericScale]
    ,DATETIME_PRECISION AS [DatetimePrecision]
    ,COLLATION_NAME AS [CollationName]
FROM INFORMATION_SCHEMA.COLUMNS
ORDER BY [FullyQualifiedColumnName]
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

rowCountsDF = runQuery(rowCounts)
primaryKeysDF = runQuery(primaryKeys)
columnsDF = runQuery(columns)
proceduresDF = runQuery(procedures)

# COMMAND ----------

display(df)
