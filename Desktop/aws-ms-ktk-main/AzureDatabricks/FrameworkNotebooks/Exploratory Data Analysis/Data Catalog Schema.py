# Databricks notebook source
# MAGIC %md # Data Catalog Schema

# COMMAND ----------

# MAGIC %md #### Initialize

# COMMAND ----------

# MAGIC %run "../Orchestration/Notebook Functions"

# COMMAND ----------

silverDataPath = "{0}/{1}".format(silverProtectedBasePath, "datacatalog")
goldDataPath = "{0}/{1}".format(goldProtectedBasePath, "datacatalog")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Silver Zone Tables

# COMMAND ----------

# MAGIC %md
# MAGIC #### DatabaseDetail

# COMMAND ----------

sql = """CREATE TABLE IF NOT EXISTS silverprotected.databaseDetail
(
  database STRING,
  location STRING,
  owner STRING,
  comment STRING
)
USING delta
LOCATION '{0}/dbdetail'
""".format(silverDataPath)
print(sql)
spark.sql(sql)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Table Detail

# COMMAND ----------

sql = """CREATE TABLE IF NOT EXISTS silverprotected.tableDetail
(
  database STRING,
  format STRING,
  id STRING,
  name STRING,
  description STRING,
  location STRING,
  createdAt TIMESTAMP,
  lastModified TIMESTAMP,
  partitionColumns ARRAY<STRING>,
  numFiles BIGINT,
  sizeInBytes BIGINT,
  properties MAP<STRING,STRING>,
  minReaderVersion INT,
  minWriterVersion INT
)
USING delta
PARTITIONED BY (database)
LOCATION '{0}/tabledetail'
""".format(silverDataPath)
print(sql)
spark.sql(sql)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Column Detail

# COMMAND ----------

sql = """CREATE TABLE IF NOT EXISTS silverprotected.columnDetail
(
  database STRING,
  tableName STRING,
  col_name STRING,
  data_type STRING,
  comment STRING
)
USING DELTA
PARTITIONED BY (database, tableName)
LOCATION '{0}/columndetail'
""".format(silverDataPath)
print(sql)
spark.sql(sql)

# COMMAND ----------

# MAGIC %md
# MAGIC #### DataCatalogValueCounts

# COMMAND ----------

sql = """CREATE TABLE IF NOT EXISTS silverprotected.dataCatalogValueCounts
(
  value STRING,
  total INT,
  Catalog STRING,
  Table STRING,
  Column STRING,
  DataType STRING,
  pk STRING
)
USING delta
PARTITIONED BY (Catalog, Table)
LOCATION '{0}/vc'
""".format(silverDataPath)
print(sql)
spark.sql(sql)

# COMMAND ----------

# MAGIC %md
# MAGIC #### DataCatalogSummary

# COMMAND ----------

sql = """CREATE TABLE IF NOT EXISTS silverprotected.dataCatalogSummary
(
  MinimumValue STRING,
  MaximumValue STRING,
  AvgValue STRING,
  StdDevValue STRING,
  DistinctCountValue INT,
  NumberOfNulls INT,
  NumberOfZeros INT,
  RecordCount INT,
  PercentNulls FLOAT,
  PercentZeros FLOAT,
  Selectivity FLOAT,
  Catalog STRING,
  Table STRING,
  Column STRING,
  DataType STRING,
  pk STRING
)
USING delta
PARTITIONED BY (Catalog, Table)
LOCATION "{0}/summary"
""".format(silverDataPath)
print(sql)
spark.sql(sql)

# COMMAND ----------

# MAGIC %md
# MAGIC #### FileDetail

# COMMAND ----------

sql = """CREATE TABLE IF NOT EXISTS silverprotected.fileDetail
(
  storageAccountName STRING,
  fileSystemName STRING,
  fileName STRING,
  sizeBytes BIGINT,
  LastModified TIMESTAMP,
  eTag STRING,
  group STRING,
  owner STRING,
  permissions STRING
)
USING delta
PARTITIONED BY (storageAccountName, fileSystemName)
LOCATION '{0}/filedetail'
""".format(silverDataPath)
print(sql)
spark.sql(sql)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Gold Zone Tables and Views

# COMMAND ----------

# MAGIC %md
# MAGIC #### vColumnDetail

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW goldprotected.vColumnDetail
# MAGIC AS
# MAGIC SELECT
# MAGIC    hash(tableName) AS tableId
# MAGIC   ,tableName AS fullyQualifiedTableName
# MAGIC   ,split(tableName,'[.]')[0] AS databaseName
# MAGIC   ,split(tableName,'[.]')[1] AS tableName
# MAGIC   ,col_name AS columnName
# MAGIC   ,data_type AS dataType
# MAGIC   ,comment
# MAGIC FROM silverprotected.columndetail

# COMMAND ----------

# MAGIC %md
# MAGIC #### vColumnSummary

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW goldprotected.vColumnSummary
# MAGIC AS
# MAGIC SELECT
# MAGIC    hash(concat(Catalog, '.', Table)) AS tableId
# MAGIC   ,concat(Catalog, '.', Table) AS fullyQualifiedTableName
# MAGIC   ,Catalog AS databaseName
# MAGIC   ,Table AS tableName
# MAGIC   ,RecordCount
# MAGIC   ,Column AS columnName
# MAGIC   ,DataType
# MAGIC   ,AvgValue
# MAGIC   ,StdDevValue
# MAGIC   ,DistinctCountValue
# MAGIC   ,NumberOfNulls
# MAGIC   ,NumberOfZeros
# MAGIC   ,PercentNulls
# MAGIC   ,PercentZeros
# MAGIC   ,Selectivity
# MAGIC   ,MinimumValue
# MAGIC   ,MaximumValue
# MAGIC FROM silverprotected.datacatalogsummary

# COMMAND ----------

# MAGIC %md
# MAGIC #### DataCatalogDatabase

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW goldprotected.vDataCatalogDatabase
# MAGIC AS
# MAGIC SELECT
# MAGIC    hash(database) AS databaseId
# MAGIC   ,database
# MAGIC   ,location
# MAGIC   ,owner
# MAGIC   ,comment
# MAGIC FROM silverprotected.databasedetail

# COMMAND ----------

sql = """
CREATE TABLE IF NOT EXISTS goldprotected.DataCatalogDatabase
(
   databaseId INT NOT NULL
  ,database STRING NOT NULL
  ,location STRING
  ,owner STRING
  ,comment STRING
  ,pk STRING NOT NULL
  ,isActive BOOLEAN NOT NULL
  ,effectiveStartDate TIMESTAMP
  ,effectiveEndDate TIMESTAMP
)
USING delta
LOCATION '{0}/database'
""".format(goldDataPath)
print(sql)
spark.sql(sql)

# COMMAND ----------

# MAGIC %md
# MAGIC #### DataCatalogTable

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW goldprotected.vDataCatalogTable
# MAGIC AS
# MAGIC SELECT
# MAGIC    hash(td.name) AS tableId
# MAGIC   ,td.name AS fullyQualifiedTableName
# MAGIC   ,hash(split(td.name,'[.]')[0]) AS databaseId
# MAGIC   ,split(td.name,'[.]')[0] AS databaseName
# MAGIC   ,split(td.name,'[.]')[1] AS tableName
# MAGIC   ,td.id
# MAGIC   ,td.description
# MAGIC   ,td.location
# MAGIC   ,td.format
# MAGIC   ,s.recordCount
# MAGIC   ,td.createdAt
# MAGIC   ,td.lastModified
# MAGIC   ,CAST(td.partitionColumns AS STRING) AS partitionColumns
# MAGIC   ,td.numFiles
# MAGIC   ,td.sizeInBytes
# MAGIC   ,CAST(td.properties AS STRING) AS properties
# MAGIC   ,td.minReaderVersion
# MAGIC   ,td.minWriterVersion
# MAGIC FROM silverprotected.tabledetail td
# MAGIC LEFT JOIN
# MAGIC (
# MAGIC   SELECT DISTINCT tableId, recordCount
# MAGIC   FROM goldprotected.vColumnSummary
# MAGIC ) s ON hash(td.name) = s.tableId

# COMMAND ----------

sql = """
CREATE TABLE IF NOT EXISTS goldprotected.DataCatalogTable
(
   tableId INT NOT NULL
  ,fullyQualifiedTableName STRING NOT NULL
  ,databaseId INT NOT NULL
  ,databaseName STRING
  ,tableName STRING
  ,id STRING
  ,description STRING
  ,location STRING
  ,format STRING
  ,recordCount BIGINT
  ,createdAt TIMESTAMP
  ,lastModified TIMESTAMP
  ,partitionColumns STRING
  ,numFiles BIGINT
  ,sizeInBytes BIGINT
  ,properties STRING
  ,minReaderVersion INT
  ,minWriterVersion INT
  ,pk STRING NOT NULL
  ,isActive BOOLEAN NOT NULL
  ,effectiveStartDate TIMESTAMP
  ,effectiveEndDate TIMESTAMP
)
USING delta
PARTITIONED BY(databaseName)
LOCATION '{0}/table'
""".format(goldDataPath)
print(sql)
spark.sql(sql)

# COMMAND ----------

# MAGIC %md
# MAGIC #### DataCatalogColumn

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW goldprotected.vDataCatalogColumn
# MAGIC AS
# MAGIC SELECT
# MAGIC    hash(cd.databaseName, cd.tableName, cd.columnName) AS columnId
# MAGIC   ,cd.tableId
# MAGIC   ,cd.fullyQualifiedTableName
# MAGIC   ,cd.databaseName
# MAGIC   ,cd.tableName
# MAGIC   ,cd.columnName
# MAGIC   ,cd.dataType
# MAGIC   ,COALESCE(cd.comment, '') AS comment
# MAGIC   ,cs.AvgValue
# MAGIC   ,cs.StdDevValue
# MAGIC   ,cs.DistinctCountValue
# MAGIC   ,cs.NumberOfNulls
# MAGIC   ,cs.NumberOfZeros
# MAGIC   ,cs.PercentNulls
# MAGIC   ,cs.PercentZeros
# MAGIC   ,cs.Selectivity
# MAGIC   ,cs.MinimumValue
# MAGIC   ,cs.MaximumValue
# MAGIC FROM goldprotected.vColumnDetail cd
# MAGIC LEFT JOIN goldprotected.vColumnSummary cs ON cd.tableId = cs.TableId AND cd.columnName = cs.ColumnName

# COMMAND ----------

sql = """
CREATE TABLE IF NOT EXISTS goldprotected.DataCatalogColumn
(
   columnId INT NOT NULL
  ,tableId INT NOT NULL
  ,fullyQualifiedTableName STRING NOT NULL
  ,databaseName STRING NOT NULL
  ,tableName STRING NOT NULL
  ,columnName STRING NOT NULL
  ,dataType STRING
  ,comment STRING
  ,AvgValue STRING
  ,StdDevValue STRING
  ,DistinctCountValue BIGINT
  ,NumberOfNulls BIGINT
  ,NumberOfZeros BIGINT
  ,PercentNulls FLOAT
  ,PercentZeros FLOAT
  ,Selectivity FLOAT
  ,MinimumValue STRING
  ,MaximumValue STRING
  ,pk STRING NOT NULL
  ,isActive BOOLEAN NOT NULL
  ,effectiveStartDate TIMESTAMP
  ,effectiveEndDate TIMESTAMP
)
USING delta
PARTITIONED BY(databaseName,tableName)
LOCATION '{0}/column'
""".format(goldDataPath)
print(sql)
spark.sql(sql)

# COMMAND ----------

# MAGIC %md
# MAGIC #### DataCatalogColumnValueCounts

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW goldprotected.vDataCatalogColumnValueCounts
# MAGIC AS
# MAGIC SELECT
# MAGIC    hash(Catalog, Table, Column) AS columnId
# MAGIC   ,Catalog AS databaseName
# MAGIC   ,Table AS tableName
# MAGIC   ,concat(Catalog, '.', Table) AS fullyQualifiedTableName
# MAGIC   ,Column AS columnName
# MAGIC   ,value
# MAGIC   ,total
# MAGIC FROM silverprotected.datacatalogvaluecounts

# COMMAND ----------

# MAGIC %md
# MAGIC #### DataCatalogFileDetail

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW goldprotected.vDataCatalogFile
# MAGIC AS
# MAGIC SELECT
# MAGIC   hash(storageAccountName, fileSystemName, fileName) AS fileId
# MAGIC  ,storageAccountName
# MAGIC  ,fileSystemName
# MAGIC  ,fileName
# MAGIC  ,concat('abfss://', fileSystemName, '@', storageAccountName, '.dfs.core.windows.net/', fileName) AS fullFileName
# MAGIC  ,sizeBytes
# MAGIC  ,LastModified
# MAGIC  ,eTag
# MAGIC  ,group
# MAGIC  ,owner
# MAGIC  ,permissions
# MAGIC FROM silverprotected.fileDetail

# COMMAND ----------

sql = """
CREATE TABLE IF NOT EXISTS goldprotected.DataCatalogFile
(
   fileId BIGINT NOT NULL
  ,storageAccountName STRING NOT NULL
  ,fileSystemName STRING NOT NULL
  ,fileName STRING NOT NULL
  ,fullFileName STRING NOT NULL
  ,sizeBytes BIGINT
  ,LastModified TIMESTAMP
  ,eTag STRING
  ,group STRING
  ,owner STRING
  ,permissions STRING
  ,pk STRING NOT NULL
  ,isActive BOOLEAN NOT NULL
  ,effectiveStartDate TIMESTAMP
  ,effectiveEndDate TIMESTAMP
)
USING delta
PARTITIONED BY(storageAccountName,fileSystemName)
LOCATION '{0}/file'
""".format(goldDataPath)
print(sql)
spark.sql(sql)

# COMMAND ----------

# MAGIC %md
# MAGIC #### vDataCatalogEffectiveDates

# COMMAND ----------

import datetime
currentDate = (datetime.datetime.utcnow() + datetime.timedelta(days=2)).strftime('%Y-%m-%d')
sequenceStartDate = (datetime.datetime.utcnow() + datetime.timedelta(days=-90)).strftime('%Y-%m-%d')
print(sequenceStartDate, currentDate)
sql = """
CREATE OR REPLACE VIEW goldprotected.vDataCatalogEffectiveDates
AS
SELECT
   explode(sequence(to_date('{0}'), to_date('{1}'), interval 1 week)) AS effectiveDate
""".format(sequenceStartDate,currentDate)
print(sql)
spark.sql(sql)

# COMMAND ----------

# MAGIC %md
# MAGIC #### vDataCatalogTableGrowth

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW goldprotected.vDataCatalogTableGrowth
# MAGIC AS
# MAGIC SELECT
# MAGIC      d.effectiveDate
# MAGIC     ,t.tableId
# MAGIC     ,t.recordCount
# MAGIC     ,t.numFiles
# MAGIC     ,t.sizeInBytes
# MAGIC FROM goldprotected.vDataCatalogEffectiveDates d
# MAGIC JOIN goldprotected.DataCatalogTable t ON to_date(d.effectiveDate) BETWEEN t.effectiveStartDate AND COALESCE(t.effectiveEndDate, to_date('2099-01-01'))

# COMMAND ----------

# MAGIC %md
# MAGIC #### vDataCatalogFileGrowth

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW goldprotected.vDataCatalogFileGrowth
# MAGIC AS
# MAGIC SELECT
# MAGIC      d.effectiveDate
# MAGIC     ,f.fileId
# MAGIC     ,f.sizeBytes
# MAGIC FROM goldprotected.vDataCatalogEffectiveDates d
# MAGIC JOIN goldprotected.DataCatalogFile f ON to_date(d.effectiveDate) BETWEEN f.effectiveStartDate AND COALESCE(f.effectiveEndDate, to_date('2099-01-01'))

# COMMAND ----------

# MAGIC %md
# MAGIC #### TermSearchResults

# COMMAND ----------

sql = """CREATE TABLE IF NOT EXISTS goldprotected.termSearchResults
(

  databaseName STRING,
  tableName STRING,
  columnName STRING,
  columnSampleValue STRING,
  searchTerm STRING,
  tableNameMatch BOOLEAN,
  columnNameMatch BOOLEAN,
  columnValueMatch BOOLEAN,
  searchTermFullMatchFlag BOOLEAN,
  fuzzyMatchConfidenceScore FLOAT,
  tableId INT,
  columnId INT

)
USING delta
LOCATION '{0}/termSearchResults'
""".format(goldDataPath)

spark.sql(sql)
