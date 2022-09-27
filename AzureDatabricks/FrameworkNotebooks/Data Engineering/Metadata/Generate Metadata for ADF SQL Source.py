# Databricks notebook source
# MAGIC %md # Generate Metadata for ADF SQL Source
# MAGIC
# MAGIC Generates Framework Hydration Script for External SQL Source intended to be ingested via Azure Data Factory.
# MAGIC
# MAGIC #### Usage
# MAGIC 1. Build a ktk Metadata Hydration pipeline using stored procedure ***HydrateADFSQLMetadataQueries***.
# MAGIC 2. Run the ADF Pipeline, which will run 4 metadata queries against the source system and save the data in the configured destination path (in the transient zone)
# MAGIC 3. Supply the above parameters to the destination path to read the files
# MAGIC 4. This notebook will generate and configure the default metadata hydration and save the results as files in the destination path directory.
# MAGIC 5. Download the files, edit as necessary and check into source control.
# MAGIC
# MAGIC #### Prerequisites
# MAGIC
# MAGIC #### Details

# COMMAND ----------

# MAGIC %md
# MAGIC ### Initialize

# COMMAND ----------

# MAGIC %run "../../Secrets/Import Data Lake Secrets"

# COMMAND ----------

# MAGIC %run "../../Development/Utilities"

# COMMAND ----------

from pyspark.sql.functions import lit, concat, translate, regexp_replace

dbutils.widgets.text(name="projectName", defaultValue="", label="Project")
dbutils.widgets.text(name="systemName", defaultValue="", label="System")
dbutils.widgets.text(name="systemSecretScope", defaultValue="internal", label="System Secret Scope")
dbutils.widgets.text(name="stageName", defaultValue="", label="Stage")
dbutils.widgets.text(name="serverName", defaultValue="", label="Server Name")
dbutils.widgets.text(name="databaseName", defaultValue="", label="Database Name")
dbutils.widgets.text(name="userName", defaultValue="", label="User Name")
dbutils.widgets.text(name="passwordKeyVaultSecretName", defaultValue="", label="Password Key Vault Secret Name")
dbutils.widgets.text(name="destinationContainerName", defaultValue="azuredatafactory", label="Container Name")
dbutils.widgets.text(name="destinationBasePath", defaultValue="", label="Base Path")
dbutils.widgets.text(name="sampleRowCount", defaultValue="1000", label="Sample Rows")
dbutils.widgets.text(name="excludeTablesWithZeroRowCount", defaultValue="true", label="Exclude Empty Tables")
dbutils.widgets.text(name="minimumTableRowCount", defaultValue="100", label="Mimum Row Count")
dbutils.widgets.text(name="columnFilterTerms", defaultValue="enc_id,acct_id,person_id,practice_id,note_id,document_id,icd_cd,charge_id,template_id", label="Column Filter Terms")

projectName = dbutils.widgets.get("projectName")
systemName = dbutils.widgets.get("systemName")
systemSecretScope = dbutils.widgets.get("systemSecretScope")
stageName = dbutils.widgets.get("stageName")
serverName = dbutils.widgets.get("serverName")
databaseName = dbutils.widgets.get("databaseName")
userName = dbutils.widgets.get("userName")
passwordKeyVaultSecretName = dbutils.widgets.get("passwordKeyVaultSecretName")
destinationContainerName = dbutils.widgets.get("destinationContainerName")
destinationBasePath = dbutils.widgets.get("destinationBasePath")
sampleRowCount = dbutils.widgets.get("sampleRowCount")
excludeTablesWithZeroRowCount = dbutils.widgets.get("excludeTablesWithZeroRowCount")
minimumTableRowCount = int(dbutils.widgets.get("minimumTableRowCount"))
columnFilterTerms = dbutils.widgets.get("columnFilterTerms")
columnFilterTermList = columnFilterTerms.split(",")
notebookPath = "On Premises Database Query to Staging"
metadataBasePath = "/mnt/{0}/{1}".format(destinationContainerName, destinationBasePath)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Validate Inputs

# COMMAND ----------

try:
  files = [f for f in dbutils.fs.ls(metadataBasePath) if "list" in f[1]]
  if len(files) != 4:
    raise ValueError("Metadata files were not found in the supplied directory. Please Run the ADF pipeline as described in the Usage notes.")
  tables = spark.read.json("{0}/listtables".format(metadataBasePath))
  columns = spark.read.json("{0}/listcolumns".format(metadataBasePath))
  keys = spark.read.json("{0}/listkeycolumns".format(metadataBasePath))
  procs = spark.read.json("{0}/listprocedures".format(metadataBasePath))
except Exception as e:
  raise(e)

# COMMAND ----------

(tables.count(), columns.count(), keys.count(), procs.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Filtering Rules

# COMMAND ----------

if excludeTablesWithZeroRowCount == "true":
  tables = tables.filter(tables["RowCount"] > 0)
tables = tables.filter(tables["RowCount"] > minimumTableRowCount)
print(tables.count())

# COMMAND ----------

tables.createOrReplaceTempView("tables")
columns.createOrReplaceTempView("columns")
keys.createOrReplaceTempView("keys")

# COMMAND ----------

# MAGIC %sql
# MAGIC --to assist in building the Column Filter Terms, may be a good idea to download the results of this query to excel to identify the key columns worth adding to the list
# MAGIC SELECT k.FullyQualifiedTableName, k.PrimaryKeyName, k.Columns, t.RowCount
# MAGIC FROM keys k
# MAGIC JOIN tables t ON k.FullyQualifiedTableName=t.FullyQualifiedTableName

# COMMAND ----------

#filter to include only tables with mimimum row counts and having columns that match the column filter term list
filteredTableList = []
filteredTableList.append(tables \
  .join(columns,"fullyQualifiedTableName") \
  .withColumn("columnsNoBrackets", translate(translate(columns["ColumnName"], "[", ""),"]","")))
filteredTableList.append(filteredTableList[-1].filter(filteredTableList[-1].columnsNoBrackets.isin(columnFilterTermList)))
filteredTableList[-1].createOrReplaceTempView("filteredTableList")
display(filteredTableList[-1])

# COMMAND ----------

#final list of tables after filtering
tables = spark.sql("""
SELECT t.*
FROM tables t
WHERE FullyQualifiedTableName IN (SELECT FullyQualifiedTableName FROM filteredTableList)
""")
display(tables)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Generate Metadata

# COMMAND ----------

# MAGIC %md
# MAGIC #### Full Tables

# COMMAND ----------

# MAGIC %md ##### Transient Zone

# COMMAND ----------

adfTableList = []
adfTableList.append(
  tables.withColumn("sql", concat(
    lit("EXEC [dbo].[HydrateADFSQL] "),
    lit("@ProjectName='"), lit(projectName), lit("_1_ADF"), lit("'"),
    lit(",@SystemName='"), lit(systemName), lit("_1_ADF"), lit("'"),
    lit(",@SystemOrder=10"),
    lit(",@SystemIsActive=1"),
    lit(",@StageName='"), lit(stageName), lit("_1_ADF"), lit("'"),
    lit(",@StageIsActive=1"),
    lit(",@StageOrder=10"),
    lit(",@JobName='"), lit(stageName), lit("_1_ADF"), lit("_table_"),
      translate(translate(tables["SchemaName"], "[", ""), "]", ""), lit("_"),
      translate(translate(tables["TableName"], "[", ""), "]", ""), lit("'"),
    lit(",@JobOrder=10"),
    lit(",@JobIsActive=1"),
    lit(",@ServerName='"), lit(serverName), lit("'"),
    lit(",@DatabaseName='"), lit(databaseName), lit("'"),
    lit(",@UserName='"), lit(userName), lit("'"),
    lit(",@PasswordKeyVaultSecretName='"), lit(passwordKeyVaultSecretName), lit("'"),
    lit(",@SchemaName='"), translate(translate(tables["SchemaName"], "[", ""), "]", ""), lit("'"),
    lit(",@TableName='"), translate(translate(tables["TableName"], "[", ""), "]", ""), lit("'"),
    lit(",@DestinationContainerName='"), lit(destinationContainerName), lit("'"),
    lit(",@DestinationFilePath='"),
      lit(destinationBasePath), lit("table/"),
      translate(translate(tables["SchemaName"], "[", ""), "]", ""), lit("/"),
      translate(translate(tables["TableName"], "[", ""), "]", ""), lit("'"),
    lit(",@ADFPipelineName='On Premises Database Table to Staging'")
    ),
  )
)

# COMMAND ----------

adfTableList[-1].select("sql").collect()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Bronze & Silver Zones

# COMMAND ----------

adbTableList = []
adbTableList.append(
  tables.withColumn("sql", concat(
    lit("EXEC [dbo].[HydrateBatchFile] "),
    lit("@ProjectName='"), lit(projectName), lit("_2_ADB"), lit("'"),
    lit(",@SystemName='"), lit(systemName), lit("_2_ADB"), lit("'"),
    lit(",@SystemSecretScope='"), lit(systemSecretScope), lit("'"),
    lit(",@SystemOrder=10"),
    lit(",@SystemIsActive=1"),
    lit(",@StageName='"), lit(stageName), lit("_2_ADB"), lit("'"),
    lit(",@StageIsActive=1"),
    lit(",@StageOrder=10"),
    lit(",@JobName='"), lit(stageName), lit("_2_ADB"), lit("_table_"),
      translate(translate(tables["SchemaName"], "[", ""), "]", ""), lit("_"),
      translate(translate(tables["TableName"], "[", ""), "]", ""), lit("'"),
    lit(",@JobOrder=10"),
    lit(",@JobIsActive=1"),
    lit(",@ExternalDataPath='mnt/"), lit(destinationContainerName), lit("/"),
      lit(destinationBasePath), lit("table/"),
      translate(translate(tables["SchemaName"], "[", ""), "]", ""), lit("/"),
      translate(translate(tables["TableName"], "[", ""), "]", ""), lit("'"),
    lit(",@SchemaName='"), translate(translate(tables["SchemaName"], "[", ""), "]", ""), lit("'"),
    lit(",@TableName='"), translate(translate(tables["TableName"], "[", ""), "]", ""), lit("'"),
    lit(",@FileExtension='json'"),
    lit(",@MultiLine='False'"),
    lit(",@IsDatePartitioned='False'"),
    lit(",@LoadType='Overwrite'"),
    lit(",@BronzeZoneNotebookPath='../Data Engineering/Bronze Zone/Batch File JSON'"),
    lit(",@SilverZoneNotebookPath='../Data Engineering/Silver Zone/Delta Load'")
    ),
  )
)

# COMMAND ----------

adbTableList[-1].select("sql").collect()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Sample Tables

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Transient Zone

# COMMAND ----------

adfTableSampleList = []
adfTableSampleList.append(
  tables.withColumn("sql", concat(
    lit("EXEC [dbo].[HydrateADFSQL] "),
    lit("@ProjectName='"), lit(projectName), lit("_1_ADF"), lit("'"),
    lit(",@SystemName='"), lit(systemName), lit("_1_ADF"), lit("'"),
    lit(",@SystemOrder=10"),
    lit(",@SystemIsActive=1"),
    lit(",@StageName='"), lit(stageName), lit("_1_ADF"), lit("'"),
    lit(",@StageIsActive=1"),
    lit(",@StageOrder=10"),
    lit(",@JobName='"), lit(stageName), lit("_1_ADF"), lit("_sample_"),
      translate(translate(tables["SchemaName"], "[", ""), "]", ""), lit("_"),
      translate(translate(tables["TableName"], "[", ""), "]", ""), lit("'"),
    lit(",@JobOrder=10"),
    lit(",@JobIsActive=1"),
    lit(",@ServerName='"), lit(serverName), lit("'"),
    lit(",@DatabaseName='"), lit(databaseName), lit("'"),
    lit(",@UserName='"), lit(userName), lit("'"),
    lit(",@PasswordKeyVaultSecretName='"), lit(passwordKeyVaultSecretName), lit("'"),
    lit(",@SchemaName='"), translate(translate(tables["SchemaName"], "[", ""), "]", ""), lit("'"),
    lit(",@TableName='"), translate(translate(tables["TableName"], "[", ""), "]", ""), lit("'"),
    lit(",@PushdownQuery='SELECT TOP "), lit(sampleRowCount), lit(" * FROM "), lit(tables["fullyQualifiedTableName"]), lit("'"),
    lit(",@DestinationContainerName='"), lit(destinationContainerName), lit("'"),
    lit(",@DestinationFilePath='"),
      lit(destinationBasePath), lit("sample/"),
      translate(translate(tables["SchemaName"], "[", ""), "]", ""), lit("/"),
      translate(translate(tables["TableName"], "[", ""), "]", ""), lit("'"),
    lit(",@ADFPipelineName='On Premises Database Query to Staging'")
    ),
  )
)

# COMMAND ----------

adfTableSampleList[-1].select("sql").collect()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Bronze & Silver Zone

# COMMAND ----------

adbTableSampleList = []
adbTableSampleList.append(
  tables.withColumn("sql", concat(
    lit("EXEC [dbo].[HydrateBatchFile] "),
    lit("@ProjectName='"), lit(projectName), lit("_2_ADB"), lit("'"),
    lit(",@SystemName='"), lit(systemName), lit("_2_ADB"), lit("'"),
    lit(",@SystemSecretScope='"), lit(systemSecretScope), lit("'"),
    lit(",@SystemOrder=10"),
    lit(",@SystemIsActive=1"),
    lit(",@StageName='"), lit(stageName), lit("_2_ADB"), lit("'"),
    lit(",@StageIsActive=1"),
    lit(",@StageOrder=10"),
    lit(",@JobName='"), lit(stageName), lit("_2_ADB"), lit("_sample_"),
      translate(translate(tables["SchemaName"], "[", ""), "]", ""), lit("_"),
      translate(translate(tables["TableName"], "[", ""), "]", ""), lit("'"),
    lit(",@JobOrder=10"),
    lit(",@JobIsActive=1"),
    lit(",@ExternalDataPath='mnt/"), lit(destinationContainerName), lit("/"),
      lit(destinationBasePath), lit("sample/"),
      translate(translate(tables["SchemaName"], "[", ""), "]", ""), lit("/"),
      translate(translate(tables["TableName"], "[", ""), "]", ""), lit("'"),
    lit(",@SchemaName='"), translate(translate(tables["SchemaName"], "[", ""), "]", ""), lit("'"),
    lit(",@TableName='"), translate(translate(tables["TableName"], "[", ""), "]", ""), lit("'"),
    lit(",@FileExtension='json'"),
    lit(",@MultiLine='False'"),
    lit(",@IsDatePartitioned='False'"),
    lit(",@LoadType='Overwrite'"),
    lit(",@BronzeZoneNotebookPath='../Data Engineering/Bronze Zone/Batch File JSON'"),
    lit(",@SilverZoneNotebookPath='../Data Engineering/Silver Zone/Delta Load'")
    ),
  )
)

# COMMAND ----------

adbTableSampleList[-1].select("sql").collect()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Pushdown Query

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Transient Zone

# COMMAND ----------

adfQueryList = []
adfQueryList.append(
  tables.withColumn("sql", concat(
    lit("EXEC [dbo].[HydrateADFSQL] "),
    lit("@ProjectName='"), lit(projectName), lit("_1_ADF"), lit("'"),
    lit(",@SystemName='"), lit(systemName), lit("_1_ADF"), lit("'"),
    lit(",@SystemOrder=10"),
    lit(",@SystemIsActive=1"),
    lit(",@StageName='"), lit(stageName), lit("_1_ADF"), lit("'"),
    lit(",@StageIsActive=1"),
    lit(",@StageOrder=10"),
    lit(",@JobName='"), lit(stageName), lit("_1_ADF"), lit("_query_"),
      translate(translate(tables["SchemaName"], "[", ""), "]", ""), lit("_"),
      translate(translate(tables["TableName"], "[", ""), "]", ""), lit("'"),
    lit(",@JobOrder=10"),
    lit(",@JobIsActive=1"),
    lit(",@ServerName='"), lit(serverName), lit("'"),
    lit(",@DatabaseName='"), lit(databaseName), lit("'"),
    lit(",@UserName='"), lit(userName), lit("'"),
    lit(",@PasswordKeyVaultSecretName='"), lit(passwordKeyVaultSecretName), lit("'"),
    lit(",@SchemaName='"), translate(translate(tables["SchemaName"], "[", ""), "]", ""), lit("'"),
    lit(",@TableName='"), translate(translate(tables["TableName"], "[", ""), "]", ""), lit("'"),
    lit(",@PushdownQuery='SELECT "), lit("* FROM "), lit(tables["fullyQualifiedTableName"]), lit(" WHERE 1=1'"),
    lit(",@DestinationContainerName='"), lit(destinationContainerName), lit("'"),
    lit(",@DestinationFilePath='"),
      lit(destinationBasePath), lit("query/"),
      translate(translate(tables["SchemaName"], "[", ""), "]", ""), lit("/"),
      translate(translate(tables["TableName"], "[", ""), "]", ""), lit("'"),
    lit(",@ADFPipelineName='On Premises Database Query to Staging'")
    ),
  )
)

# COMMAND ----------

adfQueryList[-1].select("sql").collect()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Bronze & Silver Zone

# COMMAND ----------

adbQueryList = []
adbQueryList.append(
  tables.withColumn("sql", concat(
    lit("EXEC [dbo].[HydrateBatchFile] "),
    lit("@ProjectName='"), lit(projectName), lit("_2_ADB"), lit("'"),
    lit(",@SystemName='"), lit(systemName), lit("_2_ADB"), lit("'"),
    lit(",@SystemSecretScope='"), lit(systemSecretScope), lit("'"),
    lit(",@SystemOrder=10"),
    lit(",@SystemIsActive=1"),
    lit(",@StageName='"), lit(stageName), lit("_2_ADB"), lit("'"),
    lit(",@StageIsActive=1"),
    lit(",@StageOrder=10"),
    lit(",@JobName='"), lit(stageName), lit("_2_ADB"), lit("_query_"),
      translate(translate(tables["SchemaName"], "[", ""), "]", ""), lit("_"),
      translate(translate(tables["TableName"], "[", ""), "]", ""), lit("'"),
    lit(",@JobOrder=10"),
    lit(",@JobIsActive=1"),
    lit(",@ExternalDataPath='mnt/"), lit(destinationContainerName), lit("/"),
      lit(destinationBasePath), lit("query/"),
      translate(translate(tables["SchemaName"], "[", ""), "]", ""), lit("/"),
      translate(translate(tables["TableName"], "[", ""), "]", ""), lit("'"),
    lit(",@SchemaName='"), translate(translate(tables["SchemaName"], "[", ""), "]", ""), lit("'"),
    lit(",@TableName='"), translate(translate(tables["TableName"], "[", ""), "]", ""), lit("'"),
    lit(",@FileExtension='json'"),
    lit(",@MultiLine='False'"),
    lit(",@IsDatePartitioned='False'"),
    lit(",@LoadType='Overwrite'"),
    lit(",@BronzeZoneNotebookPath='../Data Engineering/Bronze Zone/Batch File JSON'"),
    lit(",@SilverZoneNotebookPath='../Data Engineering/Silver Zone/Delta Load'")
    ),
  )
)

# COMMAND ----------

adbQueryList[-1].select("sql").collect()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Stored Procedures

# COMMAND ----------

# MAGIC %md
# MAGIC       (NOT YET IMPLEMENTED)
# MAGIC       CREATE PROCEDURE [dbo].[HydrateADFSQL]
# MAGIC        @ProjectName VARCHAR(255)
# MAGIC       ,@SystemName VARCHAR(255)
# MAGIC       ,@SystemOrder INT = 10
# MAGIC       ,@SystemIsActive BIT = 1
# MAGIC       ,@StageName VARCHAR(255)
# MAGIC       ,@StageIsActive BIT = 1
# MAGIC       ,@StageOrder INT = 10
# MAGIC       ,@JobName VARCHAR(255)
# MAGIC       ,@JobOrder INT = 10
# MAGIC       ,@JobIsActive BIT = 1
# MAGIC       ,@ServerName VARCHAR(200)
# MAGIC       ,@DatabaseName VARCHAR(100)
# MAGIC       ,@UserName VARCHAR(100)
# MAGIC       ,@PasswordKeyVaultSecretName VARCHAR(100)
# MAGIC       ,@SchemaName VARCHAR(100)
# MAGIC       ,@TableName VARCHAR(100)
# MAGIC       ,@StoredProcedureCall VARCHAR(MAX) = ''
# MAGIC       ,@QueryTimeoutMinutes SMALLINT = 120
# MAGIC       ,@IsolationLevel VARCHAR(100) = 'ReadCommitted'
# MAGIC       ,@DestinationContainerName VARCHAR(100)
# MAGIC       ,@DestinationFilePath VARCHAR(100)
# MAGIC       ,@DestinationEncoding VARCHAR(20) = 'UTF-8'
# MAGIC       ,@FilePattern VARCHAR(100) = 'Array of objects'
# MAGIC       ,@ADFPipelineName VARCHAR(255) = 'On Premises Database Stored Procedure to Staging'

# COMMAND ----------

# MAGIC %md
# MAGIC ## Export Files

# COMMAND ----------

def exportHelper (df, path):
  df.select("sql") \
    .repartition(1) \
    .write \
    .mode("OVERWRITE") \
    .option("header", False) \
    .option("delimiter", "|") \
    .csv(path)

  cleansePath(path, "csv")

  files = dbutils.fs.ls(path)
  file = [f for f in files if f.path[-len("csv"):] == "csv"]
  fileName = file[0].path.split("/")[-1]
  newFileName = "metadata.csv"
  renameFile(path, fileName, newFileName)

# COMMAND ----------

adfTableExportPath = "{0}/tablemetadata/adf/".format(metadataBasePath)
adbTableExportPath = "{0}/tablemetadata/adb/".format(metadataBasePath)
adfTableSampleExportPath = "{0}/samplemetadata/adf/".format(metadataBasePath)
adbTableSampleExportPath = "{0}/samplemetadata/adb/".format(metadataBasePath)
adfQueryExportPath = "{0}/querymetadata/adf/".format(metadataBasePath)
adbQueryExportPath = "{0}/querymetadata/adb/".format(metadataBasePath)

exportHelper(adfTableList[-1], adfTableExportPath)
exportHelper(adfTableSampleList[-1], adfTableSampleExportPath)
exportHelper(adfQueryList[-1], adfQueryExportPath)

exportHelper(adbTableList[-1], adbTableExportPath)
exportHelper(adbTableSampleList[-1], adbTableSampleExportPath)
exportHelper(adbQueryList[-1], adbQueryExportPath)