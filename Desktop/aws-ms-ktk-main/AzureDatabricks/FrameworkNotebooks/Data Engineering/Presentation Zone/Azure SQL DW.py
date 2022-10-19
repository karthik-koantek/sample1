# Databricks notebook source
# MAGIC %md # Load Azure SQL DW
# MAGIC 
# MAGIC 
# MAGIC #### Usage
# MAGIC Supply the parameters above and run the notebook.
# MAGIC 
# MAGIC #### Prerequisites
# MAGIC 1. Table must exist in the Spark Catalog.
# MAGIC 2. Create the staging schema in the destination SQL Server (CREATE SCHEMA staging AUTHORIZATION dbo;)
# MAGIC 3. Azure SQL Data Warehouse must have a database master key CREATE MASTER KEY ENCRYPTION BY PASSWORD = '<>';
# MAGIC 4. Blob Storage Account must exist for Polybase/intermediary to store bulk data
# MAGIC 5. All secrets used in this notebook must exist in databricks secret scopes
# MAGIC 
# MAGIC #### Details
# MAGIC 
# MAGIC https://docs.azuredatabricks.net/data/data-sources/azure/sql-data-warehouse.html

# COMMAND ----------

# MAGIC %md #### Initialize

# COMMAND ----------

import ktk
from ktk import utilities as u
import datetime, json

# COMMAND ----------

dbutils.widgets.text(name="stepLogGuid", defaultValue="00000000-0000-0000-0000-000000000000", label="stepLogGuid")
dbutils.widgets.text(name="stepKey", defaultValue="-1", label="stepKey")
dbutils.widgets.text(name="externalSystem", defaultValue="", label="External System")
dbutils.widgets.text(name="tableName", defaultValue="", label="Table Name")
dbutils.widgets.text(name="SchemaName", defaultValue="dbo", label="Schema Name")
dbutils.widgets.text(name="storedProcedureName", defaultValue="", label="Stored Procedure Name")
dbutils.widgets.text(name="numPartitions", defaultValue="16", label="Number of Partitions")


widgets = ["stepLogGuid", "stepKey", "externalSystem", "tableName", "SchemaName","storedProcedureName","numPartitions"]
secrets = ["SanctionedSQLServerName","SanctionedDatabaseName","SanctionedSQLServerLogin","SanctionedSQLServerPwd",
"SQLServerName","DatabaseName","Login","Pwd"]

# COMMAND ----------

snb = ktk.SingleResponsibilityNotebook(widgets, secrets)

# COMMAND ----------

destinationTableName = "{0}.{1}".format(snb.SchemaName, snb.tableName)

if snb.externalSystem == "internal":
  PlatinumSQLServerName = snb.SanctionedSQLServerName
  PlatinumDatabaseName = snb.SanctionedDatabaseName
  PlatinumSQLServerLogin = snb.SanctionedSQLServerLogin
  PlatinumSQLServerPwd = snb.SanctionedSQLServerPwd
else:
  PlatinumSQLServerName = snb.SQLServerName
  PlatinumDatabaseName = snb.DatabaseName
  PlatinumSQLServerLogin = snb.Login
  PlatinumSQLServerPwd = snb.Pwd

p = {
  "destinationTableName": destinationTableName,
  "PlatinumSQLServerName": PlatinumSQLServerName,
  "PlatinumDatabaseName": PlatinumDatabaseName,
  "PlatinumSQLServerLogin": PlatinumSQLServerLogin,
  "PlatinumSQLServerPwd": PlatinumSQLServerPwd
}
parameters = json.dumps(snb.mergeAttributes(p))
snb.log_notebook_start(parameters)
print("Parameters:")
snb.displayAttributes()

# COMMAND ----------

refreshed = u.refreshTable(snb.tableName)
if refreshed == False:
  snb.log_notebook_end(0)
  dbutils.notebook.exit("Table does not exist")
df = spark.table(snb.tableName)

# COMMAND ----------

jdbcUrl, connectionProperties = u.jdbcConnectionString(snb.PlatinumSQLServerName, snb.PlatinumDatabaseName, snb.PlatinumSQLServerLogin, snb.PlatinumSQLServerPwd)
storageAccountKeyConfig = "fs.azure.account.key.{0}.blob.core.windows.net".format(snb.TransientStorageAccountName)
tempDir = "wasbs://{0}@{1}.blob.core.windows.net/{2}".format("Polybase", snb.TransientStorageAccountName, snb.tableName)
spark.conf.set(storageAccountKeyConfig,snb.TransientStorageAccountKey)

# COMMAND ----------

try:
  df.write \
    .format("com.databricks.spark.sqldw") \
    .option("url", jdbcUrl) \
    .option("forwardSparkAzureStorageCredentials", "true") \
    .option("dbTable", snb.destinationTableName) \
    .option("tempDir", tempDir) \
    .option("user", snb.PlatinumSQLServerLogin) \
    .option("password", snb.PlatinumSQLServerPwd) \
    .save()
except Exception as e:
  err = {
    "sourceName" : "Azure SQL DW: Write",
    "errorCode" : "400",
    "errorDescription" : e.__class__.__name__
  }
  error = json.dumps(err)
  snb.log_notebook_error(error)
  raise(e)

# COMMAND ----------

if storedProcedureName != "":
  try:
    u.pyodbcStoredProcedure(snb.storedProcedureName, snb.PlatinumSQLServerName, snb.PlatinumDatabaseName, snb.PlatinumSQLServerLogin, snb.PlatinumSQLServerPwd)
  except Exception as e:
    err = {
      "sourceName" : "Sanctioned Zone Processing - Load SQL Data Warehouse: Execute Stored Procedure",
      "errorCode" : "500",
      "errorDescription" : e.__class__.__name__
    }
    error = json.dumps(err)
    snb.log_notebook_error(error)
    raise(e)

# COMMAND ----------

rows = df.count()
snb.log_notebook_end(rows)
dbutils.notebook.exit("Succeeded")
