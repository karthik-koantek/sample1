# Databricks notebook source
# MAGIC %md # SQL Spark Connector 3.0
# MAGIC
# MAGIC
# MAGIC #### Usage
# MAGIC Supply the parameters above and run the notebook.
# MAGIC
# MAGIC #### Prerequisites
# MAGIC 1. Table must exist in the Spark Catalog.
# MAGIC 2. Create the destination schema in the destination SQL Server (e.g. CREATE SCHEMA staging AUTHORIZATION dbo;)
# MAGIC 3. Create an Azure Databricks library for the Spark connector as a Maven library. Use the coordinate: com.microsoft.azure:spark-mssql-connector_2.12_3.0:1.0.0-alpha.
# MAGIC
# MAGIC #### Details
# MAGIC https://github.com/microsoft/sql-spark-connector

# COMMAND ----------

import ktk
from ktk import utilities as u
from datetime import datetime
from datetime import timedelta
import datetime, json

# COMMAND ----------

dbutils.widgets.text(name="stepLogGuid", defaultValue="00000000-0000-0000-0000-000000000000", label="stepLogGuid")
dbutils.widgets.text(name="stepKey", defaultValue="-1", label="stepKey")
dbutils.widgets.text(name="externalSystem", defaultValue="", label="External System")
dbutils.widgets.text(name="tableName", defaultValue="", label="Table Name")
dbutils.widgets.text(name="destinationSchemaName", defaultValue="dbo", label="Destination Schema Name")
dbutils.widgets.text(name="destinationTableName", defaultValue="", label="Destination Table Name")
dbutils.widgets.text(name="storedProcedureName", defaultValue="", label="Stored Procedure Name")
dbutils.widgets.text(name="numPartitions", defaultValue="16", label="Number of Partitions")
dbutils.widgets.text(name="dateToProcess", defaultValue="", label="Date to Process")
dbutils.widgets.text(name="ignoreDateToProcessFilter", defaultValue="true", label="Ignore Date to Process Filter")
dbutils.widgets.dropdown(name="loadType", defaultValue="overwrite", choices=["append", "overwrite"], label="Load Type")
dbutils.widgets.text(name="truncateTableInsteadOfDropAndReplace", defaultValue="true", label="Truncate Table")
dbutils.widgets.dropdown(name="isolationLevel", defaultValue="READ_COMMITTED", choices=["READ_COMMITTED", "READ_UNCOMMITTED", "REPEATABLE_READ", "SERIALIZABLE"])

widgets = ["stepLogGuid","stepKey","externalSystem","tableName","destinationSchemaName","destinationTableName","storedProcedureName","numPartitions"
,"dateToProcess","ignoreDateToProcessFilter","loadType","truncateTableInsteadOfDropAndReplace","isolationLevel"]
secrets = ["SanctionedSQLServerName","SanctionedDatabaseName","SanctionedSQLServerLogin","SanctionedSQLServerPwd",
"SQLServerName","DatabaseName","Login","Pwd"]

# COMMAND ----------

snb = ktk.SingleResponsibilityNotebook(widgets, secrets)

# COMMAND ----------

if snb.destinationTableName == "":
  snb.destinationTableName = snb.destinationSchemaName + "." + snb.tableName.replace(".","_")
fullyQualifiedDestinationTableName = "{0}.{1}".format(snb.destinationSchemaName, snb.destinationTableName)
if snb.truncateTableInsteadOfDropAndReplace == "false":
  snb.truncateTableInsteadOfDropAndReplace = False
else:
  snb.truncateTableInsteadOfDropAndReplace = True
if snb.ignoreDateToProcessFilter == "false":
  if snb.dateToProcess == "" or snb.dateToProcess == datetime.utcnow().strftime('%Y/%m/%d'):
    deltaHistoryMinutes = 60 * 24 * 2 # last 2 days
  elif snb.dateToProcess == "YYYY/MM/*":
    deltaHistoryMinutes = 60 * (timedelta(days=int(datetime.utcnow().strftime('%d')))).days
  elif snb.dateToProcess == "YYYY/*/*":
    deltaHistoryMinutes = 60 * (datetime(int(datetime.utcnow().strftime('%Y')), 1, 1, 0, 0, 0, 0)).year
else:
  deltaHistoryMinutes = -1

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
  "fullyQualifiedDestinationTableName": fullyQualifiedDestinationTableName,
  "PlatinumSQLServerName": PlatinumSQLServerName,
  "PlatinumDatabaseName": PlatinumDatabaseName,
  "PlatinumSQLServerLogin": PlatinumSQLServerLogin,
  "PlatinumSQLServerPwd": PlatinumSQLServerPwd,
  "deltaHistoryMinutes": deltaHistoryMinutes
}
parameters = json.dumps(snb.mergeAttributes(p))
snb.log_notebook_start(parameters)
print("Parameters:")
snb.displayAttributes()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Refresh Table

# COMMAND ----------

refreshed = u.refreshTable(snb.tableName)
if refreshed == False:
  errorDescription = "Table {0} does not exist".format(snb.tableName)
  err = {
    "sourceName": "SQL Spark Connector 3.0: Refresh Table",
    "errorCode": "100",
    "errorDescription": errorDescription
  }
  error = json.dumps(err)
  snb.log_notebook_error(error)
  raise ValueError(errorDescription)
dfList = []
dfList.append(spark.table(snb.tableName))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Obtain Change Delta

# COMMAND ----------

if deltaHistoryMinutes != -1:
  try:
    print("Obtaining time travel delta")
    dfList.append(u.getTableChangeDelta(dfList[-1], snb.tableName, snb.deltaHistoryMinutes))
    if dfList[-1].count == 0:
      snb.log_notebook_end(0)
      dbutils.notebook.exit("No new or modified rows to process.")
  except Exception as e:
    err = {
      "sourceName" : "SQL Spark Connector 3.0: Obtain Change Delta",
      "errorCode" : "200",
      "errorDescription" : e.__class__.__name__
    }
    error = json.dumps(err)
    snb.log_notebook_error(error)
    raise(e)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Write to SQL Table

# COMMAND ----------

jdbcServerName = "jdbc:sqlserver://{0}".format(snb.PlatinumSQLServerName)
url = "{0};databaseName={1};".format(jdbcServerName, snb.PlatinumDatabaseName)

try:
  dfList[-1].write \
    .format("com.microsoft.sqlserver.jdbc.spark") \
    .mode(snb.loadType) \
    .option("url", url) \
    .option("dbtable", snb.fullyQualifiedDestinationTableName) \
    .option("user", snb.PlatinumSQLServerLogin) \
    .option("password", snb.PlatinumSQLServerPwd) \
    .option("mssqlIsolationLevel", snb.isolationLevel) \
    .option("truncate", snb.truncateTableInsteadOfDropAndReplace) \
    .save()
except Exception as e:
  err = {
    "sourceName" : "SQL Spark Connector 3.0: Write to SQL Table",
    "errorCode" : "300",
    "errorDescription" : e.__class__.__name__
  }
  error = json.dumps(err)
  snb.log_notebook_error(error)
  raise(e)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Execute Stored Procedure

# COMMAND ----------

if snb.storedProcedureName != "":
  try:
    u.pyodbcStoredProcedure(snb.storedProcedureName, snb.PlatinumSQLServerName, snb.PlatinumDatabaseName, snb.PlatinumSQLServerLogin, snb.PlatinumSQLServerPwd)
  except Exception as e:
    err = {
      "sourceName" : "SQL Spark Connector 3.0: Execute Stored Procedure",
      "errorCode" : "400",
      "errorDescription" : e.__class__.__name__
    }
    error = json.dumps(err)
    snb.log_notebook_error(error)
    raise(e)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Log Completion

# COMMAND ----------

rows = dfList[-1].count()
snb.log_notebook_end(rows)
dbutils.notebook.exit("Succeeded")