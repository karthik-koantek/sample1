# Databricks notebook source
# MAGIC %md # SQL JDBC

# COMMAND ----------

import ktk
from ktk import utilities as u
import datetime, json

# COMMAND ----------

dbutils.widgets.text(name="stepLogGuid", defaultValue="00000000-0000-0000-0000-000000000000", label="stepLogGuid")
dbutils.widgets.text(name="stepKey", defaultValue="-1", label="stepKey")
dbutils.widgets.text(name="externalSystem", defaultValue="", label="External System")
dbutils.widgets.text(name="tableName", defaultValue="", label="Table Name")
dbutils.widgets.text(name="destinationSchemaName", defaultValue="dbo", label="Schema Name")
dbutils.widgets.text(name="storedProcedureName", defaultValue="", label="Stored Procedure Name")
dbutils.widgets.text(name="numPartitions", defaultValue="16", label="Number of Partitions")

widgets = ["stepLogGuid", "stepKey", "externalSystem", "tableName", "destinationSchemaName","storedProcedureName","numPartitions"]
secrets = ["SanctionedSQLServerName","SanctionedDatabaseName","SanctionedSQLServerLogin","SanctionedSQLServerPwd",
"SQLServerName","DatabaseName","Login","Pwd"]

# COMMAND ----------

snb = ktk.SingleResponsibilityNotebook(widgets, secrets)

# COMMAND ----------

destinationTableName = snb.destinationSchemaName + "." + snb.tableName.replace(".","_")
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
  snb.log_notebook_end( 0)
  dbutils.notebook.exit("Table does not exist")
df = spark.table(snb.tableName)

# COMMAND ----------

try:
  jdbcUrl, connectionProperties = u.jdbcConnectionString(snb.PlatinumSQLServerName, snb.PlatinumDatabaseName, snb.PlatinumSQLServerLogin, snb.PlatinumSQLServerPwd)
  df.repartition(int(snb.numPartitions)).write.jdbc(jdbcUrl, snb.destinationTableName, 'overwrite', connectionProperties)
except Exception as e:
  err = {
    "sourceName" : "SQL JDBC: Load SQL",
    "errorCode" : "400",
    "errorDescription" : e.__class__.__name__
  }
  error = json.dumps(err)
  snb.log_notebook_error(error)
  raise(e)

# COMMAND ----------

if snb.storedProcedureName != "":
  try:
    u.pyodbcStoredProcedure(snb.storedProcedureName, snb.PlatinumSQLServerName, snb.PlatinumDatabaseName, snb.PlatinumSQLServerLogin, snb.PlatinumSQLServerPwd)
  except Exception as e:
    err = {
      "sourceName" : "SQL JDBC: Execute Stored Procedure",
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
