# Databricks notebook source
# MAGIC %md # Batch SQL
# MAGIC
# MAGIC
# MAGIC #### Usage
# MAGIC Supply the parameters above and run the notebook. There are five general approaches to querying SQL data via jdbc, each requiring different parameters to be supplied:
# MAGIC
# MAGIC ***Full table operations:***
# MAGIC 1. Supply *schemaName*, *tableName* (these columns are always required)
# MAGIC 2. Supply *schemaName*, *tableName*, *lowerBound*, *upperBound*, *numPartitions* (to promote better parallelism on read)
# MAGIC
# MAGIC ***Incremental/Partial table operations:***
# MAGIC 3. Supply *schemaName*, *tableName*, *windowingColumn*, *lowerDateToProcess* and *dateToProcess*. (the notebook will use your parameters to build a dynamic query via windowing similar to the following):
# MAGIC <pre><code>query = "(SELECT * FROM {0}.{1} WHERE {2} BETWEEN '{3}' AND '{4}') t1".format(schemaName, tableName, windowingColumn, lowerDateToProcess, dateToProcess)</code></pre>
# MAGIC 4. Supply *schemaName*, *tableName*, custom *pushdownQuery* (**without** a WHERE clause), *windowingColumn*, *lowerDateToProcess* and *dateToProcess*. (the notebook will use your parameters to build a dynamic query via windowing simlilar to the following):
# MAGIC <pre><code>pushdownQuery = "SELECT c1, c2, c3 FROM dbo.employees"
# MAGIC query = "({0} WHERE {1} BETWEEN '{2}' AND '{3}') t1".format(pushdownQuery, windowingColumn, lowerDateToProcess, dateToProcess)</code></pre>
# MAGIC 5. Supply *schemaName*, *tableName*, custom *pushdownQuery* (**with** a WHERE clause). (the notebook will run your query as provided):
# MAGIC <pre><code>query = "(SELECT * FROM dbo.employees WHERE LASTMODTIME >= DATEADD(DAYS, -2, SYSDATETIME())) t1"</code></pre>
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
# MAGIC * https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html
# MAGIC * https://docs.databricks.com/data/data-sources/sql-databases.html

# COMMAND ----------

# MAGIC %md ###### Initialize

# COMMAND ----------

import ktk
from ktk import utilities as u
import datetime, json

# COMMAND ----------

dbutils.widgets.text(name="stepLogGuid", defaultValue="00000000-0000-0000-0000-000000000000", label="stepLogGuid")
dbutils.widgets.text(name="stepKey", defaultValue="-1", label="stepKey")
dbutils.widgets.text(name="externalSystem", defaultValue="", label="External System")
dbutils.widgets.text(name="schemaName", defaultValue="", label="Table Schema Name")
dbutils.widgets.text(name="tableName", defaultValue="", label="Table Name")
dbutils.widgets.text(name="numPartitions", defaultValue="8", label="Number of Partitions")
dbutils.widgets.text(name="partitionColumn", defaultValue="", label="Partition Column")
dbutils.widgets.text(name="windowingColumn", defaultValue="", label="Windowing Column")
dbutils.widgets.text(name="lowerBound", defaultValue="0", label="Lower Bound")
dbutils.widgets.text(name="upperBound", defaultValue="100000", label="Upper Bound")
dbutils.widgets.text(name="lowerDateToProcess", defaultValue="", label="Lower Date to Process")
dbutils.widgets.text(name="dateToProcess", defaultValue="", label="Date to Process")
dbutils.widgets.text(name="pushdownQuery", defaultValue="", label="Query")

widgets = ["stepLogGuid","stepKey","externalSystem","schemaName","tableName","numPartitions","partitionColumn","windowingColumn","lowerBound","upperBound","lowerDateToProcess","dateToProcess","pushdownQuery"]
secrets = ["SQLServerName","DatabaseName","Login","Pwd"]

# COMMAND ----------

snb = ktk.SingleResponsibilityNotebook(widgets, secrets)

# COMMAND ----------

fullyQualifiedTableName = "[{0}].[{1}]".format(snb.schemaName, snb.tableName)

p = {
  "fullyQualifiedTableName": fullyQualifiedTableName
}

parameters = json.dumps(snb.mergeAttributes(p))
snb.log_notebook_start(parameters)
print("Parameters:")
snb.displayAttributes()

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Validation

# COMMAND ----------

if (snb.schemaName == "" and snb.tableName == "") or snb.externalSystem == "":
  err = {
    "sourceName": "Batch SQL: Validation",
    "errorCode": "100",
    "errorDescription": "Invalid Parameters supplied.  schemaName, tableName and externalSystem are required parameters."
  }
  error = json.dumps(err)
  snb.log_notebook_error(error)
  raise ValueError("Invalid Parameters supplied.  schemaName, tableName and externalSystem are required parameters.")

if snb.pushdownQuery != "":
  if snb.windowingColumn != "" and snb.lowerDateToProcess != "":
    query = "({0} WHERE {1} BETWEEN '{2}' AND '{3}') t1".format(snb.pushdownQuery, snb.windowingColumn, snb.lowerDateToProcess, snb.dateToProcess)
  else:
    query = "({0}) t1".format(snb.pushdownQuery)
else:
  if snb.windowingColumn != "" and snb.lowerDateToProcess != "":
    query = "(SELECT * FROM {0} WHERE {1} BETWEEN '{2}' AND '{3}') t1".format(snb.fullyQualifiedTableName, snb.windowingColumn, snb.lowerDateToProcess, snb.dateToProcess)
  else:
    query = snb.fullyQualifiedTableName

print("Query: {0}".format(query))

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Read Dataframe

# COMMAND ----------
try:
  jdbcUrl, connectionProperties = u.jdbcConnectionString(snb.SQLServerName, snb.DatabaseName, snb.Login, snb.Pwd)
  if snb.partitionColumn != "":
    raw_df = spark.read \
      .jdbc(url=jdbcUrl,
          table=snb.fullyQualifiedTableName,
          column=snb.partitionColumn,
          lowerBound=snb.lowerBound,
          upperBound=snb.upperBound,
          numPartitions=snb.numPartitions,
          properties=connectionProperties)
  else:
    raw_df = spark.read \
      .jdbc(url=jdbcUrl,
          table=query,
          properties=connectionProperties)
except Exception as e:
  err = {
    "sourceName": "Batch SQL: Read Dataframe",
    "errorCode": "200",
    "errorDescription": e.__class__.__name__
  }
  error = json.dumps(err)
  snb.log_notebook_error(error)
  raise(e)

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Cleanse Dataframe

# COMMAND ----------

try:
  cleansed_df = u.cleanseColumns(raw_df)
except Exception as e:
  err = {
    "sourceName": "Raw Zone Processing - Batch: Cleanse Columns",
    "errorCode": "300",
    "errorDescription": e.__class__.__name__
  }
  error = json.dumps(err)
  snb.log_notebook_error(error)
  raise(e)

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Write Dataframe to Bronze Zone

# COMMAND ----------

try:
  cleansed_df \
    .write \
    .mode("OVERWRITE") \
    .json(snb.bronzeDataPath)
except Exception as e:
  err = {
    "sourceName": "Raw Zone Processing - Batch SQL: Write to Raw Zone",
    "errorCode": "400",
    "errorDescription": e.__class__.__name__
  }
  error = json.dumps(err)
  snb.log_notebook_error(error)
  raise(e)

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Complete

# COMMAND ----------

rows = cleansed_df.count()
snb.log_notebook_end(rows)
dbutils.notebook.exit("Succeeded")

# COMMAND ----------

