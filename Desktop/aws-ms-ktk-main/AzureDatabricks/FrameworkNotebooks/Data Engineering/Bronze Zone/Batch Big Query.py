# Databricks notebook source
# MAGIC %md # Batch Big Query
# MAGIC 
# MAGIC 
# MAGIC #### Usage
# MAGIC Supply the parameters above and run the notebook. Table name is an optional field and to be used only when you want to load a specific table.
# MAGIC   
# MAGIC #### Prerequisites
# MAGIC * The supplied externalSystem must have an associated secret scope (of the same name) within Databricks with the following secrets added:
# MAGIC   * credentials
# MAGIC   * accountEmail
# MAGIC   * privateKey
# MAGIC   * privateKeyId
# MAGIC   
# MAGIC Note: Provide only the actual table name. Project name and schema name are appended to table in the code.

# COMMAND ----------

import ktk
from ktk import utilities as u
import datetime, json

# COMMAND ----------

dbutils.widgets.text(name="stepLogGuid", defaultValue="00000000-0000-0000-0000-000000000000", label="stepLogGuid")
dbutils.widgets.text(name="stepKey", defaultValue="-1", label="stepKey")
dbutils.widgets.text(name="externalSystem", defaultValue="", label="External System")
dbutils.widgets.text(name="bqParentProjectName", defaultValue="", label="BQ Parent Project Name")
dbutils.widgets.text(name="bqProjectName", defaultValue="", label="BQ Project Name")
dbutils.widgets.text(name="schemaName", defaultValue="", label="Schema Name") # datasetId for bigquery
dbutils.widgets.text(name="tableName", defaultValue="", label="Table Name")

# if dbutils.widgets.get("tableName") == "":
#     print("I'm here")
#     today = datetime.date.today() - datetime.timedelta(days=2)
#     tableName = dbutils.widgets.get("bqParentProjectName") + "." + dbutils.widgets.get("schemaName")+ ".ga_sessions_" + str(today).replace("-","")
#     dbutils.widgets.remove(name="tableName")
#     dbutils.widgets.text(name="tableName", defaultValue=tableName ,label="Table Name")
#     print(tableName)
    
dbutils.widgets.text(name="numPartitions", defaultValue="8", label="Number of Partitions")
dbutils.widgets.text(name="partitionColumn", defaultValue="", label="Partition Column")
dbutils.widgets.text(name="windowingColumn", defaultValue="", label="Windowing Column")
dbutils.widgets.text(name="lowerBound", defaultValue="0", label="Lower Bound")
dbutils.widgets.text(name="upperBound", defaultValue="100000", label="Upper Bound")
dbutils.widgets.text(name="lowerDateToProcess", defaultValue="", label="Lower Date to Process")
dbutils.widgets.text(name="dateToProcess", defaultValue="", label="Date to Process")

widgets = ["stepLogGuid","stepKey","externalSystem","bqParentProjectName","bqProjectName","schemaName","tableName","numPartitions","partitionColumn","lowerDateToProcess","dateToProcess"]
secrets = ["credentials","accountEmail","privateKey","privateKeyId"]

# COMMAND ----------

snb = ktk.SingleResponsibilityNotebook(widgets, secrets)

# COMMAND ----------

# if snb.tableName == "":
#   days_to_subtract = 1
#   today = datetime.date.today() - datetime.timedelta(days=days_to_subtract)
#   snb.tableName = snb.bqParentProjectName + "." + snb.schemaName+ ".ga_sessions_" + str(today).replace("-","")

fullyQualifiedTableName = snb.tableName

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

if snb.bqParentProjectName == "" or snb.bqProjectName == "" or snb.schemaName== "" or snb.tableName == "" or snb.externalSystem == "":
  err = {
    "sourceName": "Batch Big Query: Validation",
    "errorCode": "100",
    "errorDescription": "Invalid Parameters supplied.  bqParentProjectName, bqProjectName, schemaName, tableName and externalSystem are required parameters."
  }
  error = json.dumps(err)
  snb.log_notebook_error(error)
  raise ValueError("Invalid Parameters supplied.  bqParentProjectName, bqProjectName, schemaName, tableName and externalSystem are required parameters.")

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Read Dataframe

# COMMAND ----------

try:
  spark.conf.set("credentials", snb.credentials)
  spark.conf.set("spark.hadoop.fs.gs.auth.service.account.email", snb.accountEmail)
  spark.conf.set("spark.hadoop.fs.gs.project.id", snb.bqParentProjectName)
  spark.conf.set("spark.hadoop.google.cloud.auth.service.account.enable","true")
  spark.conf.set("spark.hadoop.fs.gs.auth.service.account.private.key", snb.privateKey)
  spark.conf.set("spark.hadoop.fs.gs.auth.service.account.private.key.id", snb.privateKeyId)
  tableName = f"{snb.schemaName}.{snb.tableName}"
  raw_df = spark.read.format("bigquery").option("table", tableName).option("project", snb.bqProjectName).option("parentProject", snb.bqParentProjectName).load()
except Exception as e:
  err = {
    "sourceName": "Batch Big Query: Read Dataframe",
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
    .repartition(int(snb.numPartitions)) \
    .write \
    .mode("OVERWRITE") \
    .json(path=snb.bronzeDataPath, ignoreNullFields=False)
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
