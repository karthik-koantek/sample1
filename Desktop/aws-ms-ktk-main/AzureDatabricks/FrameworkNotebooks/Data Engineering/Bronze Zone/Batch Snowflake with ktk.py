# Databricks notebook source
# MAGIC %md # Batch Snowflake
# MAGIC 
# MAGIC 
# MAGIC #### Usage
# MAGIC Supply the parameters above and run the notebook. There are two general approaches to querying snowflake data by providing table Name or query along with schema.<br/>
# MAGIC Ex. <br/>
# MAGIC 
# MAGIC 1.Table name can be passed as < SchemaName.TableName > in sfTable Widget <br/>
# MAGIC Table name can be passed as < SchemaName >in sfSchema widget along with < TableName > in sfTable Widget <br/>
# MAGIC 
# MAGIC 2.Query can be passed as < select * from schema1.table1 join schema2.table2 > in Query widget<br/>
# MAGIC   
# MAGIC #### Prerequisites
# MAGIC * The supplied externalSystem must have an associated secret scope (of the same name) within Databricks with the following secrets added:
# MAGIC   * Snowflake URL
# MAGIC   * Snowflake Scope Name and Secret Name for fetching User and Password 
# MAGIC   * Snowflake Database Name
# MAGIC   * Snowflake Schema Name
# MAGIC   * Snowflake Warehouse Name
# MAGIC   * Table or Query
# MAGIC   * Bronze Table Name

# COMMAND ----------

# MAGIC %md ###### Initialize

# COMMAND ----------

import ktk
from ktk import utilities as u
import datetime, json

# COMMAND ----------

dbutils.widgets.text(name="stepLogGuid", defaultValue="00000000-0000-0000-0000-000000000000", label="stepLogGuid")
dbutils.widgets.text(name="stepKey", defaultValue="-1", label="stepKey")
dbutils.widgets.text(name="sfTable", defaultValue="", label="sfTable")
dbutils.widgets.text(name="sfSchema", defaultValue="", label="sfSchema")
dbutils.widgets.text(name="externalSystem", defaultValue="", label="External System")
dbutils.widgets.text(name="tableName", defaultValue="", label="Delta Table Name")
dbutils.widgets.text(name="Query", defaultValue="", label="Query")
dbutils.widgets.text(name="sfUrl", defaultValue="", label="Url")
dbutils.widgets.text(name="sfDatabase", defaultValue="", label="Database")
dbutils.widgets.text('schemaName',defaultValue="",label='Delta Schema')
dbutils.widgets.text(name="sfWarehouse", defaultValue="", label="Warehouse")
dbutils.widgets.text(name="Scope", defaultValue="", label="Scope")
dbutils.widgets.text(name="dateToProcess", defaultValue="", label="Date to Process")

widgets = ["stepLogGuid","stepKey","externalSystem", "sfTable", "sfSchema", "tableName","Query","sfUrl","sfDatabase","schemaName","sfWarehouse","Scope","dateToProcess"]
secrets = ["snowflakeUser","snowflakePassword"]

# COMMAND ----------

snb = ktk.SingleResponsibilityNotebook(widgets, secrets)

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Validation

# COMMAND ----------

if snb.Query=="" and snb.sfTable=="":
  err = {
    "sourceName": "Batch Snowflake: Validation",
    "errorCode": "100",
    "errorDescription": "Invalid Parameters supplied. At least one of Query and Table Name must be passed."
  }
  error = json.dumps(err)
  snb.log_notebook_error(error)
  raise ValueError("Invalid Parameters supplied. At least one of Query and Table Name must be passed.")

if snb.Query!="" and snb.sfTable!="":
  err = {
    "sourceName": "Batch Snowflake: Validation",
    "errorCode": "101",
    "errorDescription": "Invalid Parameters supplied. Query and Table both are not allowed at the same time."
  }
  error = json.dumps(err)
  snb.log_notebook_error(error)
  raise ValueError("Invalid Parameters supplied.Only one of Query and Table Name must be passed.")
  
#Incremental part needs to be added.



# COMMAND ----------


options = {
  "sfUrl": snb.sfUrl,
  "sfUser": snb.snowflakeUser,
  "sfPassword": snb.snowflakePassword,
  "sfDatabase": snb.sfDatabase,
  "sfSchema": snb.sfSchema,
  "sfWarehouse": snb.sfWarehouse
}


# COMMAND ----------

# MAGIC %md
# MAGIC ###### Read Dataframe

# COMMAND ----------

try:
  if snb.Query!="":
    df = spark.read\
    .format("snowflake") \
    .options(**options) \
    .option("query",  snb.Query) \
    .option("autopushdown", "off") \
    .load()
  else:  
    df = spark.read \
      .format("snowflake") \
      .options(**options) \
      .option("dbtable", snb.sfTable) \
      .load()
except Exception as e:
  err = {
    "sourceName": "Batch Snowflake: Read Dataframe",
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
  cleansed_df = u.cleanseColumns(df)
except Exception as e:
  err = {
    "sourceName": "Raw Zone Processing - Batch Snowflake: Cleanse Columns",
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

print(snb.bronzeDataPath)

# COMMAND ----------

try:
      cleansed_df \
     .write \
     .mode("OVERWRITE") \
     .json(snb.bronzeDataPath)
except Exception as e:
  err = {
    "sourceName": "Raw Zone Processing - Batch Snowflake: Write to Raw Zone",
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
