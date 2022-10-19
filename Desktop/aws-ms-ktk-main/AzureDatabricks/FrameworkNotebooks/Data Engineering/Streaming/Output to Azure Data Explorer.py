# Databricks notebook source
# MAGIC %md # Output to Azure Data Explorer
# MAGIC 
# MAGIC Structured Streaming to load a Delta table to an Azure Data Explorer Database/Table.
# MAGIC 
# MAGIC #### Usage
# MAGIC Supply the parameters above and run the notebook.
# MAGIC 
# MAGIC #### Prerequisites
# MAGIC * Assumes a databricks secret scope has been created with the same name as the External System.
# MAGIC * Databricks Cluster must have the following kusto libraries installed:
# MAGIC   * Maven: com.microsoft.azure.kusto:spark-kusto-connector:1.1.0
# MAGIC   * PyPi: azure-kusto-data
# MAGIC   * PyPi: azure-mgmt-kusto
# MAGIC * Azure Data Explorer should have a service principal/app registration created with owner permissions to the cluster.  Gather the applicationId, applicationSecret and TenantId and make sure they are saved into databricks the (or Key Vault) secret scope refrenced by this notebook.
# MAGIC * Table must exist in the Spark Catalog.
# MAGIC 
# MAGIC #### Details
# MAGIC To create External System secret scope, run Powershell script /ApplicationConfiguration/Powershell/Scripts/ExternalSystemSecrets/CreateExternalSecretsForAzureDataExplorer.ps1
# MAGIC 
# MAGIC https://docs.microsoft.com/en-us/azure/data-explorer/spark-connector

# COMMAND ----------

import ktk
from ktk import utilities as u
import datetime, json
from pyspark.sql.types import StructType
from pyspark.sql.functions import explode, col, unix_timestamp, expr, lit
import time

# COMMAND ----------

# MAGIC %md
# MAGIC ####Initialize

# COMMAND ----------

dbutils.widgets.text(name="stepLogGuid", defaultValue="00000000-0000-0000-0000-000000000000", label="stepLogGuid")
dbutils.widgets.text(name="stepKey", defaultValue="-1", label="stepKey")
dbutils.widgets.text(name="externalSystem", defaultValue="AzureDataExplorer", label="External System")
dbutils.widgets.dropdown(name="trigger", defaultValue="Once", choices=["Once", "Microbatch"], label="Trigger")
dbutils.widgets.text(name="maxEventsPerTrigger", defaultValue="10000", label="Max Events Per Trigger")
dbutils.widgets.text(name="interval", defaultValue="10", label="Interval Seconds")
dbutils.widgets.text(name="databaseName", defaultValue="", label="Database Name")
dbutils.widgets.text(name="tableName", defaultValue="", label="Table Name")
dbutils.widgets.text(name="outputTableName", defaultValue="", label="Output Table Name")
dbutils.widgets.text(name="stopSeconds", defaultValue="120", label="Stop after Seconds of No Activity")
dbutils.widgets.dropdown(name="destination", defaultValue="silvergeneral", choices=["silverprotected", "silvergeneral"], label="Destination")

widgets = ["stepLogGuid","stepKey","externalSystem","trigger","maxEventsPerTrigger","interval","databaseName","tableName","outputTableName","stopSeconds","destination"]
secrets = ["ADXApplicationId","ADXApplicationKey","ADXApplicationAuthorityId","ADXClusterName"]

# COMMAND ----------

snb = ktk.SingleResponsibilityNotebook(widgets, secrets)

# COMMAND ----------

checkpointPath = "{0}/checkpoint/{1}/{2}/{3}/".format(snb.basePath, snb.externalSystem, snb.databaseName, snb.tableName)
queryName = "{0}_{1}_{2} query".format(snb.externalSystem, snb.databaseName, snb.tableName)

p = {
  "checkpointPath": checkpointPath,
  "queryName": queryName
}

parameters = json.dumps(snb.mergeAttributes(p))
snb.log_notebook_start(parameters)
print("Parameters:")
snb.displayAttributes()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Get Table

# COMMAND ----------

try:
  refreshed = u.refreshTable(snb.tableName)
  if refreshed == False:
    snb.log_notebook_end(0)
    dbutils.notebook.exit("Table does not exist")
  df = spark.readStream.format("delta").table(snb.tableName)
  df.printSchema
except Exception as e:
  err = {
    "sourceName": "Output to Azure Data Explorer: Get Table",
    "errorCode": "100",
    "errorDescription": e.__class__.__name__
  }
  error = json.dumps(err)
  snb.log_notebook_error(error)
  raise(e)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Write to Azure Data Explorer

# COMMAND ----------

try:
  if(snb.trigger=="Once"):
    dfQuery = (df
      .writeStream
      .format("com.microsoft.kusto.spark.datasink.KustoSinkProvider")
      .option("kustoCluster", snb.ADXClusterName)
      .option("kustoDatabase", snb.databaseName)
      .option("kustoTable", snb.outputTableName)
      .option("kustoAADClientID", snb.ADXApplicationId)
      .option("kustoClientAADClientPassword", snb.ADXApplicationKey)
      .option("kustoAADAuthorityID", snb.ADXApplicationAuthorityId)
      .option("tableCreateOptions","CreateIfNotExist")
      .option("checkpointLocation", snb.checkpointPath)
      .outputMode("Append")
      .queryName(snb.queryName)
      .trigger(once=True)
      .start()
    )
  else:
    if(snb.trigger=="Microbatch"):
      processingTime = "{0} seconds".format(snb.interval)
      dfQuery = (df
        .writeStream
        .format("com.microsoft.kusto.spark.datasink.KustoSinkProvider")
        .option("kustoCluster", snb.ADXClusterName)
        .option("kustoDatabase", snb.databaseName)
        .option("kustoTable", snb.outputTableName)
        .option("kustoAADClientID", snb.ADXApplicationId)
        .option("kustoClientAADClientPassword", snb.ADXApplicationKey)
        .option("kustoAADAuthorityID", snb.ADXApplicationAuthorityId)
        .option("tableCreateOptions","CreateIfNotExist")
        .option("checkpointLocation", snb.checkpointPath)
        .outputMode("Append")
        .queryName(snb.queryName)
        .trigger(processingTime=processingTime)
        .start()
      )
except Exception as e:
  err = {
    "sourceName": "Output to Azure Data Explorer: Write to Azure Data Explorer",
    "errorCode": "200",
    "errorDescription": e.__class__.__name__
  }
  error = json.dumps(err)
  snb.log_notebook_error(error)
  raise(e)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Stop Inactive Streams

# COMMAND ----------

try:
  if trigger == "Microbatch":
    time.sleep(snb.stopSeconds)
    streams = [s for s in spark.streams.active if s.name in [queryName]]
    while (1==1):
      streams = [s for s in spark.streams.active if s.name in [queryName]]
      if len(streams) == 0:
        break
      for stream in streams:
        if stream.lastProgress['numInputRows'] == 0:
          stream.stop()
      time.sleep(stopSeconds)
except Exception as e:
  err = {
    "sourceName": "Output to Azure Data Explorer: Stop Inactive Streams",
    "errorCode": "300",
    "errorDescription": e.__class__.__name__
  }
  error = json.dumps(err)
  snb.log_notebook_error(error)
  raise(e)

# COMMAND ----------

snb.log_notebook_end(0)
dbutils.notebook.exit("Succeeded")
