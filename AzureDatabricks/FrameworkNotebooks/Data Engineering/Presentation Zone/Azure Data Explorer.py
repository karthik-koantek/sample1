# Databricks notebook source
# MAGIC %md # Azure Data Explorer
# MAGIC 
# MAGIC Batch Load to Azure Data Explorer (Kusto)
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

# MAGIC %md #### Initialize

# COMMAND ----------

import ktk
from ktk import utilities as u
import datetime, json

# COMMAND ----------

dbutils.widgets.text(name="stepLogGuid", defaultValue="00000000-0000-0000-0000-000000000000", label="stepLogGuid")
dbutils.widgets.text(name="stepKey", defaultValue="-1", label="stepKey")
dbutils.widgets.text(name="databaseName", defaultValue="", label="Database Name")
dbutils.widgets.text(name="tableName", defaultValue="table", label="Table Name")
dbutils.widgets.text(name="externalSystem", defaultValue="AzureDataExplorer", label="External System")

widgets = ["stepLogGuid", "stepKey", "databaseName", "tableName", "externalSystem"]
secrets = ["ADXApplicationId","ADXApplicationKey","ADXApplicationAuthorityId","ADXClusterName"]

# COMMAND ----------

snb = ktk.SingleResponsibilityNotebook(widgets, secrets)

# COMMAND ----------

p = {}
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

# MAGIC %md #### Load Azure Data Explorer

# COMMAND ----------

try:
  df.write \
    .format("com.microsoft.kusto.spark.datasource") \
    .option("kustoCluster", snb.ADXClusterName) \
    .option("kustoDatabase", snb.databaseName) \
    .option("kustoTable", snb.tableName) \
    .option("kustoAADClientID", snb.ADXApplicationId) \
    .option("kustoClientAADClientPassword", snb.ADXApplicationKey) \
    .option("kustoAADAuthorityID", snb.ADXApplicationAuthorityId) \
    .option("tableCreateOptions","CreateIfNotExist") \
    .mode("Append") \
    .save()
except Exception as e:
  err = {
    "sourceName" : "Azure Data Explorer: Write",
    "errorCode" : "400",
    "errorDescription" : e.__class__.__name__
  }
  error = json.dumps(err)
  snb.log_notebook_error(error)
  raise(e)

# COMMAND ----------

# MAGIC %md #### Log Completion

# COMMAND ----------

rows = df.count()
snb.log_notebook_end(rows)
dbutils.notebook.exit("Succeeded")
