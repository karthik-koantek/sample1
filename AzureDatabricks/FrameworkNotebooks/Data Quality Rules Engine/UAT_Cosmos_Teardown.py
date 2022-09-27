# Databricks notebook source
# MAGIC %md
# MAGIC # UAT_Cosmos_Teardown

# COMMAND ----------

# MAGIC %md
# MAGIC #### Initialize

# COMMAND ----------

import ktk
from ktk import utilities as u
import json

# COMMAND ----------

nb = ktk.Notebook()
print("Parameters:")
nb.displayAttributes()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Teardown

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS silverprotected.cosmosdb_cosmosdbingest_usecases;
# MAGIC DROP TABLE IF EXISTS bronze.cosmosdb_cosmosdbingest_usecases_staging;

# COMMAND ----------

silverPath = "{0}/CosmosDB/cosmosdbingest/usecases".format(nb.SilverProtectedBasePath)
dbutils.fs.rm(silverPath, True)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Cosmos DB Database and Container

# COMMAND ----------

cosmosEndpoint = dbutils.secrets.get(scope="CosmosDB", key="EndPoint")
cosmosMasterKey = dbutils.secrets.get(scope="CosmosDB", key="MasterKey")
cosmosDatabaseName = dbutils.secrets.get(scope="CosmosDB", key="Database")
cosmosContainerName = "usecases"

cfg = {
  "spark.cosmos.accountEndpoint" : cosmosEndpoint,
  "spark.cosmos.accountKey" : cosmosMasterKey,
  "spark.cosmos.database" : cosmosDatabaseName,
  "spark.cosmos.container" : cosmosContainerName,
}

# COMMAND ----------

# Configure Catalog Api to be used
spark.conf.set("spark.sql.catalog.cosmosCatalog", "com.azure.cosmos.spark.CosmosCatalog")
spark.conf.set("spark.sql.catalog.cosmosCatalog.spark.cosmos.accountEndpoint", cosmosEndpoint)
spark.conf.set("spark.sql.catalog.cosmosCatalog.spark.cosmos.accountKey", cosmosMasterKey)

# create a cosmos database using catalog api
spark.sql("CREATE DATABASE IF NOT EXISTS cosmosCatalog.{};".format(cosmosDatabaseName))

# create a cosmos container using catalog api
spark.sql("CREATE TABLE IF NOT EXISTS cosmosCatalog.{}.{} using cosmos.oltp TBLPROPERTIES(partitionKeyPath = '/usecaseId', manualThroughput = '400')".format(cosmosDatabaseName, cosmosContainerName))
