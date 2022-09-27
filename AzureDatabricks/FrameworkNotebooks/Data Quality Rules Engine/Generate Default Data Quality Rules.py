# Databricks notebook source
# MAGIC %md
# MAGIC # Generate Default Data Quality Rules

# COMMAND ----------

# MAGIC %md
# MAGIC #### Initialize

# COMMAND ----------

# MAGIC %run "../Orchestration/Notebook Functions"

# COMMAND ----------

# MAGIC %run ../Development/Utilities

# COMMAND ----------

# MAGIC %run "../Data Quality Rules Engine/Create Data Quality Rules Schema"

# COMMAND ----------

import datetime, json
from multiprocessing.pool import ThreadPool
import great_expectations as ge
from pyspark.sql.functions import explode_outer, explode
from great_expectations.profile.basic_dataset_profiler import BasicDatasetProfiler
from great_expectations.dataset.sparkdf_dataset import SparkDFDataset
from great_expectations.render.renderer import ProfilingResultsPageRenderer, ExpectationSuitePageRenderer, ValidationResultsPageRenderer
from great_expectations.render.view import DefaultJinjaPageView

dbutils.widgets.dropdown(name="databaseCatalog", defaultValue="default", choices=["default","bronze","silvergeneral","silverprotected","goldgeneral","goldprotected","sandbox","archive"])
dbutils.widgets.dropdown(name="dataLakeZone", defaultValue="default", choices=["bronze","silver","gold","platinum","sandbox","default"])
dbutils.widgets.dropdown(name="objectType", defaultValue="TABLE", choices=["TABLE","VIEW"])
dbutils.widgets.text(name="threadPool", defaultValue="1", label="Thread Pool")
dbutils.widgets.text(name="timeoutSeconds", defaultValue="1800", label="Timeout Seconds")

databaseCatalog = dbutils.widgets.get("databaseCatalog")
dataLakeZone = dbutils.widgets.get("dataLakeZone")
objectType = dbutils.widgets.get("objectType")
threadPool = int(dbutils.widgets.get("threadPool"))
timeout = int(dbutils.widgets.get("timeoutSeconds"))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Get Object List for Database and Object Type

# COMMAND ----------

allTables = spark.catalog.listTables(dbName=databaseCatalog)
if objectType == "TABLE":
  objects = [t.name for t in allTables if '_upsert' not in t.name and t.tableType!='VIEW' and t.isTemporary==False]
else:
  objects = [t.name for t in allTables if t.tableType == 'VIEW' and t.isTemporary==False]
objects

# COMMAND ----------

# MAGIC %md
# MAGIC #### Run Data Quality Profiling and Rules Development for each Table

# COMMAND ----------

notebook = "../Data Quality Rules Engine/Data Quality Profiling and Rules Development"
pool = ThreadPool(threadPool)
argsList = []

for t in objects:
  fullyQualifiedTableName = "{0}.{1}".format(databaseCatalog, t)
  args = {
    "tableName": fullyQualifiedTableName,
    "profile": "True",
    "useGEProfilingExpectations": "True"
  }
  argsList.append(args)

pool.map(lambda args: runWithRetry(notebook, timeout, args, max_retries=0), argsList)