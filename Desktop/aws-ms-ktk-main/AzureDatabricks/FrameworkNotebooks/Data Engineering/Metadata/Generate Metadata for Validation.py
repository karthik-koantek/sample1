# Databricks notebook source
# MAGIC %md # Generate Metadata for Validation
# MAGIC
# MAGIC Generates Framework Hydration Script for Validation.
# MAGIC
# MAGIC #### Usage
# MAGIC
# MAGIC #### Prerequisites
# MAGIC
# MAGIC #### Details

# COMMAND ----------

# MAGIC %run ../../Development/Utilities

# COMMAND ----------

# MAGIC %md
# MAGIC #### Initialize

# COMMAND ----------

import json
import hashlib
import uuid
import sys, traceback
from functools import reduce
from pyspark.sql import DataFrame, Row
from collections import OrderedDict
from pyspark.sql.functions import lit, concat

dbutils.widgets.dropdown(name="databaseCatalog", defaultValue="default", choices=["default","bronze","silvergeneral","silverprotected","goldgeneral","goldprotected","sandbox","archive"])
dbutils.widgets.dropdown(name="dataLakeZone", defaultValue="default", choices=["bronze","silver","gold","platinum","sandbox","default"])
dbutils.widgets.dropdown(name="objectType", defaultValue="TABLE", choices=["TABLE","VIEW"])

databaseCatalog = dbutils.widgets.get("databaseCatalog")
dataLakeZone = dbutils.widgets.get("dataLakeZone")
objectType = dbutils.widgets.get("objectType")

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
# MAGIC #### Get Column List and generate List of Dictionary Rows

# COMMAND ----------

expectedNewOrModifiedRows2Days = 0
expectedNewOrModifiedRows6Days = 0
argsAll = []

for object in objects:
  tableOrViewName = "{0}.{1}".format(databaseCatalog, object)
  try:
    columns = spark.table(tableOrViewName).columns
    columns.sort()
    cols = ",".join(columns)
    args = {
      "objectType": objectType,
      "tableOrViewName": tableOrViewName,
      "expectedColumns": cols,
      "expectedNewOrModifiedRows2Days": expectedNewOrModifiedRows2Days,
      "expectedNewOrModifiedRows6Days": expectedNewOrModifiedRows6Days
    }
    argsAll.append(args)
  except Exception as e:
    print("{0} failed.".format(tableOrViewName))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Convert to Dataframe

# COMMAND ----------

def convert_to_row(d: dict) -> Row:
    return Row(**OrderedDict(sorted(d.items())))
dfList = []
dfList.append(sc.parallelize(argsAll).map(convert_to_row).toDF())
dfList.append(dfList[-1].withColumn("dataLakeZone", lit(dataLakeZone)).withColumn("databaseCatalog", lit(databaseCatalog)))
dfList[-1].createOrReplaceTempView("df")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Build Hydration Calls

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW HydrationCalls
# MAGIC AS
# MAGIC SELECT
# MAGIC  concat("EXEC [dbo].[HydrateValidationTest] @DataLakeZone='", dataLakeZone, "',@DatabaseCatalog='", databaseCatalog, "',@ObjectType='", objectType, "',@ObjectName='", tableOrViewName, "',@IsActive=1, @IsRestart=1,@ExpectedNewOrModifiedRows2Days=", expectedNewOrModifiedRows2Days, ",@ExpectedNewOrModifiedRows6Days=", expectedNewOrModifiedRows6Days,",@ExpectedColumns='", expectedColumns, "';") AS hydrationProcedureCall
# MAGIC FROM df

# COMMAND ----------

dfList.append(spark.sql("SELECT hydrationProcedureCall FROM HydrationCalls"))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Generate Export File

# COMMAND ----------

summaryPath = "{0}/export/validation/{1}/{2}".format(goldGeneralBasePath, databaseCatalog, objectType)
dfList[-1].repartition(1) \
    .write \
    .mode("OVERWRITE") \
    .option("header", False) \
    .option("delimiter", "|") \
    .csv(summaryPath)