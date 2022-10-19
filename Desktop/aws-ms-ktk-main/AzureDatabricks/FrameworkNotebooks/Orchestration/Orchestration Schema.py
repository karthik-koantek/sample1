# Databricks notebook source
[dbutils.fs.rm(f.path, True) for f in dbutils.fs.ls("/FileStore/jars") if f.name != "maven/"]

# COMMAND ----------

# MAGIC %md
# MAGIC * Shared method to take JSON or python dict and convert to MAP<STRING,STRING> column. 
# MAGIC * INSERT Parameters, Context AS MAP<STRING,STRING> columns
# MAGIC * UPDATE Error AS MAP<STRING,STRING> column

# COMMAND ----------

# MAGIC %md
# MAGIC #### Orchestration Notebook

# COMMAND ----------

import ktk
onb = ktk.OrchestrationNotebook()
onb.displayAttributes()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Drop Tables

# COMMAND ----------

silverDataPath = onb.SilverProtectedBasePath + "/orchestration"
spark.sql("DROP TABLE IF EXISTS silverprotected.notebooklog")
dbutils.fs.rm(silverDataPath + "/notebooklog", True)

# COMMAND ----------

dbutils.fs.ls(silverDataPath)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create Tables

# COMMAND ----------

sql = """
CREATE TABLE IF NOT EXISTS silverprotected.notebooklogtest
(
 Name STRING,
 Number LONG,
 Parameters MAP<STRING,STRING>,
 Context STRING,
 Error MAP<STRING,STRING>
)
USING delta
LOCATION '{0}/notebooklogtest'
""".format(silverDataPath)
spark.sql(sql)

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE silverprotected.notebooklogtest;
# MAGIC --SELECT * FROM silverprotected.notebooklogtest_error;

# COMMAND ----------

# MAGIC %md
# MAGIC #### Single Responsibility 

# COMMAND ----------

import ktk
from ktk import utilities as u
import datetime, json

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

snb = ktk.SingleResponsibilityNotebook(widgets, secrets)

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
# MAGIC #### createMapFromDictionary

# COMMAND ----------

from pyspark.sql.functions import col, create_map, lit
from itertools import chain
import json

def createMapFromDictionary(dictionary_or_string):
  if isinstance(dictionary_or_string, str):
    dictionary_or_string = json.loads(dictionary_or_string)
  return create_map([lit(x) for x in chain(*dictionary_or_string.items())])

# COMMAND ----------

parametersMap = createMapFromDictionary(json.loads(parameters))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Insert Test

# COMMAND ----------

l = [('Eddie', 1)]
df = spark.createDataFrame(l)
df = df.withColumn("Parameters", parametersMap)
df = df.withColumn("Context", lit(snb.context))

# COMMAND ----------

df.createOrReplaceTempView("df")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM silverprotected.notebooklogtest

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO silverprotected.notebooklogtest (Name, Number, Parameters, Context, Error) 
# MAGIC SELECT _1, _2, Parameters, Context, NULL FROM df

# COMMAND ----------

# MAGIC %md
# MAGIC #### Log Error Test

# COMMAND ----------

import json 
err = {
    "sourceName": "Raw Zone Processing - Batch SQL: Write to Raw Zone",
    "errorCode": "400",
    "errorDescription": "e.__class__.__name__"
  }
error = json.dumps(err)
#snb.log_notebook_error(error)
json.loads(error)

# COMMAND ----------

errorMap = createMapFromDictionary(error)

# COMMAND ----------

dfList = []
dfList.append(spark.createDataFrame([(snb.notebookLogGuid)],"STRING"))
dfList.append(dfList[-1].withColumnRenamed("value","notebookLogGuid"))
dfList.append(dfList[-1].withColumn("Error", errorMap))
dfList[-1].createOrReplaceTempView("errorDF")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM errorDF

# COMMAND ----------

l = [('Eddie', 1)]
df = spark.createDataFrame(l)
df = df.withColumn("Error", errorMap)
df.createOrReplaceTempView("df")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM df

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO silverprotected.notebooklogtest AS n
# MAGIC USING df ON df._1 = n.Name
# MAGIC WHEN MATCHED THEN UPDATE SET n.Error = df.Error

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM silverprotected.notebooklogtest

# COMMAND ----------

# MAGIC %md
# MAGIC #### Log End Test

# COMMAND ----------

rows = 10

dfList = []
dfList.append(spark.createDataFrame([(snb.notebookLogGuid, rows)]))
dfList.append(dfList[-1].withColumnRenamed("_1","notebookLogGuid"))
dfList.append(dfList[-1].withColumnRenamed("_2","rows"))
dfList[-1].createOrReplaceTempView("endDF")
      

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM endDF
