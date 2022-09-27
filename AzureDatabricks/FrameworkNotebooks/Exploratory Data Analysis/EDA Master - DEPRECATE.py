# Databricks notebook source
# MAGIC %md # EDA Master
# MAGIC
# MAGIC Run Notebook EDA for each table in the data lake to produce individual EDA_ tables. Combine all the EDA_ tables into a single one as a view, export as .csv to Summary Export Zone and to Sanctioned Zone.
# MAGIC
# MAGIC #### Usage
# MAGIC
# MAGIC #### Prerequisites
# MAGIC
# MAGIC #### Details

# COMMAND ----------

# MAGIC %md #### Initialize

# COMMAND ----------

# MAGIC %run "../Orchestration/Notebook Functions"

# COMMAND ----------

# MAGIC %run ../Development/Utilities

# COMMAND ----------

# MAGIC %scala
# MAGIC //Log Starting
# MAGIC val notebookPath = dbutils.notebook.getContext.notebookPath.get
# MAGIC val logMessage = "Starting"
# MAGIC val notebookContext = dbutils.notebook.getContext().toJson
# MAGIC log_to_framework_db_scala (notebookPath:String, logMessage:String, notebookContext:String)

# COMMAND ----------

dbutils.widgets.text(name="threadPool", defaultValue="2", label="Thread Pool")
threadPool = int(dbutils.widgets.get("threadPool"))

# COMMAND ----------

notebookName = "EDA Master"

# COMMAND ----------

# MAGIC %md #### Generate all EDA Tables

# COMMAND ----------

tables = spark.catalog.listTables()

# COMMAND ----------

nonEDATables = [
  t.name for t in tables if "eda_" not in t.name
  and "logmessage" != t.name
  and "datadictionary" != t.name
  and "topvaluecounts" != t.name
  and "_data_delta_to_upsert" not in t.name
  and t.tableType == 'EXTERNAL'
               ]

# COMMAND ----------

from multiprocessing.pool import ThreadPool
pool = ThreadPool(threadPool)
notebook = "../EDA"
timeout = 1200

pool.map(
  lambda tableName: run_with_retry(notebook, timeout, args = {"parentPipeLineExecutionLogKey": notebookExecutionLogKey, "tableName": tableName}, max_retries = 0), nonEDATables
  )

# COMMAND ----------

# MAGIC %md #### EDA Combined Table

# COMMAND ----------

from pyspark.sql.functions import col

tablesList = spark.sql("SHOW TABLES") \
  .select("tableName") \
  .where(col("isTemporary") == False) \
  .collect()

EDATables = [t.tableName for t in tablesList
          if "eda_" in t.tableName
          and "_topvaluecounts" not in t.tableName
         ]

# COMMAND ----------

DataDictionary = spark.table(EDATables[0])
EDAValueCounts = spark.table("{0}_topvaluecounts".format(EDATables[0]))

for t in range(1,len(EDATables)):
  df = spark.table(EDATables[t])
  vc = spark.table("{0}_topvaluecounts".format(EDATables[t]))

  DataDictionary = DataDictionary \
    .union(df)

  EDAValueCounts = EDAValueCounts \
    .union(vc)

# COMMAND ----------

path = "{0}/summary/EDA/datadictionary".format(adlbasepath)
DataDictionary \
    .repartition(8) \
    .write \
    .mode("OVERWRITE") \
    .option('path', path) \
    .saveAsTable("DataDictionary")

vcpath = "{0}/summary/EDA/topvaluecounts".format(adlbasepath)
EDAValueCounts \
    .repartition(8) \
    .write \
    .mode("OVERWRITE") \
    .option('path', vcpath) \
    .saveAsTable("TopValueCounts")

# COMMAND ----------

# MAGIC %md #### Export CSV

# COMMAND ----------

run_with_retry("../DataEngineering/SummaryZone/Power BI File", 600, {"tableName": "DataDictionary"})
run_with_retry("../DataEngineering/SummaryZone/Power BI File", 600, {"tableName": "TopValueCounts"})

# COMMAND ----------

# MAGIC %md #### Write to Sanctioned Zone

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE VIEW vDataDictionary
AS
SELECT
 MinimumValue
,MaximumValue
,CAST(AvgValue AS STRING) AS AvgValue
,CAST(StdDevValue AS STRING) AS StdDevValue
,DistinctCountValue
,NumberOfNulls
,NumberOfZeros
,RecordCount
,PercentNulls
,PercentZeros
,Selectivity
,TableName
,ColumnName
,ColumnDataType
FROM DataDictionary
""")

# COMMAND ----------

run_with_retry("../DataEngineering/SanctionedZone/SQL JDBC", 600, {"tableName": "vDataDictionary"})
run_with_retry("../DataEngineering/SanctionedZone/SQL JDBC", 600, {"tableName": "TopValueCounts"})

# COMMAND ----------

# MAGIC %md #### Log Completion

# COMMAND ----------

# MAGIC %scala
# MAGIC //Log Completed
# MAGIC val logMessage = "Completed"
# MAGIC val notebookContext = ""
# MAGIC log_to_framework_db_scala (notebookPath:String, logMessage:String, notebookContext:String)

# COMMAND ----------

dbutils.notebook.exit("Succeeded")