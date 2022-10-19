# Databricks notebook source
# MAGIC %md # Batch File JSON (AppInsights)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Initialize

# COMMAND ----------

import ktk
from ktk import utilities as u
import datetime, json
import fnmatch
from pyspark.sql.types import StructType

# COMMAND ----------

dbutils.widgets.text(name="stepLogGuid", defaultValue="00000000-0000-0000-0000-000000000000", label="stepLogGuid")
dbutils.widgets.text(name="stepKey", defaultValue="-1", label="stepKey")
dbutils.widgets.text(name="externalSystem", defaultValue="", label="External System")
dbutils.widgets.text(name="externalDataPath", defaultValue="", label="External Data Path")
dbutils.widgets.text(name="numPartitions", defaultValue="8", label="Number of Partitions")
dbutils.widgets.text(name="schemaName", defaultValue="", label="Schema Name")
dbutils.widgets.text(name="tableName", defaultValue="", label="Table Name")
dbutils.widgets.text(name="dateToProcess", defaultValue="", label="Date to Process")
dbutils.widgets.text(name="fileExtension", defaultValue="", label="File Extension")
dbutils.widgets.text(name="multiLine", defaultValue="False", label="Multiline")
dbutils.widgets.text(name="dateDelimiter", defaultValue="-", label="Date Delimiter")

widgets =  ["stepLogGuid" ,"stepKey" ,"externalSystem" ,"externalDataPath" ,"numPartitions" ,"schemaName" ,"tableName" ,"dateToProcess" ,"fileExtension" ,"multiLine", "dateDelimiter"]
secrets = ["StorageAccountName","StorageAccountKey","BasePath"]

# COMMAND ----------

snb = ktk.SingleResponsibilityNotebook(widgets, secrets)

# COMMAND ----------

if snb.externalSystem == "internal":
  snb.StorageAccountName = ""
  snb.StorageAccountKey = ""
  snb.BasePath = ""
else:
  spark.conf.set(snb.StorageAccountName,snb.StorageAccountKey)

validateDataPath = "{0}/{1}".format(snb.BasePath, snb.externalDataPath)
fullExternalDataPath = "{0}/{1}/{2}/*/*.{3}".format(snb.BasePath, snb.externalDataPath, snb.dateToProcess, snb.fileExtension)
schemaPath = "{0}/schemas/appinsights/{1}".format(snb.BronzeBasePath, snb.schemaName)
schemaFile = schemaPath + "/schema.json"

p = {
  "validateDataPath": validateDataPath,
  "fullExternalDataPath": fullExternalDataPath,
  "schemaPath": schemaPath,
  "schemaFile": schemaFile
}

parameters = json.dumps(snb.mergeAttributes(p))
snb.log_notebook_start(parameters)
print("Parameters:")
snb.displayAttributes()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Validate External Data Path

# COMMAND ----------

try:
  found = dbutils.fs.ls(snb.validateDataPath)
  match = "*/" + snb.dateToProcess.replace("*","??") + "/*"
  filtered = fnmatch.filter([f[0] for f in found], match)
  print(filtered)
  if len(filtered) == 0:
    err = {
    "sourceName" : "Batch File JSON (AppInsights): Validate External Data Path",
    "errorCode" : "100",
    "errorDescription" : "No files were found to process in directory".format(snb.validateDataPath)
    }
    error = json.dumps(err)
    snb.log_notebook_error(error)
    snb.log_notebook_end(0)
    dbutils.notebook.exit("No {0} files to process".format(snb.fileExtension))
except Exception as e:
  err = {
    "sourceName" : "Batch File JSON (AppInsights): Validate External Data Path",
    "errorCode" : "100",
    "errorDescription" : e.__class__.__name__
  }
  error = json.dumps(err)
  snb.log_notebook_error(error)
  snb.log_notebook_end(0)
  raise(e)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Read Schema

# COMMAND ----------

try:
  head = dbutils.fs.head(snb.schemaFile, 256000)
except Exception as e:
  err = {
    "sourceName": "Batch File JSON (AppInsights): Read Schema",
    "errorCode": "100",
    "errorDescription": "Schema was not found in directory: {0}".format(snb.schemaFile)
  }
  error = json.dumps(err)
  snb.log_notebook_error(error)
  raise(e)

schema = StructType.fromJson(json.loads(head))
schema

# COMMAND ----------

# MAGIC %md
# MAGIC #### Read Dataframe

# COMMAND ----------

try:
  raw_df = spark.read \
    .schema(schema) \
    .option("badRecordsPath", snb.badRecordsPath) \
    .option("MultiLine", snb.multiLine) \
    .json(snb.validateDataPath) \
    .dropDuplicates()
except Exception as e:
  err = {
    "sourceName" : "Batch File JSON: Read Dataframe",
    "errorCode" : "200",
    "errorDescription" : e.__class__.__name__
  }
  error = json.dumps(err)
  snb.log_notebook_error(error)
  raise(e)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Validate Dataframe

# COMMAND ----------

from pyspark.sql.types import TimestampType, StringType
expected = [date.split("/")[-2] for date in filtered]
expectedDF = spark.createDataFrame(expected, StringType())
expectedDF.createOrReplaceTempView("expected")
raw_df.createOrReplaceTempView("validate")
sql = """
SELECT *
FROM expected
LEFT JOIN
(
  SELECT
    date_format(CAST(context.data.eventTime AS timestamp), "yyyy-MM-dd") AS date
    ,COUNT(1) AS rowcount
  FROM validate
  GROUP BY date
) actual ON actual.date = expected.value
ORDER BY expected.value
"""
validate = spark.sql(sql).collect()
validate

# COMMAND ----------

emptyOrMissing = [v for v in validate if v[1] == None or v[2] == 0]
print(emptyOrMissing)
if len(emptyOrMissing) > 0:
  errorDescription = "Not all data was read from the external data path."
  err = {
    "sourceName" : "Batch File JSON (AppInsights): Validate Dataframe",
    "errorCode" : "300",
    "errorDescription" : errorDescription
  }
  error = json.dumps(err)
  snb.log_notebook_error(error)
  raise ValueError(errorDescription)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Cleanse Columns

# COMMAND ----------

try:
  cleansed_df = u.cleanseColumns(raw_df)
except Exception as e:
  err = {
    "sourceName" : "Batch File JSON (AppInsights): Cleanse Columns",
    "errorCode" : "300",
    "errorDescription" : e.__class__.__name__
  }
  error = json.dumps(err)
  snb.log_notebook_error(error)
  raise(e)

# COMMAND ----------

try:
  cleansed_df \
    .repartition(int(snb.numPartitions)) \
    .write \
    .mode("OVERWRITE") \
    .json(snb.bronzeDataPath)
except Exception as e:
  err = {
    "sourceName" : "Batch File JSON (AppInsights): Write Data to Raw Zone",
    "errorCode" : "400",
    "errorDescription" : e.__class__.__name__
  }
  error = json.dumps(err)
  snb.log_notebook_error(error)
  raise(e)

# COMMAND ----------

try:
  dbutils.fs.ls(snb.badRecordsPath)
  errorDescription = "Rows were written to badRecordsPath: {0} for table {1}.{2}.".format(snb.badRecordsPath, snb.schemaName, snb.tableName)
  err = {
    "sourceName" : "Batch File JSON (AppInsights): Bad Records",
    "errorCode" : "500",
    "errorDescription" : errorDescription
  }
  error = json.dumps(err)
  snb.log_notebook_error(error)
  raise ValueError(errorDescription)
except:
  print("success")

# COMMAND ----------

# MAGIC %md #### Log Completion

# COMMAND ----------

rows = cleansed_df.count()
snb.log_notebook_end(rows)
dbutils.notebook.exit("Succeeded")