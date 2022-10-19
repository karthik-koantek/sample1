# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest XML File
# MAGIC
# MAGIC ###### Prerequisites:
# MAGIC * This XML connector requires the external source to be a path to an existing Mount Point or internal DBFS storage (anything that does not require direct authentication)
# MAGIC
# MAGIC ###### Dependencies:
# MAGIC * Cluster or job must have this maven library installed: com.databricks:spark-xml_2.12:0.10.0
# MAGIC
# MAGIC ###### Details:
# MAGIC https://github.com/databricks/spark-xml

# COMMAND ----------

# MAGIC %md
# MAGIC #### Initialize

# COMMAND ----------

import ktk
from ktk import utilities as u
import datetime, json

# COMMAND ----------

dbutils.widgets.text(name="stepLogGuid", defaultValue="00000000-0000-0000-0000-000000000000", label="stepLogGuid")
dbutils.widgets.text(name="stepKey", defaultValue="-1", label="stepKey")
dbutils.widgets.text(name="externalSystem", defaultValue="", label="External System")
dbutils.widgets.text(name="externalDataPath", defaultValue="", label="External Data Path")
dbutils.widgets.text(name="schemaName", defaultValue="", label="Schema Name")
dbutils.widgets.text(name="tableName", defaultValue="", label="Table Name")
dbutils.widgets.text(name="rowTag", defaultValue="result", label="Row Tag")
dbutils.widgets.text(name="rootTag", defaultValue="", label="Root Tag")
dbutils.widgets.text(name="numPartitions", defaultValue="8", label="Number of Partitions")
dbutils.widgets.text(name="dateToProcess", defaultValue="", label="Date to Process")
dbutils.widgets.text(name="fileExtension", defaultValue="xml", label="File Extension")

widgets = ["stepLogGuid", "stepKey", "externalSystem", "externalDataPath", "schemaName", "tableName", "rowTag", "rootTag", "numPartitions", "dateToProcess", "fileExtension"]
secrets = ["StorageAccountName","StorageAccountKey","BasePath"]

# COMMAND ----------

snb = ktk.SingleResponsibilityNotebook(widgets, secrets)

# COMMAND ----------

if snb.externalSystem == "internal" and "mnt/" in snb.externalDataPath:
  snb.StorageAccountName = ""
  snb.StorageAccountKey = ""
  snb.BasePath = ""
else:
  sc._jsc.hadoopConfiguration().set(snb.StorageAccountName,snb.StorageAccountKey)

fullExternalDataPath = "{0}/{1}".format(snb.BasePath, snb.externalDataPath)

p = {
  "fullExternalDataPath": fullExternalDataPath
}

parameters = json.dumps(snb.mergeAttributes(p))
snb.log_notebook_start(parameters)
print("Parameters:")
snb.displayAttributes()

# COMMAND ----------

validatedExternalDataPath = u.pathHasData(snb.fullExternalDataPath, snb.fileExtension)
if validatedExternalDataPath == "":
  snb.log_notebook_end(0)
  dbutils.notebook.exit("No {0} files to process".format(snb.fileExtension))

# COMMAND ----------

try:
  raw_df = spark.read \
    .format("xml") \
    .option("rowTag", snb.rowTag) \
    .option("rootTag", snb.rootTag) \
    .load(snb.fullExternalDataPath)
except Exception as e:
  err = {
    "sourceName": "Batch File XML: Read Dataframe",
    "errorCode": "200",
    "errorDescription": e.__class__.__name__
  }
  error = json.dumps(err)
  snb.log_notebook_error(error)
  raise(e)

# COMMAND ----------

try:
  cleansed_df = u.cleanseColumns(raw_df)
except Exception as e:
  err = {
    "sourceName" : "Batch File XML: Cleanse Columns",
    "errorCode" : "300",
    "errorDescription" : e.__class__.__name__
  }
  error = json.dumps(err)
  snb.log_notebook_error(error)
  raise(e)

# COMMAND ----------

try:
  cleansed_df \
    .repartition(8) \
    .write \
    .mode("OVERWRITE") \
    .json(snb.bronzeDataPath)
except Exception as e:
  err = {
    "sourceName": "Batch File XML: Write Data to Raw Zone",
    "errorCode": "400",
    "errorDescription": e.__class__.__name__
  }
  error = json.dumps(err)
  snb.log_notebook_error(error)
  raise(e)

# COMMAND ----------

rows = cleansed_df.count()
snb.log_notebook_end(0)
dbutils.notebook.exit("Succeeded")