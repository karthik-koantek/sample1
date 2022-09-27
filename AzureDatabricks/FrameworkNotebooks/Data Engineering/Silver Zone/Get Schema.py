# Databricks notebook source
# MAGIC %md # Get Schema

# COMMAND ----------

import ktk
from ktk import utilities as u
import json

dbutils.widgets.text(name="stepKey", defaultValue="-1", label="stepKey")
dbutils.widgets.text(name="stepLogGuid", defaultValue="00000000-0000-0000-0000-000000000000", label="stepLogGuid")
dbutils.widgets.text(name="dataPath", defaultValue="", label="Data Lake Raw Data Path")
dbutils.widgets.text(name="schemaPath", defaultValue="", label="Schema Path")
dbutils.widgets.text(name="externalSystem", defaultValue="", label="External System")
dbutils.widgets.text(name="schemaName", defaultValue="", label="Schema Name")
dbutils.widgets.text(name="tableName", defaultValue="", label="Table Name")
dbutils.widgets.text(name="samplingRatio", defaultValue=".5", label="Sampling Ratio")
dbutils.widgets.text(name="delimiter", defaultValue="", label="File Delimiter")
dbutils.widgets.text(name="hasHeader", defaultValue="False", label="Header Row")
dbutils.widgets.text(name="multiLine", defaultValue="False", label="MultiLine")

widgets = ["stepKey", "stepLogGuid","dataPath","schemaPath","externalSystem","schemaName","tableName","samplingRatio","delimiter","hasHeader","multiLine"]
secrets = ["StorageAccountName","StorageAccountKey"]

# COMMAND ----------

snb = ktk.SingleResponsibilityNotebook(widgets, secrets)

# COMMAND ----------

snb.samplingRatio = float(snb.samplingRatio)
snb.schemaPath = snb.schemaPath.replace("/schema.json","").replace("schema.json","")
snb.schemaFile = snb.schemaPath + "/schema.json"
if snb.multiLine == "True":
  ml = True
else:
  ml = False

p = {}
parameters = json.dumps(snb.mergeAttributes(p))
snb.log_notebook_start(parameters)
print("Parameters:")
snb.displayAttributes()

# COMMAND ----------

if snb.externalSystem != "internal":
  try:
    spark.conf.set(snb.StorageAccountName,snb.StorageAccountKey)
  except Exception as e:
    pass

# COMMAND ----------

try:
  if snb.delimiter != "":
    df = spark \
      .read \
      .option("InferSchema", True) \
      .option("Header", snb.hasHeader) \
      .option("delimiter", snb.delimiter) \
      .options(samplingRatio=snb.samplingRatio) \
      .csv(snb.dataPath)
  else:
    df = spark \
      .read \
      .option("InferSchema", True) \
      .option("MultiLine", ml) \
      .options(samplingRatio=snb.samplingRatio) \
      .json(snb.dataPath)
except Exception as e:
  err = {
    "sourceName": "Get Schema: Reading Source",
    "errorCode": "200",
    "errorDescription": e.__class__.__name__
  }
  error = json.dumps(err)
  snb.log_notebook_error(error)
  raise(e)

# COMMAND ----------

try:
  schema = df.schema
  schema_json = df.schema.json()
  dbutils.fs.mkdirs(snb.schemaPath)
  dbutils.fs.put(snb.schemaFile, schema_json, True)
except Exception as e:
  err = {
    "sourceName": "Get Schema: Save Schema",
    "errorCode": "400",
    "errorDescription": e.__class__.__name__
  }
  error = json.dumps(err)
  snb.log_notebook_error(error)
  raise(e)
schema

# COMMAND ----------

snb.log_notebook_end(0)
dbutils.notebook.exit("Succeeded")
