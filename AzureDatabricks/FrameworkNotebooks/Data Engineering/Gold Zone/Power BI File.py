# Databricks notebook source
# MAGIC %md # Power BI File

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
dbutils.widgets.text(name="tableName", defaultValue="", label="Table Name")
dbutils.widgets.text(name="cleansePath", defaultValue="1", label="Cleanse Path")
dbutils.widgets.text(name="renameFile", defaultValue="1", label="Rename File")
dbutils.widgets.dropdown(name="fileFormat", defaultValue="csv", choices=["csv", "json"], label="Format")
dbutils.widgets.dropdown(name="destination", defaultValue="goldgeneral", choices=["goldprotected", "goldgeneral"], label="Destination")

widgets = ["stepLogGuid", "stepKey","tableName", "cleansePath","renameFile","fileFormat","destination"]
secrets = []

# COMMAND ----------

snb = ktk.SingleResponsibilityNotebook(widgets, secrets)

# COMMAND ----------

goldPath = "{0}/export/{1}".format(snb.basePath, snb.tableName)

p = {
  "goldPath": goldPath
}
parameters = json.dumps(snb.mergeAttributes(p))
snb.log_notebook_start(parameters)
print("Parameters:")
snb.displayAttributes()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Validation

# COMMAND ----------

refreshed = u.refreshTable(snb.tableName)
if refreshed == False:
  snb.log_notebook_end(0)
  dbutils.notebook.exit("Table does not exist")
df = spark.table(snb.tableName)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Write to Gold Zone

# COMMAND ----------

try:
  if snb.fileFormat == "csv":
    df.repartition(1) \
      .write \
      .mode("OVERWRITE") \
      .option("header", True) \
      .csv(snb.goldPath)
  else:
    df.repartition(1) \
      .write \
      .mode("OVERWRITE") \
      .json(snb.goldPath)
except Exception as e:
  err = {
    "sourceName" : "Power BI File: Write to Summary Zone",
    "errorCode" : "400",
    "errorDescription" : e.__class__.__name__
  }
  error = json.dumps(err)
  snb.log_notebook_error(error)
  raise(e)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Cleanse Path

# COMMAND ----------

try:
  if snb.cleansePath == "1":
    u.cleansePath(snb.goldPath, snb.fileFormat)
except Exception as e:
  err = {
    "sourceName" : "Power BI File: Cleanse Path",
    "errorCode" : "400",
    "errorDescription" : e.__class__.__name__
  }
  error = json.dumps(err)
  snb.log_notebook_error(error)
  raise(e)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Rename File

# COMMAND ----------

try:
  if snb.renameFile == "1":
    files = dbutils.fs.ls(snb.goldPath)
    pbifile = [f for f in files if f.path[-len(snb.fileFormat):] == snb.fileFormat]
    fileName = pbifile[0].path.split("/")[-1]
    newFileName = "{0}.{1}".format(snb.tableName, snb.fileFormat)
    u.renameFile(snb.goldPath, fileName, newFileName)
except Exception as e:
  err = {
    "sourceName" : "Power BI File: Rename File",
    "errorCode" : "400",
    "errorDescription" : e.__class__.__name__
  }
  error = json.dumps(err)
  snb.log_notebook_error(error)
  raise(e)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Log Completion

# COMMAND ----------

rows = df.count()
snb.log_notebook_end(rows)
dbutils.notebook.exit("Succeeded")