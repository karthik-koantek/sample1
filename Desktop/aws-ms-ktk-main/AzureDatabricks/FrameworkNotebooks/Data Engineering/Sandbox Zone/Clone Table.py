# Databricks notebook source
# MAGIC %md # Clone Table

# COMMAND ----------

# MAGIC %md
# MAGIC #### Initialize

# COMMAND ----------

import ktk
from ktk import utilities as u
import datetime, json
from pyspark.sql.functions import current_timestamp, lit, unix_timestamp
from pyspark.sql.types import TimestampType, BooleanType

# COMMAND ----------

dbutils.widgets.text(name="stepLogGuid", defaultValue="00000000-0000-0000-0000-000000000000", label="stepLogGuid")
dbutils.widgets.text(name="stepKey", defaultValue="-1", label="stepKey")
dbutils.widgets.text(name="sourceTableName", defaultValue="", label="Source Table Name")
dbutils.widgets.dropdown(name="destination", defaultValue="sandbox", choices=["archive", "silverprotected", "silvergeneral", "goldprotected", "goldgeneral", "sandbox"], label="Destination")
dbutils.widgets.text(name="destinationRelativePath", defaultValue="", label="Destination Relative Path")
dbutils.widgets.text(name="destinationTableName", defaultValue="", label="Destination Table Name")
dbutils.widgets.text(name="appendTodaysDateToTableName", defaultValue="True", label="Append Today's Date to Table Name")
dbutils.widgets.text(name="overwriteTable", defaultValue="True", label="Overwrite Table")
dbutils.widgets.text(name="destinationTableComment", defaultValue="", label="Destination Table Comment")
dbutils.widgets.dropdown(name="cloneType", defaultValue="SHALLOW", choices=["SHALLOW", "DEEP"])
dbutils.widgets.text(name="timeTravelTimestampExpression", defaultValue="", label="Timestamp as of")
dbutils.widgets.text(name="timeTravelVersionExpression", defaultValue="", label="Version as of")
dbutils.widgets.text(name="logRetentionDurationDays", defaultValue="7", label="Log Retention Days")
dbutils.widgets.text(name="deletedFileRetentionDurationDays", defaultValue="7", label="Deleted File Retention Days")

widgets = ["stepLogGuid","stepKey","sourceTableName","destination","destinationRelativePath","destinationTableName","appendTodaysDateToTableName",
"overwriteTable","destinationTableComment","cloneType","timeTravelTimestampExpression","timeTravelVersionExpression",
"logRetentionDurationDays","deletedFileRetentionDurationDays"]
secrets = []

# COMMAND ----------

snb = ktk.SingleResponsibilityNotebook(widgets, secrets)

# COMMAND ----------

if snb.appendTodaysDateToTableName == "True":
  snb.dateToProcess = datetime.datetime.utcnow().strftime('%Y%m%d')
else:
  snb.dateToProcess = ""
destinationTableName = snb.destination + "." + snb.destinationTableName + snb.dateToProcess

dataPath = "{0}/{1}/{2}".format(snb.basePath, snb.destinationRelativePath, snb.destinationTableName.replace(".", "_"))
if snb.timeTravelTimestampExpression != "":
  timeTravelVersion = "TIMESTAMP AS OF '{0}'".format(snb.timeTravelTimestampExpression)
elif snb.timeTravelVersionExpression != "":
  timeTravelVersion = "VERSION AS OF {0}".format(snb.timeTravelVersionExpression)
else:
  timeTravelVersion = ""

p = {
  "dataPath": dataPath,
  "destinationTableName": destinationTableName,
  "timeTravelVersion": timeTravelVersion
}
parameters = json.dumps(snb.mergeAttributes(p))
snb.log_notebook_start(parameters)
print("Parameters:")
snb.displayAttributes()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Validation

# COMMAND ----------

tableExists = u.sparkTableExists(snb.sourceTableName)
if tableExists == True:
  if u.checkDeltaFormat(snb.sourceTableName) != "delta":
    errorDescription = "Table {0} is not in Delta format.".format(snb.sourceTableName)
    err = {
      "sourceName": "Clone Table: Table Validation",
      "errorCode": "100",
      "errorDescription": errorDescription
    }
    error = json.dumps(err)
    snb.log_notebook_error(error)
    raise ValueError(errorDescription)
else:
  snb.log_notebook_end(0)
  dbutils.notebook.exit("Source Table does not exist")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Clone Table

# COMMAND ----------

try:
  u.createTableClone(snb.destinationTableName, snb.cloneType, snb.sourceTableName, snb.dataPath, snb.timeTravelVersion, bool(snb.overwriteTable))
except Exception as e:
  err = {
    "sourceName": "Clone Table: Clone Table",
    "errorCode": "200",
    "errorDescription": e.__class__.__name__
  }
  error = json.dumps(err)
  snb.log_notebook_error(error)
  raise(e)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Set Table Properties

# COMMAND ----------

try:
  sql = """ALTER TABLE {0}
  SET TBLPROPERTIES (
    delta.logRetentionDuration = '{1} days',
    delta.deletedFileRetentionDuration = '{2} days',
    'comment' = '{3}'
  )
  """.format(snb.destinationTableName, snb.logRetentionDurationDays, snb.deletedFileRetentionDurationDays, snb.destinationTableComment)
  print(sql)
  spark.sql(sql)
except Exception as e:
  err = {
    "sourceName": "Clone Table: Set Table Properties",
    "errorCode": "300",
    "errorDescription": e.__class__.__name__
  }
  error = json.dumps(err)
  snb.log_notebook_error(error)
  raise(e)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Log Completion

# COMMAND ----------

snb.log_notebook_end(0)
dbutils.notebook.exit("Succeeded")