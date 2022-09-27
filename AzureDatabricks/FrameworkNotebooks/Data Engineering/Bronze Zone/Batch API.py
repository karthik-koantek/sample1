# Databricks notebook source
# MAGIC %md # Batch API

# COMMAND ----------

# MAGIC %md #### Initialize

# COMMAND ----------

#%pip install loguru

# COMMAND ----------

import ktk
from ktk import utilities as u
import datetime, json
import sys, os

# COMMAND ----------

#these will be passed in from configuration (orchestration metadata or parent notebooks)
dbutils.widgets.text(name="stepLogGuid", defaultValue="00000000-0000-0000-0000-000000000000", label="stepLogGuid")
dbutils.widgets.text(name="stepKey", defaultValue="-1", label="stepKey")
dbutils.widgets.text(name="externalSystem", defaultValue="", label="externalSystem")
dbutils.widgets.text(name="classPath", defaultValue="", label="classPath")
dbutils.widgets.text(name="classImportFileName", defaultValue="OutreachAPI", label="classImportFileName")
dbutils.widgets.text(name="instantiateAPICall", defaultValue="", label="Instantiate API Class")
dbutils.widgets.text(name="APICall", defaultValue="", label="API CAll")
dbutils.widgets.text(name="destinationFilePath", defaultValue="", label="Destination File Path")
dbutils.widgets.text(name="isInitialLoad", defaultValue="False", label="Is Initial Load")

#add all notebook widgets to this widgets list
widgets = ["stepLogGuid","stepKey","externalSystem","classPath","classImportFileName","instantiateAPICall", "APICall","destinationFilePath","isInitialLoad"]

#add secret names to this list.  Note that this requires widget "externalSystem", and these secrets must exist under secret scope of the value of externalSystem.  
secrets = ["ClientId", "ClientSecret","accessToken","refreshToken"]

# COMMAND ----------

snb = ktk.SingleResponsibilityNotebook(widgets, secrets)

# COMMAND ----------

# Configure additional variables as required.  Add to p dictionary so that they are merged into the snb object.
p = {}

parameters = json.dumps(snb.mergeAttributes(p))
snb.log_notebook_start(parameters)
print("Parameters:")
snb.displayAttributes()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Validation

# COMMAND ----------

#Pattern for validation.  Validate input parameters and other conditions and raise error for any conditions not supported
if (snb.classPath == "" or snb.classImportFileName == "" or snb.instantiateAPICall == "" or snb.APICall == ""):
  err = {
    "sourceName": "Batch API: Validation",
    "errorCode": "100",
    "errorDescription": "Invalid Parameters supplied.  classPath, classImportFileName, instantiateAPICall, APICall are required parameters."
  }
  error = json.dumps(err)
  snb.log_notebook_error(error)
  raise ValueError("Invalid Parameters supplied.  classPath, classImportFileName, instantiateAPICall, APICall are required parameters.")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Import Class

# COMMAND ----------

try:
  sys.path.append(os.path.abspath(snb.classPath))
  import_command = "from {0} import *".format(snb.classImportFileName)
  exec(import_command)
except Exception as e:
  err = {
    "sourceName": "Batch API: Import Class",
    "errorCode": "200",
    "errorDescription": e.__class__.__name__
  }
  error = json.dumps(err)
  snb.log_notebook_error(error)
  raise(e)

# COMMAND ----------

# MAGIC %md #### Instantiate

# COMMAND ----------

snb.instantiateAPICall

# COMMAND ----------

try:
  exec(snb.instantiateAPICall)
except Exception as e:
  err = {
    "sourceName": "Batch API: Instantiate Class",
    "errorCode": "300",
    "errorDescription": e.__class__.__name__
  }
  error = json.dumps(err)
  snb.log_notebook_error(error)
  raise(e)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Call

# COMMAND ----------

try:
  exec(snb.APICall)
except Exception as e:
  err = {
    "sourceName": "Batch API: Call",
    "errorCode": "400",
    "errorDescription": e.__class__.__name__
  }
  error = json.dumps(err)
  snb.log_notebook_error(error)
  raise(e)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Save to Transient Zone

# COMMAND ----------

if snb.classImportFileName not in ["OutreachAPI"]:
  if (('bulk_export_activities' not in snb.APICall) and ('bulk_export_leads' not in snb.APICall)):
    try:
      jsondata=json.dumps(data)
      recordsCount=len(data)
      dbutils.fs.put(snb.destinationFilePath, jsondata, True)
    except Exception as e:
      err = {
        "sourceName": "Batch API: Save to Transient Zone",
        "errorCode": "500",
        "errorDescription": e.__class__.__name__
      }
      error = json.dumps(err)
      snb.log_notebook_error(error)
      raise(e)

# COMMAND ----------

# MAGIC %md #### Log Completion

# COMMAND ----------

#collect row count from the above save and set rows
rows=0
snb.log_notebook_end(rows)
dbutils.notebook.exit("Succeeded")
