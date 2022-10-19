# Databricks notebook source
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
dbutils.widgets.text(name="externalSystem", defaultValue="outreach", label="externalSystem")
dbutils.widgets.text(name="classPath", defaultValue="/Workspace/Repos/7ddf3fa1-ce34-4d63-8a5d-548d48e4b6f8/NintexFeature/ApplicationConfiguration/Clients/Outreach/", label="classPath")
dbutils.widgets.text(name="classImportFileName", defaultValue="OutreachAPI", label="classImportFileName")
dbutils.widgets.text(name="instantiateAPICall", defaultValue="api = OutreachAPI(snb.ClientId, snb.ClientSecret, authorization_code=snb.AuthorizationCode)", label="Instantiate API Class")
dbutils.widgets.text(name="AuthorizationCode", defaultValue="ULx5An8WzP6shlWwmvOCzfPLEuBOhTUQP-jWNzB5sNA", label="Authorization Code")


#add all notebook widgets to this widgets list
widgets = ["stepLogGuid","stepKey","externalSystem","classPath","classImportFileName","instantiateAPICall","AuthorizationCode"]

#add secret names to this list.  Note that this requires widget "externalSystem", and these secrets must exist under secret scope of the value of externalSystem.  
secrets = ["ClientId", "ClientSecret"]

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
if (snb.classPath == "" or snb.classImportFileName == "" or snb.instantiateAPICall == ""):
  err = {
    "sourceName": "Batch API: Validation",
    "errorCode": "100",
    "errorDescription": "Invalid Parameters supplied.  classPath, classImportFileName, instantiateAPICall, APICall are required parameters."
  }
  error = json.dumps(err)
  snb.log_notebook_error(snb.notebookLogGuid, error)
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

# MAGIC %md #### Log Completion

# COMMAND ----------

#collect row count from the above save and set rows
snb.log_notebook_end(0)
dbutils.notebook.exit("Succeeded")
