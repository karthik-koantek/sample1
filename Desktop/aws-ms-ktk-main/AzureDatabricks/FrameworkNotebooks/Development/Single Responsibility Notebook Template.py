# Databricks notebook source
# MAGIC %md # Single Responsibility Notebook Template

# COMMAND ----------

# MAGIC %md #### Initialize

# COMMAND ----------

import ktk
from ktk import utilities as u
import datetime, json

# COMMAND ----------

dbutils.widgets.text(name="stepLogGuid", defaultValue="00000000-0000-0000-0000-000000000000", label="stepLogGuid")
dbutils.widgets.text(name="stepKey", defaultValue="-1", label="stepKey")
#add additional widgets as required

#add all notebook widgets to this widgets list
widgets = ["stepLogGuid","stepKey"]

#add secret names to this list.  Note that this requires widget "externalSystem", and these secrets must exist under secret scope of the value of externalSystem.  
secrets = []

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
if (1==0):
  err = {
    "sourceName": "<Notebook Name>: Validation",
    "errorCode": "100",
    "errorDescription": "Invalid Parameters supplied.  ... are required parameters."
  }
  error = json.dumps(err)
  snb.log_notebook_error(snb.notebookLogGuid, error)
  raise ValueError("Invalid Parameters supplied.  ... are required parameters.")

# COMMAND ----------

# MAGIC %md #### Code Goes Here
# MAGIC Rename or replace this heading with one or more sections representing the body of the notebook
# MAGIC 
# MAGIC Examples:
# MAGIC * Read Dataframe, Cleanse Dataframe, Write Dataframe
# MAGIC 
# MAGIC Increment the errorCode for each section by 100 (e.g. 200, 300, 400)

# COMMAND ----------

try:
except Exception as e:
  err = {
    "sourceName": "<Notebook Name>: <Section Name>",
    "errorCode": "200",
    "errorDescription": e.__class__.__name__
  }
  error = json.dumps(err)
  snb.log_notebook_error(error)
  raise(e)

# COMMAND ----------

# MAGIC %md #### Log Completion

# COMMAND ----------

rows = 0 #update with rows affected
snb.log_notebook_end(rows)
dbutils.notebook.exit("Succeeded")
