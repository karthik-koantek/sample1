-- Databricks notebook source
-- MAGIC %md # SQL Notebook Template

-- COMMAND ----------

-- MAGIC %md #### Initialize

-- COMMAND ----------

-- MAGIC %py
-- MAGIC import dvc
-- MAGIC from dvc import utilities as u
-- MAGIC import datetime, json

-- COMMAND ----------

-- MAGIC %py
-- MAGIC dbutils.widgets.text(name="stepLogGuid", defaultValue="00000000-0000-0000-0000-000000000000", label="stepLogGuid")
-- MAGIC dbutils.widgets.text(name="stepKey", defaultValue="-1", label="stepKey")
-- MAGIC #add additional widgets as required
-- MAGIC 
-- MAGIC #add all notebook widgets to this widgets list
-- MAGIC widgets = ["stepLogGuid","stepKey"]
-- MAGIC 
-- MAGIC #add secret names to this list.  Note that this requires widget "externalSystem", and these secrets must exist under secret scope of the value of externalSystem.  
-- MAGIC secrets = []

-- COMMAND ----------

-- MAGIC %py
-- MAGIC snb = dvc.SingleResponsibilityNotebook(widgets, secrets)

-- COMMAND ----------

-- MAGIC %py
-- MAGIC # Configure additional variables as required.  Add to p dictionary so that they are merged into the snb object.
-- MAGIC p = {}
-- MAGIC 
-- MAGIC parameters = json.dumps(snb.mergeAttributes(p))
-- MAGIC snb.log_notebook_start(parameters)
-- MAGIC print("Parameters:")
-- MAGIC snb.displayAttributes()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Validation

-- COMMAND ----------

-- MAGIC %py
-- MAGIC #Pattern for validation.  Validate input parameters and other conditions and raise error for any conditions not supported
-- MAGIC if (1==0):
-- MAGIC   err = {
-- MAGIC     "sourceName": "<Notebook Name>: Validation",
-- MAGIC     "errorCode": "100",
-- MAGIC     "errorDescription": "Invalid Parameters supplied.  ... are required parameters."
-- MAGIC   }
-- MAGIC   error = json.dumps(err)
-- MAGIC   snb.log_notebook_error(error)
-- MAGIC   raise ValueError("Invalid Parameters supplied.  ... are required parameters.")

-- COMMAND ----------

-- MAGIC %md #### Code Goes Here
-- MAGIC Rename or replace this heading with one or more sections representing the body of the notebook
-- MAGIC 
-- MAGIC Examples:
-- MAGIC * Read Dataframe, Cleanse Dataframe, Write Dataframe
-- MAGIC 
-- MAGIC Increment the errorCode for each section by 100 (e.g. 200, 300, 400)

-- COMMAND ----------

SHOW DATABASES;
USE goldgeneral;
SHOW TABLES;

-- COMMAND ----------

-- MAGIC %py
-- MAGIC try:
-- MAGIC   print("do something here")
-- MAGIC except Exception as e:
-- MAGIC   err = {
-- MAGIC     "sourceName": "<Notebook Name>: <Section Name>",
-- MAGIC     "errorCode": "200",
-- MAGIC     "errorDescription": e.__class__.__name__
-- MAGIC   }
-- MAGIC   error = json.dumps(err)
-- MAGIC   snb.log_notebook_error(error)
-- MAGIC   raise(e)
-- MAGIC   

-- COMMAND ----------

-- MAGIC %md #### Log Completion

-- COMMAND ----------

-- MAGIC %py
-- MAGIC rows = 0 #update with rows affected
-- MAGIC snb.log_notebook_end(rows)
-- MAGIC dbutils.notebook.exit("Succeeded")
