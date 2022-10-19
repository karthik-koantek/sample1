# Databricks notebook source
import pyodbc
import uuid
import datetime
import sys, traceback
import json

# COMMAND ----------

# MAGIC %run "../Orchestration/Shared Functions"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Notebook Functions

# COMMAND ----------

def log_notebook_start (guid, stepGuid, stepKey, parameters, context, server, database, login, pwd):
  query = """EXEC dbo.LogNotebookStart
   @NotebookLogGuid='{0}'
  ,@StepLogGuid='{1}'
  ,@StepKey={2}
  ,@Parameters='{3}'
  ,@Context='{4}';
  """.format(guid, stepGuid, stepKey, parameters, context)

  pyodbcStoredProcedure(query, server, database, login, pwd)

# COMMAND ----------

def log_notebook_error (guid, error, server, database, login, pwd):
  query = """EXEC dbo.LogNotebookError
   @NotebookLogGuid='{0}'
  ,@Error='{1}';
  """.format(guid, error)

  pyodbcStoredProcedure(query, server, database, login, pwd)

# COMMAND ----------

def log_notebook_end (guid, rows, server, database, login, pwd):
  query = """EXEC dbo.LogNotebookEnd
   @NotebookLogGuid='{0}'
  ,@RowsAffected={1}
  """.format(guid, rows)

  pyodbcStoredProcedure(query, server, database, login, pwd)

# COMMAND ----------

def log_validationlog (validationLogGuid, stepLogGuid, validationKey, validationStatus, error, parameters, server, database, login, pwd):
  query = """EXEC dbo.LogValidationLog
     @ValidationLogGuid='{0}'
    ,@StepLogGuid='{1}'
    ,@ValidationKey={2}
    ,@ValidationStatus='{3}'
    ,@Error='{4}'
    ,@Parameters='{5}';
  """.format(validationLogGuid, stepLogGuid, validationKey, validationStatus, error, parameters)

  pyodbcStoredProcedure(query, server, database, login, pwd)