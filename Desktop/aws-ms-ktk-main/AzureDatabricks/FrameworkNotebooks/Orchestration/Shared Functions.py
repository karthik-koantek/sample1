# Databricks notebook source
import pyodbc
import uuid
import datetime
import sys, traceback
import json

# COMMAND ----------

# MAGIC %run "../Secrets/Import Data Lake Secrets"

# COMMAND ----------

def getCurrentTimestamp():
  dateTimeObj = datetime.datetime.utcnow()
  timestampStr = dateTimeObj.strftime("%d-%b-%Y (%H:%M:%S.%f)")
  return timestampStr

# COMMAND ----------

# MAGIC %md
# MAGIC #### Run Notebook Functions

# COMMAND ----------

def runWithRetryLogExceptions(notebook, timeout, args = {}, max_retries = 0):
  num_retries = 0
  while True:
    try:
      print("\nrunning notebook {0} with args {1}\n".format(notebook, args))
      return dbutils.notebook.run(notebook, timeout, args)
    except Exception as e:
      exc_type, exc_value, exc_traceback = sys.exc_info()
      error = repr(traceback.format_exception(exc_type, exc_value, exc_traceback)).replace("'","")
      if num_retries >= max_retries:
        print(error)
        log_exception(args, error, server, database, login, pwd)
        raise e
        print("skipping notebook {0}".format(notebook))
        break
      else:
        traceback.print_exception(exc_type, exc_value, exc_traceback, limit=2, file=sys.stdout)
        print("Retrying error")
        num_retries += 1

# COMMAND ----------

def runWithRetry(notebook, timeout, args = {}, max_retries = 0):
  num_retries = 0
  while True:
    try:
      print("\n{0}:running notebook {1} with args {2}\n".format(getCurrentTimestamp(), notebook, args))
      return dbutils.notebook.run(notebook, timeout, args)
    except Exception as e:
      if num_retries >= max_retries:
        raise e
        print("skipping notebook {0}".format(notebook))
        break
      else:
        print ("Retrying error", e)
        num_retries += 1

# COMMAND ----------

# MAGIC %md
# MAGIC #### pyodbc & jdbc Functions

# COMMAND ----------

def pyodbcConnectionString(server, database, login, pwd):
  connection = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+login+';PWD='+ pwd)
  return connection

# COMMAND ----------

def pyodbcStoredProcedureResults(sp, server, database, login, pwd):
  try:
    connection = pyodbcConnectionString(server, database, login, pwd)
    cursor = connection.cursor()
    connection.autocommit = True
    cursor.execute(sp)
    results = cursor.fetchall()
    connection.close()
    return results
  except Exception as e:
    print("Failed to call stored procedure: {0}".format(sp))

# COMMAND ----------

def pyodbcStoredProcedure(sp, server, database, login, pwd):
  try:
    connection = pyodbcConnectionString(server, database, login, pwd)
    connection.autocommit = True
    connection.execute(sp)
    connection.close()
  except Exception as e:
    print("Failed to call stored procedure: {0}".format(sp))
    raise e

# COMMAND ----------

def jdbcConnectionString(server, database, login, pwd):
  port = 1433
  url = "jdbc:sqlserver://{0}:{1};database={2}".format(server, port, database)
  properties = {
    "user" : login,
    "password" : pwd,
    "driver" : "com.microsoft.sqlserver.jdbc.SQLServerDriver"
  }
  return url, properties

# COMMAND ----------

# MAGIC %scala
# MAGIC def jdbcConnectionStringScala(server:String, database:String, login:String, pwd:String):(String, java.util.Properties) = {
# MAGIC    val port = 1433
# MAGIC    val driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
# MAGIC    val url = s"jdbc:sqlserver://${server}:${port};database=${database}"
# MAGIC    import java.util.Properties
# MAGIC    val properties = new Properties()
# MAGIC    properties.put("user",s"${login}")
# MAGIC    properties.put("password",s"${pwd}")
# MAGIC    properties.setProperty("Driver", driver)
# MAGIC    (url, properties)
# MAGIC }

# COMMAND ----------

def log_exception(args, error, server, database, login, pwd):
  if args.get("stepLogGuid", "") != "":
    log_step_error (args.get("stepLogGuid"), args.get("stepKey",-1), error, server, database, login, pwd)
  elif args.get("jobLogGuid", "") != "":
    log_job_error (args.get("jobLogGuid"), error, args.get("jobKey",-1), server, database, login, pwd)
  elif args.get("stageLogGuid", "") != "":
    log_stage_error (args.get("stageLogGuid"), error, args.get("stageKey",-1), server, database, login, pwd)
  elif args.get("systemLogGuid", "") != "":
    log_system_error (args.get("systemLogGuid"), error, args.get("systemKey",-1), server, database, login, pwd)
  elif args.get("projectLogGuid", '') != "":
    log_project_error (args.get("projectLogGuid"), error, server, database, login, pwd, args.get("projectKey",-1))
  else:
    #notebook errors?
    print("Unable to log exception")