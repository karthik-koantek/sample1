# Databricks notebook source
import pyodbc
import uuid
import datetime
import sys, traceback

# COMMAND ----------

# MAGIC %run "../Secrets/Import Data Lake Secrets"

# COMMAND ----------

# MAGIC %scala
# MAGIC Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver")

# COMMAND ----------

def runWithRetryLogExceptions(notebook, timeout, args = {}, max_retries = 0):
  num_retries = 0
  while True:
    try:
      print("\nrunning notebook {0} with args {1}".format(notebook, args))
      return dbutils.notebook.run(notebook, timeout, args)
    except Exception as e:
      exc_type, exc_value, exc_traceback = sys.exc_info()
      error = repr(traceback.format_exception(exc_type, exc_value, exc_traceback)).replace("'","")
      if num_retries >= max_retries:
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
      print("\nrunning notebook {0} with args {1}".format(notebook, args))
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

def pyodbcConnectionString(server, database, login, pwd):
  connection = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+login+';PWD='+ pwd)
  return connection

# COMMAND ----------

def pyodbcStoredProcedureResults(sp, server, database, login, pwd):
  connection = pyodbcConnectionString(server, database, login, pwd)
  cursor = connection.cursor()
  connection.autocommit = True
  cursor.execute(sp)
  results = cursor.fetchall()
  conn.close()
  return results

# COMMAND ----------

def pyodbcStoredProcedure(sp, server, database, login, pwd):
  connection = pyodbcConnectionString(server, database, login, pwd)
  connection.autocommit = True
  connection.execute(sp)
  connection.close()

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

def log_step_start (guid, jobGuid, stepKey, parameters, context, server, database, login, pwd):
  query = """EXEC dbo.LogStepStart
   @StepLogGuid='{0}'
  ,@JobLogGuid='{1}'
  ,@StepKey={2}
  ,@Parameters='{3}'
  ,@Context='{4}';
  """.format(guid, jobGuid, stepKey, parameters, context)

  pyodbcStoredProcedure(query, server, database, login, pwd)

# COMMAND ----------

def log_job_start (guid, stageGuid, jobKey, parameters, context, server, database, login, pwd):
  query = """EXEC dbo.LogJobStart
   @JobLogGuid='{0}'
  ,@StageLogGuid='{1}'
  ,@JobKey={2}
  ,@Parameters='{3}'
  ,@Context='{4}';
  """.format(guid, stageGuid, jobKey, parameters, context)

  pyodbcStoredProcedure(query, server, database, login, pwd)

# COMMAND ----------

def log_stage_start (guid, systemGuid, stageKey, parameters, context, server, database, login, pwd):
  query = """EXEC dbo.LogStageStart
   @StageLogGuid='{0}'
  ,@SystemLogGuid='{1}'
  ,@StageKey={2}
  ,@Parameters='{3}'
  ,@Context='{4}';
  """.format(guid, systemGuid, stageKey, parameters, context)

  pyodbcStoredProcedure(query, server, database, login, pwd)

# COMMAND ----------

def log_system_start (guid, projectGuid, systemKey, parameters, context, server, database, login, pwd):
  query = """EXEC dbo.LogSystemStart
   @SystemLogGuid='{0}'
  ,@ProjectLogGuid='{1}'
  ,@SystemKey={2}
  ,@Parameters='{3}'
  ,@Context='{4}';
  """.format(guid, projectGuid, systemKey, parameters, context)

  pyodbcStoredProcedure(query, server, database, login, pwd)

# COMMAND ----------

def log_project_start (guid, parameters, context, server, database, login, pwd):
  query = """EXEC dbo.LogProjectStart
   @ProjectLogGuid='{0}'
  ,@Parameters='{1}'
  ,@Context='{2}';
  """.format(guid, parameters, context)

  pyodbcStoredProcedure(query, server, database, login, pwd)

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

# COMMAND ----------

def log_notebook_error (guid, error, server, database, login, pwd):
  query = """EXEC dbo.LogNotebookError
   @NotebookLogGuid='{0}'
  ,@Error='{1}';
  """.format(guid, error)

  pyodbcStoredProcedure(query, server, database, login, pwd)

# COMMAND ----------

def log_step_error (guid, stepKey, error, server, database, login, pwd):
  query = """EXEC dbo.LogStepError
   @StepLogGuid='{0}'
  ,@StepKey={1}
  ,@Error='{2}';
  """.format(guid, stepKey, error)

  pyodbcStoredProcedure(query, server, database, login, pwd)

# COMMAND ----------

def log_job_error (guid, error, jobKey, server, database, login, pwd):
  query = """EXEC dbo.LogJobError
   @JobLogGuid='{0}'
  ,@JobKey={1}
  ,@Error='{2}';
  """.format(guid, jobKey, error)

  pyodbcStoredProcedure(query, server, database, login, pwd)

# COMMAND ----------

def log_stage_error (guid, error, stageKey, server, database, login, pwd):
  query = """EXEC dbo.LogStageError
   @StageLogGuid='{0}'
  ,@StageKey={1}
  ,@Error='{2}';
  """.format(guid, stageKey, error)

  pyodbcStoredProcedure(query, server, database, login, pwd)

# COMMAND ----------

def log_system_error (guid, error, systemKey, server, database, login, pwd):
  query = """EXEC dbo.LogSystemError
   @SystemLogGuid='{0}'
  ,@SystemKey={1}
  ,@Error='{2}';
  """.format(guid, systemKey, error)

  pyodbcStoredProcedure(query, server, database, login, pwd)

# COMMAND ----------

def log_project_error (guid, error, server, database, login, pwd, projectKey=None):
  query = """EXEC dbo.LogProjectError
   @ProjectLogGuid='{0}'
  ,@ProjectKey={1}
  ,@Error='{2}';
  """.format(guid, projectKey, error)

  pyodbcStoredProcedure(query, server, database, login, pwd)

# COMMAND ----------

def log_notebook_end (guid, rows, server, database, login, pwd):
  query = """EXEC dbo.LogNotebookEnd
   @NotebookLogGuid='{0}'
  ,@RowsAffected={1}
  """.format(guid, rows)

  pyodbcStoredProcedure(query, server, database, login, pwd)

# COMMAND ----------

def log_step_end (jobKey, guid, stepKey, server, database, login, pwd, windowStart=None, windowEnd=None):
  query = "EXEC dbo.LogStepEnd @StepLogGuid='{0}' ,@StepKey={1}, @JobKey={2}".format(guid, stepKey, jobKey)
  if windowStart is not None:
    query += ",@WindowStart='{0}'".format(windowStart)
  if windowEnd is not None:
    query += ",@WindowEnd='{0}'".format(windowEnd)
  pyodbcStoredProcedure(query, server, database, login, pwd)

# COMMAND ----------

def log_job_end (guid, jobKey, server, database, login, pwd):
  query = """EXEC dbo.LogJobEnd
   @JobLogGuid='{0}'
  ,@JobKey={1};
  """.format(guid, jobKey)

  pyodbcStoredProcedure(query, server, database, login, pwd)

# COMMAND ----------

def log_stage_end (guid, stageKey, server, database, login, pwd):
  query = """EXEC dbo.LogStageEnd
   @StageLogGuid='{0}'
  ,@StageKey={1};
  """.format(guid, stageKey)

  pyodbcStoredProcedure(query, server, database, login, pwd)

# COMMAND ----------

def log_system_end (guid, systemKey, server, database, login, pwd):
  query = """EXEC dbo.LogSystemEnd
   @SystemLogGuid='{0}'
  ,@SystemKey={1};
  """.format(guid, systemKey)

  pyodbcStoredProcedure(query, server, database, login, pwd)

# COMMAND ----------

def log_project_end (guid, projectKey, server, database, login, pwd):
  query = """EXEC dbo.LogProjectEnd
   @ProjectLogGuid='{0}'
  ,@ProjectKey={1};
  """.format(guid, projectKey)

  pyodbcStoredProcedure(query, server, database, login, pwd)

# COMMAND ----------

def reset_job(jobKey, server, database, login, pwd):
  query = """EXEC dbo.ResetJob
   @JobKey={0};
  """.format(jobKey)

  pyodbcStoredProcedure(query, server, database, login, pwd)

# COMMAND ----------

def reset_stage(stageKey, server, database, login, pwd):
  query = """EXEC dbo.ResetStage
   @StageKey={0};
  """.format(stageKey)

  pyodbcStoredProcedure(query, server, database, login, pwd)

# COMMAND ----------

def reset_system(systemKey, server, database, login, pwd):
  query = """EXEC dbo.ResetSystem
   @SystemKey={0};
  """.format(systemKey)

  pyodbcStoredProcedure(query, server, database, login, pwd)

# COMMAND ----------

def reset_project (projectKey, server, database, login, pwd):
  query = """EXEC dbo.ResetProject
   @ProjectKey={0};
  """.format(projectKey)

  pyodbcStoredProcedure(query, server, database, login, pwd)

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

def get_all_jobs(server, database, login, pwd):
  url, properties = jdbcConnectionString(server, database, login, pwd)
  query = """(
  SELECT TOP 100 PERCENT
   j.JobKey
  ,j.JobName
  ,j.StageKey
  ,s.StepKey
  ,s.StepName
  ,s.StepOrder
  FROM dbo.Job j
  JOIN dbo.Step s ON j.JobKey=s.JobKey
  ORDER BY j.JobName
  ) t"""
  df = spark.read.jdbc(url=url, table=query, properties=properties)
  return df

# COMMAND ----------

def get_job(jobName, jobKey, server, database, login, pwd):
  url, properties = jdbcConnectionString(server, database, login, pwd)
  query = """(
  SELECT TOP 100 PERCENT
   j.JobKey
  ,j.JobName
  ,j.StageKey
  ,s.StepKey
  ,s.StepName
  ,s.StepOrder
  FROM dbo.Job j
  JOIN dbo.Step s ON j.JobKey=s.JobKey
  WHERE j.JobName = '{0}'
  AND j.JobKey = {1}
  AND s.IsActive = 1
  AND s.IsRestart = 1
  ORDER BY s.StepOrder
  ) t""".format(jobName, jobKey)
  df = spark.read.jdbc(url=url, table=query, properties=properties)
  return df

# COMMAND ----------

def get_all_stages(server, database, login, pwd):
  url, properties = jdbcConnectionString(server, database, login, pwd)
  query = """(
  SELECT TOP 100 PERCENT
   s.StageKey
  ,s.StageName
  ,s.SystemKey
  ,j.JobKey
  ,j.JobName
  ,j.JobOrder
  FROM dbo.Stage s
  JOIN dbo.Job j ON s.StageKey=j.StageKey
  ORDER BY s.StageName
  ) t"""
  df = spark.read.jdbc(url=url, table=query, properties=properties)
  return df

# COMMAND ----------

def get_stage(stageName, server, database, login, pwd):
  url, properties = jdbcConnectionString(server, database, login, pwd)
  query = """(
  SELECT TOP 100 PERCENT
   s.StageKey
  ,s.StageName
  ,s.SystemKey
  ,j.JobKey
  ,j.JobName
  ,j.JobOrder
  FROM dbo.Stage s
  JOIN dbo.Job j ON s.StageKey=j.StageKey
  WHERE s.StageName = '{0}'
  AND j.IsActive = 1
  AND j.IsRestart = 1
  ORDER BY j.JobOrder
  ) t""".format(stageName)
  df = spark.read.jdbc(url=url, table=query, properties=properties)
  return df

# COMMAND ----------

def get_all_systems(server, database, login, pwd):
  jdbcUrl, connectionProperties = jdbcConnectionString(server, database, login, pwd)
  query = """(
  SELECT TOP 100 PERCENT
   es.SystemKey
  ,es.SystemName
  ,es.ProjectKey
  ,s.StageKey
  ,s.StageName
  ,s.StageOrder
  ,s.numberOfThreads
  FROM dbo.[System] es
  JOIN dbo.Stage s ON es.SystemKey=s.SystemKey
  ORDER BY es.SystemName
  ) t"""
  df = spark.read.jdbc(url=jdbcUrl, table=query, properties=connectionProperties)
  return df

# COMMAND ----------

def get_system(systemName, server, database, login, pwd):
  jdbcUrl, connectionProperties = jdbcConnectionString(server, database, login, pwd)
  query = """(
  SELECT TOP 100 PERCENT
   es.SystemKey
  ,es.SystemName
  ,es.ProjectKey
  ,s.StageKey
  ,s.StageName
  ,s.StageOrder
  ,s.numberOfThreads
  FROM dbo.[System] es
  JOIN dbo.Stage s ON es.SystemKey=s.SystemKey
  WHERE es.SystemName = '{0}'
  AND s.IsActive = 1
  AND s.IsRestart = 1
  ORDER BY s.StageOrder
  ) t""".format(systemName)
  df = spark.read.jdbc(url=jdbcUrl, table=query, properties=connectionProperties)
  return df

# COMMAND ----------

def get_all_projects(server, database, login, pwd):
  url, properties = jdbcConnectionString(server, database, login, pwd)
  query = """(
  SELECT TOP 100 PERCENT
   p.ProjectKey
  ,p.ProjectName
  ,es.SystemKey
  ,es.SystemName
  ,es.SystemSecretScope
  ,es.SystemOrder
  FROM dbo.Project p
  JOIN dbo.[System] es ON p.ProjectKey=es.ProjectKey
  ORDER BY p.ProjectName
  ) t"""
  df = spark.read.jdbc(url=url, table=query, properties=properties)
  return df

# COMMAND ----------

def get_project(projectName, server, database, login, pwd):
  url, properties = jdbcConnectionString(server, database, login, pwd)
  query = """(
  SELECT TOP 100 PERCENT
   p.ProjectKey
  ,p.ProjectName
  ,es.SystemKey
  ,es.SystemName
  ,es.SystemSecretScope
  ,es.SystemOrder
  FROM dbo.Project p
  JOIN dbo.[System] es ON p.ProjectKey=es.ProjectKey
  WHERE p.ProjectName = '{0}'
  AND es.IsActive = 1
  AND es.IsRestart = 1
  ORDER BY es.SystemOrder
  ) t""".format(projectName)
  df = spark.read.jdbc(url=url, table=query, properties=properties)
  return df

# COMMAND ----------

def get_all_parameters(server, database, login, pwd):
  url, properties = jdbcConnectionString(server, database, login, pwd)
  query = """(
  SELECT
  s.StepKey
  ,s.StepName
  ,s.JobKey
  ,p.ParameterName
  ,p.ParameterValue
  FROM dbo.Step s
  JOIN dbo.Parameter p ON s.StepKey=p.StepKey
  UNION
  SELECT
   s.StepKey
  ,s.StepName
  ,s.JobKey
  ,w.ParameterName
  ,w.ParameterValue
  FROM dbo.Step s
  CROSS APPLY dbo.GetWindow(s.StepKey) w
  ) t"""
  df = spark.read.jdbc(url=url, table=query, properties=properties)
  return df

# COMMAND ----------

def get_parameters(stepKey, server, database, login, pwd):
  url, properties = jdbcConnectionString(server, database, login, pwd)
  query = """(
  SELECT
   s.StepKey
  ,s.StepName
  ,s.JobKey
  ,p.ParameterName
  ,p.ParameterValue
  FROM dbo.Step s
  JOIN dbo.Parameter p ON s.StepKey=p.StepKey
  WHERE s.StepKey = {0}
  UNION
  SELECT StepKey, StepName, JobKey, ParameterName, ParameterValue
  FROM dbo.GetWindow({0})
  ) t""".format(stepKey)
  df = spark.read.jdbc(url=url, table=query, properties=properties)
  return df