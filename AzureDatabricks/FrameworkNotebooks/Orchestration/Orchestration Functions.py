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
# MAGIC #### Step Functions

# COMMAND ----------

def getStepOrder(step):
  return step['StepOrder']

# COMMAND ----------

def run_step(jobLogGuid, jobKey, step, context, dateToProcess, threadPool, timeout, server, database, login, pwd):
  p = {
  "jobLogGuid": jobLogGuid,
  "jobKey": jobKey,
  "stepName": step['StepName'],
  "stepKey": step['StepKey'],
  "dateToProcess": dateToProcess,
  "threadPool": threadPool,
  "timeout": timeout
  }
  parameters = json.dumps(p)
  stepLogGuid = str(uuid.uuid4())
  log_step_start(stepLogGuid, jobLogGuid, step['StepKey'], parameters, context, server, database, login, pwd)
  try:
    parameters = step['pm']
    args = {
      "stepLogGuid": stepLogGuid,
      "stepKey": step['StepKey'],
      "dateToProcess": dateToProcess,
      "timeoutSeconds": timeout
    }
    for parameter in parameters:
      args[parameter['ParameterName']] = parameter['ParameterValue']
    print("{0}: running step: {1}".format(getCurrentTimestamp(), step['StepName']))
    runWithRetryLogExceptions(step['StepName'], timeout, args, max_retries=0)
  except Exception as e:
    err = {
      "sourceName": "Orchestration: run_step",
      "errorCode": 104,
      "errorDescription": e.__class__.__name__
    }
    error = json.dumps(err)
    log_step_error(stepLogGuid, step['StepKey'], error, server, database, login, pwd)
    raise e

  windowStart = args.get("lowerDateToProcess", None)
  windowEnd = args.get("dateToProcess", None)
  log_step_end(jobKey, stepLogGuid, step['StepKey'], server, database, login, pwd, windowStart, windowEnd)

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

def log_step_error (guid, stepKey, error, server, database, login, pwd):
  query = """EXEC dbo.LogStepError
   @StepLogGuid='{0}'
  ,@StepKey={1}
  ,@Error='{2}';
  """.format(guid, stepKey, error)

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

# COMMAND ----------

# MAGIC %md
# MAGIC #### Job Functions

# COMMAND ----------

def getJobOrder(job):
  return job['JobOrder']

# COMMAND ----------

def run_job(stageLogGuid, job, context, dateToProcess, threadPool, timeout, server, database, login, pwd):
  p = {
  "stageLogGuid": stageLogGuid,
  "jobName": job['JobName'],
  "jobKey": job['JobKey'],
  "dateToProcess": dateToProcess,
  "threadPool": threadPool,
  "timeout": timeout
  }
  parameters = json.dumps(p)

  jobLogGuid = str(uuid.uuid4())
  log_job_start(jobLogGuid, stageLogGuid, job['JobKey'], parameters, context, server, database, login, pwd)
  try:
    steps = job['stp']
    steps.sort(key=getStepOrder)
    for step in steps:
      run_step(jobLogGuid, job['JobKey'], step, context, dateToProcess, threadPool, timeout, server, database, login, pwd)
  except Exception as e:
    err = {
      "sourceName": "Orchestration: run_job",
      "errorCode": 104,
      "errorDescription": e.__class__.__name__
    }
    error = json.dumps(err)
    log_job_error(jobLogGuid, error, job['JobKey'], server, database, login, pwd)
    raise e

  log_job_end(jobLogGuid, job['JobKey'], server, database, login, pwd)
  reset_job(job['JobKey'], server, database, login, pwd)

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

def log_job_error (guid, error, jobKey, server, database, login, pwd):
  query = """EXEC dbo.LogJobError
   @JobLogGuid='{0}'
  ,@JobKey={1}
  ,@Error='{2}';
  """.format(guid, jobKey, error)

  pyodbcStoredProcedure(query, server, database, login, pwd)

# COMMAND ----------

def log_job_end (guid, jobKey, server, database, login, pwd):
  query = """EXEC dbo.LogJobEnd
   @JobLogGuid='{0}'
  ,@JobKey={1};
  """.format(guid, jobKey)

  pyodbcStoredProcedure(query, server, database, login, pwd)

# COMMAND ----------

def reset_job(jobKey, server, database, login, pwd):
  query = """EXEC dbo.ResetJob
   @JobKey={0};
  """.format(jobKey)

  pyodbcStoredProcedure(query, server, database, login, pwd)

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

# MAGIC %md
# MAGIC #### Stage Functions

# COMMAND ----------

def getStageOrder(stage):
  return stage['StageOrder']

# COMMAND ----------

def run_stage(systemLogGuid, stage, context, dateToProcess, threadPool, timeout, server, database, login, pwd):
  p = {
    "systemLogGuid": systemLogGuid,
    "stageName": stage['StageName'],
    "stageKey": stage['StageKey'],
    "dateToProcess": dateToProcess,
    "threadPool": threadPool,
    "timeout": timeout
  }
  parameters = json.dumps(p)

  stageLogGuid = str(uuid.uuid4())
  log_stage_start(stageLogGuid, systemLogGuid, stage['StageKey'], parameters, context, server, database, login, pwd)
  try:
    jobs = stage['j']
    jobs.sort(key=getJobOrder)
    pool = ThreadPool(threadPool)

    for job in jobs:
        print("{0}: running job: {1}".format(getCurrentTimestamp(), job['JobName']))

    if threadPool == 1:
      for job in jobs:
        run_job(stageLogGuid, job, context, dateToProcess, threadPool, timeout, server, database, login, pwd)
    else:
      pool.map(lambda job: run_job(stageLogGuid, job, context, dateToProcess, threadPool, timeout, server, database, login, pwd), jobs)

  except Exception as e:
    err = {
      "sourceName": "Orchestration: run_stage",
      "errorCode": 103,
      "errorDescription": e.__class__.__name__
    }
    error = json.dumps(err)
    log_stage_error(stageLogGuid, error, stage['StageKey'], server, database, login, pwd)
    raise e

  log_stage_end(stageLogGuid, stage['StageKey'], server, database, login, pwd)
  reset_stage(stage['StageKey'], server, database, login, pwd)

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

def log_stage_error (guid, error, stageKey, server, database, login, pwd):
  query = """EXEC dbo.LogStageError
   @StageLogGuid='{0}'
  ,@StageKey={1}
  ,@Error='{2}';
  """.format(guid, stageKey, error)

  pyodbcStoredProcedure(query, server, database, login, pwd)

# COMMAND ----------

def log_stage_end (guid, stageKey, server, database, login, pwd):
  query = """EXEC dbo.LogStageEnd
   @StageLogGuid='{0}'
  ,@StageKey={1};
  """.format(guid, stageKey)

  pyodbcStoredProcedure(query, server, database, login, pwd)

# COMMAND ----------

def reset_stage(stageKey, server, database, login, pwd):
  query = """EXEC dbo.ResetStage
   @StageKey={0};
  """.format(stageKey)

  pyodbcStoredProcedure(query, server, database, login, pwd)

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

# MAGIC %md
# MAGIC #### System Functions

# COMMAND ----------

def getSystemOrder(system):
  return system['SystemOrder']

# COMMAND ----------

def run_system(projectLogGuid, system, context, dateToProcess, threadPool, timeout, server, database, login, pwd):
  p = {
  "projectLogGuid": projectLogGuid,
  "systemName": system['SystemName'],
  "systemKey": system['SystemKey'],
  "dateToProcess": dateToProcess,
  "threadPool": threadPool,
  "timeout": timeout
  }
  parameters = json.dumps(p)

  systemLogGuid = str(uuid.uuid4())
  log_system_start(systemLogGuid, projectLogGuid, system['SystemKey'], parameters, context, server, database, login, pwd)
  try:
    stages = system['st']
    stages.sort(key=getStageOrder)
    for stage in stages:
      print("{0}: running stage: {1}".format(getCurrentTimestamp(), stage['StageName']))
      run_stage(systemLogGuid, stage, context, dateToProcess, stage['NumberOfThreads'], timeout, server, database, login, pwd)
  except Exception as e:
    err = {
      "sourceName": "Orchestration: run_system",
      "errorCode": 102,
      "errorDescription": e.__class__.__name__
    }
    error = json.dumps(err)
    log_system_error(systemLogGuid, error, system['SystemKey'], server, database, login, pwd)
    raise e

  log_system_end(systemLogGuid, system['SystemKey'], server, database, login, pwd)
  reset_system(system['SystemKey'], server, database, login, pwd)

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

def log_system_error (guid, error, systemKey, server, database, login, pwd):
  query = """EXEC dbo.LogSystemError
   @SystemLogGuid='{0}'
  ,@SystemKey={1}
  ,@Error='{2}';
  """.format(guid, systemKey, error)

  pyodbcStoredProcedure(query, server, database, login, pwd)

# COMMAND ----------

def log_system_end (guid, systemKey, server, database, login, pwd):
  query = """EXEC dbo.LogSystemEnd
   @SystemLogGuid='{0}'
  ,@SystemKey={1};
  """.format(guid, systemKey)

  pyodbcStoredProcedure(query, server, database, login, pwd)

# COMMAND ----------

def reset_system(systemKey, server, database, login, pwd):
  query = """EXEC dbo.ResetSystem
   @SystemKey={0};
  """.format(systemKey)

  pyodbcStoredProcedure(query, server, database, login, pwd)

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

# MAGIC %md
# MAGIC #### Project Functions

# COMMAND ----------

def run_project(projectName, dateToProcess, threadPool, timeout, server, database, login, pwd):
  context = dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson()
  p = {
    "projectName": projectName,
    "dateToProcess": dateToProcess,
    "threadPool": threadPool,
    "timeout": timeout
  }
  parameters = json.dumps(p)
  projectLogGuid = str(uuid.uuid4())
  log_project_start(projectLogGuid, parameters, context, server, database, login, pwd)
  try:
    results = get_project_json(projectName, server, database, login, pwd)
    project = json.loads(results[0].results)
    systems = project['s']
    systems.sort(key=getSystemOrder)
    for system in systems:
      print("{0}: running system: {1}".format(getCurrentTimestamp(), system['SystemName']))
      run_system(projectLogGuid, system, context, dateToProcess, threadPool, timeout, server, database, login, pwd)
  except Exception as e:
    err = {
      "sourceName": "Orchestration: run_project",
      "errorCode": 101,
      "errorDescription": e.__class__.__name__
    }
    error = json.dumps(err)
    log_project_error(projectLogGuid, error, server, database, login, pwd, project['ProjectKey'])
    raise e

  log_project_end(projectLogGuid, project['ProjectKey'], server, database, login, pwd)
  reset_project(project['ProjectKey'], server, database, login, pwd)

# COMMAND ----------

def get_project_json (projectName, server, database, login, pwd):
  query = """
  DECLARE @JSON VARCHAR(MAX);
  EXEC dbo.RunProject
   @ProjectName = '{0}'
  ,@JSON=@JSON OUTPUT;
  SELECT @JSON AS results;
  """.format(projectName)

  results = pyodbcStoredProcedureResults(query, server, database, login, pwd)
  return results

# COMMAND ----------

def log_project_start (guid, parameters, context, server, database, login, pwd):
  query = """EXEC dbo.LogProjectStart
   @ProjectLogGuid='{0}'
  ,@Parameters='{1}'
  ,@Context='{2}';
  """.format(guid, parameters, context)

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

def log_project_end (guid, projectKey, server, database, login, pwd):
  query = """EXEC dbo.LogProjectEnd
   @ProjectLogGuid='{0}'
  ,@ProjectKey={1};
  """.format(guid, projectKey)

  pyodbcStoredProcedure(query, server, database, login, pwd)

# COMMAND ----------

def reset_project (projectKey, server, database, login, pwd):
  query = """EXEC dbo.ResetProject
   @ProjectKey={0};
  """.format(projectKey)

  pyodbcStoredProcedure(query, server, database, login, pwd)

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