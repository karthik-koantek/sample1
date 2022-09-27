from .notebook import Notebook
import sys, traceback
import datetime
import json
import uuid
from multiprocessing.pool import ThreadPool
from pyspark.sql import SparkSession
from pyspark.dbutils import DBUtils
from pyspark.sql.functions import lit
spark = SparkSession.builder.appName('DatabricksFramework').getOrCreate()

class OrchestrationNotebook(Notebook):
  def __init__(self):
    super().__init__()
    self.setFromWidget("ADFProjectName")
    self.setFromWidget("projectName")
    self.setFromWidget("threadPool")
    self.setFromWidget("timeoutSeconds")
    self.setFromWidget("whatIf")
    self.setDateToProcess()
    
    self.createOrchestrationSchema()

  def getCurrentTimestamp(self):
    dateTimeObj = datetime.datetime.utcnow()
    timestampStr = dateTimeObj.strftime("%d-%b-%Y (%H:%M:%S.%f)")
    return timestampStr

  def log_exception(self, args, error):
    if args.get("stepLogGuid", "") != "":
      guid = args.get("stepLogGuid")
      key = args.get("stepKey",-1)
      query = "EXEC dbo.LogStepError @StepLogGuid='{0}',@StepKey={1},@Error='{2}';".format(guid, key, error)
    elif args.get("jobLogGuid", "") != "":
      guid = args.get("jobLogGuid")
      key = args.get("jobKey",-1)
      query = """EXEC dbo.LogJobError @JobLogGuid='{0}',@JobKey={1},@Error='{2}';""".format(guid, key, error)
    elif args.get("stageLogGuid", "") != "":
      guid = args.get("stageLogGuid")
      key = args.get("stageKey",-1)
      query = "EXEC dbo.LogStageError @StageLogGuid='{0}',@StageKey={1},@Error='{2}';".format(guid, key, error)
    elif args.get("systemLogGuid", "") != "":
      guid = args.get("systemLogGuid")
      key = args.get("systemKey",-1)
      query = "EXEC dbo.LogSystemError @SystemLogGuid='{0}',@SystemKey={1},@Error='{2}';".format(guid, key, error)
    elif args.get("projectLogGuid", '') != "":
      self.log_project_error (args.get("projectLogGuid"), error, args.get("projectKey",-1))
      guid = args.get("projectLogGuid")
      key = args.get("projectKey",-1)
      query = "EXEC dbo.LogProjectError @ProjectLogGuid='{0}',@ProjectKey={1},@Error='{2}';".format(guid, key, error)
    else:
      #notebook errors?
      print("Unable to log exception")
      query = ""
    if query != "":
      self.pyodbcStoredProcedure(query)

  def runWithRetryLogExceptions(self, notebook, args = {}, max_retries = 0):
    dbutils = self.get_dbutils()
    num_retries = 0
    while True:
      try:
        print("\n{0}:running notebook {1} with args {2}\n".format(self.getCurrentTimestamp(), notebook, args))
        return dbutils.notebook.run(notebook, int(self.timeoutSeconds), args)
      except Exception as e:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        error = repr(traceback.format_exception(exc_type, exc_value, exc_traceback)).replace("'","")
        if num_retries >= max_retries:
          print(error)
          self.log_exception(args, error)
          raise e
          print("skipping notebook {0}".format(notebook))
          break
        else:
          traceback.print_exception(exc_type, exc_value, exc_traceback, limit=2, file=sys.stdout)
          print("Retrying error")
          num_retries += 1

#Step
  def getStepOrder(self, step):
    return step['StepOrder']

  def run_step(self, jobLogGuid, jobKey, step):
    p = {
      "jobLogGuid": jobLogGuid,
      "jobKey": jobKey,
      "stepName": step['StepName'],
      "stepKey": step['StepKey'],
      "dateToProcess": self.dateToProcess,
      "threadPool": self.threadPool,
      "timeout": self.timeoutSeconds
    }
    parameters = json.dumps(p)
    stepLogGuid = str(uuid.uuid4())
    self.log_step_start(stepLogGuid, jobLogGuid, step['StepKey'], parameters)
    try:
      parameters = step['pm']
      args = {
        "stepLogGuid": stepLogGuid,
        "stepKey": step['StepKey'],
        "dateToProcess": self.dateToProcess,
        "timeoutSeconds": self.timeoutSeconds
      }
      for parameter in parameters:
        args[parameter['ParameterName']] = parameter['ParameterValue']
      print("{0}: running step: {1}".format(self.getCurrentTimestamp(), step['StepName']))
      if self.whatIf == "0":
        self.runWithRetryLogExceptions(step['StepName'], args, max_retries=0)
    except Exception as e:
      err = {
        "sourceName": "Orchestration: run_step",
        "errorCode": 104,
        "errorDescription": e.__class__.__name__
      }
      error = json.dumps(err)
      self.log_step_error(stepLogGuid, step['StepKey'], error)
      raise e

    windowStart = args.get("lowerDateToProcess", None)
    windowEnd = args.get("dateToProcess", None)
    self.log_step_end(jobKey, stepLogGuid, step['StepKey'], windowStart, windowEnd)

  def log_step_start (self, guid, jobGuid, stepKey, parameters):
    if self.LogMethod in (1,3):
      query = """EXEC dbo.LogStepStart
        @StepLogGuid='{0}'
        ,@JobLogGuid='{1}'
        ,@StepKey={2}
        ,@Parameters='{3}'
        ,@Context='{4}';
        """.format(guid, jobGuid, stepKey, parameters, self.context)
      self.pyodbcStoredProcedure(query)

    if self.LogMethod in (2, 3):
      startDateTime = datetime.datetime.now()
      parametersMap = self.create_map_from_dictionary(parameters)
      dfList = []
      dfList.append(spark.createDataFrame([(guid)], "STRING"))
      dfList.append(dfList[-1].withColumnRenamed("value", "stepLogGuid"))
      dfList.append(dfList[-1].withColumn("jobLogGuid", lit(jobGuid)))
      dfList.append(dfList[-1].withColumn("stepKey", lit(int(stepKey))))      
      dfList.append(dfList[-1].withColumn("startDateTime", lit(startDateTime)))
      dfList.append(dfList[-1].withColumn("Parameters", parametersMap))
      dfList.append(dfList[-1].withColumn("Context", lit(self.context.replace("'","''"))))
      dfList[-1].createOrReplaceTempView("startDF") 
      query = """
      INSERT INTO silverprotected.stepLog (StepLogGuid, JobLogGuid, StepKey, StartDateTime, EndDateTime, LogStatusKey, Parameters, Context, Error)
      SELECT stepLogGuid, jobLogGuid, stepKey, startDateTime, NULL, 1, Parameters, Context, NULL FROM startDF
      """
      spark.sql(query) 

  def log_step_error (self, guid, stepKey, error):
    if self.LogMethod in (1, 3):
      query = """EXEC dbo.LogStepError
      @StepLogGuid='{0}'
      ,@StepKey={1}
      ,@Error='{2}';
      """.format(guid, stepKey, error)
      self.pyodbcStoredProcedure(query)
    if self.LogMethod in (2, 3):
      errorMap = self.create_map_from_dictionary(error)
      endDateTime = datetime.datetime.now()
      dfList = []
      dfList.append(spark.createDataFrame([(guid)], "STRING"))
      dfList.append(dfList[-1].withColumnRenamed("value", "stepLogGuid"))
      dfList.append(dfList[-1].withColumn("endDateTime", lit(endDateTime)))
      dfList.append(dfList[-1].withColumn("error", errorMap))
      dfList[-1].createOrReplaceTempView("errorDF")

      query = """
      INSERT INTO silverprotected.stepLog (StepLogGuid, JobLogGuid, StepKey, StartDateTime, EndDateTime, LogStatusKey, Parameters, Context, Error)
      SELECT stepLogGuid, NULL, NULL, NULL, endDateTime, 3, NULL, NULL, error FROM errorDF
      """
      spark.sql(query)

  def log_step_end (self, jobKey, guid, stepKey, windowStart=None, windowEnd=None):
    if self.LogMethod in (1, 3):
      query = "EXEC dbo.LogStepEnd @StepLogGuid='{0}' ,@StepKey={1}, @JobKey={2}".format(guid, stepKey, jobKey)
      if windowStart is not None:
        query += ",@WindowStart='{0}'".format(windowStart)
      if windowEnd is not None:
        query += ",@WindowEnd='{0}'".format(windowEnd)
      self.pyodbcStoredProcedure(query)
    if self.LogMethod in (2, 3):
      endDateTime = datetime.datetime.now()
      dfList = []
      dfList.append(spark.createDataFrame([(guid)], "STRING"))
      dfList.append(dfList[-1].withColumnRenamed("value", "stepLogGuid"))
      dfList.append(dfList[-1].withColumn("endDateTime", lit(endDateTime)))
      dfList[-1].createOrReplaceTempView("endDF")
      query = """
      INSERT INTO silverprotected.stepLog (StepLogGuid, JobLogGuid, StepKey, StartDateTime, EndDateTime, LogStatusKey, Parameters, Context, Error)
      SELECT stepLogGuid, NULL, NULL, NULL, endDateTime, 2, NULL, NULL, NULL FROM endDF
      """
      spark.sql(query)
      
  def get_all_parameters(self):
    url, properties = self.jdbcConnectionString()
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

  def get_parameters(self, stepKey):
    url, properties = self.jdbcConnectionString()
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
#Job
  def getJobOrder(self, job):
    return job['JobOrder']

  def run_job(self, stageLogGuid, job):
    p = {
      "stageLogGuid": stageLogGuid,
      "jobName": job['JobName'],
      "jobKey": job['JobKey'],
      "dateToProcess": self.dateToProcess,
      "threadPool": self.threadPool,
      "timeout": self.timeoutSeconds
    }
    parameters = json.dumps(p)
    jobLogGuid = str(uuid.uuid4())
    self.log_job_start(jobLogGuid, stageLogGuid, job['JobKey'], parameters)
    try:
      steps = job['stp']
      steps.sort(key=self.getStepOrder)
      for step in steps:
        self.run_step(jobLogGuid, job['JobKey'], step)
    except Exception as e:
      err = {
        "sourceName": "Orchestration: run_job",
        "errorCode": 104,
        "errorDescription": e.__class__.__name__
      }
      error = json.dumps(err)
      self.log_job_error(jobLogGuid, error, job['JobKey'])
      raise e
    self.log_job_end(jobLogGuid, job['JobKey'])
    self.reset_job(job['JobKey'])

  def log_job_start (self, guid, stageGuid, jobKey, parameters):
    if self.LogMethod in (1, 3):
      query = """EXEC dbo.LogJobStart
        @JobLogGuid='{0}'
        ,@StageLogGuid='{1}'
        ,@JobKey={2}
        ,@Parameters='{3}'
        ,@Context='{4}';
      """.format(guid, stageGuid, jobKey, parameters, self.context)
      self.pyodbcStoredProcedure(query)
    
    if self.LogMethod in (2, 3):
      startDateTime = datetime.datetime.now()
      parametersMap = self.create_map_from_dictionary(parameters)
      dfList = []
      dfList.append(spark.createDataFrame([(guid)], "STRING"))
      dfList.append(dfList[-1].withColumnRenamed("value", "jobLogGuid"))
      dfList.append(dfList[-1].withColumn("stageLogGuid", lit(stageGuid)))
      dfList.append(dfList[-1].withColumn("jobKey", lit(int(jobKey)))) 
      dfList.append(dfList[-1].withColumn("startDateTime", lit(startDateTime)))
      dfList.append(dfList[-1].withColumn("Parameters", parametersMap))
      dfList.append(dfList[-1].withColumn("Context", lit(self.context.replace("'","''"))))
      dfList[-1].createOrReplaceTempView("startDF") 
      query = """
      INSERT INTO silverprotected.jobLog (JobLogGuid, StageLogGuid, JobKey, StartDateTime, EndDateTime, LogStatusKey, Parameters, Context, Error)
      SELECT jobLogGuid, stageLogGuid, jobKey, startDateTime, NULL, 1, Parameters, Context, NULL FROM startDF
      """
      spark.sql(query) 

  def log_job_error (self, guid, error, jobKey):
    if self.LogMethod in (1, 3):
      query = """EXEC dbo.LogJobError
        @JobLogGuid='{0}'
        ,@JobKey={1}
        ,@Error='{2}';
      """.format(guid, jobKey, error)
      self.pyodbcStoredProcedure(query)
    
    if self.LogMethod in (2, 3):
      errorMap = self.create_map_from_dictionary(error)
      endDateTime = datetime.datetime.now()
      dfList = []
      dfList.append(spark.createDataFrame([(guid)], "STRING"))
      dfList.append(dfList[-1].withColumnRenamed("value", "jobLogGuid"))
      dfList.append(dfList[-1].withColumn("endDateTime", lit(endDateTime)))
      dfList.append(dfList[-1].withColumn("error", errorMap))
      dfList[-1].createOrReplaceTempView("errorDF")     

      query = """
      INSERT INTO silverprotected.jobLog (JobLogGuid, StageLogGuid, JobKey, StartDateTime, EndDateTime, LogStatusKey, Parameters, Context, Error)
      SELECT jobLogGuid, NULL, NULL, NULL, endDateTime, 3, NULL, NULL, error FROM errorDF
      """
      spark.sql(query)        

  def log_job_end (self, guid, jobKey):
    if self.LogMethod in (1, 3):
      query = """EXEC dbo.LogJobEnd
        @JobLogGuid='{0}'
        ,@JobKey={1};
      """.format(guid, jobKey)
      self.pyodbcStoredProcedure(query)
    
    if self.LogMethod in (2, 3):
      endDateTime = datetime.datetime.now()
      dfList = []
      dfList.append(spark.createDataFrame([(guid)], "STRING"))
      dfList.append(dfList[-1].withColumnRenamed("value", "jobLogGuid"))
      dfList.append(dfList[-1].withColumn("endDateTime", lit(endDateTime)))
      dfList[-1].createOrReplaceTempView("endDF")

      query = """
      INSERT INTO silverprotected.jobLog (JobLogGuid, StageLogGuid, JobKey, StartDateTime, EndDateTime, LogStatusKey, Parameters, Context, Error)
      SELECT jobLogGuid, NULL, NULL, NULL, endDateTime, 2, NULL, NULL, NULL FROM endDF
      """
      spark.sql(query)        

  def reset_job(self, jobKey):
    query = """EXEC dbo.ResetJob
    @JobKey={0};
    """.format(jobKey)
    self.pyodbcStoredProcedure(query)

  def get_job(self, jobName, jobKey):
    url, properties = self.jdbcConnectionString()
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

  def get_all_jobs(self):
    url, properties = self.jdbcConnectionString()
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
#Stage
  def getStageOrder(self, stage):
    return stage['StageOrder']

  def run_stage(self, systemLogGuid, stage, threadPool):
    p = {
      "systemLogGuid": systemLogGuid,
      "stageName": stage['StageName'],
      "stageKey": stage['StageKey'],
      "dateToProcess": self.dateToProcess,
      "threadPool": self.threadPool,
      "timeout": self.timeoutSeconds
    }
    parameters = json.dumps(p)
    stageLogGuid = str(uuid.uuid4())
    self.log_stage_start(stageLogGuid, systemLogGuid, stage['StageKey'], parameters)
    try:
      jobs = stage['j']
      jobs.sort(key=self.getJobOrder)
      pool = ThreadPool(threadPool)
      for job in jobs:
        print("{0}: running job: {1}".format(self.getCurrentTimestamp(), job['JobName']))
      if self.threadPool == 1:
        for job in jobs:
          self.run_job(stageLogGuid, job)
      else:
        pool.map(lambda job: self.run_job(stageLogGuid, job), jobs)
    except Exception as e:
      err = {
        "sourceName": "Orchestration: run_stage",
        "errorCode": 103,
        "errorDescription": e.__class__.__name__
      }
      error = json.dumps(err)
      self.log_stage_error(stageLogGuid, error, stage['StageKey'])
      raise e
    self.log_stage_end(stageLogGuid, stage['StageKey'])
    self.reset_stage(stage['StageKey'])

  def log_stage_start (self, guid, systemGuid, stageKey, parameters):
    if self.LogMethod in (1, 3):
      query = """EXEC dbo.LogStageStart
      @StageLogGuid='{0}'
      ,@SystemLogGuid='{1}'
      ,@StageKey={2}
      ,@Parameters='{3}'
      ,@Context='{4}';
      """.format(guid, systemGuid, stageKey, parameters, self.context)
      self.pyodbcStoredProcedure(query)
    
    if self.LogMethod in (2, 3):
      startDateTime = datetime.datetime.now()
      parametersMap = self.create_map_from_dictionary(parameters)
      dfList = []
      dfList.append(spark.createDataFrame([(guid)], "STRING"))
      dfList.append(dfList[-1].withColumnRenamed("value", "stageLogGuid"))
      dfList.append(dfList[-1].withColumn("systemLogGuid", lit(systemGuid)))
      dfList.append(dfList[-1].withColumn("stageKey", lit(int(stageKey))))      
      dfList.append(dfList[-1].withColumn("startDateTime", lit(startDateTime)))
      dfList.append(dfList[-1].withColumn("Parameters", parametersMap))
      dfList.append(dfList[-1].withColumn("Context", lit(self.context.replace("'","''"))))
      dfList[-1].createOrReplaceTempView("startDF") 

      query = """
      INSERT INTO silverprotected.stageLog (StageLogGuid, SystemLogGuid, StageKey, StartDateTime, EndDateTime, LogStatusKey, Parameters, Context, Error)
      SELECT stageLogGuid, systemLogGuid, stageKey, startDateTime, NULL, 1, Parameters, Context, NULL FROM startDF
      """
      spark.sql(query) 

  def log_stage_error (self, guid, error, stageKey):
    if self.LogMethod in (1, 3):
      query = """EXEC dbo.LogStageError
      @StageLogGuid='{0}'
      ,@StageKey={1}
      ,@Error='{2}';
      """.format(guid, stageKey, error)
      self.pyodbcStoredProcedure(query)
    
    if self.LogMethod in (2, 3):
      errorMap = self.create_map_from_dictionary(error)
      endDateTime = datetime.datetime.now()
      dfList = []
      dfList.append(spark.createDataFrame([(guid)], "STRING"))
      dfList.append(dfList[-1].withColumnRenamed("value", "stageLogGuid"))
      dfList.append(dfList[-1].withColumn("endDateTime", lit(endDateTime)))
      dfList.append(dfList[-1].withColumn("error", errorMap))
      dfList[-1].createOrReplaceTempView("errorDF")

      query = """
      INSERT INTO silverprotected.stageLog (StageLogGuid, SystemLogGuid, StageKey, StartDateTime, EndDateTime, LogStatusKey, Parameters, Context, Error)
      SELECT stageLogGuid, NULL, NULL, NULL, endDateTime, 3, NULL, NULL, error FROM errorDF
      """
      spark.sql(query)

  def log_stage_end (self, guid, stageKey):
    if self.LogMethod in (1, 3):
      query = """EXEC dbo.LogStageEnd
      @StageLogGuid='{0}'
      ,@StageKey={1};
      """.format(guid, stageKey)
      self.pyodbcStoredProcedure(query)
    
    if self.LogMethod in (2, 3):
      endDateTime = datetime.datetime.now()
      dfList = []
      dfList.append(spark.createDataFrame([(guid)], "STRING"))
      dfList.append(dfList[-1].withColumnRenamed("value", "stageLogGuid"))
      dfList.append(dfList[-1].withColumn("endDateTime", lit(endDateTime)))
      dfList[-1].createOrReplaceTempView("endDF")
    
      query = """
      INSERT INTO silverprotected.stageLog (StageLogGuid, SystemLogGuid, StageKey, StartDateTime, EndDateTime, LogStatusKey, Parameters, Context, Error)
      SELECT stageLogGuid, NULL, NULL, NULL, endDateTime, 2, NULL, NULL, NULL FROM endDF
      """
      spark.sql(query)

  def reset_stage(self, stageKey):
    query = """EXEC dbo.ResetStage
    @StageKey={0};
    """.format(stageKey)
    self.pyodbcStoredProcedure(query)

  def get_stage(self, stageName):
    url, properties = self.jdbcConnectionString()
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

  def get_all_stages(self):
    url, properties = self.jdbcConnectionString()
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
#System
  def getSystemOrder(self, system):
    return system['SystemOrder']

  def run_system(self, projectLogGuid, system):
    p = {
    "projectLogGuid": projectLogGuid,
    "systemName": system['SystemName'],
    "systemKey": system['SystemKey'],
    "dateToProcess": self.dateToProcess,
    "threadPool": self.threadPool,
    "timeout": self.timeoutSeconds
    }
    parameters = json.dumps(p)
    systemLogGuid = str(uuid.uuid4())
    self.log_system_start(systemLogGuid, projectLogGuid, system['SystemKey'], parameters)
    try:
      stages = system['st']
      stages.sort(key=self.getStageOrder)
      for stage in stages:
        print("{0}: running stage: {1}".format(self.getCurrentTimestamp(), stage['StageName']))
        self.run_stage(systemLogGuid, stage, stage['NumberOfThreads'])
    except Exception as e:
      err = {
        "sourceName": "Orchestration: run_system",
        "errorCode": 102,
        "errorDescription": e.__class__.__name__
      }
      error = json.dumps(err)
      self.log_system_error(systemLogGuid, error, system['SystemKey'])
      raise e
    self.log_system_end(systemLogGuid, system['SystemKey'])
    self.reset_system(system['SystemKey'])

  def log_system_start (self, guid, projectGuid, systemKey, parameters):
    if self.LogMethod in (1, 3):
      query = """EXEC dbo.LogSystemStart
      @SystemLogGuid='{0}'
      ,@ProjectLogGuid='{1}'
      ,@SystemKey={2}
      ,@Parameters='{3}'
      ,@Context='{4}';
      """.format(guid, projectGuid, systemKey, parameters, self.context)
      self.pyodbcStoredProcedure(query)
    if self.LogMethod in (2, 3):
      startDateTime = datetime.datetime.now()
      parametersMap = self.create_map_from_dictionary(parameters)
      dfList = []
      dfList.append(spark.createDataFrame([(guid)], "STRING"))
      dfList.append(dfList[-1].withColumnRenamed("value", "systemLogGuid"))
      dfList.append(dfList[-1].withColumn("projectLogGuid", lit(projectGuid)))
      dfList.append(dfList[-1].withColumn("systemKey", lit(int(systemKey))))      
      dfList.append(dfList[-1].withColumn("startDateTime", lit(startDateTime)))
      dfList.append(dfList[-1].withColumn("Parameters", parametersMap))
      dfList.append(dfList[-1].withColumn("Context", lit(self.context.replace("'","''"))))
      dfList[-1].createOrReplaceTempView("startDF") 
      query = """
      INSERT INTO silverprotected.systemLog (SystemLogGuid, ProjectLogGuid, SystemKey, StartDateTime, EndDateTime, LogStatusKey, Parameters, Context, Error)
      SELECT systemLogGuid, projectLogGuid, systemKey, startDateTime, NULL, 1, Parameters, Context, NULL FROM startDF
      """
      spark.sql(query) 

  def log_system_error (self, guid, error, systemKey):
    if self.LogMethod in (1, 3):
      query = """EXEC dbo.LogSystemError
      @SystemLogGuid='{0}'
      ,@SystemKey={1}
      ,@Error='{2}';
      """.format(guid, systemKey, error)
      self.pyodbcStoredProcedure(query)
    if self.LogMethod in (2, 3):
      errorMap = self.create_map_from_dictionary(error)
      endDateTime = datetime.datetime.now()
      dfList = []
      dfList.append(spark.createDataFrame([(guid)], "STRING"))
      dfList.append(dfList[-1].withColumnRenamed("value", "systemLogGuid"))
      dfList.append(dfList[-1].withColumn("endDateTime", lit(endDateTime)))
      dfList.append(dfList[-1].withColumn("error", errorMap))
      dfList[-1].createOrReplaceTempView("errorDF")
      query = """
      INSERT INTO silverprotected.systemLog (SystemLogGuid, ProjectLogGuid, SystemKey, StartDateTime, EndDateTime, LogStatusKey, Parameters, Context, Error)
      SELECT systemLogGuid, NULL, NULL, NULL, endDateTime, 3, NULL, NULL, error FROM errorDF
      """
      spark.sql(query)     
    
  def log_system_end (self, guid, systemKey):
    if self.LogMethod in (1, 3):
      query = """EXEC dbo.LogSystemEnd
      @SystemLogGuid='{0}'
      ,@SystemKey={1};
      """.format(guid, systemKey)
      self.pyodbcStoredProcedure(query)
    if self.LogMethod in (2, 3):
      endDateTime = datetime.datetime.now()
      dfList = []
      dfList.append(spark.createDataFrame([(guid)], "STRING"))
      dfList.append(dfList[-1].withColumnRenamed("value", "systemLogGuid"))
      dfList.append(dfList[-1].withColumn("endDateTime", lit(endDateTime)))
      dfList[-1].createOrReplaceTempView("endDF")
      query = """
      INSERT INTO silverprotected.systemLog (SystemLogGuid, ProjectLogGuid, SystemKey, StartDateTime, EndDateTime, LogStatusKey, Parameters, Context, Error)
      SELECT systemLogGuid, NULL, NULL, NULL, endDateTime, 2, NULL, NULL, NULL FROM endDF
      """
      spark.sql(query)    
          
  def reset_system(self, systemKey):
    query = """EXEC dbo.ResetSystem
    @SystemKey={0};
    """.format(systemKey)
    self.pyodbcStoredProcedure(query)

  def get_system(self, systemName):
    jdbcUrl, connectionProperties = self.jdbcConnectionString()
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

  def get_all_systems(self):
    jdbcUrl, connectionProperties = self.jdbcConnectionString()
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
#project
  def run_project(self):
    p = {
      "projectName": self.projectName,
      "dateToProcess": self.dateToProcess,
      "threadPool": self.threadPool,
      "timeout": self.timeoutSeconds
    }
    parameters = json.dumps(p)
    projectLogGuid = str(uuid.uuid4())
    self.log_project_start(projectLogGuid, parameters)
    try:
      results = self.get_project_json()
      project = json.loads(results[0].results)
      systems = project['s']
      systems.sort(key=self.getSystemOrder)
      for system in systems:
        print("{0}: running system: {1}".format(self.getCurrentTimestamp(), system['SystemName']))
        self.run_system(projectLogGuid, system)
    except Exception as e:
      err = {
        "sourceName": "Orchestration: run_project",
        "errorCode": 101,
        "errorDescription": e.__class__.__name__
      }
      error = json.dumps(err)
      self.log_project_error(projectLogGuid, error, project['ProjectKey'])
      raise e
    self.log_project_end(projectLogGuid, project['ProjectKey'])
    self.reset_project(project['ProjectKey'])

  def get_project_json (self):
    query = """
      DECLARE @JSON VARCHAR(MAX);
      EXEC dbo.RunProject
      @ProjectName = '{0}'
      ,@JSON=@JSON OUTPUT;
      SELECT @JSON AS results;
    """.format(self.projectName)
    results = self.pyodbcStoredProcedureResults(query)
    return results

  def log_project_start (self, guid, parameters):
    if self.LogMethod in (1, 3):
      query = """EXEC dbo.LogProjectStart
        @ProjectLogGuid='{0}'
        ,@Parameters='{1}'
        ,@Context='{2}';
      """.format(guid, parameters, self.context)
      self.pyodbcStoredProcedure(query)
    if self.LogMethod in (2, 3):
      startDateTime = datetime.datetime.now()
      parametersMap = self.create_map_from_dictionary(parameters)
      dfList = []
      dfList.append(spark.createDataFrame([(guid)], "STRING"))
      dfList.append(dfList[-1].withColumnRenamed("value", "projectLogGuid"))    
      dfList.append(dfList[-1].withColumn("startDateTime", lit(startDateTime)))
      dfList.append(dfList[-1].withColumn("Parameters", parametersMap))
      dfList.append(dfList[-1].withColumn("Context", lit(self.context.replace("'","''"))))
      dfList[-1].createOrReplaceTempView("startDF") 
      query = """
      INSERT INTO silverprotected.projectLog (ProjectLogGuid, ProjectKey, StartDateTime, EndDateTime, LogStatusKey, Parameters, Context, Error)
      SELECT projectLogGuid, NULL, startDateTime, NULL, 1, Parameters, Context, NULL FROM startDF
      """
      spark.sql(query)       

  def log_project_error (self, guid, error, projectKey=None):
    if self.LogMethod in (1, 3):    
      query = """EXEC dbo.LogProjectError
        @ProjectLogGuid='{0}'
        ,@ProjectKey={1}
        ,@Error='{2}';
      """.format(guid, projectKey, error)
      self.pyodbcStoredProcedure(query)
    if self.LogMethod in (2, 3):
      errorMap = self.create_map_from_dictionary(error)
      endDateTime = datetime.datetime.now()
      dfList = []
      dfList.append(spark.createDataFrame([(guid)], "STRING"))
      dfList.append(dfList[-1].withColumnRenamed("value", "projectLogGuid"))
      dfList.append(dfList[-1].withColumn("endDateTime", lit(endDateTime)))
      dfList.append(dfList[-1].withColumn("error", errorMap))
      dfList[-1].createOrReplaceTempView("errorDF")      
      query = """
      INSERT INTO silverprotected.projectLog (ProjectLogGuid, ProjectKey, StartDateTime, EndDateTime, LogStatusKey, Parameters, Context, Error)
      SELECT projectLogGuid, NULL, NULL, endDateTime, 3, NULL, NULL, error FROM errorDF
      """
      spark.sql(query)       

  def log_project_end (self, guid, projectKey):
    if self.LogMethod in (1, 3):
      query = """EXEC dbo.LogProjectEnd
      @ProjectLogGuid='{0}'
      ,@ProjectKey={1};
      """.format(guid, projectKey)
      self.pyodbcStoredProcedure(query)
    if self.LogMethod in (2, 3): 
      endDateTime = datetime.datetime.now()
      dfList = []
      dfList.append(spark.createDataFrame([(guid)], "STRING"))
      dfList.append(dfList[-1].withColumnRenamed("value", "projectLogGuid"))
      dfList.append(dfList[-1].withColumn("endDateTime", lit(endDateTime)))
      dfList[-1].createOrReplaceTempView("endDF")    
      query = """
      INSERT INTO silverprotected.projectLog (ProjectLogGuid, ProjectKey, StartDateTime, EndDateTime, LogStatusKey, Parameters, Context, Error)
      SELECT projectLogGuid, NULL, NULL, endDateTime, 2, NULL, NULL, NULL FROM endDF
      """
      spark.sql(query)       

  def reset_project (self, projectKey):
    query = """EXEC dbo.ResetProject
    @ProjectKey={0};
    """.format(projectKey)
    self.pyodbcStoredProcedure(query)

  def get_project(self):
    url, properties = self.jdbcConnectionString()
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
    ) t""".format(self.projectName)
    df = spark.read.jdbc(url=url, table=query, properties=properties)
    return df

  def get_all_projects(self):
    url, properties = self.jdbcConnectionString()
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

  def createOrchestrationSchema(self):
    if self.LogMethod in (2, 3):
      silverDataPath = "{0}/{1}".format(self.SilverProtectedBasePath, "orchestration")

      notebooklogsql = """
      CREATE TABLE IF NOT EXISTS silverprotected.notebooklog
      (
            NotebookLogGuid STRING NOT NULL 
          ,StepLogGuid STRING 
          ,StepKey INT 
          ,StartDateTime TIMESTAMP 
          ,EndDateTime TIMESTAMP 
          ,LogStatusKey INT NOT NULL
          ,RowsAffected INT 
          ,Parameters MAP<STRING,STRING>
          ,Context STRING 
          ,Error MAP<STRING,STRING>
      )
      USING delta 
      LOCATION '{0}/notebooklog'
      """.format(silverDataPath)
      spark.sql(notebooklogsql)

      notebooklogviewsql = """
      CREATE VIEW IF NOT EXISTS silverprotected.vNotebookLog
      AS
      WITH nlCTE AS
      (
        SELECT
          NotebookLogGuid,
          MAX(StepLogGuid) AS StepLogGuid,
          MAX(StepKey) AS StepKey,
          MAX(StartDateTime) AS StartDateTime,
          MAX(EndDateTime) AS EndDateTime,
          MAX(LogStatusKey) AS LogStatusKey,
          MAX(RowsAffected) AS RowsAffected,
          MAX(Context) AS Context
        FROM silverprotected.notebooklog
        GROUP BY NotebookLogGuid
      )
      SELECT nlCTE.NotebookLogGuid, nlCTE.StepLogGuid, nlCTE.StepKey, nlCTE.StartDateTime, nlCTE.EndDateTime, nlCTE.LogStatusKey, nlCTE.RowsAffected, nl.Parameters, nlCTE.Context, COALESCE(nle.Error, nl.Error) AS Error
      FROM nlCTE 
      JOIN silverprotected.notebooklog nl ON nlCTE.NotebookLogGuid = nl.NotebookLogGuid AND nl.LogStatusKey = 1
      LEFT JOIN silverprotected.notebooklog nle ON nlCTE.NotebookLogGuid = nle.NotebookLogGuid AND nle.LogStatusKey = 3
      """
      spark.sql(notebooklogviewsql)

      steplogsql = """
      CREATE TABLE IF NOT EXISTS silverprotected.steplog
      (
            StepLogGuid STRING NOT NULL 
          ,JobLogGuid STRING 
          ,StepKey INT 
          ,StartDateTime TIMESTAMP 
          ,EndDateTime TIMESTAMP 
          ,LogStatusKey INT NOT NULL
          ,Parameters MAP<STRING,STRING>
          ,Context STRING 
          ,Error MAP<STRING,STRING>
      )
      USING delta 
      LOCATION '{0}/steplog'
      """.format(silverDataPath)
      spark.sql(steplogsql)

      steplogviewsql = """
      CREATE VIEW IF NOT EXISTS silverprotected.vStepLog
      AS
      WITH slCTE AS
      (
        SELECT
          StepLogGuid,
          MAX(StepKey) AS StepKey,
          MAX(StartDateTime) AS StartDateTime,
          MAX(EndDateTime) AS EndDateTime,
          MAX(LogStatusKey) AS LogStatusKey,
          MAX(Context) AS Context
        FROM silverprotected.steplog
        GROUP BY StepLogGuid
      )
      SELECT slCTE.StepLogGuid, slCTE.StepKey, slCTE.StartDateTime, slCTE.EndDateTime, slCTE.LogStatusKey, sl.Parameters, slCTE.Context, COALESCE(sle.Error, sl.Error) AS Error
      FROM slCTE 
      JOIN silverprotected.steplog sl ON slCTE.StepLogGuid = sl.StepLogGuid AND sl.LogStatusKey = 1
      LEFT JOIN silverprotected.steplog sle ON slCTE.StepLogGuid = sle.StepLogGuid AND sle.LogStatusKey = 3
      """
      spark.sql(steplogviewsql)

      joblogsql = """
      CREATE TABLE IF NOT EXISTS silverprotected.jobLog
      (
            JobLogGuid STRING NOT NULL 
          ,StageLogGuid STRING
          ,JobKey INT 
          ,StartDateTime TIMESTAMP 
          ,EndDateTime TIMESTAMP 
          ,LogStatusKey INT NOT NULL
          ,Parameters MAP<STRING,STRING>
          ,Context STRING 
          ,Error MAP<STRING,STRING>
      )
      USING delta 
      LOCATION '{0}/joblog'
      """.format(silverDataPath)
      spark.sql(joblogsql)

      joblogviewsql = """
      CREATE VIEW IF NOT EXISTS silverprotected.vJobLog
      AS
      WITH jlCTE AS
      (
        SELECT
          JobLogGuid,
          MAX(JobKey) AS JobKey,
          MAX(StartDateTime) AS StartDateTime,
          MAX(EndDateTime) AS EndDateTime,
          MAX(LogStatusKey) AS LogStatusKey,
          MAX(Context) AS Context
        FROM silverprotected.joblog 
        GROUP BY JobLogGuid
      )
      SELECT jlCTE.JobLogGuid, jlCTE.JobKey, jlCTE.StartDateTime, jlCTE.EndDateTime, jlCTE.LogStatusKey, jl.Parameters, jlCTE.Context, COALESCE(jle.Error, jl.Error) AS Error
      FROM jlCTE 
      JOIN silverprotected.joblog jl ON jlCTE.JobLogGuid = jl.JobLogGuid AND jl.LogStatusKey = 1
      LEFT JOIN silverprotected.joblog jle ON jlCTE.JobLogGuid = jle.JobLogGuid AND jle.LogStatusKey = 3
      """
      spark.sql(joblogviewsql)

      stagelogsql = """
      CREATE TABLE IF NOT EXISTS silverprotected.stagelog
      (
            StageLogGuid STRING NOT NULL 
          ,SystemLogGuid STRING
          ,StageKey INT 
          ,StartDateTime TIMESTAMP 
          ,EndDateTime TIMESTAMP 
          ,LogStatusKey INT NOT NULL
          ,Parameters MAP<STRING,STRING>
          ,Context STRING 
          ,Error MAP<STRING,STRING>
      )
      USING delta 
      LOCATION '{0}/stagelog'
      """.format(silverDataPath)
      spark.sql(stagelogsql)

      stagelogviewsql = """
      CREATE VIEW IF NOT EXISTS silverprotected.vStageLog
      AS
      WITH slCTE AS
      (
        SELECT
          StageLogGuid,
          MAX(StageKey) AS StageKey,
          MAX(StartDateTime) AS StartDateTime,
          MAX(EndDateTime) AS EndDateTime,
          MAX(LogStatusKey) AS LogStatusKey,
          MAX(Context) AS Context
        FROM silverprotected.stagelog 
        GROUP BY StageLogGuid
      )
      SELECT slCTE.StageLogGuid, slCTE.StageKey, slCTE.StartDateTime, slCTE.EndDateTime, slCTE.LogStatusKey, sl.Parameters, slCTE.Context, COALESCE(sle.Error, sl.Error) AS Error
      FROM slCTE 
      JOIN silverprotected.stagelog sl ON slCTE.StageLogGuid = sl.StageLogGuid AND sl.LogStatusKey = 1
      LEFT JOIN silverprotected.stagelog sle ON slCTE.StageLogGuid = sle.StageLogGuid AND sle.LogStatusKey = 3
      """
      spark.sql(stagelogviewsql)

      systemlogsql = """
      CREATE TABLE IF NOT EXISTS silverprotected.systemlog
      (
            SystemLogGuid STRING NOT NULL 
          ,ProjectLogGuid STRING
          ,SystemKey INT 
          ,StartDateTime TIMESTAMP 
          ,EndDateTime TIMESTAMP 
          ,LogStatusKey INT NOT NULL
          ,Parameters MAP<STRING,STRING>
          ,Context STRING 
          ,Error MAP<STRING,STRING>
      )
      USING delta 
      LOCATION '{0}/systemlog'
      """.format(silverDataPath)
      spark.sql(systemlogsql)

      systemlogviewsql = """
      CREATE VIEW IF NOT EXISTS silverprotected.vSystemLog
      AS
      WITH slCTE AS
      (
        SELECT
          SystemLogGuid,
          MAX(SystemKey) AS SystemKey,
          MAX(StartDateTime) AS StartDateTime,
          MAX(EndDateTime) AS EndDateTime,
          MAX(LogStatusKey) AS LogStatusKey,
          MAX(Context) AS Context
        FROM silverprotected.systemlog 
        GROUP BY SystemLogGuid
      )
      SELECT slCTE.SystemLogGuid, slCTE.SystemKey, slCTE.StartDateTime, slCTE.EndDateTime, slCTE.LogStatusKey, sl.Parameters, slCTE.Context, COALESCE(sle.Error, sl.Error) AS Error
      FROM slCTE 
      JOIN silverprotected.systemlog sl ON slCTE.SystemLogGuid = sl.SystemLogGuid AND sl.LogStatusKey = 1
      LEFT JOIN silverprotected.systemlog sle ON slCTE.SystemLogGuid = sle.SystemLogGuid AND sle.LogStatusKey = 3
      """
      spark.sql(systemlogviewsql)

      projectlogsql = """
      CREATE TABLE IF NOT EXISTS silverprotected.projectlog
      (
            ProjectLogGuid STRING NOT NULL 
          ,ProjectKey INT 
          ,StartDateTime TIMESTAMP 
          ,EndDateTime TIMESTAMP 
          ,LogStatusKey INT NOT NULL
          ,Parameters MAP<STRING,STRING>
          ,Context STRING 
          ,Error MAP<STRING,STRING>
      )
      USING delta 
      LOCATION '{0}/projectlog'
      """.format(silverDataPath)
      spark.sql(projectlogsql)

      projectlogviewsql = """
      CREATE VIEW IF NOT EXISTS silverprotected.vProjectLog
      AS
      WITH plCTE AS
      (
        SELECT
          ProjectLogGuid,
          MAX(ProjectKey) AS ProjectKey,
          MAX(StartDateTime) AS StartDateTime,
          MAX(EndDateTime) AS EndDateTime,
          MAX(LogStatusKey) AS LogStatusKey,
          MAX(Context) AS Context
        FROM silverprotected.projectlog 
        GROUP BY ProjectLogGuid
      )
      SELECT plCTE.ProjectLogGuid, plCTE.ProjectKey, plCTE.StartDateTime, plCTE.EndDateTime, plCTE.LogStatusKey, pl.Parameters, plCTE.Context, COALESCE(ple.Error, pl.Error) AS Error
      FROM plCTE 
      JOIN silverprotected.projectlog pl ON plCTE.ProjectLogGuid = pl.ProjectLogGuid AND pl.LogStatusKey = 1
      LEFT JOIN silverprotected.projectlog ple ON plCTE.ProjectLogGuid = ple.ProjectLogGuid AND ple.LogStatusKey = 3
      """
      spark.sql(projectlogviewsql)