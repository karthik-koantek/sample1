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

class OrchestrationDeltaNotebook(Notebook):
  def __init__(self, cloud="Azure", logMethod=2):
    super().__init__(cloud, logMethod)
    self.setFromWidget("ADFProjectName")
    self.setFromWidget("projectName")
    self.setFromWidget("threadPool")
    self.setFromWidget("timeoutSeconds")
    self.setFromWidget("whatIf")
    self.setFromWidget("repoPath")
    self.setFromWidget("hydrationBehavior")
    self.setDateToProcess()
    #self.createOrchestrationSchema()

  def getCurrentTimestamp(self):
    dateTimeObj = datetime.datetime.utcnow()
    timestampStr = dateTimeObj.strftime("%d-%b-%Y (%H:%M:%S.%f)")
    return timestampStr

  def runWithRetryLogExceptions(self, notebook, args = {}, max_retries = 0):
    dbutils = self.get_dbutils()
    num_retries = 0
    while True:
      try:
        print("\n{0}:running notebook {1} with args {2}\n".format(self.getCurrentTimestamp(), notebook, args))
        return dbutils.notebook.run(notebook, int(self.timeoutSeconds), args)
      except Exception as e:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        #error = repr(traceback.format_exception(exc_type, exc_value, exc_traceback)).replace("'","")
        error = {
          "exc_type": exc_type,
          "exc_value": exc_value,
          "exc_traceback": exc_traceback
        }
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

#Parameter
  def validate_input(self, parameter):
    if parameter == "" or parameter is None:
      raise ValueError("The value for parameter is not supported.")

  def create_parameters_df(self, parameters, step_name, job_name):
    from pyspark.sql.functions import sha2, concat, lit, col
    step_name_salt = self.generate_step_name(step_name, 18)
    parameters_dict = json.dumps(parameters)
    parameters_map = self.create_map_from_dictionary(parameters_dict)
    df_list = []
    parameters_tuple = [(1, step_name)]
    df_list.append(spark.createDataFrame(parameters_tuple))
    df_list.append(df_list[-1].withColumnRenamed("_2", "step_name"))
    df_list.append(df_list[-1].withColumn("job_key", sha2(lit(job_name), 256)))
    df_list.append(df_list[-1].withColumn("step_key", sha2(concat(df_list[-1].job_key,lit(step_name_salt)), 256)))
    df_list.append(df_list[-1].drop(col("_1")))
    df_list.append(df_list[-1].drop(col("step_name")))
    df_list.append(df_list[-1].drop(col("job_key")))
    df_list.append(df_list[-1].withColumn("parameters", parameters_map))
    return df_list[-1], df_list[-1].select("step_key").head()[0]

  def insert_parameters(self,
    project_name, 
    system_name, 
    stage_name,
    job_name,
    step_name,
    system_secret_scope, 
    parameters,
    system_is_active=True, 
    stage_is_active=True, 
    job_is_active=True, 
    step_is_active=True,
    system_is_restart=True, 
    stage_is_restart=True, 
    job_is_restart=True, 
    step_is_restart=True, 
    system_order=10,
    stage_order=10,
    job_order=10,
    step_order=10
    ):

    parameters_df, step_key = self.create_parameters_df(parameters, step_name, job_name)
    parameters_df.createOrReplaceTempView("parameters_df")
    
    sql = """
    INSERT INTO bronze.parameter_{0} (StepKey, Parameters, CreatedDate, ModifiedDate) 
    SELECT step_key, parameters, current_timestamp(), NULL FROM parameters_df
    """.format(project_name)
    spark.sql(sql)
    
    self.insert_step(project_name=project_name, system_name=system_name, system_secret_scope=system_secret_scope, system_order=system_order, system_is_active=system_is_active, system_is_restart=system_is_restart, stage_name=stage_name, stage_is_active=stage_is_active, stage_is_restart=stage_is_restart, stage_order=stage_order, job_name=job_name, job_order=job_order, job_is_active=job_is_active, job_is_restart=job_is_restart, step_key=step_key, step_name=step_name, step_order=step_order, step_is_active=step_is_active, step_is_restart=step_is_restart)

  def load_orchestration_parameter(self, project_name):
    sql = """
      MERGE INTO silverprotected.parameter tgt
      USING bronze.parameter_{0} src ON tgt.StepKey=src.StepKey
      WHEN MATCHED THEN UPDATE SET 
        tgt.Parameters = src.Parameters, 
        tgt.ModifiedDate = current_timestamp()
      WHEN NOT MATCHED THEN 
      INSERT (StepKey, Parameters, CreatedDate) 
      VALUES (src.StepKey, src.Parameters, current_timestamp())
    """.format(project_name)
    spark.sql(sql) 

#windowed extraction
  def generate_numbers(self, numbers):
    sql = """
    WITH 
    E1(N) AS (SELECT 1 UNION ALL SELECT 1),
    E2(N) AS (SELECT 1 FROM E1 a, E1 b),
    E4(N) AS (SELECT 1 FROM E2 a, E2 b),
    E8(N) AS (SELECT 1 FROM E4 a, E4 b),
    E16(N) AS (SELECT 1 FROM E8 a, E8 b),
    Tally(N) AS (SELECT row_number() OVER(ORDER BY(SELECT NULL)) FROM E16 LIMIT {0})
    SELECT N FROM Tally
    """.format(numbers)
    return spark.sql(sql)

  def generate_dates(self, from_date, numbers=10000):
    numbers = self.generate_numbers(numbers)
    numbers.createOrReplaceTempView("numbers")
    dt = spark.sql("SELECT date_add(date('{0}'), n.N-1) as DT FROM numbers n".format(from_date))
    dt.createOrReplaceTempView("dt")
    dp = spark.sql("""
    SELECT
      DT,
      TIMESTAMP(DT) AS DTT,
      date_part('YEAR', DT) AS YY,
      date_part('MONTH', DT) AS MM,
      date_part('day', DT) AS DD,
      date_part('dow', DT) AS DW,
      date_part('DOY', DT) AS DY,
      last_day(DT) AS LastDayOfMonth
    FROM dt
    """)
    return dp

  def insert_windowed_extraction (self, project_name, job_name, step_name, windowed_extraction_begin_date, windowed_extraction_end_date, windowed_extraction_interval='Y', windowed_extraction_process_latest_window_first=True, delete_existing=True):
    from pyspark.sql.functions import date_add, col, min, max, sha2, concat, lit, row_number
    from pyspark.sql.window import Window
                
    dp = []
    dp.append(self.generate_dates(windowed_extraction_begin_date, numbers=10000))
    
    if windowed_extraction_interval=="W":
      dp.append(dp[-1] \
        .filter(col("DW")==1) \
        .withColumn("WindowStart", col("DT")) \
        .withColumn("WindowEnd", date_add(col("DT"), 7)) \
        .select("WindowStart", "WindowEnd"))
    else:
      if windowed_extraction_interval == "Y":
        group_by = ["YY"]
      else:
        group_by = ["YY", "MM"]
      dp.append(
        dp[-1] \
          .groupBy(group_by) \
          .agg( 
              min("DT").alias("WindowStart"), \
              max("DT").alias("WindowEnd")
              ) \
          .select("WindowStart", "WindowEnd")
      )

    dp.append(dp[-1].withColumn("JobKey", sha2(lit(job_name), 256)))
    dp.append(dp[-1].withColumn("StepKey", sha2(concat(col("JobKey"), lit(step_name)), 256)))
    dp.append(dp[-1].drop(col("JobKey")))
    dp.append(dp[-1].withColumn("IsActive", lit(True)))
    dp.append(dp[-1].filter(col("WindowStart")>lit(windowed_extraction_begin_date)))
    dp.append(dp[-1].filter(col("WindowEnd")<lit(windowed_extraction_end_date)))
    if windowed_extraction_process_latest_window_first==True:
      w = Window.orderBy(col("WindowStart").desc())
    else:
      w = Window.orderBy(col("WindowStart").asc())
    dp.append(dp[-1].withColumn("WindowOrder", row_number().over(w)))

    dp[-1].createOrReplaceTempView("dp")
    
    sql = """
    INSERT INTO bronze.windowedextraction_{0} (StepKey, WindowStart, WindowEnd, WindowOrder, IsActive, ExtractionTimestamp, QueryZoneTimestamp)
    SELECT StepKey, WindowStart, WindowEnd, WindowOrder, IsActive, NULL, NULL FROM dp
    """.format(project_name)
    spark.sql(sql)

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
      parameters = self.get_parameters(step['StepKey'])
      args = {
        "stepLogGuid": stepLogGuid,
        "stepKey": step['StepKey'],
        "dateToProcess": self.dateToProcess,
        "timeoutSeconds": self.timeoutSeconds
      }
      for parameter in parameters:
        args[parameter['key']] = parameter['value']
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
        ,@StepKey='{2}'
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
      dfList.append(dfList[-1].withColumn("stepKey", lit(stepKey)))      
      dfList.append(dfList[-1].withColumn("startDateTime", lit(startDateTime)))
      dfList.append(dfList[-1].withColumn("Parameters", parametersMap))
      dfList.append(dfList[-1].withColumn("Context", lit(self.context.replace("'","''"))))
      dfList[-1].createOrReplaceTempView("startDF") 
      query = """
      INSERT INTO silverprotected.stepLog (StepLogGuid, JobLogGuid, StepKey, StartDateTime, EndDateTime, LogStatusKey, Parameters, Context, Error)
      SELECT stepLogGuid, jobLogGuid, stepKey, startDateTime, NULL, 1, Parameters, Context, NULL FROM startDF
      """
      spark.sql(query) 

  def log_exception(self, args, error): 
    if args.get("stepLogGuid", "") != "":
      guid = args.get("stepLogGuid")
      key = args.get("stepKey", -1)
      self.log_step_error(guid, key, error)
    elif args.get("jobLogGuid", "") != "":
      guid = args.get("jobLogGuid")
      key = args.get("jobKey", -1)
      self.log_step_error(args, key, error)
    elif args.get("stageLogGuid", "") != "":
      guid = args.get("stageLogGuid")
      key = args.get("stageKey", -1)
      self.log_step_error(guid, key, error)
    elif args.get("systemLogGuid", "") != "":
      guid = args.get("systemLogGuid")
      key = args.get("systemKey", -1)
      self.log_step_error(guid, key, error)
    elif args.get("projectLogGuid", "") != "":
      self.log_project_error(args.get("projectLogGuid"), error, args.get("projectKey", -1))
      guid = args.get("projectLogGuid")
      key = args.get("projectKey", -1)
      self.log_step_error(guid, key, error) 
    else:
      print("Unable to log exception")
  def log_step_error (self, guid, stepKey, error):
    if self.LogMethod in (1, 3):
      query = """EXEC dbo.LogStepError
      @StepLogGuid='{0}'
      ,@StepKey='{1}'
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
      query = "EXEC dbo.LogStepEnd @StepLogGuid='{0}' ,@StepKey='{1}', @JobKey='{2}'".format(guid, stepKey, jobKey)
      if windowStart is not None:
        query += ",@WindowStart='{0}'".format(windowStart)
      if windowEnd is not None:
        query += ",@WindowEnd='{0}'".format(windowEnd)
      self.pyodbcStoredProcedure(query)
    if self.LogMethod in (2, 3):
      endDateTime = datetime.datetime.now()
      query = """
      INSERT INTO silverprotected.stepLog (StepLogGuid, JobLogGuid, StepKey, StartDateTime, EndDateTime, LogStatusKey, Parameters, Context, Error)
      VALUES ('{0}', NULL, '{1}', NULL, '{2}', 2, NULL, NULL, NULL)
      """.format(guid, stepKey, endDateTime)
      spark.sql(query)
      
      query = """
      UPDATE silverprotected.Step
      SET
      IsRestart = False
      WHERE StepKey = '{0}'
      """.format(stepKey)
      
      self.run_query_with_retry(query)

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
    query = """
    SELECT
       s.StepKey
      ,s.StepName
      ,s.JobKey
      ,explode(p.Parameters)
    FROM silverprotected.Step s
    JOIN silverprotected.Parameter p ON s.StepKey=p.StepKey
    WHERE s.StepKey = '{0}'
    --windowed extraction not yet implemented for delta config
    --need to express dbo.GetWindow sql function as a python function and pyspark udf
    --UNION
    --SELECT StepKey, StepName, JobKey, ParameterName, ParameterValue
    --FROM dbo.GetWindow({0})
    """.format(stepKey)
    return spark.sql(query).collect()

  def insert_step(self,
    project_name,
    system_name,
    stage_name,
    job_name,
    step_key,
    step_name,
    system_secret_scope,
    system_is_active=True,
    stage_is_active=True,
    job_is_active=True,
    step_is_active=True,
    system_is_restart=True,
    stage_is_restart=True,
    job_is_restart=True,
    step_is_restart=True,
    system_order=10,
    stage_order=10,
    job_order=10,
    step_order=10
    ):
    
    sql = """
    CREATE OR REPLACE TEMPORARY VIEW src
    AS
    SELECT 
      '{0}' as step_key,
      '{1}' as step_name,
      sha2('{2}', 256) as job_key,
      {3} as step_order,
      {4} as step_is_active,
      {5} as step_is_restart
    WHERE '{0}' NOT IN (SELECT StepKey FROM bronze.step_{6})
    """.format(step_key, step_name, job_name, step_order, step_is_active, step_is_restart, project_name)
    spark.sql(sql)
    
    sql = """
    INSERT INTO bronze.step_{0} (StepKey, StepName, JobKey, StepOrder, IsActive, IsRestart, CreatedDate, ModifiedDate)
    SELECT step_key, step_name, job_key, step_order, cast(step_is_active as boolean), step_is_restart, current_timestamp(), NULL from src
    """.format(project_name)
    spark.sql(sql)
    self.insert_job(project_name=project_name, system_name=system_name, system_secret_scope=system_secret_scope, system_order=system_order, system_is_active=system_is_active, system_is_restart=system_is_restart, stage_name=stage_name, stage_is_active=stage_is_active, stage_is_restart=stage_is_restart, stage_order=stage_order, job_name=job_name, job_order=job_order, job_is_active=job_is_active, job_is_restart=job_is_restart)

  def load_orchestration_step(self, project_name):
    sql = """
      MERGE INTO silverprotected.step tgt
      USING bronze.step_{0} src ON tgt.StepKey=src.StepKey
      WHEN MATCHED THEN UPDATE SET 
        tgt.StepOrder=src.StepOrder, 
        tgt.IsActive=src.IsActive, 
        tgt.IsRestart=src.IsRestart, 
        tgt.ModifiedDate=current_timestamp()
      WHEN NOT MATCHED THEN 
        INSERT (StepKey, StepName, JobKey, StepOrder, IsActive, IsRestart, CreatedDate) 
        VALUES (src.StepKey, src.StepName, src.JobKey, src.StepOrder, src.IsActive, src.IsRestart, current_timestamp())
    """.format(project_name)
    spark.sql(sql)

#Job
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
      steps = self.get_job(job["JobName"], job["JobKey"])
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
        ,@JobKey='{2}'
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
      dfList.append(dfList[-1].withColumn("jobKey", lit(jobKey))) 
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
        ,@JobKey='{1}'
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
        ,@JobKey='{1}';
      """.format(guid, jobKey)
      self.pyodbcStoredProcedure(query)
    
    if self.LogMethod in (2, 3):
      endDateTime = datetime.datetime.now()
      query = """
      INSERT INTO silverprotected.jobLog (JobLogGuid, StageLogGuid, JobKey, StartDateTime, EndDateTime, LogStatusKey, Parameters, Context, Error)
      VALUES ('{0}', NULL, '{1}', NULL, '{2}', 2, NULL, NULL, NULL)
      """.format(guid, jobKey, endDateTime)
      spark.sql(query)        

      ### update job
      query = """
      UPDATE silverprotected.Job
      SET
      IsRestart = False
      WHERE JobKey = '{0}'
      """.format(jobKey)
      spark.sql(query)

  def reset_job(self, jobKey):
    stepKeysSQL = """
    SELECT StepKey
    FROM silverprotected.step
    WHERE JobKey == '{0}'
    """.format(jobKey)
    stepKeys = spark.sql(stepKeysSQL)
    stepKeys.createOrReplaceTempView('stepKeys')
    stepKeys = [s.StepKey for s in spark.sql(stepKeysSQL).collect()]
    for stepKey in stepKeys:
        query = """
        UPDATE silverprotected.step
        SET IsRestart = True 
        WHERE StepKey == '{0}'
        """.format(stepKey)
        self.run_query_with_retry(query)         
            
  def run_query_with_retry(self, query, attempts=3):
    import time
    try:
      spark.sql(query)
    except Exception as e:
      if attempts > 1 and ('ConcurrentAppendException' in str(e) or 'ConcurrentDeleteException' in str(e)):
        time.sleep(5)
        return self.run_query_with_retry(query, attempts-1)
      else:
        raise
      
  def get_job(self, jobName, jobKey):
    query = """
    SELECT 
       j.JobKey
      ,j.JobName
      ,j.StageKey
      ,s.StepKey
      ,s.StepName
      ,s.StepOrder
    FROM silverprotected.Job j
    JOIN silverprotected.Step s ON j.JobKey=s.JobKey
    WHERE j.JobName = '{0}'
    AND j.JobKey == '{1}'
    AND s.IsActive == True
    AND s.IsRestart == True
    ORDER BY s.StepOrder
    """.format(jobName, jobKey)
    return spark.sql(query).collect()

  def get_all_jobs(self):
    query = """
    SELECT 
      j.JobKey
      ,j.JobName
      ,j.StageKey
      ,s.StepKey
      ,s.StepName
      ,s.StepOrder
    FROM silverprotected.Job j
    JOIN silverprotected.Step s ON j.JobKey=s.JobKey
    ORDER BY j.JobName
    """
    return spark.sql(query)

  def insert_job(self,
    project_name, 
    system_name,
    stage_name,
    job_name,
    system_secret_scope,
    system_is_active=True,
    stage_is_active=True,
    job_is_active=True,
    system_is_restart=True,
    stage_is_restart=True,
    job_is_restart=True,
    system_order=10,
    stage_order=10,
    job_order=10
    ):
    
    sql = """
    CREATE OR REPLACE TEMPORARY VIEW src
    AS
    SELECT 
      '{0}' as job_name,
      sha2('{1}', 256) as stage_key,
      {2} as job_order,
      {3} as job_is_active,
      {4} as job_is_restart
    WHERE '{0}' NOT IN (SELECT JobName FROM bronze.job_{5})
    """.format(job_name, stage_name, job_order, job_is_active, job_is_restart, project_name)
    spark.sql(sql)
    
    sql = """
    INSERT INTO bronze.job_{0} (JobKey, JobName, StageKey, JobOrder, IsActive, IsRestart, CreatedDate, ModifiedDate) 
    SELECT (sha2(job_name, 256)), job_name, stage_key, job_order, job_is_active, job_is_restart, current_timestamp(), NULL FROM src
    """.format(project_name)
    
    spark.sql(sql)
    self.insert_stage(project_name=project_name, system_name=system_name, system_secret_scope=system_secret_scope, system_order=system_order, system_is_active=system_is_active, system_is_restart=system_is_restart, stage_name=stage_name, stage_is_active=stage_is_active, stage_is_restart=stage_is_restart, stage_order=stage_order)

  def load_orchestration_job(self, project_name):
      sql = """
        MERGE INTO silverprotected.job tgt
        USING bronze.job_{0} src ON tgt.JobName=src.JobName
        WHEN MATCHED THEN UPDATE SET 
          tgt.JobOrder=src.JobOrder, 
          tgt.IsActive=src.IsActive, 
          tgt.IsRestart=src.IsRestart, 
          tgt.ModifiedDate=current_timestamp()
        WHEN NOT MATCHED THEN 
          INSERT (JobName, StageKey, JobOrder, IsActive, IsRestart, CreatedDate) 
          VALUES (src.JobName, src.StageKey, src.JobOrder, src.IsActive, src.IsRestart, current_timestamp())
      """.format(project_name)
      spark.sql(sql)  

#Stage
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
      jobs = self.get_stage(stage["StageName"])
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
      ,@StageKey='{2}'
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
      dfList.append(dfList[-1].withColumn("stageKey", lit(stageKey)))      
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
      ,@StageKey='{1}'
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
      ,@StageKey='{1}';
      """.format(guid, stageKey)
      self.pyodbcStoredProcedure(query)
    
    if self.LogMethod in (2, 3):
      endDateTime = datetime.datetime.now()
      query = """
      INSERT INTO silverprotected.stageLog (StageLogGuid, SystemLogGuid, StageKey, StartDateTime, EndDateTime, LogStatusKey, Parameters, Context, Error)
      VALUES ('{0}', NULL, '{1}', NULL, '{2}', 2, NULL, NULL, NULL)
      """.format(guid, stageKey, endDateTime)
      spark.sql(query)

      query = """
      UPDATE silverprotected.Stage
      SET
      IsRestart = False
      WHERE StageKey = '{0}'
      """.format(stageKey)
      spark.sql(query)

  def reset_stage(self, stageKey):
    query = """
    UPDATE silverprotected.job
    SET IsRestart = True
    WHERE StageKey == '{0}'
    """.format(stageKey)
    spark.sql(query)

  def get_stage(self, stageName):
    query = """
    SELECT 
       s.StageKey
      ,s.StageName
      ,s.SystemKey
      ,j.JobKey
      ,j.JobName
      ,j.JobOrder
    FROM silverprotected.Stage s
    JOIN silverprotected.Job j ON s.StageKey=j.StageKey
    WHERE s.StageName == '{0}'
    AND j.IsActive == True
    AND j.IsRestart == True
    ORDER BY j.JobOrder
    """.format(stageName)
    return spark.sql(query).collect()

  def get_all_stages(self):
    query = """
    SELECT 
       s.StageKey
      ,s.StageName
      ,s.SystemKey
      ,j.JobKey
      ,j.JobName
      ,j.JobOrder
    FROM silverprotected.Stage s
    JOIN silverprotected.Job j ON s.StageKey=j.StageKey
    ORDER BY s.StageName
    """
    return spark.sql(query)

  def update_stage_threads(self,
    project_name,
    stage_name,
    threads
    ):
    
    sql = """
    CREATE OR REPLACE TEMPORARY VIEW src
    AS 
    SELECT '{1}' as stage_name,
    {2} as threads
    WHERE '{1}' NOT IN (SELECT StageName FROM bronze.stage_threads_{0})
    """.format(project_name, stage_name, threads)
    spark.sql(sql)
    
    sql = """
    INSERT INTO bronze.stage_threads_{0} (StageName, NumberOfThreads)
    SELECT stage_name, threads FROM src
    """.format(project_name)
    spark.sql(sql)

  def insert_stage(self,
    project_name,
    system_name,
    stage_name,
    system_secret_scope,
    system_is_active=True,
    stage_is_active=True,
    stage_is_restart=True,
    system_is_restart=True,
    system_order=10,
    stage_order=10
    ):
    
    sql = """
    CREATE OR REPLACE TEMPORARY VIEW src
    AS
    SELECT 
      '{0}' as stage_name,
      sha2('{1}', 256) as system_key,
      {2} as stage_order,
      {3} as stage_is_active,
      {4} as stage_is_restart
    WHERE '{0}' NOT IN (SELECT StageName FROM bronze.stage_{5})
    """.format(stage_name, system_name, stage_order, stage_is_active, stage_is_restart, project_name)
    spark.sql(sql)

    sql = """
    INSERT INTO bronze.stage_{0} (StageKey, StageName, SystemKey, StageOrder, IsActive, IsRestart, NumberOfThreads, CreatedDate, ModifiedDate)
    SELECT (sha2(stage_name, 256)), stage_name, system_key, stage_order, stage_is_active, stage_is_restart, 1, current_timestamp(), NULL FROM src
    """.format(project_name)
    spark.sql(sql)
    self.insert_system(project_name=project_name, system_name=system_name, system_secret_scope=system_secret_scope, system_order=system_order, system_is_active=system_is_active, system_is_restart=system_is_restart)

  def load_orchestration_stage(self, project_name):
    sql = """
    
    WITH src 
    AS
    (
      SELECT src.StageKey, src.StageName, src.SystemKey, src.StageOrder, src.IsActive, src.IsRestart, COALESCE(th.NumberOfThreads, 1) AS NumberOfThreads, current_timestamp() AS CurrentTimestamp
      FROM bronze.stage_{0} src 
      LEFT JOIN bronze.stage_threads_{0} th ON src.StageName = th.StageName 
    ) 
    MERGE INTO silverprotected.stage tgt
          USING src ON tgt.StageKey=src.StageKey
          WHEN MATCHED THEN UPDATE SET 
            tgt.StageOrder=src.StageOrder, 
            tgt.IsActive=src.IsActive, 
            tgt.IsRestart=src.IsRestart, 
            tgt.NumberOfThreads=COALESCE(src.NumberOfThreads, 1), 
            tgt.ModifiedDate=src.CurrentTimestamp
          WHEN NOT MATCHED THEN 
            INSERT (StageName, SystemKey, StageOrder, IsActive, IsRestart, NumberOfThreads, CreatedDate) 
            VALUES (src.StageName, src.SystemKey, src.StageOrder, src.IsActive, src.IsRestart, COALESCE(src.NumberOfThreads, 1), src.CurrentTimestamp)
    """.format(project_name)
    spark.sql(sql)    

#System
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
      stages = self.get_system(system['SystemName'])
      for stage in stages:
        print("{0}: running stage: {1}".format(self.getCurrentTimestamp(), stage['StageName']))
        self.run_stage(systemLogGuid, stage, stage['numberOfThreads'])
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
      ,@SystemKey='{2}'
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
      dfList.append(dfList[-1].withColumn("systemKey", lit(systemKey)))      
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
      ,@SystemKey='{1}'
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
      ,@SystemKey='{1}';
      """.format(guid, systemKey)
      self.pyodbcStoredProcedure(query)
    if self.LogMethod in (2, 3):
      endDateTime = datetime.datetime.now()
      query = """
      INSERT INTO silverprotected.systemLog (SystemLogGuid, ProjectLogGuid, SystemKey, StartDateTime, EndDateTime, LogStatusKey, Parameters, Context, Error)
      VALUES ('{0}', NULL, '{1}', NULL, '{2}', 2, NULL, NULL, NULL)
      """.format(guid, systemKey, endDateTime)
      spark.sql(query)

      query = """
      UPDATE silverprotected.system
      SET 
      IsRestart = False
      WHERE SystemKey = '{0}'
      """.format(systemKey)
      spark.sql(query)    
          
  def reset_system(self, systemKey):
    query = """
    UPDATE silverprotected.stage
    SET IsRestart = True
    WHERE SystemKey == '{0}'
    """.format(systemKey)
    spark.sql(query)

  def get_system(self, systemName):
    query = """
    SELECT 
       es.SystemKey
      ,es.SystemName
      ,es.ProjectKey
      ,s.StageKey
      ,s.StageName
      ,s.StageOrder
      ,s.numberOfThreads
    FROM silverprotected.System es
    JOIN silverprotected.Stage s ON es.SystemKey=s.SystemKey
    WHERE es.SystemName == '{0}'
    AND s.IsActive == True
    AND s.IsRestart == True
    ORDER BY s.StageOrder
    """.format(systemName)
    return spark.sql(query).collect()

  def get_all_systems(self):
    query = """
    SELECT 
       es.SystemKey
      ,es.SystemName
      ,es.ProjectKey
      ,s.StageKey
      ,s.StageName
      ,s.StageOrder
      ,s.numberOfThreads
    FROM silverprotected.System es
    JOIN silverprotected.Stage s ON es.SystemKey=s.SystemKey
    ORDER BY es.SystemName
    """
    return spark.sql(query)

  def insert_system(self,
    project_name,
    system_name,
    system_secret_scope,
    system_order,
    system_is_active,
    system_is_restart
    ):
    
    sql = """
    CREATE OR REPLACE TEMPORARY VIEW src
    AS
    SELECT 
      '{0}' as system_name,
      sha2('{1}', 256) as project_key,
      {2} as system_order,
      {3} as system_is_active,
      {4} as system_is_restart,
      '{5}' as system_secret_scope
    WHERE '{0}' NOT IN (SELECT SystemName FROM bronze.system_{1})
    """.format(system_name, project_name, system_order, system_is_active, system_is_restart, system_secret_scope)
    spark.sql(sql)

    sql = """
    INSERT INTO bronze.system_{0} (SystemKey, SystemName, SystemSecretScope, ProjectKey, SystemOrder, IsActive, IsRestart, CreatedDate, ModifiedDate)
    SELECT (sha2(system_name, 256)), system_name, system_secret_scope, project_key, system_order, system_is_active, system_is_restart, current_timestamp(), NULL FROM src      
    """.format(project_name)
    
    spark.sql(sql)
    self.insert_project(project_name=project_name)

  def load_orchestration_system(self, project_name):
    sql = """
      MERGE INTO silverprotected.system tgt
      USING bronze.system_{0} src ON tgt.SystemKey=src.SystemKey
      WHEN MATCHED THEN UPDATE SET 
        tgt.SystemOrder=src.SystemOrder, 
        tgt.IsActive=src.IsActive, 
        tgt.IsRestart=src.IsRestart, 
        tgt.ModifiedDate=current_timestamp()
      WHEN NOT MATCHED THEN 
        INSERT (SystemName, SystemSecretScope, ProjectKey, SystemOrder, IsActive, IsRestart, CreatedDate) 
        VALUES (src.SystemName, src.SystemSecretScope, src.ProjectKey, src.SystemOrder, src.IsActive, src.IsRestart, CreatedDate)
    """.format(project_name)
    spark.sql(sql)

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
      systems = self.get_project()
      if len(systems) > 0:
        for system in systems:
          print("{0}: running system: {1}".format(self.getCurrentTimestamp(), system['SystemName']))
          self.run_system(projectLogGuid, system)
      else:
        error_description = "{0}: Configuration for project {1} does not exist.".format(self.getCurrentTimestamp(), self.projectName)
        err = {
        "sourceName": "Orchestration: run_project",
        "errorCode": 100,
        "errorDescription": error_description
        }
        error = json.dumps(err)
        self.log_project_error(projectLogGuid, error, None)
        raise ValueError(error_description)
    except Exception as e:
      err = {
        "sourceName": "Orchestration: run_project",
        "errorCode": 101,
        "errorDescription": e.__class__.__name__
      }
      error = json.dumps(err)
      self.log_project_error(projectLogGuid, error, system['ProjectKey'])
      raise e
    self.log_project_end(projectLogGuid, system['ProjectKey'])
    self.reset_project(system['ProjectKey'])

  def get_project (self):
    query = """
      SELECT s.*
      FROM silverprotected.system s
      JOIN silverprotected.project p ON s.ProjectKey=p.ProjectKey
      WHERE s.IsActive == True 
      AND s.IsRestart == True 
      AND p.ProjectName = '{0}' 
      ORDER BY s.SystemOrder
    """.format(self.projectName)
    return spark.sql(query).collect()

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
        ,@ProjectKey='{1}'
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
      ,@ProjectKey='{1}';
      """.format(guid, projectKey)
      self.pyodbcStoredProcedure(query)
    if self.LogMethod in (2, 3): 
      endDateTime = datetime.datetime.now()
      query = """
      INSERT INTO silverprotected.projectLog (ProjectLogGuid, ProjectKey, StartDateTime, EndDateTime, LogStatusKey, Parameters, Context, Error)
      VALUES ('{0}', '{1}', NULL, '{2}', 2, NULL, NULL, NULL)
      """.format(guid, projectKey, endDateTime)
      spark.sql(query)

  def reset_project (self, projectKey):
    query = """
    UPDATE silverprotected.system
    SET IsRestart = True
    WHERE ProjectKey == '{0}'
    """.format(projectKey)
    spark.sql(query)

  def get_all_projects(self):
    query = """
    SELECT 
     p.ProjectKey
    ,p.ProjectName
    ,es.SystemKey
    ,es.SystemName
    ,es.SystemSecretScope
    ,es.SystemOrder
    FROM silverprotected.Project p
    JOIN silverprotected.System es ON p.ProjectKey=es.ProjectKey
    ORDER BY p.ProjectName
    """
    return spark.sql(query)

  def insert_project(self, project_name):
    sql = """
    CREATE OR REPLACE TEMPORARY VIEW src
    AS
    SELECT 
      '{0}' as project_name
    WHERE '{0}' NOT IN (SELECT ProjectName FROM bronze.project_{0})
    """.format(project_name)
    spark.sql(sql)
    
    sql = """
    INSERT INTO bronze.project_{0} (ProjectKey, ProjectName, CreatedDate)
    SELECT (sha2(project_name, 256)), project_name, current_timestamp() FROM src
    """.format(project_name)
    spark.sql(sql)

  def load_orchestration_project(self, project_name):
    sql = """
      MERGE INTO silverprotected.project tgt
      USING bronze.project_{0} src ON tgt.ProjectKey=src.ProjectKey
      WHEN NOT MATCHED THEN 
        INSERT (ProjectName, CreatedDate) 
        VALUES (src.ProjectName, current_timestamp())
    """.format(project_name)
    spark.sql(sql)  

  def delete_project_from_configuration(self, project_name):
    #delete parameter
    sql = """
      DELETE FROM silverprotected.parameter
      WHERE StepKey IN (
        SELECT StepKey FROM silverprotected.projectconfiguration WHERE ProjectName='{0}'
      )
    """.format(project_name)
    spark.sql(sql)

    #delete step
    sql = """
      DELETE FROM silverprotected.step
      WHERE JobKey IN (
        SELECT JobKey FROM silverprotected.projectconfiguration WHERE ProjectName='{0}'
      )
    """.format(project_name)
    spark.sql(sql)

    #delete job
    sql = """
      DELETE FROM silverprotected.job
      WHERE StageKey IN (
        SELECT StageKey FROM silverprotected.projectconfiguration WHERE ProjectName='{0}'
      )
    """.format(project_name)
    spark.sql(sql)

    #delete stage
    sql = """
      DELETE FROM silverprotected.stage
      WHERE SystemKey IN (
        SELECT SystemKey FROM silverprotected.projectconfiguration WHERE ProjectName='{0}'
      )
    """.format(project_name)
    spark.sql(sql)

    #delete system
    sql = """
      DELETE FROM silverprotected.system
      WHERE ProjectKey IN (
        SELECT ProjectKey FROM silverprotected.projectconfiguration WHERE ProjectName='{0}'
      )
    """.format(project_name)
    spark.sql(sql)
    
    #delete project
    sql = """
      DELETE FROM silverprotected.project
      WHERE ProjectName='{0}'
    """.format(project_name)
    spark.sql(sql)

  def get_project_configuration(self, project_key=None, project_name=None, system_key=None, system_name=None, system_is_active=None, system_is_restart=None, stage_key=None, stage_name=None, stage_is_active=None, stage_is_restart=None, job_key=None, job_name=None, job_is_active=None, job_is_restart=None, step_key=None, step_name=None, step_is_active=None, step_is_restart=None, parameter_name=None):

    select = """ 
    SELECT 
    p.ProjectKey
    ,p.ProjectName
    ,p.CreatedDate
    ,s.SystemKey
    ,s.SystemName
    ,s.SystemSecretScope 
    ,s.SystemOrder
    ,s.IsActive AS SystemIsActive
    ,s.IsRestart AS SystemIsRestart
    ,s.CreatedDate AS SystemCreatedDate
    ,s.ModifiedDate AS SystemModifiedDate
    ,st.StageKey
    ,st.StageName
    ,st.IsActive AS StageIsActive
    ,st.IsRestart AS StageIsRestart
    ,st.StageOrder
    ,st.NumberOfThreads
    ,st.CreatedDate AS StageCreatedDate
    ,st.ModifiedDate AS StageModifiedDate
    ,j.JobKey
    ,j.JobName
    ,j.JobOrder
    ,j.IsActive AS JobIsActive
    ,j.IsRestart AS JobIsRestart
    ,j.CreatedDate AS JobCreatedDate
    ,j.ModifiedDate AS JobModifiedDate
    ,stp.StepKey
    ,stp.StepName
    ,stp.StepOrder
    ,stp.IsActive AS StepIsActive
    ,stp.IsRestart AS StepIsRestart
    ,stp.CreatedDate AS StepCreatedDate
    ,stp.ModifiedDate AS StepModifiedDate
    ,pm.Parameters
    ,pm.CreatedDate 
    FROM silverprotected.Project p 
    JOIN silverprotected.System s ON p.ProjectKey = s.ProjectKey 
    JOIN silverprotected.Stage st ON s.SystemKey = st.SystemKey 
    JOIN silverprotected.Job j ON st.StageKey = j.StageKey 
    JOIN silverprotected.Step stp ON j.JobKey = stp.JobKey 
    JOIN silverprotected.Parameter pm ON stp.StepKey = pm.StepKey 
    """    

    where = "WHERE 1=1 "
    if project_key is not None:
        where += "AND p.ProjectKey='{0}' ".format(project_key)
    if project_name is not None:
        where += "AND p.ProjectName='{0}' ".format(project_name)
    if system_key is not None:
        where += "AND s.SystemKey='{0}' ".format(system_key)
    if system_name is not None:
        where += "AND s.SystemName='{0}' ".format(system_name)
    if system_is_active is not None:
        where += "AND s.IsActive='{0}' ".format(system_is_active)
    if system_is_restart is not None:
        where += "AND s.IsRestart='{0}' ".format(system_is_restart)
    if stage_key is not None:
        where += "AND st.StageKey='{0}' ".format(stage_key)
    if stage_name is not None:
        where += "AND st.StageName='{0}' ".format(stage_name)
    if stage_is_active is not None:
        where += "AND st.IsActive='{0}' ".format(stage_is_active)
    if stage_is_restart is not None:
        where += "AND st.isRestart='{0}' ".format(stage_is_restart)
    if job_key is not None:
        where += "AND j.JobKey='{0}' ".format(job_key)
    if job_name is not None:
        where += "AND j.jobName='{0}' ".format(job_name)
    if job_is_active is not None:
        where += "AND j.IsActive='{0}' ".format(job_is_active)
    if job_is_restart is not None:
        where += "AND j.IsRestart='{0}' ".format(job_is_restart)
    if step_key is not None:
        where += "AND stp.StepKey='{0}' ".format(step_key)
    if step_name is not None:
        where += "AND stp.StepName='{0}' ".format(step_name)
    if step_is_active is not None:
        where += "AND stp.IsActive='{0}' ".format(step_is_active)
    if step_is_restart is not None:
        where += "AND stp.IsRestart='{0}' ".format(step_is_restart)
    if parameter_name is not None:
        where += "AND p.ParameterName='{0}' ".format(parameter_name)

    order_by = "ORDER BY p.ProjectKey, s.SystemOrder, st.StageOrder, j.JobOrder, stp.StepOrder "

    sql = select + where + order_by + ";"
    #print(sql)

    return spark.sql(sql)

#loading silver protected
  def load_orchestration_tables(self, project_name):
    self.load_orchestration_parameter(project_name)
    self.load_orchestration_step(project_name)
    self.load_orchestration_job(project_name)
    self.load_orchestration_stage(project_name)
    self.load_orchestration_system(project_name)
    self.load_orchestration_project(project_name)

#hydration
  def generate_step_name(self, notebook_path, append_range=18):
    import random 
    DIGITS = "0123456789"
    return notebook_path + ''.join(random.choice(DIGITS) for i in range(append_range))

  def hydrate_batch_sql(self,
      project_name, 
      system_name,
      stage_name,
      job_name,
      system_secret_scope,
      schema_name,
      table_name,
      primary_key_columns, 
      pushdown_query = '',
      system_is_active = True,
      stage_is_active = True,
      job_is_active = True,
      system_order = 10,
      stage_order = 10,
      job_order = 10,
      lower_bound = '',
      upper_bound = '',
      num_partitions = '8',
      use_windowed_extraction = False,
      windowing_column = '',
      windowed_extraction_begin_date = None,
      windowed_extraction_end_date = None,
      windowed_extraction_interval = None,
      windowed_extraction_process_latest_window_first = False,
      explode_and_flatten = 'True',
      cleanse_column_names = 'True',
      timestamp_columns = '',
      timestamp_format = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'",
      encrypt_columns = '',
      load_type = 'Merge',
      destination = 'silvergeneral',
      silver_zone_partition_column = '',
      silver_zone_cluster_column = 'pk',
      silver_zone_cluster_buckets = '8',
      optimize_where = '',
      optimize_z_order_by = '',
      vacuum_retention_hours = '168',
      partition_column = '',
      bronze_zone_notebook_path = '../Data Engineering/Bronze Zone/Batch SQL',
      silver_zone_notebook_path = '../Data Engineering/Silver Zone/Delta Load'
    ):
    
    validate_columns = [project_name, system_name, stage_name, job_name, system_secret_scope, schema_name, table_name, primary_key_columns] 
    [self.validate_input(c) for c in validate_columns]
    
    bronze_parameters = {
      "schemaName": schema_name,
      "tableName": table_name,
      "partitionColumn": partition_column,
      "lowerBound": lower_bound,
      "upperBound": upper_bound,
      "numPartitions": num_partitions,
      "externalSystem": system_secret_scope,
      "useWindowedExtraction": str(use_windowed_extraction),
      "windowingColumn": windowing_column,
      "pushdownQuery": pushdown_query
    }
    
    silver_parameters = {
      "schemaName": schema_name,
      "tableName": table_name,
      "numPartitions": num_partitions,
      "primaryKeyColumns": primary_key_columns,
      "externalSystem": system_secret_scope,
      "partitionCol": silver_zone_partition_column,
      "clusterCol": silver_zone_cluster_column,
      "clusterBuckets": silver_zone_cluster_buckets,
      "optimizeWhere": optimize_where,
      "optimizeZOrderBy": optimize_z_order_by,
      "vacuumRetentionHours": vacuum_retention_hours,
      "loadType": load_type,
      "destination": destination,
      "explodeAndFlatten": explode_and_flatten,
      "cleanseColumnNames": cleanse_column_names,
      "timestampColumns": timestamp_columns,
      "timestampFormat": timestamp_format,
      "encryptColumns": encrypt_columns
    }
    
    self.insert_parameters(project_name=project_name, system_name=system_name, system_secret_scope=system_secret_scope, system_order=system_order, system_is_active=system_is_active, system_is_restart=True, stage_name=stage_name, stage_is_active=stage_is_active, stage_is_restart=True, stage_order=stage_order, job_name=job_name, job_order=job_order, job_is_active=job_is_active, job_is_restart=True, step_name=bronze_zone_notebook_path, step_order=10, step_is_active=True, step_is_restart=True, parameters=bronze_parameters)
    
    if use_windowed_extraction == True:
      self.insert_windowed_extraction(project_name, job_name, bronze_zone_notebook_path, windowed_extraction_begin_date, windowed_extraction_end_date, windowed_extraction_interval, windowed_extraction_process_latest_window_first)
    
    self.insert_parameters(project_name=project_name, system_name=system_name, system_secret_scope=system_secret_scope, system_order=system_order, system_is_active=system_is_active, system_is_restart=True, stage_name=stage_name, stage_is_active=stage_is_active, stage_is_restart=True, stage_order=stage_order, job_name=job_name, job_order=job_order, job_is_active=job_is_active, job_is_restart=True, step_name=silver_zone_notebook_path, step_order=20, step_is_active=True, step_is_restart=True, parameters=silver_parameters)
  
  def hydrate_batch_snowflake(self,
      sfSchema ,
      sfTable ,
			externalSystem ,
			Query ,
			sfUrl ,
			sfDatabase ,
			delta_schema_name ,
			sfWarehouse ,
			Scope ,
      project_name, 
      system_name,
      stage_name,
      job_name,
      delta_table_name,
      primary_key_columns,
      system_is_active = True,
      stage_is_active = True,
      job_is_active = True,
      system_order = 10,
      stage_order = 10,
      job_order = 10,
      num_partitions = '8',
      use_windowed_extraction = False,
      windowed_extraction_begin_date = None,
      windowed_extraction_end_date = None,
      windowed_extraction_interval = None,
      windowed_extraction_process_latest_window_first = False,
      explode_and_flatten = 'True',
      cleanse_column_names = 'True',
      timestamp_columns = '',
      timestamp_format = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'",
      encrypt_columns = '',
      load_type = 'Merge',
      destination = 'silvergeneral',
      silver_zone_partition_column = '',
      silver_zone_cluster_column = 'pk',
      silver_zone_cluster_buckets = '8',
      optimize_where = '',
      optimize_z_order_by = '',
      vacuum_retention_hours = '168',
      bronze_zone_notebook_path = '../Data Engineering/Bronze Zone/Batch Snowflake with ktk',
      silver_zone_notebook_path = '../Data Engineering/Silver Zone/Delta Load'
    ):
    
    validate_columns = [project_name, system_name, stage_name, job_name, externalSystem, delta_schema_name, delta_table_name, primary_key_columns] 
    [self.validate_input(c) for c in validate_columns]
    
    bronze_parameters = {
      "sfSchema": sfSchema,
      "sfTable": sfTable,
      "schemaName": delta_schema_name,
      "externalSystem": externalSystem,
      "tableName": delta_table_name,
      "Query": Query,
      "sfUrl": sfUrl,
      "sfDatabase": sfDatabase,
      "sfWarehouse": sfWarehouse,
      "Scope": Scope
    }
    
    silver_parameters = {
      "schemaName": delta_schema_name,
      "tableName": delta_table_name,
      "numPartitions": num_partitions,
      "primaryKeyColumns": primary_key_columns,
      "externalSystem": externalSystem,
      "partitionCol": silver_zone_partition_column,
      "clusterCol": silver_zone_cluster_column,
      "clusterBuckets": silver_zone_cluster_buckets,
      "optimizeWhere": optimize_where,
      "optimizeZOrderBy": optimize_z_order_by,
      "vacuumRetentionHours": vacuum_retention_hours,
      "loadType": load_type,
      "destination": destination,
      "explodeAndFlatten": explode_and_flatten,
      "cleanseColumnNames": cleanse_column_names,
      "timestampColumns": timestamp_columns,
      "timestampFormat": timestamp_format,
      "encryptColumns": encrypt_columns
    }
    
    self.insert_parameters(project_name=project_name, system_name=system_name, system_secret_scope="", system_order=system_order, system_is_active=system_is_active, system_is_restart=True, stage_name=stage_name, stage_is_active=stage_is_active, stage_is_restart=True, stage_order=stage_order, job_name=job_name, job_order=job_order, job_is_active=job_is_active, job_is_restart=True, step_name=bronze_zone_notebook_path, step_order=10, step_is_active=True, step_is_restart=True, parameters=bronze_parameters)
    
    if use_windowed_extraction == True:
      self.insert_windowed_extraction(project_name, job_name, bronze_zone_notebook_path, windowed_extraction_begin_date, windowed_extraction_end_date, windowed_extraction_interval, windowed_extraction_process_latest_window_first)
    
    self.insert_parameters(project_name=project_name, system_name=system_name, system_secret_scope="", system_order=system_order, system_is_active=system_is_active, system_is_restart=True, stage_name=stage_name, stage_is_active=stage_is_active, stage_is_restart=True, stage_order=stage_order, job_name=job_name, job_order=job_order, job_is_active=job_is_active, job_is_restart=True, step_name=silver_zone_notebook_path, step_order=20, step_is_active=True, step_is_restart=True, parameters=silver_parameters)
  
  
  def hydrate_generic_notebook(self,
      project_name, 
      system_name,
      stage_name,
      job_name,
      system_secret_scope,
      parameters,
      notebook_path,
      system_is_active = True,
      stage_is_active = True,
      job_is_active = True,
      system_order = 10,
      stage_order = 10,
      job_order = 10
    ):
    
    validate_columns = [project_name, system_name, stage_name, job_name, system_secret_scope] 
    [self.validate_input(c) for c in validate_columns]
    
    self.insert_parameters(project_name=project_name, system_name=system_name, system_secret_scope=system_secret_scope, system_order=system_order, system_is_active=system_is_active, system_is_restart=True, stage_name=stage_name, stage_is_active=stage_is_active, stage_is_restart=True, stage_order=stage_order, job_name=job_name, job_order=job_order, job_is_active=job_is_active, job_is_restart=True, step_name=notebook_path, step_order=10, step_is_active=True, step_is_restart=True, parameters=parameters)
      
  def hydrate_gold_zone(self,
      project_name,
      system_name,
      stage_name,
      job_name,
      view_name,
      system_secret_scope,
      table_name,
      system_is_active = True,
      stage_is_active = True,
      job_is_active = True,
      primary_key_columns = '',
      system_order = 10,
      stage_order = 10,
      job_order = 10,
      purpose = 'general',
      partition_column = '',
      cluster_column = 'pk',
      cluster_buckets = '8',
      optimize_where = '',
      optimize_z_order_by = '',
      vacuum_retention_hours = '168',
      load_type = 'Merge',
      merge_schema = 'False',
      delete_not_in_source = 'false',
      destination = 'goldgeneral',
      concurrent_processing_partition_literal = '',
      gold_zone_notebook_path = '../Data Engineering/Gold Zone/Gold Load'
    ):

    validate_columns = [project_name, system_name, stage_name, job_name, system_secret_scope, view_name, table_name]
    [self.validate_input(c) for c in validate_columns]

    gold_zone_parameters = {
      "tableName": table_name,
      "viewName": view_name,
      "purpose": purpose,
      "primaryKeyColumns": primary_key_columns,
      "partitionCol": partition_column,
      "clusterCol": cluster_column,
      "clusterBuckets": cluster_buckets,
      "optimizeWhere": optimize_where,
      "optimizeZOrderBy":optimize_z_order_by,
      "vacuumRetentionHours": vacuum_retention_hours,
      "loadType": load_type,
      "merge_schema": merge_schema,
      "deleteNotInSource": delete_not_in_source,
      "destination": destination,
      "concurrentProcessingPartitionLiteral": concurrent_processing_partition_literal
    }

    self.insert_parameters(project_name=project_name, system_name=system_name, system_secret_scope=system_secret_scope, system_order=system_order, system_is_active=system_is_active, stage_name=stage_name, stage_is_active=stage_is_active, stage_order=stage_order, job_name=job_name, job_order=job_order, job_is_active=job_is_active, step_name=gold_zone_notebook_path, step_order=10, step_is_active=True, parameters=gold_zone_parameters)

  def hydrate_platinum_zone(self,
      project_name,
      system_name,
      stage_name,
      job_name,
      system_secret_scope,
      table_name,
      system_is_active = True,
      stage_is_active = True,
      job_is_active = True,
      system_order = 10,
      stage_order = 10,
      job_order = 10,
      destination_schema_name = '',
      destination_table_name = '',
      stored_procedure_name = '',
      num_partitions = '8',
      ignore_date_to_process_filter = 'true',
      load_type = 'overwrite',
      truncate_table_instead_of_drop_and_replace = 'true',
      isolation_level = 'READ_COMMITTED',
      presentation_zone_notebook_path = '../Data Engineering/Presentation Zone/SQL Spark Connector 3.0'

    ):
    
    validate_columns = [project_name, system_name, system_secret_scope, stage_name, job_name, table_name, presentation_zone_notebook_path]
    [self.validate_input(c) for c in validate_columns]

    platinum_zone_parameters = {
      "externalSystem":system_secret_scope,
      "tableName":table_name,
      "destinationSchemaName":destination_schema_name,
      "destinationTableName":destination_table_name,
      "storedProcedureName":stored_procedure_name,
      "numPartitions":num_partitions,
      "ignoreDateToProcessFilter":ignore_date_to_process_filter,
      "loadType":load_type,
      "truncateTableInsteadOfDropAndReplace":truncate_table_instead_of_drop_and_replace,
      "isolationLevel":isolation_level
    }

    self.insert_parameters(project_name=project_name, system_name=system_name, system_secret_scope=system_secret_scope, system_order=system_order, system_is_active=system_is_active, stage_name=stage_name, stage_is_active=stage_is_active, stage_order=stage_order, job_name=job_name, job_order=job_order, job_is_active=job_is_active, step_name=presentation_zone_notebook_path, step_order=10, step_is_active=True, parameters=platinum_zone_parameters)

  def hydrate_presentation_zone(self,
      project_name,
      system_name,
      stage_name,
      job_name,
      system_secret_scope,
      table_name,
      system_is_active = True,
      stage_is_active = True,
      job_is_active = True,
      system_order = 10,
      stage_order = 10,
      job_order = 10,
      database_name = '',
      collection_name = '',
      id_column = '',
      destination_schema_name = 'dbo',
      stored_procedure_name = '',
      save_mode = 'overwrite',
      bulk_copy_batch_size = '2500',
      bulk_copy_table_lock = 'true',
      bulk_copy_timeout = '600',
      presentation_zone_notebook_path = '../Data Engineering/Presentation Zone/SQL Spark Connector'
    ):

    validate_columns = [project_name, system_name, system_secret_scope, stage_name, job_name, table_name, presentation_zone_notebook_path, ]
    [self.validate_input(c) for c in validate_columns]

    presentation_zone_parameters = {
      "tableName":table_name,
      "destinationSchemaName":destination_schema_name,
      "storedProcedureName":stored_procedure_name,
      "saveMode":save_mode,
      "bulkCopyBatchSize": bulk_copy_batch_size,
      "bulkCopyTableLock": bulk_copy_table_lock,
      "bulkCopyTimeout": bulk_copy_table_lock,
      "externalSystem": system_secret_scope,
      "collection": collection_name,
      "databaseName": database_name,
      "idColumn": id_column
    }

    self.insert_parameters(project_name=project_name, system_name=system_name, system_secret_scope=system_secret_scope, system_order=system_order, system_is_active=system_is_active, stage_name=stage_name, stage_is_active=stage_is_active, stage_order=stage_order, job_name=job_name, job_order=job_order, job_is_active=job_is_active, step_name=presentation_zone_notebook_path, step_order=10, step_is_active=True, parameters=presentation_zone_parameters)

  def hydrate_data_quality_assessment(self,
      project_name,
      system_name,
      stage_name,
      job_name,
      system_secret_scope,
      table_name,
      system_is_active = True,
      stage_is_active = True,
      job_is_active = True,
      system_order = 10,
      stage_order = 10,
      job_order = 10,
      delta_history_minutes = -1,
      notebook_path = '../Data Quality Rules Engine/Data Quality Assessment' 
    ):

    validate_columns = [project_name, system_name, system_secret_scope, stage_name, job_name, table_name, notebook_path]
    [self.validate_input(c) for c in validate_columns]

    parameters = {
      "tableName":table_name,
      "deltaHistoryMinutes":delta_history_minutes
    }

    self.insert_parameters(project_name=project_name, system_name=system_name, system_secret_scope=system_secret_scope, system_order=system_order, system_is_active=system_is_active, stage_name=stage_name, stage_is_active=stage_is_active, stage_order=stage_order, job_name=job_name, job_order=job_order, job_is_active=job_is_active, step_name=notebook_path, step_order=10, step_is_active=True, parameters=parameters)

  def hydrate_batch_file(self,
      project_name,
      system_name,
      stage_name,
      job_name,
      system_secret_scope,
      table_name,
      schema_name,
      system_is_active = True,
      stage_is_active = True,
      job_is_active = True,
      system_order = 10,
      stage_order = 10,
      job_order = 10,
      external_data_path = '',
      file_extension = '',
      delimiter = '',
      header = 'False',
      multi_line = 'False',
      row_tag = '',
      root_tag = '',
      is_date_partitioned = 'True',
      silver_zone_partition_column = '',
      silver_zone_cluster_column = 'pk',
      silver_zone_cluster_buckets = '8',
      optimize_where = '',
      optimize_z_order_by = '',
      vacuum_retention_hours = 168,
      partition_column = '',
      num_partitions = '8',
      primary_key_columns = '',
      explode_and_flatten = 'True',
      cleanse_column_names = 'True',
      timestamp_columns = '',
      timestamp_format = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'",
      encrypt_columns = '',
      load_type = 'Merge',
      destination = 'silvergeneral',
      bronze_notebook_path = '../Data Engineering/Bronze Zone/Batch File Parquet',
      silver_notebook_path = '../Data Engineering/Silver Zone/Delta Load'
    ):
    
    if primary_key_columns == '' and load_type == 'Merge':
      raise ValueError("PrimaryKeyColumn must be specified if LoadType='Merge'.")
  
    validate_columns = [project_name, system_name, system_secret_scope, stage_name, job_name, external_data_path, schema_name, table_name, bronze_notebook_path, silver_notebook_path]
    [self.validate_input(c) for c in validate_columns]

    bronze_parameters = {
      "schemaName":schema_name,
      "tableName":table_name,
      "partitionColumn":partition_column,
      "numPartitions":num_partitions,
      "externalSystem":system_secret_scope,
      "externalDataPath":external_data_path,
      "fileExtension":file_extension,
      "delimeter":delimiter,
      "header":header,
      "multiLine":multi_line,
      "rowTag":row_tag,
      "rootTag":root_tag
    }
    if is_date_partitioned == 'True':
      bronze_parameters['dateToProcess'] = '-1'
    silver_parameters = {
      "schema":schema_name,
      "tableName":table_name,
      "numPartitions":num_partitions,
      "primaryKeyColumns":primary_key_columns,
      "externalSystem":system_secret_scope,
      "partitionCol":silver_zone_partition_column,
      "clusterCol":silver_zone_cluster_buckets,
      "optimizeZOrderBy":optimize_z_order_by,
      "vacuumRetentionHours":vacuum_retention_hours,
      "loadType":load_type,
      "destination":destination,
      "explodeAndFlatten":explode_and_flatten,
      "cleanseColumnNames":cleanse_column_names,
      "timestampColumns":timestamp_columns,
      "timestampFormat":timestamp_format,
      "encryptColumns":encrypt_columns
    }  
    if is_date_partitioned == 'True':
      silver_parameters['dateToProcess'] = '-1'

    self.insert_parameters(project_name=project_name, system_name=system_name, system_secret_scope=system_secret_scope, system_order=system_order, system_is_active=system_is_active, stage_name=stage_name, stage_is_active=stage_is_active, stage_order=stage_order, job_name=job_name, job_order=job_order, job_is_active=job_is_active, step_name=bronze_notebook_path, step_order=10, step_is_active=True, parameters=bronze_parameters)          
    self.insert_parameters(project_name=project_name, system_name=system_name, system_secret_scope=system_secret_scope, system_order=system_order, system_is_active=system_is_active, stage_name=stage_name, stage_is_active=stage_is_active, stage_order=stage_order, job_name=job_name, job_order=job_order, job_is_active=job_is_active, step_name=silver_notebook_path, step_order=20, step_is_active=True, parameters=silver_parameters)          

  def hydrate_gold_zone_power_bi(self,
      project_name,
      system_name,
      stage_name,
      job_name,
      system_secret_scope,
      table_name,
      system_is_active = True,
      stage_is_active = True,
      job_is_active = True,
      system_order = 10,
      stage_order = 10,
      job_order = 10,
      cleanse_path = True,
      rename_file = True,
      gold_zone_notebook_path = '../Data Engineering/Gold Zone/Power BI File'
  ):

    validate_columns = [project_name, system_name, system_secret_scope, stage_name, job_name, table_name, gold_zone_notebook_path]
    [self.validate_input(c) for c in validate_columns]

    gold_parameters = {
      "tableName":table_name,
      "cleansePath":cleanse_path,
      "renameFile":rename_file
    }
    self.insert_parameters(project_name=project_name, system_name=system_name, system_secret_scope=system_secret_scope, system_order=system_order, system_is_active=system_is_active, stage_name=stage_name, stage_is_active=stage_is_active, stage_order=stage_order, job_name=job_name, job_order=job_order, job_is_active=job_is_active, step_name=gold_zone_notebook_path, step_order=10, step_is_active=True, parameters=gold_parameters)

  def hydrate_clone(self,
      project_name,
      system_name,
      stage_name,
      job_name,
      system_secret_scope,
      source_table_name,
      destination_relative_path,
      destination_table_name,
      system_order = 10,
      stage_order = 10,
      job_order = 10,
      system_is_active = True,
      stage_is_active = True,
      job_is_active = True,
      destination = 'sandbox',
      append_todays_date_to_table_name = 'True',
      overwrite_table = 'True',
      destination_table_comment ='',
      clone_type = 'SHALLOW',
      time_travel_timestamp_expression = '',
      time_travel_version_expression = '',
      log_retention_duration_days = '7',
      deleted_file_retention_duration_days = '7',
      notebook_path = '../Data Engineering/Sandbox Zone/Clone Table'
    ):

    validate_columns = [project_name, system_name, system_secret_scope, stage_name, job_name, source_table_name, destination_relative_path, destination_table_name, destination_table_comment]
    [self.validate_input(c) for c in validate_columns]

    parameters = {
      "sourceTableName":source_table_name,
      "destination":destination,
      "destinationRelativePath":destination_relative_path,
      "destinationTableName":destination_table_name,
      "destinationTableComment":destination_table_comment,
      "cloneType":clone_type,
      "timeTravelTimestampExpression":time_travel_timestamp_expression,
      "timeTravelVersionExpression":time_travel_version_expression,
      "logRetentionDurationDays":log_retention_duration_days,
      "deletedFileRetentionDurationDays":deleted_file_retention_duration_days,
      "appendTodaysDateToTableName":append_todays_date_to_table_name,
      "overwriteTable":overwrite_table
    }
    self.insert_parameters(project_name=project_name, system_name=system_name, system_secret_scope=system_secret_scope, system_order=system_order, system_is_active=system_is_active, stage_name=stage_name, stage_is_active=stage_is_active, stage_order=stage_order, job_name=job_name, job_order=job_order, job_is_active=job_is_active, step_name=notebook_path, step_order=10, step_is_active=True, parameters=parameters)

  def hydrate_ML_experiment_training(self,
      project_name,
      system_name,
      stage_name,
      job_name,
      system_secret_scope,
      table_name,
      label,
      notebook_path,
      system_order = 10,
      stage_order = 10,
      job_order = 10,
      system_is_active = True,
      stage_is_active = True,
      job_is_active = True,
      experiment_name = '',
      experiment_id = '',
      excluded_columns = '',
      continuous_columns = '',
      regularization_parameters = '0.1, 0.01',
      prediction_column = 'prediction',
      folds = '2',
      train_test_split = '0.7, 0.3',
    ):
    
    validate_columns = [project_name, system_name, system_secret_scope, stage_name, job_name, table_name, experiment_name, label, notebook_path]
    [self.validate_input(c) for c in validate_columns]

    parameters = {
      "tableName":table_name,
      "experimentName":experiment_name,
      "label":label,
      "continuousColumns":continuous_columns,
      "regularizationParameters":regularization_parameters,
      "predictionColumn":prediction_column,
      "folds":folds,
      "trainTestSplit":train_test_split
    }
    self.insert_parameters(project_name=project_name, system_name=system_name, system_secret_scope=system_secret_scope, system_order=system_order, system_is_active=system_is_active, stage_name=stage_name, stage_is_active=stage_is_active, stage_order=stage_order, job_name=job_name, job_order=job_order, job_is_active=job_is_active, step_name=notebook_path, step_order=10, step_is_active=True, parameters=parameters)

  def hydrate_ML_batch_inference(self,
      project_name,
      system_name,
      stage_name,
      job_name,
      system_secret_scope,
      table_name,
      destination_table_name,
      notebook_path,
      system_order = 10,
      stage_order = 10,
      job_order = 10,
      system_is_active = True,
      stage_is_active = True,
      job_is_active = True,
      experiment_notebook_path = '',
      experiment_id = '',
      metric_clause = '',
      delta_history_minutes = -1,
      excluded_columns = '',
      partitions = '8',
      primary_columns = '',
      partition_col = '',
      cluster_col= '',
      cluster_buckets = '8',
      optimize_where = '',
      optimize_z_order_by = '',
      vacuum_retention_hours = 168,
      load_type = 'Merge',
      destination = 'silvergeneral',
    ):
    
    validate_columns = [project_name, system_name, system_secret_scope, stage_name, job_name, table_name, destination_table_name, notebook_path]
    [self.validate_input(c) for c in validate_columns]

    parameters = {
      "exerimentNotebookPath":experiment_notebook_path,
      "experimentId":experiment_id,
      "metricClause":metric_clause,
      "tableName":table_name,
      "deltaHistoryMinutes":delta_history_minutes,
      "excludedColumns":excluded_columns,
      "partitions":partitions,
      "primaryKeyColumns":primary_columns,
      "destinationTablename":destination_table_name,
      "partitionCol":partition_col,
      "clusterCol":cluster_col,
      "clusterBuckets":cluster_buckets,
      "optimizeWhere":optimize_where,
      "optimizeZOrderBy":optimize_z_order_by,
      "vacuumRetentionHours":vacuum_retention_hours,
      "loadType":load_type,
      "destination":destination
    }

    self.insert_parameters(project_name=project_name, system_name=system_name, system_secret_scope=system_secret_scope, system_order=system_order, system_is_active=system_is_active, stage_name=stage_name, stage_is_active=stage_is_active, stage_order=stage_order, job_name=job_name, job_order=job_order, job_is_active=job_is_active, step_name=notebook_path, step_order=10, step_is_active=True, parameters=parameters)

  def hydrate_ML_streaming_inference(self,
      project_name,
      system_name,
      stage_name,
      job_name,
      system_secret_scope,
      table_name,
      destination_table_name,
      notebook_path,
      system_order = 10,
      stage_order = 10,
      job_order = 10,
      system_is_active = True,
      stage_is_active = True,
      job_is_active = True,
      experiment_notebook_path = '',
      experiment_id = '',
      metric_clause = '',
      excluded_columns = '',
      destination = 'silvergeneral',
    ):

    validate_columns = [project_name, system_name, system_secret_scope, stage_name, job_name, table_name, destination_table_name, notebook_path]
    [self.validate_input(c) for c in validate_columns]

    parameters = {
      "experimentNotebookPath":experiment_notebook_path,
      "experimentId":experiment_id,
      "metricClause":metric_clause,
      "tableName":table_name,
      "excludedColumns":excluded_columns,
      "destinationTableName":destination_table_name,
      "destination":destination
    }
    self.insert_parameters(project_name=project_name, system_name=system_name, system_secret_scope=system_secret_scope, system_order=system_order, system_is_active=system_is_active, stage_name=stage_name, stage_is_active=stage_is_active, stage_order=stage_order, job_name=job_name, job_order=job_order, job_is_active=job_is_active, step_name=notebook_path, step_order=10, step_is_active=True, parameters=parameters)

  def hydrate_eda_notebook(self,
      project_name,
      system_name,
      stage_name,
      job_name,
      system_secret_scope,
      table_name,
      notebook_path,
      system_order = 10,
      stage_order = 10,
      job_order = 10,
      system_is_active = True,
      stage_is_active = True,
      job_is_active = True,
      delta_history_minutes = '-1',
      sample_percent = '-1',
      column_to_analyze = '',
      column_to_analyze2 = '',
    ):
    
    validate_columns = [project_name, system_name, system_secret_scope, stage_name, job_name, table_name, notebook_path]
    [self.validate_input(c) for c in validate_columns]

    parameters = {
      "tableName":table_name,
      "deltaHistoryMinutes":delta_history_minutes,
      "samplePercent":sample_percent,
      "columnToAnalyze":column_to_analyze,
      "columnToAnalyze2":column_to_analyze2
    }
    self.insert_parameters(project_name=project_name, system_name=system_name, system_secret_scope=system_secret_scope, system_order=system_order, system_is_active=system_is_active, stage_name=stage_name, stage_is_active=stage_is_active, stage_order=stage_order, job_name=job_name, job_order=job_order, job_is_active=job_is_active, step_name=notebook_path, step_order=10, step_is_active=True, parameters=parameters)
  
  def hydrate_streaming_notebook(self,
      project_name, 
      system_name,
      stage_name,
      job_name,
      system_secret_scope,
      trigger,
      table_name,
      system_order = 10,
      system_is_active = True,
      stage_is_active = True,
      stage_order = 10,
      job_order = 10,
      job_is_active = True,
      external_system = '',
      max_events_per_trigger = 10000,
      offset = 0,
      interval = 0,
      event_hub_starting_position = 'fromStartOfStream',
      database_name = '',
      destination = 'silvergeneral',
      stop_seconds = 120,
      column_name = '',
      output_table_name = '',
      notebook_path = '../Data Engineering/Streaming/Output to Azure Data Explorer'
    ):
    
    validate_columns = [project_name, system_name, system_secret_scope, stage_name, job_name, external_system, table_name, notebook_path] 
    [self.validate_input(c) for c in validate_columns]
    
    step_name = self.generate_step_name(notebook_path, 18)
    
    parameters = {
      "tableName":table_name,
      "database_name":database_name,
      "destination":destination,
      "externalSystem":external_system,
      "trigger":trigger,
      "maxEventsPerTrigger":max_events_per_trigger,
      "interval":interval,
      "offset":offset,
      "columnName":column_name,
      "outputTableName":output_table_name,
      "stopSeconds":stop_seconds,
      "eventHubStartingPosition":event_hub_starting_position
    }
    
    self.insert_parameters(project_name=project_name, system_name=system_name, system_secret_scope=system_secret_scope, system_order=system_order, system_is_active=system_is_active, stage_name=stage_name, stage_is_active=stage_is_active, stage_order=stage_order, job_name=job_name, job_order=job_order, job_is_active=job_is_active, step_name=notebook_path, step_order=10, step_is_active=True, parameters=parameters)
    
  def hydrate_cosmos(self,
      project_name, 
      system_name,
      stage_name,
      job_name,
      system_secret_scope,
      collection,
      schema_name,
      table_name,
      primary_key_columns,
      partition_column,
      pushdown_predicate,
      system_order = 10,
      system_is_active = True,
      stage_is_active = True,
      stage_order = 10,
      job_order = 10,
      job_is_active = True,
      silver_zone_partition_column = '',
      silver_zone_cluster_column = 'pk',
      silver_zone_cluster_buckets = '8',
      optmize_where = '',
      optimize_z_order_by = '',
      vaccum_retention_hours = 168,
      num_partitions = 8,
      explode_and_flatten = 'True',
      cleanse_column_names = 'True',
      timestamp_columns = '',
      timestamp_format = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'",
      encrypt_columns = '',
      load_type = 'Merge',
      destination = 'silvergeneral',
      bronze_zone_notebook_path = '../Data Engineering/Bronze Zone/Batch CosmosDB',
      silver_zone_notebook_path = '../Data Engineering/Silver Zone/Delta Load',
      gold_zone_notebook_path = '',
      presentation_zone_notebook_path = ''
  ):
    validate_columns = [project_name, system_name, system_secret_scope, stage_name, job_name, collection, schema_name, table_name, primary_key_columns, bronze_zone_notebook_path, silver_zone_notebook_path]
    [self.validate_input(c) for c in validate_columns]
    bronze_parameters = {
      "collection":collection,
      "partitionColumn":partition_column,
      "externalSystem":system_secret_scope,
      "schemaName":schema_name,
      "tableName":table_name,
      "pushdownPredicate":pushdown_predicate
    }
    silver_parameters = {
      "schemaName":schema_name,
      "tableName":table_name,
      "numPartitions":num_partitions,
      "primaryKeyColumns":primary_key_columns,
      "externalSystem":system_secret_scope,
      "partitionCol":partition_column,
      "clusterCol":silver_zone_cluster_column,
      "clusterBuckets":silver_zone_cluster_buckets,
      "optmizeWhere":optmize_where,
      "optimizeZOrderBy":optimize_z_order_by,
      "vacuumRetentionHours":vaccum_retention_hours,
      "loadType":load_type,
      "destination":destination,
      "explodeAndFlatten":explode_and_flatten,
      "cleanseColumnNames":cleanse_column_names,
      "timestampColumns":timestamp_columns,
      "timestampFormat":timestamp_format,
      "encryptColumns":encrypt_columns
    }
    
    bronze_step_name = self.generate_step_name(bronze_zone_notebook_path, 18)
    silver_step_name = self.generate_step_name(silver_zone_notebook_path, 18)
    
    self.insert_parameters(project_name=project_name, system_name=system_name, system_secret_scope=system_secret_scope, system_order=system_order, system_is_active=system_is_active, stage_name=stage_name, stage_is_active=stage_is_active, stage_order=stage_order, job_name=job_name, job_order=job_order, job_is_active=job_is_active, step_name=bronze_step_name, step_order=10, step_is_active=True, parameters=bronze_parameters)
    self.insert_parameters(project_name=project_name, system_name=system_name, system_secret_scope=system_secret_scope, system_order=system_order, system_is_active=system_is_active, stage_name=stage_name, stage_is_active=stage_is_active, stage_order=stage_order, job_name=job_name, job_order=job_order, job_is_active=job_is_active, step_name=silver_step_name, step_order=20, step_is_active=True, parameters=silver_parameters)
  
  def hydrate_data_catalog(self,
      project_name, 
      system_name,
      stage_name,
      job_name,
      system_secret_scope,
      system_order = 10,
      system_is_active = True,
      stage_is_active = True,
      stage_order = 10,
      job_order = 10,
      job_is_active = True,
      exclude_databases = '',
      exclude_tables = '',
      thread_pool = '1',
      timeout_seconds = 180000,
      vaccum_retention_hours = 672,
      notebook_path = '../Exploratory Data Analysis/Data Catalog Master'
  ):
    validate_columns = [project_name, system_name, system_secret_scope, stage_name, job_name, notebook_path] 
    [self.validate_input(c) for c in validate_columns]
    
    step_name = self.generate_step_name(notebook_path, 18)
    
    parameters = {
      "excludeDatabases":exclude_databases,
      "excludeTables":exclude_tables,
      "threadPool":thread_pool,
      "timeoutSeconds":timeout_seconds,
      "vaccumRetentionHours":vaccum_retention_hours
    }
    
    self.insert_parameters(project_name=project_name, system_name=system_name, system_secret_scope=system_secret_scope, system_order=system_order, system_is_active=system_is_active, stage_name=stage_name, stage_is_active=stage_is_active, stage_order=stage_order, job_name=job_name, job_order=job_order, job_is_active=job_is_active, step_name=notebook_path, step_order=10, step_is_active=True, parameters=parameters)
     
  def hydrate_summary_zone(self,
      project_name,
      system_name,
      stage_name,
      job_name,
      table_name,
      system_secret_scope,
      system_order = 10,
      system_is_active = True,
      stage_is_active = True,
      stage_order = 10,
      job_order = 10,
      job_is_active = True,
      cleanse_path = True,
      rename_file = True,
      summary_zone_notebook_path = '../DataEngineering/SummaryZone/Power BI File'
    ):
    
    validate_columns = [project_name, system_name, system_secret_scope, stage_name, job_name, table_name, summary_zone_notebook_path]
    [self.validate_input(c) for c in validate_columns]

    parameters = {
      "tableName":table_name,
      "cleansePath":cleanse_path,
      "renameFile":rename_file
    }
    
    self.insert_parameters(project_name=project_name, system_name=system_name, system_secret_scope=system_secret_scope, system_order=system_order, system_is_active=system_is_active, stage_name=stage_name, stage_is_active=stage_is_active, stage_order=stage_order, job_name=job_name, job_order=job_order, job_is_active=job_is_active, step_name=summary_zone_notebook_path, step_order=10, step_is_active=True, parameters=parameters)
    
  def hydrate_pipeline_validation(self,
      project_name,
      system_name,
      stage_name,
      job_name,
      system_secret_scope,
      experiment_name,
      database_catalog,
      system_order = 10,
      system_is_active = True,
      stage_is_active = True,
      stage_order = 10,
      job_order = 10,
      job_is_active = True,
      thread_pool = '1',
      timeout_seconds = 1800,
      notebook_path = '../Orchestration/Validate Pipeline'
    ):
    
    validate_columns = [project_name, system_name, system_secret_scope, stage_name, job_name, experiment_name, database_catalog, notebook_path]
    [self.validate_input(c) for c in validate_columns]

    parameters = {
      "experimentName":experiment_name,
      "database":database_catalog,
      "threadPool":thread_pool,
      "timeoutSeconds":timeout_seconds
    }
    
    self.insert_parameters(project_name=project_name, system_name=system_name, system_secret_scope=system_secret_scope, system_order=system_order, system_is_active=system_is_active, stage_name=stage_name, stage_is_active=stage_is_active, stage_order=stage_order, job_name=job_name, job_order=job_order, job_is_active=job_is_active, step_name=notebook_path, step_order=10, step_is_active=True, parameters=parameters)

  def hydrate_batch_parquet(self,
      project_name,
      system_name,
      stage_name,
      job_name,
      system_secret_scope,
      table_name,
      schema_name,
      primary_key_columns,
      system_order = 10,
      system_is_active = True,
      stage_is_active = True,
      stage_order = 10,
      job_order = 10,
      job_is_active = True,
      mount_point = '',
      external_data_path = '',
      query_zone_partition_column = '',
      query_zone_cluster_column = 'pk',
      query_zone_cluster_buckets = '8',
      optmize_where = '',
      optmize_z_order_by = '',
      vaccum_retention_hours = 168,
      partition_column = '',
      num_partitions = '8',
      raw_zone_notebook_path = '../DataEngineering/RawZone/Batch File Parquet',
      query_zone_notebook_path ='../DataEngineering/QueryZone/Delta Load'
    ):
    
    validate_columns = [project_name, system_name, system_secret_scope, stage_name, job_name, mount_point, external_data_path, primary_key_columns, schema_name, table_name, raw_zone_notebook_path, query_zone_notebook_path]
    [self.validate_input(c) for c in validate_columns]

    raw_parameters = {
      "schemaName":schema_name,
      "tableName":table_name,
      "partitionColumn":partition_column,
      "numPartitions":num_partitions,
      "externalSystem":system_secret_scope,
      "mountPoint":mount_point,
      "externalDataPath":external_data_path
    }
    
    self.insert_parameters(project_name=project_name, system_name=system_name, system_secret_scope=system_secret_scope, system_order=system_order, system_is_active=system_is_active, stage_name=stage_name, stage_is_active=stage_is_active, stage_order=stage_order, job_name=job_name, job_order=job_order, job_is_active=job_is_active, step_name=raw_zone_notebook_path, step_order=10, step_is_active=True, parameters=raw_parameters)
    
    query_parameters = {
      "schemaName":schema_name,
      "tableName":table_name,
      "numPartitions":num_partitions,
      "primaryKeyColumns":primary_key_columns,
      "externalSystem":system_secret_scope,
      "clusterCol":query_zone_cluster_column,
      "clusterBuckets":query_zone_cluster_buckets,
      "optmizeWhere":optmize_where,
      "optmizeZOrderBy":optmize_z_order_by,
      "vaccumRetentionHours":vaccum_retention_hours
    }
    
    self.insert_parameters(project_name=project_name, system_name=system_name, system_secret_scope=system_secret_scope, system_order=system_order, system_is_active=system_is_active, stage_name=stage_name, stage_is_active=stage_is_active, stage_order=stage_order, job_name=job_name, job_order=job_order, job_is_active=job_is_active, step_name=raw_zone_notebook_path, step_order=20, step_is_active=True, parameters=query_parameters)
  
  def hydrate_spark_table_maintenance(self,
      project_name,
      system_name,
      stage_name,
      job_name,
      table_name,
      system_secret_scope,
      system_order=10,
      system_is_active=True, 
      stage_is_active = True, 
      stage_order = 10,
      job_is_active=True,
      job_order=10,
      run_optimize = '1',
      run_vacuum = '1',
      optimize_where = '',
      optimize_z_order_by = '',
      vacuum_retention_hours = 168,
      notebook_path = '../Orchestration/Spark Table Maintenance'
    ):
    
    validate_columns = [project_name, system_name, system_secret_scope, stage_name, job_name,  table_name, notebook_path] 
    [self.validate_input(c) for c in validate_columns]
    
    step_name = self.generate_step_name(notebook_path, 18)
    
    parameters = {
      "tableName":table_name,
      "runOptimize":run_optimize,
      "runVacuum":run_vacuum,
      "optimizeWhere":optimize_where,
      "optimizeZOrderBy":optimize_z_order_by,
      "vacuumRetentionHours":vacuum_retention_hours
    }
    
    self.insert_parameters(project_name=project_name, system_name=system_name, system_secret_scope=system_secret_scope, system_order=system_order, system_is_active=system_is_active, stage_name=stage_name, stage_is_active=stage_is_active, stage_order=stage_order, job_name=job_name, job_order=job_order, job_is_active=job_is_active, step_name=notebook_path, step_order=10, step_is_active=True, parameters=parameters)
    
  def hydrate_sanctioned_zone(self,
      project_name,
      system_name,
      stage_name,
      job_name,
      table_name,
      system_secret_scope,
      system_order = 10,
      system_is_active = True,
      stage_is_active = True,
      stage_order = 10,
      job_order = 10,
      job_is_active = True,
      destination_schema_name = '',
      stored_procedure_name = '',
      save_mode = 'overwrite',
      bulk_copy_batch_size = '2500',
      bulk_copy_table_lock = 'true',
      bulk_copy_timeout = '600',
      sanctioned_zone_notebook_path ='../DataEngineering/SanctionedZone/SQL Spark Connector'
    ):
    
    validate_columns = [project_name, system_name, system_secret_scope, stage_name, job_name, table_name, sanctioned_zone_notebook_path]
    [self.validate_input(c) for c in validate_columns]

    parameters = {
      "tableName":table_name,
      "destinationSchemaName":destination_schema_name,
      "storedProcedureName":stored_procedure_name,
      "saveMode":save_mode,
      "bulkCopyBatchSize":bulk_copy_batch_size,
      "bulkCopyTableLock":bulk_copy_table_lock,
      "bulkCopyTimeout":bulk_copy_timeout
    }
    
    self.insert_parameters(project_name=project_name, system_name=system_name, system_secret_scope=system_secret_scope, system_order=system_order, system_is_active=system_is_active, stage_name=stage_name, stage_is_active=stage_is_active, stage_order=stage_order, job_name=job_name, job_order=job_order, job_is_active=job_is_active, step_name=sanctioned_zone_notebook_path, step_order=10, step_is_active=True, parameters=parameters)
    
  def hydrate_adf_sql(self,
      project_name,
      system_name,
      stage_name,
      job_name,
      table_name,
      database_name,
      user_name,
      schema_name,
      system_secret_scope,
      password_key_vault_secret_name,
      destination_container_name,
      destination_file_path,
      server_name,
      system_order = 10,
      system_is_active = True,
      stage_is_active = True,
      stage_order = 10,
      job_order = 10,
      job_is_active = True,
      pushdown_query = '',
      stored_procedure_call = '',
      query_timeout_minutes = 120,
      isolation_level = 'ReadCommitted',
      destination_encoding = 'UTF-8',
      file_pattern = 'Array of objects',
      adf_pipeline_name ='On Premises Database Query to Staging'
    ):
    
    validate_columns = [adf_pipeline_name, project_name, system_name, system_secret_scope, stage_name, job_name, server_name, database_name, user_name, password_key_vault_secret_name, schema_name, table_name, destination_file_path]
    [self.validate_input(c) for c in validate_columns]

    parameters = {
      "serverName":server_name,
      "databaseName":database_name,
      "userName":user_name,
      "passwordKeyVaultSecretName":password_key_vault_secret_name,
      "schemaName":schema_name,
      "tableName":table_name,
      "destinationContainerName":destination_container_name,
      "destinationFilePath":destination_file_path,
      "destinationEncoding":destination_encoding,
      "filePattern":file_pattern,
      "queryTimeoutMinutes":query_timeout_minutes,
      "isolationLevel":isolation_level
    }
    
    if adf_pipeline_name == 'On Premises Database Query to Staging':
      parameters['pushdownQuery']= pushdown_query
    
    if adf_pipeline_name == 'On Premises Database Stored Procedure to Staging':
      parameters['pushdownQuery']= stored_procedure_call
    
    self.insert_parameters(project_name=project_name, system_name=system_name, system_secret_scope='', system_order=system_order, system_is_active=system_is_active, stage_name=stage_name, stage_is_active=stage_is_active, stage_order=stage_order, job_name=job_name, job_order=job_order, job_is_active=job_is_active, step_name=adf_pipeline_name, step_order=20, step_is_active=True, parameters=parameters)
    
  def hydrate_adf_batch_file(self,
      project_name,
      system_name,
      stage_name,
      job_name,
      local_file_path,
      local_file_name,
      destination_container_name,
      destination_file_path,
      system_secret_scope,
      system_order = 10,
      system_is_active = True,
      stage_is_active = True,
      stage_order = 10,
      job_order = 10,
      job_is_active = True,
      local_encoding = 'UTF-8',
      recursive = '1',
      enable_partition_discovery = '0',
      destination_encoding = 'UTF-8',
      column_delimiter = ',',
      row_delimiter = '\r\n',
      escape_character = '\\',
      quote_character = '"',
      first_row_as_header = '1',
      null_value = 'NULL',
      name_spaces = '1',
      detect_data_type = '1',
      adf_pipeline_name ='On Premises JSON to Staging'
    ):
    
    validate_columns = [adf_pipeline_name, project_name, system_name, system_secret_scope, stage_name, job_name, local_file_path, local_file_name, destination_container_name, destination_file_path]
    [self.validate_input(c) for c in validate_columns]

    parameters = {
      "localFilePath":local_file_path,
      "localFileName":local_file_name,
      "localEncoding":local_encoding,
      "recursive":recursive,
      "enablePartitionDiscovery":enable_partition_discovery,
      "destinationContainerName":destination_container_name,
      "destinationFilePath":destination_file_path,
      "destinationEncoding":destination_encoding
    }
    
    if adf_pipeline_name == 'On Premises Delimited to Staging':
      parameters['columnDelimiter'] = column_delimiter
      parameters['rowDelimiter'] = row_delimiter
      parameters['escapeCharacter'] = escape_character
      parameters['quoteCharacter'] = quote_character
      parameters['firstRowAsHeader'] = first_row_as_header
      parameters['nullValue'] = null_value
    
    if adf_pipeline_name == 'On Premises XML to Staging':
      parameters['nullValue'] = null_value
      parameters['namespaces'] = name_spaces
      parameters['detectDataType'] = detect_data_type
    
    self.insert_parameters(project_name=project_name, system_name=system_name, system_secret_scope='', system_order=system_order, system_is_active=system_is_active, stage_name=stage_name, stage_is_active=stage_is_active, stage_order=stage_order, job_name=job_name, job_order=job_order, job_is_active=job_is_active, step_name=adf_pipeline_name, step_order=20, step_is_active=True, parameters=parameters)

  def hydrate_app_insights(self,
      project_name,
      system_name,
      stage_name,
      job_name,
      system_secret_scope = 'external',
      system_order = 10,
      system_is_active = True,
      stage_is_active = True,
      stage_order = 10,
      job_order = 10,
      job_is_active = True,
      app_insights_instance_name = '',
      streaming_notebook_path = '../Data Engineering/Streaming/AppInsights/Stream Processing - App Insights - Event'
    ):
    
    validate_columns = [project_name, system_name, system_secret_scope, stage_name, job_name, app_insights_instance_name, streaming_notebook_path]
    [self.validate_input(c) for c in validate_columns]

    parameters = {
      "appInsightsInstanceName":app_insights_instance_name,
      "externalSystem":system_secret_scope
    }
    
    self.insert_parameters(project_name=project_name, system_name=system_name, system_secret_scope='N/A', system_order=system_order, system_is_active=system_is_active, stage_name=stage_name, stage_is_active=stage_is_active, stage_order=stage_order, job_name=job_name, job_order=job_order, job_is_active=job_is_active, step_name=streaming_notebook_path, step_order=10, step_is_active=True, parameters=parameters)
  
#schema 
  def create_orchestration_schema(self):
    if self.LogMethod in (2, 3):
      silverDataPath = "{0}/{1}".format(self.SilverProtectedBasePath, "orchestration")
      self.create_databases()
      self.create_orchestration_tables(silverDataPath)
      self.create_orchestration_logging_tables(silverDataPath)
      self.create_orchestration_views()
      
  def create_databases(self):
    spark.sql("CREATE DATABASE IF NOT EXISTS bronze COMMENT 'Stores restricted data engineering (raw) data.'")
    spark.sql("CREATE DATABASE IF NOT EXISTS silverprotected COMMENT 'Stores query-ready ACID compliant tables with restricted or protected data sensitivity classifications.'")
    spark.sql("CREATE DATABASE IF NOT EXISTS silvergeneral COMMENT 'Stores query-ready ACID compliant tables with non-sensitive data sensitivity classification.'")
    spark.sql("CREATE DATABASE IF NOT EXISTS goldprotected COMMENT 'Stores business-ready ACID compliant tables with restricted or protected data sensitivity classifications.'")
    spark.sql("CREATE DATABASE IF NOT EXISTS goldgeneral COMMENT 'Stores business-ready ACID compliant tables with non-sensitive data sensitivity classification.'")
    spark.sql("CREATE DATABASE IF NOT EXISTS sandbox COMMENT 'Stores data used for exploration, prototyping, feature engineering, ML development, model tuning etc.'")
    spark.sql("CREATE DATABASE IF NOT EXISTS archive COMMENT 'Stores periodic snapshots of tables for validation and historical purposes.'")

  def create_orchestration_staging_tables(self, project_name):
    bronzeDataPath = "{0}/{1}".format(self.BronzeBasePath, "orchestration")
    self.delete_orchestration_staging_tables(bronzeDataPath, project_name)
    
    projectsql = """
        CREATE TABLE IF NOT EXISTS bronze.project_{1}
        (
         ProjectKey STRING  
        ,ProjectName STRING 
        ,CreatedDate TIMESTAMP  
        )
        USING parquet
        LOCATION '{0}/project_{1}'
        """.format(bronzeDataPath, project_name)
    spark.sql(projectsql)
    
    systemsql = """
      CREATE TABLE IF NOT EXISTS bronze.system_{1}
        (
         SystemKey STRING  
        ,SystemName STRING 
        ,SystemSecretScope STRING 
        ,ProjectKey STRING 
        ,SystemOrder INT 
        ,IsActive BOOLEAN 
        ,IsRestart BOOLEAN 
        ,CreatedDate TIMESTAMP 
        ,ModifiedDate TIMESTAMP 
        )
        USING parquet
        LOCATION '{0}/system_{1}'
        """.format(bronzeDataPath, project_name)
    spark.sql(systemsql)
      
    stagesql = """
      CREATE TABLE IF NOT EXISTS bronze.stage_{1}
        (
         StageKey STRING 
        ,StageName STRING 
        ,SystemKey STRING 
        ,StageOrder INT 
        ,IsActive BOOLEAN 
        ,IsRestart BOOLEAN  
        ,NumberOfThreads INT 
        ,CreatedDate TIMESTAMP 
        ,ModifiedDate TIMESTAMP 
        )
        USING parquet
        LOCATION '{0}/stage_{1}'
        """.format(bronzeDataPath, project_name)
    spark.sql(stagesql)
      
    jobsql = """
      CREATE TABLE IF NOT EXISTS bronze.job_{1}
        (
         JobKey STRING 
        ,JobName STRING
        ,StageKey STRING
        ,JobOrder INT 
        ,IsActive BOOLEAN 
        ,IsRestart BOOLEAN 
        ,CreatedDate TIMESTAMP 
        ,ModifiedDate TIMESTAMP 
        )
        USING parquet
        LOCATION '{0}/job_{1}'
        """.format(bronzeDataPath, project_name)
    spark.sql(jobsql)
      
    stepsql = """
      CREATE TABLE IF NOT EXISTS bronze.step_{1}
        (
         StepKey STRING  
        ,StepName STRING 
        ,JobKey STRING 
        ,StepOrder INT 
        ,IsActive BOOLEAN 
        ,IsRestart BOOLEAN 
        ,CreatedDate TIMESTAMP 
        ,ModifiedDate TIMESTAMP 
        )
        USING parquet
        LOCATION '{0}/step_{1}'
        """.format(bronzeDataPath, project_name)
    spark.sql(stepsql)
      
    parametersql = """
      CREATE TABLE IF NOT EXISTS bronze.parameter_{1}
        (
           StepKey STRING 
          ,Parameters MAP<STRING, STRING> 
          ,CreatedDate TIMESTAMP 
          ,ModifiedDate TIMESTAMP 
        )
        USING parquet
        LOCATION '{0}/parameter_{1}'
        """.format(bronzeDataPath, project_name)
    spark.sql(parametersql)
      
    windowedextractionsql = """
      CREATE TABLE IF NOT EXISTS bronze.windowedextraction_{1}
        (
          StepKey STRING 
          ,WindowStart STRING 
          ,WindowEnd STRING 
          ,WindowOrder INT 
          ,IsActive BOOLEAN 
          ,ExtractionTimestamp TIMESTAMP 
          ,QueryZoneTimestamp TIMESTAMP 
        )
        USING parquet
        LOCATION '{0}/windowedextraction_{1}'
        """.format(bronzeDataPath, project_name)
    spark.sql(windowedextractionsql)
    
    #INSERT INTO bronze.stage_threads{0} (StageName, NumberOfThreads)
    #SELECT stage_name, threads FROM src
    #create a table here called bronze.stage_threads{project_name}
    stagethreadssql = """
    CREATE TABLE IF NOT EXISTS bronze.stage_threads_{1}
    (
      StageName STRING,
      NumberOfThreads BIGINT
    )
    USING parquet
    LOCATION '{0}/stage_threads_{1}'
    """.format(bronzeDataPath, project_name)
    spark.sql(stagethreadssql)

  def create_orchestration_tables(self, silverDataPath):
    projectsql = """
      CREATE TABLE IF NOT EXISTS silverprotected.project
      (
       ProjectKey STRING NOT NULL GENERATED ALWAYS AS (sha2(ProjectName, 256)) COMMENT 'Project Identifier Key as sha2'
      ,ProjectName STRING NOT NULL COMMENT 'Project Name'
      ,CreatedDate TIMESTAMP NOT NULL COMMENT 'metadata generated timestamp' 
      )
      USING delta
      LOCATION '{0}/project'
      PARTITIONED BY (ProjectKey)
      """.format(silverDataPath)
    spark.sql(projectsql)

    systemsql = """
      CREATE TABLE IF NOT EXISTS silverprotected.system
      (
       SystemKey STRING NOT NULL GENERATED ALWAYS AS (sha2(SystemName, 256)) COMMENT 'System Identifier Key as sha2'
      ,SystemName STRING NOT NULL COMMENT 'System Name'
      ,SystemSecretScope STRING COMMENT 'Optional secret scope for this system which contains connection info etc.'
      ,ProjectKey STRING NOT NULL COMMENT 'Foreign Key to silverprotected.project'
      ,SystemOrder INT NOT NULL COMMENT 'Order by which this system will run within the project'
      ,IsActive BOOLEAN NOT NULL COMMENT 'Activate/Deactivate this system'
      ,IsRestart BOOLEAN NOT NULL COMMENT 'System Status with respect to the project pipeline (true=run, false=skip (already run))'
      ,CreatedDate TIMESTAMP NOT NULL COMMENT 'metadata generated timestamp'
      ,ModifiedDate TIMESTAMP COMMENT 'metadata updated timestamp'
      )
      USING delta
      LOCATION '{0}/system'
      PARTITIONED BY (SystemKey)
      """.format(silverDataPath)
    spark.sql(systemsql)

    stagesql = """
      CREATE TABLE IF NOT EXISTS silverprotected.stage
      (
       StageKey STRING NOT NULL GENERATED ALWAYS AS (sha2(StageName, 256)) COMMENT 'Stage Identifier Key as sha2'
      ,StageName STRING NOT NULL COMMENT 'Stage Name'
      ,SystemKey STRING NOT NULL COMMENT 'Foreign Key to silverprotected.system'
      ,StageOrder INT NOT NULL COMMENT 'Order by which this stage will run within the system'
      ,IsActive BOOLEAN NOT NULL COMMENT 'Activate/Deactivate this stage'
      ,IsRestart BOOLEAN NOT NULL COMMENT 'Stage Status with respect to the system pipeline (true=run, false=skip (already run))'
      ,NumberOfThreads INT COMMENT 'Number of parallel threads with which to execute this stage (causing jobs to run in parallel)'
      ,CreatedDate TIMESTAMP NOT NULL COMMENT 'metadata generated timestamp'
      ,ModifiedDate TIMESTAMP COMMENT 'metadata updated timestamp'
      )
      USING delta
      LOCATION '{0}/stage'
      PARTITIONED BY (StageKey)
      """.format(silverDataPath)
    spark.sql(stagesql)

    jobsql = """
      CREATE TABLE IF NOT EXISTS silverprotected.job
      (
       JobKey STRING NOT NULL GENERATED ALWAYS AS (sha2(JobName, 256)) COMMENT 'Job Identifier Key as sha2'
      ,JobName STRING NOT NULL COMMENT 'Job Name'
      ,StageKey STRING NOT NULL COMMENT 'Foreign Key to silverprotected.stage'
      ,JobOrder INT NOT NULL COMMENT 'Order by which this job will run within the stage'
      ,IsActive BOOLEAN NOT NULL COMMENT 'Activate/Deactivate this job'
      ,IsRestart BOOLEAN NOT NULL COMMENT 'Job Status with respect to the stage pipeline (true=run, false=skip (already run))'
      ,CreatedDate TIMESTAMP NOT NULL COMMENT 'metadata generated timestamp'
      ,ModifiedDate TIMESTAMP COMMENT 'metadata updated timestamp'
      )
      USING delta
      LOCATION '{0}/job'
      PARTITIONED BY (JobKey)
      """.format(silverDataPath)
    spark.sql(jobsql)

    stepsql = """
      CREATE TABLE IF NOT EXISTS silverprotected.step
      (
       StepKey STRING NOT NULL COMMENT 'Step Identifier Key as sha2'
      ,StepName STRING NOT NULL COMMENT 'Step Name'
      ,JobKey STRING NOT NULL COMMENT 'Foreign Key to silverprotected.job'
      ,StepOrder INT NOT NULL COMMENT 'Order by which this step will run within the job'
      ,IsActive BOOLEAN NOT NULL COMMENT 'Activate/Deactivate this step'
      ,IsRestart BOOLEAN NOT NULL COMMENT 'Step Status with respect to the job pipeline (true=run, false=skip (already run))'
      ,CreatedDate TIMESTAMP NOT NULL COMMENT 'metadata generated timestamp'
      ,ModifiedDate TIMESTAMP COMMENT 'metadata updated timestamp'
      )
      USING delta
      LOCATION '{0}/step'
      PARTITIONED BY (StepKey)
      """.format(silverDataPath)
    spark.sql(stepsql)

    parametersql = """
      CREATE TABLE IF NOT EXISTS silverprotected.parameter
      (
         StepKey STRING NOT NULL COMMENT 'Foreign Key to silverprotected.step'
        ,Parameters MAP<STRING, STRING> NOT NULL COMMENT 'Parameter Map for this step'
        ,CreatedDate TIMESTAMP NOT NULL COMMENT 'metadata generated timestamp'
        ,ModifiedDate TIMESTAMP COMMENT 'metadata updated timestamp'
      )
      USING delta
      LOCATION '{0}/parameter'
      PARTITIONED BY (StepKey)
      """.format(silverDataPath)
    spark.sql(parametersql)

    windowedextractionsql = """
      CREATE TABLE IF NOT EXISTS silverprotected.windowedextraction
      (
        StepKey STRING NOT NULL COMMENT 'Foreign Key to silverprotected.parameter and silverprotected.step'
        ,WindowStart STRING NOT NULL COMMENT 'Window start range'
        ,WindowEnd STRING NOT NULL COMMENT 'Window end range'
        ,WindowOrder INT NOT NULL COMMENT 'Order of operations for windows within this step'
        ,IsActive BOOLEAN NOT NULL COMMENT 'Active/Deactivate this window'
        ,ExtractionTimestamp TIMESTAMP COMMENT 'Last Modified Timestamp for this window (Bronze Zone)'
        ,QueryZoneTimestamp TIMESTAMP COMMENT 'Last Modified Timestamp for this window (Silver Zone)'
      )
      USING delta
      LOCATION '{0}/windowedextraction'
      PARTITIONED BY (StepKey)
      """.format(silverDataPath)
    spark.sql(windowedextractionsql)

  def create_orchestration_logging_tables(self, silverDataPath):
    projectlogsql = """
      CREATE TABLE IF NOT EXISTS silverprotected.projectlog
      (
           ProjectLogGuid STRING NOT NULL 
          ,ProjectKey STRING 
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

    systemlogsql = """
      CREATE TABLE IF NOT EXISTS silverprotected.systemlog
      (
            SystemLogGuid STRING NOT NULL 
          ,ProjectLogGuid STRING
          ,SystemKey STRING 
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

    stagelogsql = """
      CREATE TABLE IF NOT EXISTS silverprotected.stagelog
      (
           StageLogGuid STRING NOT NULL 
          ,SystemLogGuid STRING
          ,StageKey STRING 
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

    joblogsql = """
      CREATE TABLE IF NOT EXISTS silverprotected.jobLog
      (
           JobLogGuid STRING NOT NULL 
          ,StageLogGuid STRING
          ,JobKey STRING 
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

    steplogsql = """
      CREATE TABLE IF NOT EXISTS silverprotected.steplog
      (
            StepLogGuid STRING NOT NULL 
          ,JobLogGuid STRING 
          ,StepKey STRING 
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

    notebooklogsql = """
      CREATE TABLE IF NOT EXISTS silverprotected.notebooklog
      (
            NotebookLogGuid STRING NOT NULL 
          ,StepLogGuid STRING 
          ,StepKey STRING 
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

  def create_orchestration_views(self):
    projectlogviewsql = """
      CREATE OR REPLACE VIEW silverprotected.vProjectLog
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
      SELECT plCTE.ProjectLogGuid, plCTE.ProjectKey, pl.Parameters.projectName AS ProjectName, plCTE.StartDateTime, plCTE.EndDateTime, plCTE.LogStatusKey, pl.Parameters, plCTE.Context, COALESCE(ple.Error, pl.Error) AS Error
      FROM plCTE 
      JOIN silverprotected.projectlog pl ON plCTE.ProjectLogGuid = pl.ProjectLogGuid AND pl.LogStatusKey = 1
      LEFT JOIN silverprotected.projectlog ple ON plCTE.ProjectLogGuid = ple.ProjectLogGuid AND ple.LogStatusKey = 3
      """
    spark.sql(projectlogviewsql)

    systemlogviewsql = """
      CREATE OR REPLACE VIEW silverprotected.vSystemLog
      AS
      WITH slCTE AS
      (
        SELECT
          ProjectLogGuid,
          SystemLogGuid,
          MAX(SystemKey) AS SystemKey,
          MAX(StartDateTime) AS StartDateTime,
          MAX(EndDateTime) AS EndDateTime,
          MAX(LogStatusKey) AS LogStatusKey,
          MAX(Context) AS Context
        FROM silverprotected.systemlog 
        GROUP BY ProjectLogGuid, SystemLogGuid
      )
      SELECT sl.Parameters.projectLogGuid AS ProjectLogGuid, slCTE.SystemLogGuid, sl.Parameters.systemKey AS SystemKey, sl.Parameters.systemName AS SystemName, sl.StartDateTime, slCTE.EndDateTime, slCTE.LogStatusKey, sl.Parameters, slCTE.Context, COALESCE(sle.Error, sl.Error) AS Error
      FROM slCTE 
      JOIN silverprotected.systemlog sl ON slCTE.SystemLogGuid = sl.SystemLogGuid AND sl.LogStatusKey = 1
      LEFT JOIN silverprotected.systemlog sle ON slCTE.SystemLogGuid = sle.SystemLogGuid AND sle.LogStatusKey = 3
      """
    spark.sql(systemlogviewsql)

    stagelogviewsql = """
      CREATE OR REPLACE VIEW silverprotected.vStageLog
      AS
      WITH slCTE AS
      (
        SELECT
          SystemLogGuid,
          StageLogGuid,
          MAX(StageKey) AS StageKey,
          MAX(StartDateTime) AS StartDateTime,
          MAX(EndDateTime) AS EndDateTime,
          MAX(LogStatusKey) AS LogStatusKey,
          MAX(Context) AS Context
        FROM silverprotected.stagelog 
        GROUP BY SystemLogGuid, StageLogGuid
      )
      SELECT sl.Parameters.systemLogGuid AS SystemLogGuid, slCTE.StageLogGuid, sl.Parameters.stageKey AS StageKey, sl.Parameters.stageName AS StageName, sl.StartDateTime, slCTE.EndDateTime, slCTE.LogStatusKey, sl.Parameters, slCTE.Context, COALESCE(sle.Error, sl.Error) AS Error
      FROM slCTE 
      JOIN silverprotected.stagelog sl ON slCTE.StageLogGuid = sl.StageLogGuid AND sl.LogStatusKey = 1
      LEFT JOIN silverprotected.stagelog sle ON slCTE.StageLogGuid = sle.StageLogGuid AND sle.LogStatusKey = 3
      """
    spark.sql(stagelogviewsql)

    joblogviewsql = """
      CREATE OR REPLACE VIEW silverprotected.vJobLog
      AS
      WITH jlCTE AS
      (
        SELECT
          StageLogGuid,
          JobLogGuid,
          MAX(JobKey) AS JobKey,
          MAX(StartDateTime) AS StartDateTime,
          MAX(EndDateTime) AS EndDateTime,
          MAX(LogStatusKey) AS LogStatusKey,
          MAX(Context) AS Context
        FROM silverprotected.joblog 
        GROUP BY StageLogGuid, JobLogGuid
      )
      SELECT jl.Parameters.stageLogGuid AS StageLogGuid, jlCTE.JobLogGuid, jl.Parameters.jobKey AS JobKey, jl.Parameters.jobName AS JobName, jl.StartDateTime, jlCTE.EndDateTime, jlCTE.LogStatusKey, jl.Parameters, jlCTE.Context, COALESCE(jle.Error, jl.Error) AS Error
      FROM jlCTE 
      JOIN silverprotected.joblog jl ON jlCTE.JobLogGuid = jl.JobLogGuid AND jl.LogStatusKey = 1
      LEFT JOIN silverprotected.joblog jle ON jlCTE.JobLogGuid = jle.JobLogGuid AND jle.LogStatusKey = 3
      """
    spark.sql(joblogviewsql)

    steplogviewsql = """
      CREATE OR REPLACE VIEW silverprotected.vStepLog
      AS
      WITH slCTE AS
      (
        SELECT
          JobLogGuid,
          StepLogGuid,
          MAX(StepKey) AS StepKey,
          MAX(StartDateTime) AS StartDateTime,
          MAX(EndDateTime) AS EndDateTime,
          MAX(LogStatusKey) AS LogStatusKey,
          MAX(Context) AS Context
        FROM silverprotected.steplog
        GROUP BY JobLogGuid, StepLogGuid
      )
      SELECT sl.Parameters.jobLogGuid AS JobLogGuid, slCTE.StepLogGuid, sl.Parameters.stepKey AS StepKey, sl.Parameters.stepName AS StepName, sl.StartDateTime, slCTE.EndDateTime, slCTE.LogStatusKey, sl.Parameters, slCTE.Context, COALESCE(sle.Error, sl.Error) AS Error
      FROM slCTE 
      JOIN silverprotected.steplog sl ON slCTE.StepLogGuid = sl.StepLogGuid AND sl.LogStatusKey = 1
      LEFT JOIN silverprotected.steplog sle ON slCTE.StepLogGuid = sle.StepLogGuid AND sle.LogStatusKey = 3
      """
    spark.sql(steplogviewsql)

    notebooklogviewsql = """
      CREATE OR REPLACE VIEW silverprotected.vNotebookLog
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
        GROUP BY StepLogGuid, NotebookLogGuid
      )
      SELECT nl.Parameters.notebookLogGuid AS NotebookLogGuid, nl.Parameters.stepLogGuid AS StepLogGuid, nl.Parameters.stepKey AS StepKey, nl.StartDateTime, nlCTE.EndDateTime, nlCTE.LogStatusKey, nlCTE.RowsAffected, nl.Parameters, nlCTE.Context, COALESCE(nle.Error, nl.Error) AS Error
      FROM nlCTE 
      JOIN silverprotected.notebooklog nl ON nlCTE.NotebookLogGuid = nl.NotebookLogGuid AND nl.LogStatusKey = 1
      LEFT JOIN silverprotected.notebooklog nle ON nlCTE.NotebookLogGuid = nle.NotebookLogGuid AND nle.LogStatusKey = 3
      """
    spark.sql(notebooklogviewsql)
    
    projectconfigurationsql = """
      CREATE VIEW IF NOT EXISTS silverprotected.projectconfiguration
      AS SELECT
        p.ProjectKey, p.ProjectName, p.CreatedDate
        ,s.SystemKey, s.SystemName, s.SystemSecretScope, s.SystemOrder, s.IsActive AS SystemIsActive, s.IsRestart AS SystemIsRestart, s.CreatedDate AS SystemCreatedDate, s.ModifiedDate AS SystemModifiedDate
        ,st.StageKey, st.StageName, st.IsActive AS StageIsActive, st.IsRestart AS StageIsRestart, st.StageOrder, st.NumberOfThreads, st.CreatedDate AS StageCreatedDate, st.ModifiedDate AS StageModifiedDate
        ,j.JobKey, j.JobName, j.JobOrder, j.IsActive AS JobIsActive, j.IsRestart AS JobIsRestart, j.CreatedDate AS JobCreatedDate, j.ModifiedDate AS JobModifiedDate
        ,stp.StepKey, stp.StepName, stp.StepOrder, stp.IsActive AS StepIsActive, stp.IsRestart AS StepIsRestart, stp.CreatedDate AS StepCreatedDate, stp.ModifiedDate AS StepModifiedDate
        FROM silverprotected.Project p
        JOIN silverprotected.System s ON p.ProjectKey = s.ProjectKey
        JOIN silverprotected.Stage st ON s.SystemKey = st.SystemKey
        JOIN silverprotected.Job j ON st.StageKey = j.StageKey
        JOIN silverprotected.Step stp ON j.JobKey = stp.JobKey
    """
    spark.sql(projectconfigurationsql)

    orchestrationruns = """
      CREATE OR REPLACE VIEW silverprotected.OrchestrationRuns
      AS
      SELECT
        CASE pl.LogStatusKey WHEN 1 THEN 'started' WHEN 2 THEN 'Completed' WHEN 3 THEN 'Failed' ELSE 'Unknown' END as LogStatus,
        pl.ProjectLogGuid, sha2(pl.ProjectName, 256) AS ProjectKey, pl.StartDateTime, pl.EndDateTime, pl.LogStatusKey, pl.Error
        ,pl.ProjectName
        ,pl.Parameters.dateToProcess AS DateToProcess
        ,pl.Parameters.timeout AS TimeoutSeconds
        ,pl.Parameters.threadPool AS ThreadPool
        ,pl.Parameters
        ,pl.Context
      FROM silverprotected.vprojectlog pl
      ORDER BY pl.StartDateTime DESC;
      """
    spark.sql(orchestrationruns)

    orchestrationrundetail = """
      CREATE OR REPLACE VIEW silverprotected.OrchestrationRunDetail
      AS
      SELECT
        CASE pl.LogStatusKey WHEN 1 THEN 'started' WHEN 2 THEN 'Completed' WHEN 3 THEN 'Failed' ELSE 'Unknown' END as ProjectLogStatus
        ,pl.ProjectLogGuid
        ,sha2(pl.ProjectName, 256) AS ProjectKey
        ,pl.ProjectName
        ,pl.StartDateTime AS ProjectStartTime
        ,pl.EndDateTime AS ProjectEndTime
        ,pl.Parameters AS ProjectParameters
        ,pl.Error AS ProjectErrors
        ,CASE sl.LogStatusKey WHEN 1 THEN 'started' WHEN 2 THEN 'Completed' WHEN 3 THEN 'Failed' ELSE 'Unknown' END as SystemLogStatus
        ,sl.SystemLogGuid
        ,sl.SystemKey
        ,sl.SystemName
        ,s.SystemOrder AS SystemOrder
        ,s.IsRestart AS SystemIsRestart
        ,sl.StartDateTime AS SystemStartTime
        ,CASE WHEN sl.StartDateTime IS NOT NULL THEN COALESCE(sl.EndDateTime, pl.EndDateTime) ELSE NULL END AS SystemEndTime
        ,sl.Parameters AS SystemParameters
        ,sl.Error AS SystemErrors
        ,CASE stl.LogStatusKey WHEN 1 THEN 'started' WHEN 2 THEN 'Completed' WHEN 3 THEN 'Failed' ELSE 'Unknown' END as StageLogStatus
        ,stl.StageLogGuid
        ,stl.StageKey
        ,stl.StageName
        ,st.StageOrder AS StageOrder
        ,st.IsRestart AS StageIsRestart
        ,stl.StartDateTime AS StageStartTime
        ,CASE WHEN stl.StartDateTime IS NOT NULL THEN COALESCE(stl.EndDateTime, sl.EndDateTime, pl.EndDateTime) ELSE NULL END AS StageEndTime
        ,stl.Parameters AS StageParameters
        ,stl.Error AS StageErrors
        ,CASE jl.LogStatusKey WHEN 1 THEN 'started' WHEN 2 THEN 'Completed' WHEN 3 THEN 'Failed' ELSE 'Unknown' END as JobStatusKey
        ,jl.JobLogGuid
        ,jl.JobKey
        ,jl.JobName
        ,j.JobOrder AS JobOrder
        ,j.IsRestart AS JobIsRestart
        ,jl.StartDateTime AS JobStartTime
        ,CASE WHEN jl.StartDateTime IS NOT NULL THEN COALESCE(jl.EndDateTime, stl.EndDateTime, sl.EndDateTime, pl.EndDateTime) ELSE NULL END AS JobEndTime
        ,jl.Parameters AS JobParameters
        ,jl.Error AS JobErrors
        ,CASE stpl.LogStatusKey WHEN 1 THEN 'started' WHEN 2 THEN 'Completed' WHEN 3 THEN 'Failed' ELSE 'Unknown' END as StepStatusKey
        ,stpl.StepLogGuid
        ,stpl.StepKey
        ,stpl.StepName
        ,stp.StepOrder AS StepOrder
        ,stp.IsRestart AS StepIsRestart
        ,stpl.StartDateTime AS StepStartTime
        ,CASE WHEN stpl.StartDateTime IS NOT NULL THEN COALESCE(stpl.EndDateTime, jl.EndDateTime, stl.EndDateTime, sl.EndDateTime, pl.EndDateTime) ELSE NULL END AS StepEndTime
        ,stpl.Parameters AS StepParameters
        ,stpl.Error AS StepErrors
        ,n.NotebookLogGuid
        ,n.StartDateTime AS NotebookStartTime
        ,CASE WHEN n.StartDateTime IS NOT NULL THEN COALESCE(n.EndDateTime, stpl.EndDateTime, jl.EndDateTime, stl.EndDateTime, sl.EndDateTime, pl.EndDateTime) ELSE NULL END AS NotebookEndTime
        ,n.Error.sourceName AS NotebookSourceName
        ,n.Error.errorDescription AS NotebookDetails
        ,n.Parameters AS NotebookParameters
        ,n.Error AS NotebookErrors
        ,n.RowsAffected AS RowsAffected
      FROM silverprotected.vprojectlog pl
      LEFT JOIN silverprotected.vsystemlog sl ON pl.ProjectLogGuid = sl.ProjectLogGuid
      LEFT JOIN silverprotected.vstagelog stl ON sl.SystemLogGuid = stl.SystemLogGuid
      LEFT JOIN silverprotected.vjoblog jl ON stl.StageLogGuid = jl.StageLogGuid 
      LEFT JOIN silverprotected.vsteplog stpl ON jl.JobLogGuid = stpl.JobLogGuid
      LEFT JOIN silverprotected.vnotebooklog n ON stpl.StepLogGuid = n.StepLogGuid
      LEFT JOIN silverprotected.project p ON pl.ProjectKey = p.ProjectKey
      LEFT JOIN silverprotected.system s ON sl.SystemKey = s.SystemKey
      LEFT JOIN silverprotected.stage st ON stl.StageKey = st.StageKey
      LEFT JOIN silverprotected.job j ON jl.JobKey = j.JobKey
      LEFT JOIN silverprotected.step stp ON stpl.StepKey = stp.StepKey    
    """

  def delete_orchestration_staging_tables(self, bronzeDataPath, project_name):
    dbutils = self.get_dbutils()
    t = ["project", "system", "stage", "job", "step", "parameter", "windowedextraction"]
    for table in t:
      sql = "DROP TABLE IF EXISTS bronze.{0}_{1}".format(table, project_name)
      spark.sql(sql)
      b = "{0}/{1}_{2}".format(bronzeDataPath, table, project_name)
      dbutils.fs.rm(b, True)
      
  def delete_orchestration_views(self):
    spark.sql("DROP VIEW IF EXISTS silverprotected.vprojectlog")
    spark.sql("DROP VIEW IF EXISTS silverprotected.vsystemlog")
    spark.sql("DROP VIEW IF EXISTS silverprotected.vstagelog")
    spark.sql("DROP VIEW IF EXISTS silverprotected.vjoblog")
    spark.sql("DROP VIEW IF EXISTS silverprotected.vsteplog")
    spark.sql("DROP VIEW IF EXISTS silverprotected.vnotebooklog")
    spark.sql("DROP VIEW IF EXISTS silverprotected.projectconfiguration")
          
  def delete_orchestration_schema(self):
    dbutils = self.get_dbutils()
    silverDataPath = self.SilverProtectedBasePath + "/orchestration"

    spark.sql("DROP TABLE IF EXISTS silverprotected.project")
    spark.sql("DROP TABLE IF EXISTS silverprotected.system")
    spark.sql("DROP TABLE IF EXISTS silverprotected.stage")
    spark.sql("DROP TABLE IF EXISTS silverprotected.job")
    spark.sql("DROP TABLE IF EXISTS silverprotected.step")
    spark.sql("DROP TABLE IF EXISTS silverprotected.windowedextraction")
    spark.sql("DROP TABLE IF EXISTS silverprotected.parameter")
    
    spark.sql("DROP TABLE IF EXISTS silverprotected.projectlog")
    spark.sql("DROP TABLE IF EXISTS silverprotected.systemlog")
    spark.sql("DROP TABLE IF EXISTS silverprotected.stagelog")
    spark.sql("DROP TABLE IF EXISTS silverprotected.joblog")
    spark.sql("DROP TABLE IF EXISTS silverprotected.steplog")
    spark.sql("DROP TABLE IF EXISTS silverprotected.notebooklog")
    
    self.delete_orchestration_views()

    dbutils.fs.rm(silverDataPath, True)

  def validate_orchestration_schema(self):
    def assert_exists(schema_name, table_name):
      fully_qualified_table_name = "{0}.{1}".format(schema_name, table_name)
      assert spark.table(fully_qualified_table_name) is not None
      
    schema_name = "silverprotected"
    
    tables = ["project", "system", "stage", "job", "step", "parameter", "windowedextraction", "projectlog", "systemlog", "stagelog", "joblog", "steplog", "notebooklog"]
    [assert_exists(schema_name, table_name) for table_name in tables]
    
    views = ["vProjectLog", "vSystemLog", "vStageLog", "vJobLog", "vStepLog", "vNotebookLog"]
    [assert_exists(schema_name, view_name) for view_name in views]


  def getLastRunStatusForProject(self, project_name):
    assert(project_name is not None or '', "project_name was not provided.")
    query = """
    SELECT LogStatusKey FROM silverprotected.projectlog WHERE Parameters.projectName == '{0}'
    ORDER BY StartDateTime DESC
    LIMIT 1
    """.format(project_name)
    last_status = spark.sql(query).first()['LogStatusKey']
    return last_status


  def hydrate_bigquery_bronze(self,
      project_name, 
      system_name,
      stage_name,
      job_name,
      system_secret_scope,
      bq_parent_project_name,
      bq_project_name,
      schema_name,
      table_name,
      system_is_active = True,
      stage_is_active = True,
      job_is_active = True,
      system_order = 10,
      stage_order = 10,
      job_order = 10,
      lower_bound = '',
      upper_bound = '',
      num_partitions = '56',
      use_windowed_extraction = False,
      windowing_column = '',
      windowed_extraction_begin_date = None,
      windowed_extraction_end_date = None,
      windowed_extraction_interval = None,
      windowed_extraction_process_latest_window_first = False,
      partition_column = '',
      bronze_zone_notebook_path = '../Data Engineering/Bronze Zone/Batch Big Query',
    ):
    
    validate_columns = [project_name, system_name, stage_name, job_name, system_secret_scope, schema_name, table_name, bq_parent_project_name, bq_project_name] 
    [self.validate_input(c) for c in validate_columns]
    
    bronze_parameters = {
      "bqParentProjectName": bq_parent_project_name,
      "bqProjectName": bq_project_name,
      "schemaName": schema_name,
      "tableName": table_name,
      "partitionColumn": partition_column,
      "lowerBound": lower_bound,
      "upperBound": upper_bound,
      "numPartitions": num_partitions,
      "externalSystem": system_secret_scope,
      "useWindowedExtraction": str(use_windowed_extraction),
      "windowingColumn": windowing_column
    }
    
    self.insert_parameters(project_name=project_name, system_name=system_name, system_secret_scope=system_secret_scope, system_order=system_order, system_is_active=system_is_active, system_is_restart=True, stage_name=stage_name, stage_is_active=stage_is_active, stage_is_restart=True, stage_order=stage_order, job_name=job_name, job_order=job_order, job_is_active=job_is_active, job_is_restart=True, step_name=bronze_zone_notebook_path, step_order=10, step_is_active=True, step_is_restart=True, parameters=bronze_parameters)
    
    if use_windowed_extraction == True:
      self.insert_windowed_extraction(project_name, job_name, bronze_zone_notebook_path, windowed_extraction_begin_date, windowed_extraction_end_date, windowed_extraction_interval, windowed_extraction_process_latest_window_first)
    

  def hydrate_bigquery_silver(self,
      project_name, 
      system_name,
      stage_name,
      job_name,
      system_secret_scope,
      bq_parent_project_name,
      bq_project_name,
      schema_name,
      table_name,
      adswerve_table_name,
      primary_key_columns= '', 
      system_is_active = True,
      stage_is_active = True,
      job_is_active = True,
      system_order = 10,
      stage_order = 10,
      job_order = 10,
      num_partitions = '56',
      explode_and_flatten = 'True',
      cleanse_column_names = 'True',
      timestamp_columns = '',
      timestamp_format = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'",
      encrypt_columns = '',
      load_type = 'Overwrite',
      destination = 'silvergeneral',
      silver_zone_partition_column = '',
      silver_zone_cluster_column = 'pk',
      silver_zone_cluster_buckets = '8',
      optimize_where = '',
      optimize_z_order_by = '',
      vacuum_retention_hours = '168',
      partition_column = '',
      silver_zone_notebook_path = '../Data Engineering/Silver Zone/Big Query Delta Load'
    ):
    
    validate_columns = [project_name, system_name, stage_name, job_name, system_secret_scope, schema_name, table_name, bq_parent_project_name, bq_project_name] 
    [self.validate_input(c) for c in validate_columns]
    
    silver_parameters = {
      "schemaName": schema_name,
      "tableName": table_name,
      "numPartitions": num_partitions,
      "primaryKeyColumns": primary_key_columns,
      "externalSystem": system_secret_scope,
      "partitionCol": silver_zone_partition_column,
      "clusterCol": silver_zone_cluster_column,
      "clusterBuckets": silver_zone_cluster_buckets,
      "optimizeWhere": optimize_where,
      "optimizeZOrderBy": optimize_z_order_by,
      "vacuumRetentionHours": vacuum_retention_hours,
      "loadType": load_type,
      "destination": destination,
      "explodeAndFlatten": explode_and_flatten,
      "cleanseColumnNames": cleanse_column_names,
      "timestampColumns": timestamp_columns,
      "timestampFormat": timestamp_format,
      "encryptColumns": encrypt_columns,
      "adswerveTableName": adswerve_table_name
    }
    
    self.insert_parameters(project_name=project_name, system_name=system_name, system_secret_scope=system_secret_scope, system_order=system_order, system_is_active=system_is_active, system_is_restart=True, stage_name=stage_name, stage_is_active=stage_is_active, stage_is_restart=True, stage_order=stage_order, job_name=job_name, job_order=job_order, job_is_active=job_is_active, job_is_restart=True, step_name=silver_zone_notebook_path, step_order=20, step_is_active=True, step_is_restart=True, parameters=silver_parameters)
  

  def hydrate_bigquery_gold(self,
      project_name, 
      system_name,
      stage_name,
      job_name,
      system_secret_scope,
      schema_name,
      source_table_name,
      source_destination,
      destinatation_table_name,
      adswerve_table_names,
      purpose='general',
      partition_columns = '',
      cluster_columns = '',
      cluster_buckets = 50,
      merge_schema= False,
      primary_key_columns= '', 
      system_is_active = True,
      stage_is_active = True,
      job_is_active = True,
      system_order = 10,
      stage_order = 30,
      job_order = 10,
      num_partitions = '56',
      load_type = 'Overwrite',
      target_destination = 'goldgeneral',
      gold_zone_notebook_path = '../Data Engineering/Gold Zone/Big Query Gold Load',
    ):
    
    validate_columns = [project_name, system_name, stage_name, job_name, system_secret_scope, schema_name, source_table_name, source_destination, destinatation_table_name, target_destination, adswerve_table_names] 
    [self.validate_input(c) for c in validate_columns]
    
    gold_parameters = {
      "sourceTableName": source_table_name,
      "adswerveTableName": adswerve_table_names,
      "clusterBuckets": cluster_buckets,
      "clusterCol": cluster_columns,
      "targetDestination": target_destination,
      "destinationTableName": destinatation_table_name,
      "loadType": load_type,
      "mergeSchema": merge_schema,
      "partitionCol": partition_columns,
      "primaryKeyColumns": primary_key_columns,
      "purpose": purpose,
      "sourceDestination": source_destination,
      "schemaName": schema_name,
      "sourcetableName": source_table_name,
      "numPartitions": num_partitions,
      "primaryKeyColumns": primary_key_columns,
      "externalSystem": system_secret_scope,
    }
    
    self.insert_parameters(project_name=project_name, system_name=system_name, system_secret_scope=system_secret_scope, system_order=system_order, system_is_active=system_is_active, system_is_restart=True, stage_name=stage_name, stage_is_active=stage_is_active, stage_is_restart=True, stage_order=stage_order, job_name=job_name, job_order=job_order, job_is_active=job_is_active, job_is_restart=True, step_name=gold_zone_notebook_path, step_order=10, step_is_active=True, step_is_restart=True, parameters=gold_parameters)

  
