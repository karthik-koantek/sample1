# Databricks notebook source
# MAGIC %md
# MAGIC #### Orchestration Notebook

# COMMAND ----------

import ktk
from pyspark.sql.functions import col, create_map, lit, col, crc32, concat
from itertools import chain
import json

onb = ktk.OrchestrationNotebook()
onb.displayAttributes()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Drop Tables

# COMMAND ----------

silverDataPath = onb.SilverProtectedBasePath + "/orchestrationDev"
spark.sql("DROP TABLE IF EXISTS silverprotected.project")
spark.sql("DROP TABLE IF EXISTS silverprotected.system")
spark.sql("DROP TABLE IF EXISTS silverprotected.stage")
spark.sql("DROP TABLE IF EXISTS silverprotected.job")
spark.sql("DROP TABLE IF EXISTS silverprotected.step")
spark.sql("DROP TABLE IF EXISTS silverprotected.parameter")
dbutils.fs.rm(silverDataPath, True)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create Tables

# COMMAND ----------

sql = """
CREATE TABLE IF NOT EXISTS silverprotected.project
(
  ProjectKey BIGINT NOT NULL GENERATED ALWAYS AS (crc32(ProjectName)) COMMENT 'Project Identifier Key as crc32'
 ,ProjectName STRING NOT NULL COMMENT 'Project Name'
 ,CreatedDate TIMESTAMP NOT NULL COMMENT 'metadata generated timestamp' 
)
USING delta
LOCATION '{0}/project'
PARTITIONED BY (ProjectKey)
""".format(silverDataPath)
spark.sql(sql)

# COMMAND ----------

sql = """
CREATE TABLE IF NOT EXISTS silverprotected.system
(
  SystemKey BIGINT NOT NULL GENERATED ALWAYS AS (crc32(SystemName)) COMMENT 'System Identifier Key as crc32'
 ,SystemName STRING NOT NULL COMMENT 'System Name'
 ,SystemSecretScope STRING COMMENT 'Optional secret scope for this system which contains connection info etc.'
 ,ProjectKey BIGINT NOT NULL COMMENT 'Foreign Key to silverprotected.project'
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
spark.sql(sql)

# COMMAND ----------

sql = """
CREATE TABLE IF NOT EXISTS silverprotected.stage
(
  StageKey BIGINT NOT NULL GENERATED ALWAYS AS (crc32(StageName)) COMMENT 'Stage Identifier Key as crc32'
 ,StageName STRING NOT NULL COMMENT 'Stage Name'
 ,SystemKey BIGINT NOT NULL COMMENT 'Foreign Key to silverprotected.system'
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
spark.sql(sql)

# COMMAND ----------

sql = """
CREATE TABLE IF NOT EXISTS silverprotected.job
(
  JobKey BIGINT NOT NULL GENERATED ALWAYS AS (crc32(JobName)) COMMENT 'Job Identifier Key as crc32'
 ,JobName STRING NOT NULL COMMENT 'Job Name'
 ,StageKey BIGINT NOT NULL COMMENT 'Foreign Key to silverprotected.stage'
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
spark.sql(sql)

# COMMAND ----------

sql = """
CREATE TABLE IF NOT EXISTS silverprotected.step
(
  StepKey BIGINT NOT NULL GENERATED ALWAYS AS (crc32(concat(cast(JobKey as STRING),StepName))) COMMENT 'Step Identifier Key as crc32'
 ,StepName STRING NOT NULL COMMENT 'Step Name'
 ,JobKey BIGINT NOT NULL COMMENT 'Foreign Key to silverprotected.job'
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
spark.sql(sql)

# COMMAND ----------

sql = """
CREATE TABLE IF NOT EXISTS silverprotected.parameter
(
   StepKey BIGINT NOT NULL COMMENT 'Foreign Key to silverprotected.step'
  ,Parameters MAP<STRING, STRING> NOT NULL COMMENT 'Parameter Map for this step'
  ,CreatedDate TIMESTAMP NOT NULL COMMENT 'metadata generated timestamp'
  ,ModifiedDate TIMESTAMP COMMENT 'metadata updated timestamp'
)
USING delta
LOCATION '{0}/parameter'
PARTITIONED BY (StepKey)
""".format(silverDataPath)
spark.sql(sql)

# COMMAND ----------

silverDataPath = onb.SilverProtectedBasePath + "/orchestrationDev"

# COMMAND ----------

sql = """
CREATE TABLE IF NOT EXISTS silverprotected.windowedextraction
(
   StepKey BIGINT NOT NULL COMMENT 'Foreign Key to silverprotected.parameter and silverprotected.step'
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
spark.sql(sql)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create Functions

# COMMAND ----------

def create_map_from_dictionary(dictionary_or_string):
  if isinstance(dictionary_or_string, str):
    dictionary_or_string = json.loads(dictionary_or_string)
  return create_map([lit(x) for x in chain(*dictionary_or_string.items())])

# COMMAND ----------

def validate_input(parameter):
  if parameter == "" or parameter is None:
    raise ValueError("The value for parameter is not supported.")

# COMMAND ----------

def create_parameters_df(parameters, step_name, job_name):
  parameters_dict = json.dumps(parameters)
  parameters_map = create_map_from_dictionary(parameters_dict)
  df_list = []
  parameters_tuple = [(1, step_name)]
  df_list.append(spark.createDataFrame(parameters_tuple))
  df_list.append(df_list[-1].withColumnRenamed("_2", "step_name"))
  df_list.append(df_list[-1].withColumn("job_key", crc32(lit(job_name)).cast("string")))
  df_list.append(df_list[-1].withColumn("step_key", crc32(concat(df_list[-1].job_key,lit(step_name)))))
  df_list.append(df_list[-1].drop(col("_1")))
  df_list.append(df_list[-1].drop(col("step_name")))
  df_list.append(df_list[-1].drop(col("job_key")))
  df_list.append(df_list[-1].withColumn("parameters", parameters_map))
  return df_list[-1]

# COMMAND ----------

def insert_project(
  project_name
  ):
  
  sql = """
  CREATE OR REPLACE TEMPORARY VIEW src
  AS
  SELECT 
    '{0}' as project_name
  """.format(project_name)
  spark.sql(sql)
    
  spark.sql("""
    MERGE INTO silverprotected.project tgt
    USING src ON tgt.ProjectName=src.project_name
    WHEN NOT MATCHED THEN 
      INSERT (ProjectName, CreatedDate) 
      VALUES (src.project_name, current_timestamp())
  """)

# COMMAND ----------

def insert_system(
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
    crc32('{1}') as project_key,
    {2} as system_order,
    {3} as system_is_active,
    {4} as system_is_restart,
    '{5}' as system_secret_scope
  """.format(system_name, project_name, system_order, system_is_active, system_is_restart, system_secret_scope)
  spark.sql(sql)
    
  spark.sql("""
    MERGE INTO silverprotected.system tgt
    USING src ON tgt.SystemName=src.system_name
    WHEN MATCHED THEN UPDATE SET 
      tgt.SystemOrder=src.system_order, 
      tgt.IsActive=src.system_is_active, 
      tgt.IsRestart=src.system_is_restart, 
      tgt.ModifiedDate=current_timestamp()
    WHEN NOT MATCHED THEN 
      INSERT (SystemName, ProjectKey, SystemOrder, IsActive, IsRestart, CreatedDate) 
      VALUES (src.system_name, src.project_key, src.system_order, src.system_is_active, src.system_is_restart, current_timestamp())
  """)
  
  insert_project(project_name=project_name)

# COMMAND ----------

def insert_stage(
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
    crc32('{1}') as system_key,
    {2} as stage_order,
    {3} as stage_is_active,
    {4} as stage_is_restart
  """.format(stage_name, system_name, stage_order, stage_is_active, stage_is_restart)
  spark.sql(sql)
    
  spark.sql("""
    MERGE INTO silverprotected.stage tgt
    USING src ON tgt.StageName=src.stage_name
    WHEN MATCHED THEN UPDATE SET 
      tgt.StageOrder=src.stage_order, 
      tgt.IsActive=src.stage_is_active, 
      tgt.IsRestart=src.stage_is_restart, 
      tgt.ModifiedDate=current_timestamp()
    WHEN NOT MATCHED THEN 
      INSERT (StageName, SystemKey, StageOrder, IsActive, IsRestart, CreatedDate) 
      VALUES (src.stage_name, src.system_key, src.stage_order, src.stage_is_active, src.stage_is_restart, current_timestamp())
  """)
  
  insert_system(project_name=project_name, system_name=system_name, system_secret_scope=system_secret_scope, system_order=system_order, system_is_active=system_is_active, system_is_restart=system_is_restart)

# COMMAND ----------

def insert_job(
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
    crc32('{1}') as stage_key,
    {2} as job_order,
    {3} as job_is_active,
    {4} as job_is_restart
  """.format(job_name, stage_name, job_order, job_is_active, job_is_restart)
  spark.sql(sql)
    
  spark.sql("""
    MERGE INTO silverprotected.job tgt
    USING src ON tgt.JobName=src.job_name
    WHEN MATCHED THEN UPDATE SET 
      tgt.JobOrder=src.job_order, 
      tgt.IsActive=src.job_is_active, 
      tgt.IsRestart=src.job_is_restart, 
      tgt.ModifiedDate=current_timestamp()
    WHEN NOT MATCHED THEN 
      INSERT (JobName, StageKey, JobOrder, IsActive, IsRestart, CreatedDate) 
      VALUES (src.job_name, src.stage_key, src.job_order, src.job_is_active, src.job_is_restart, current_timestamp())
  """)
  
  insert_stage(project_name=project_name, system_name=system_name, system_secret_scope=system_secret_scope, system_order=system_order, system_is_active=system_is_active, system_is_restart=system_is_restart, stage_name=stage_name, stage_is_active=stage_is_active, stage_is_restart=stage_is_restart, stage_order=stage_order)

# COMMAND ----------

def insert_step(
  project_name,
  system_name,
  stage_name,
  job_name,
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
    '{0}' as step_name,
    cast(crc32('{1}') as string) as job_key,
    {2} as step_order,
    {3} as step_is_active,
    {4} as step_is_restart
  """.format(step_name, job_name, step_order, step_is_active, step_is_restart)
  spark.sql(sql)
      
  spark.sql("""
    MERGE INTO silverprotected.step tgt
    USING src ON tgt.StepKey=crc32(concat(cast(src.job_key as string),src.step_name))
    WHEN MATCHED THEN UPDATE SET 
      tgt.StepOrder=src.step_order, 
      tgt.IsActive=src.step_is_active, 
      tgt.IsRestart=src.step_is_restart, 
      tgt.ModifiedDate=current_timestamp()
    WHEN NOT MATCHED THEN 
      INSERT (StepName, JobKey, StepOrder, IsActive, IsRestart, CreatedDate) 
      VALUES (src.step_name, src.job_key, src.step_order, src.step_is_active, src.step_is_restart, current_timestamp())
  """)
  
  insert_job(project_name=project_name, system_name=system_name, system_secret_scope=system_secret_scope, system_order=system_order, system_is_active=system_is_active, system_is_restart=system_is_restart, stage_name=stage_name, stage_is_active=stage_is_active, stage_is_restart=stage_is_restart, stage_order=stage_order, job_name=job_name, job_order=job_order, job_is_active=job_is_active, job_is_restart=job_is_restart)

# COMMAND ----------

def generate_numbers(numbers):
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

# COMMAND ----------

def generate_dates(from_date, numbers=10000):
  numbers = generate_numbers(numbers)
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

# COMMAND ----------

def insert_windowed_extraction (job_name, step_name, windowed_extraction_begin_date, windowed_extraction_end_date, windowed_extraction_interval='Y', windowed_extraction_process_latest_window_first=True, delete_existing=True):
  
  from pyspark.sql.functions import date_add, col, min, max, crc32, concat, lit, row_number
  from pyspark.sql.window import Window

  if delete_existing == True:
    delete_sql = "DELETE FROM silverprotected.windowedextraction WHERE StepKey == cast(crc32(concat(cast(crc32('{0}') as string), '{1}')) as bigint)".format(job_name, step_name)
    spark.sql(delete_sql)
               
  dp = []
  dp.append(generate_dates(windowed_extraction_begin_date, numbers=10000))
  
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

  dp.append(dp[-1].withColumn("JobKey", crc32(lit(job_name))))
  dp.append(dp[-1].withColumn("StepKey", crc32(concat(col("JobKey"), lit(step_name)))))
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
  INSERT INTO silverprotected.windowedextraction (StepKey, WindowStart, WindowEnd, WindowOrder, IsActive, ExtractionTimestamp, QueryZoneTimestamp)
  SELECT StepKey, WindowStart, WindowEnd, WindowOrder, IsActive, NULL, NULL FROM dp
  """
  spark.sql(sql)

# COMMAND ----------

def insert_parameters(
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

  parameters_df = create_parameters_df(parameters, step_name, job_name)
  parameters_df.createOrReplaceTempView("parameters_df")
  spark.sql("""
    MERGE INTO silverprotected.parameter tgt
    USING parameters_df src ON tgt.StepKey=src.step_key
    WHEN MATCHED THEN UPDATE SET 
      tgt.Parameters = src.parameters, 
      tgt.ModifiedDate = current_timestamp()
    WHEN NOT MATCHED THEN 
    INSERT (StepKey, Parameters, CreatedDate) 
    VALUES (src.step_key, src.parameters, current_timestamp())
  """)
  
  insert_step(project_name=project_name, system_name=system_name, system_secret_scope=system_secret_scope, system_order=system_order, system_is_active=system_is_active, system_is_restart=system_is_restart, stage_name=stage_name, stage_is_active=stage_is_active, stage_is_restart=stage_is_restart, stage_order=stage_order, job_name=job_name, job_order=job_order, job_is_active=job_is_active, job_is_restart=job_is_restart, step_name=step_name, step_order=step_order, step_is_active=step_is_active, step_is_restart=step_is_restart)

# COMMAND ----------

def hydrate_batch_sql(
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
    timestamp_format = 'yyyy-MM-dd''T''HH:mm:ss.SSS''Z''',
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
  [validate_input(c) for c in validate_columns]
  
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
  
  insert_parameters(project_name=project_name, system_name=system_name, system_secret_scope=system_secret_scope, system_order=system_order, system_is_active=system_is_active, system_is_restart=True, stage_name=stage_name, stage_is_active=stage_is_active, stage_is_restart=True, stage_order=stage_order, job_name=job_name, job_order=job_order, job_is_active=job_is_active, job_is_restart=True, step_name=bronze_zone_notebook_path, step_order=10, step_is_active=True, step_is_restart=True, parameters=bronze_parameters)
  
  if use_windowed_extraction == True:
    insert_windowed_extraction(job_name, step_name, windowed_extraction_begin_date, windowed_extraction_end_date, windowed_extraction_interval, windowed_extraction_process_latest_window_first)
  
  insert_parameters(project_name=project_name, system_name=system_name, system_secret_scope=system_secret_scope, system_order=system_order, system_is_active=system_is_active, system_is_restart=True, stage_name=stage_name, stage_is_active=stage_is_active, stage_is_restart=True, stage_order=stage_order, job_name=job_name, job_order=job_order, job_is_active=job_is_active, job_is_restart=True, step_name=silver_zone_notebook_path, step_order=20, step_is_active=True, step_is_restart=True, parameters=silver_parameters)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Hydration

# COMMAND ----------

project_name = 'UAT_SQL'
system_name = 'UAT_SQL_adventureworkslt_ingest'
stage_name = 'UAT_SQL_adventureworkslt_ingest_daily'
system_secret_scope = 'adventureworkslt'
system_order = 20
stage_order = 10
job_order = 10

hydrate_batch_sql(project_name=project_name,system_name=system_name,stage_name=stage_name,job_name = 'UAT_SQL_adventureworkslt_SalesLT_SalesOrderHeader',system_secret_scope=system_secret_scope,system_order=system_order,stage_order=stage_order,job_order=job_order,schema_name='SalesLT',table_name='SalesOrderHeader',primary_key_columns='SalesOrderID',timestamp_columns='DueDate,ModifiedDate,OrderDate,ShipDate')

hydrate_batch_sql(project_name=project_name,system_name=system_name,stage_name=stage_name,job_name = 'UAT_SQL_adventureworkslt_dbo_BuildVersion',system_secret_scope=system_secret_scope,system_order=system_order,stage_order=stage_order,job_order=job_order,schema_name='dbo',table_name='BuildVersion',primary_key_columns='SystemInformationID',timestamp_columns='DueDate,ModifiedDate,OrderDate,ShipDate',load_type='Append',pushdown_query = 'SELECT * FROM dbo.BuildVersion WHERE 1=1 OR ModifiedDate >= DATEADD(DAY, -30, SYSDATETIME())')

hydrate_batch_sql(project_name=project_name,system_name=system_name,stage_name=stage_name,job_name = 'UAT_SQL_adventureworkslt_SalesLT_ProductModel',system_secret_scope=system_secret_scope,system_order=system_order,stage_order=stage_order,job_order=job_order,schema_name='SalesLT',table_name='ProductModel',primary_key_columns='ProductModelID',timestamp_columns='ModifiedDate', use_windowed_extraction = True, windowing_column = 'ModifiedDate', windowed_extraction_begin_date = '2002-01-01', windowed_extraction_end_date '2021-01-01', windowed_extraction_interval = 'Y', windowed_extraction_process_latest_window_first = False)

hydrate_batch_sql(project_name=project_name,system_name=system_name,stage_name=stage_name,job_name = 'UAT_SQL_adventureworkslt_SalesLT_Address',system_secret_scope=system_secret_scope,system_order=system_order,stage_order=stage_order,job_order=job_order,schema_name='SalesLT',table_name='Address',primary_key_columns='AddressID',timestamp_columns='ModifiedDate', silver_zone_partition_column = 'StateProvince', encrypt_columns = 'AddressLine1,AddressLine2', load_type = 'Overwrite', destination = 'silverprotected')

spark.sql("UPDATE silverprotected.stage SET NumberOfThreads = 4 WHERE StageName = '{0}'".format(stage_name))

# COMMAND ----------

stage_name = 'UAT_SQL_adventureworkslt_ingest_daily_rerun'
stage_order = 20

hydrate_batch_sql(project_name=project_name,system_name=system_name,stage_name=stage_name,job_name = 'UAT_SQL_adventureworkslt_SalesLT_SalesOrderHeader_rerun',system_secret_scope=system_secret_scope,system_order=system_order,stage_order=stage_order,job_order=job_order,schema_name='SalesLT',table_name='SalesOrderHeader',primary_key_columns='SalesOrderID',timestamp_columns='DueDate,ModifiedDate,OrderDate,ShipDate')

hydrate_batch_sql(project_name=project_name,system_name=system_name,stage_name=stage_name,job_name = 'UAT_SQL_adventureworkslt_dbo_BuildVersion_rerun',system_secret_scope=system_secret_scope,system_order=system_order,stage_order=stage_order,job_order=job_order,schema_name='dbo',table_name='BuildVersion',primary_key_columns='SystemInformationID',timestamp_columns='DueDate,ModifiedDate,OrderDate,ShipDate',load_type='Append',pushdown_query = 'SELECT * FROM dbo.BuildVersion WHERE 1=1 OR ModifiedDate >= DATEADD(DAY, -30, SYSDATETIME())')

hydrate_batch_sql(project_name=project_name,system_name=system_name,stage_name=stage_name,job_name = 'UAT_SQL_adventureworkslt_SalesLT_Address_rerun',system_secret_scope=system_secret_scope,system_order=system_order,stage_order=stage_order,job_order=job_order,schema_name='SalesLT',table_name='Address',primary_key_columns='AddressID',timestamp_columns='ModifiedDate', silver_zone_partition_column = 'StateProvince', encrypt_columns = 'AddressLine1,AddressLine2', load_type = 'Overwrite', destination = 'silverprotected')

spark.sql("UPDATE silverprotected.stage SET NumberOfThreads = 4 WHERE StageName = '{0}'".format(stage_name))

# COMMAND ----------

system_name = 'UAT_SQL_adventureworkslt_ingest_second_system'
stage_name = 'UAT_SQL_adventureworkslt_ingest__second_system_daily'
system_secret_scope = 'adventureworkslt'
system_order = 30
stage_order = 10
job_order = 10

hydrate_batch_sql(project_name=project_name,system_name=system_name,stage_name=stage_name,job_name = 'UAT_SQL_adventureworkslt_SalesLT_SalesOrderHeader_rerun2',system_secret_scope=system_secret_scope,system_order=system_order,stage_order=stage_order,job_order=job_order,schema_name='SalesLT',table_name='SalesOrderHeader',primary_key_columns='SalesOrderID',timestamp_columns='DueDate,ModifiedDate,OrderDate,ShipDate')

hydrate_batch_sql(project_name=project_name,system_name=system_name,stage_name=stage_name,job_name = 'UAT_SQL_adventureworkslt_dbo_BuildVersion_rerun2',system_secret_scope=system_secret_scope,system_order=system_order,stage_order=stage_order,job_order=job_order,schema_name='dbo',table_name='BuildVersion',primary_key_columns='SystemInformationID',timestamp_columns='DueDate,ModifiedDate,OrderDate,ShipDate',load_type='Append',pushdown_query = 'SELECT * FROM dbo.BuildVersion WHERE 1=1 OR ModifiedDate >= DATEADD(DAY, -30, SYSDATETIME())')

hydrate_batch_sql(project_name=project_name,system_name=system_name,stage_name=stage_name,job_name = 'UAT_SQL_adventureworkslt_SalesLT_Address_rerun2',system_secret_scope=system_secret_scope,system_order=system_order,stage_order=stage_order,job_order=job_order,schema_name='SalesLT',table_name='Address',primary_key_columns='AddressID',timestamp_columns='ModifiedDate', silver_zone_partition_column = 'StateProvince', encrypt_columns = 'AddressLine1,AddressLine2', load_type = 'Overwrite', destination = 'silverprotected')

spark.sql("UPDATE silverprotected.stage SET NumberOfThreads = 4 WHERE StageName = '{0}'".format(stage_name))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Orchestration Using Delta Tables

# COMMAND ----------

def get_project_json(project_name):
  sql = """
  SELECT
   p.ProjectKey, p.ProjectName
  ,s.SystemKey, s.SystemName, s.SystemSecretScope, s.SystemOrder
  ,st.StageKey, st.StageName, st.StageOrder, st.NumberOfThreads
  ,j.JobKey, j.JobName, j.JobOrder
  ,stp.StepKey, stp.StepName, stp.StepOrder
  ,pm.parameters
  FROM silverprotected.Project p
  JOIN silverprotected.System s ON p.ProjectKey = s.ProjectKey
  JOIN silverprotected.Stage st ON s.SystemKey = st.SystemKey
  JOIN silverprotected.Job j ON st.StageKey = j.StageKey
  JOIN silverprotected.Step stp ON j.JobKey = stp.JobKey
  JOIN silverprotected.Parameter pm ON stp.StepKey = pm.StepKey
  WHERE p.ProjectName = '{0}'
  AND s.IsActive = True AND st.IsActive = True AND j.IsActive = True AND stp.IsActive = True
  AND s.IsRestart = True AND st.IsRestart = True AND j.IsRestart = True AND stp.IsRestart = True
  """.format(project_name)
  return spark.sql(sql)
  

# COMMAND ----------

results = get_project_json("UAT_SQL")
display(results)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Review

# COMMAND ----------

sql = """
SELECT
 p.ProjectKey, p.ProjectName
,s.SystemKey, s.SystemName, s.SystemSecretScope, s.SystemOrder
,st.StageKey, st.StageName, st.StageOrder, st.NumberOfThreads
,j.JobKey, j.JobName, j.JobOrder
,stp.StepKey, stp.StepName, stp.StepOrder
,pm.parameters
FROM silverprotected.Project p
JOIN silverprotected.System s ON p.ProjectKey = s.ProjectKey
JOIN silverprotected.Stage st ON s.SystemKey = st.SystemKey
JOIN silverprotected.Job j ON st.StageKey = j.StageKey
JOIN silverprotected.Step stp ON j.JobKey = stp.JobKey
JOIN silverprotected.Parameter pm ON stp.StepKey = pm.StepKey
WHERE p.ProjectName = '{0}'
AND s.IsActive = True AND st.IsActive = True AND j.IsActive = True AND stp.IsActive = True
AND s.IsRestart = True AND st.IsRestart = True AND j.IsRestart = True AND stp.IsRestart = True
""".format("UAT_SQL")
df = spark.sql(sql)
df.createOrReplaceTempView("df")

# COMMAND ----------

sys = spark.sql("SELECT * FROM silverprotected.system WHERE IsActive == True AND IsRestart == True AND ProjectKey = 3293071135 ORDER BY SystemOrder")
display(sys)

# COMMAND ----------

stg = spark.sql("SELECT * FROM silverprotected.stage WHERE SystemKey = 2024537708 ORDER BY StageOrder")
display(stg)

# COMMAND ----------

job = spark.sql("SELECT * FROM silverprotected.job WHERE StageKey = 1277149447 ORDER BY JobOrder")
display(job)

# COMMAND ----------

step = spark.sql("SELECT * FROM silverprotected.step WHERE JobKey = 4180320511 ORDER BY StepOrder")
display(step)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM silverprotected.parameter WHERE StepKey = 3985156606

# COMMAND ----------

args = [row.asDict() for row in spark.sql("SELECT Parameters FROM silverprotected.parameter WHERE StepKey = 3985156606").collect()][0]["Parameters"]
args

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE silverprotected.system
# MAGIC SET IsRestart = 1
# MAGIC WHERE ProjectKey = 3293071135

# COMMAND ----------

def get_project (projectName):
  query = """
    SELECT s.*
    FROM silverprotected.system s
    JOIN silverprotected.project p
    WHERE s.IsActive == True 
    AND s.IsRestart == True 
    AND p.ProjectName = '{0}' 
    ORDER BY s.SystemOrder
  """.format(projectName)
  return [row.asDict() for row in spark.sql(query).collect()]

# COMMAND ----------

systems = get_project("UAT_SQL")

# COMMAND ----------

systems
