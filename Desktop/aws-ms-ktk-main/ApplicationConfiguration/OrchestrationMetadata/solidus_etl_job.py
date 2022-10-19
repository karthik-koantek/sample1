
def compose_client_id_detection_query(solidus_client: str, exec_time_millis: float, days_back: int, database_name: str) -> str:
    return f"""
        SELECT client_id, ARRAY_AGG(DISTINCT accounts) accounts
        FROM (
          SELECT client_id, account accounts, 'private orders' data_type
          FROM "{database_name}"."MS"."PRIVATE_ORDERS"
          WHERE solidus_client = '{solidus_client}'
            AND TO_DATE(transact_time) >= DATEADD(DAY,-1 * {days_back}, TO_TIMESTAMP_NTZ({float(exec_time_millis) / 1000}))
            
        UNION ALL
            
          SELECT client_id, internal_account_id accounts, 'transactions' data_type
          FROM "{database_name}"."TM"."TRANSACTIONS"
          WHERE solidus_client = '{solidus_client}'
            AND TO_DATE(transact_time) >= DATEADD(DAY,-1 * {days_back}, TO_TIMESTAMP_NTZ({float(exec_time_millis) / 1000}))
        ) 
        GROUP BY client_id
    """

def hydrate(project_name):
    import ktk
    from pyspark.sql.functions import col, create_map, lit, col, crc32, concat
    from itertools import chain
    import json
  
    onb = ktk.OrchestrationDeltaNotebook()
    onb.create_orchestration_staging_tables(project_name)
    
  #teardown
               
  
  #hydrate batch sql (empty)
    system_name = 'New_client_detection'
    stage_name = 'New_client_detection'
    system_secret_scope = ''
    tableName= "ClientAccount"
    schemaName= "Client"
    destination= "silvergeneral"
    externalSystem= "internal"
    solidus_client="TEST_CLIENT"




    onb.hydrate_batch_snowflake(
	 Query= compose_client_id_detection_query(solidus_client= solidus_client, exec_time_millis = 1649073849000, days_back = 3, database_name = 'SOLIDUS_PERFORM'),
  sfSchema="",
  sfTable="",  
	 sfUrl = "https://ua97033.us-east-1.snowflakecomputing.com/",
	 sfDatabase = "SOLIDUS_PERFORM",
	 delta_schema_name =schemaName,
	 sfWarehouse = "MULTI_C_PERFORM_LOAD",
	 Scope = "Soliduslabs_RND",
  project_name=project_name, 
  system_name=system_name,
  stage_name=stage_name,
  job_name="ETL",
  delta_table_name=tableName,
  primary_key_columns="CLIENT_ID",
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
  load_type = 'Overwrite',
  destination = destination,
  silver_zone_partition_column = '',
  silver_zone_cluster_column = 'pk',
  silver_zone_cluster_buckets = '8',
  optimize_where = '',
  optimize_z_order_by = '',
  vacuum_retention_hours = '168',
  bronze_zone_notebook_path = '../Data Engineering/Bronze Zone/Batch Snowflake with ktk',
  silver_zone_notebook_path = '../Data Engineering/Silver Zone/Delta Load',
  externalSystem=externalSystem)
    onb.hydrate_generic_notebook(project_name=project_name,system_name=system_name,stage_name=stage_name,job_name='New_client_detection',system_secret_scope='internal',parameters =  {
  "job_args": "\"exec_time_millis=1649073849000\", \"days_back=3\", \"ucv_batch_size=1000\", \"auth_service_master_password=C49Agjf9DN\", \"auth_service_url=https://auth.perform.soliduslabs.app\", \"client_store_service_url=https://client-store.perform.soliduslabs.app\"",
  "job_exec_name": "TEST_CLIENT_rmf",
  "job_name": "new_client_detection_v1_etl",
  "solidus_client": solidus_client,
  "env": "perform",
  "tableName": tableName,
  "schemaName": schemaName,
  "destination": destination,
  "externalSystem": externalSystem
},notebook_path = '../ExternalProjects/Soliduslabs/src/jobs/New_client_detection_v1_etl',system_is_active = True,stage_is_active = True,job_is_active = True,system_order = 10,stage_order = 10,job_order = 20)
    
    onb.load_orchestration_tables(project_name="solidus_etl_job")
     
        