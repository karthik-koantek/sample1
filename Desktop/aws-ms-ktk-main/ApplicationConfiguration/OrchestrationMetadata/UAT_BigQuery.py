def hydrate(project_name):
  import ktk
  from pyspark.sql.functions import col, create_map, lit, col, crc32, concat
  import datetime
  from itertools import chain
  import json

  onb = ktk.OrchestrationDeltaNotebook()
  onb.create_orchestration_staging_tables(project_name)
  
  #teardown
#   onb.hydrate_generic_notebook(project_name=project_name,system_name='UAT_BigQuery_Teardown',stage_name='UAT_BigQuery_Teardown_daily',job_name='UAT_BigQuery_Teardown',system_secret_scope='external',parameters = {},notebook_path = '../Data Quality Rules Engine/UAT_BigQuery_Teardown',system_is_active = True,stage_is_active = True,job_is_active = True,system_order = 10,stage_order = 10,job_order = 10)
  
  #hydrate batch sql (empty)
  system_name = 'Arena_ingest'
  stage_name = 'Arena Ingest Bronze'
  system_secret_scope = 'arena_big_query'
  system_order = 10
  stage_order = 10
  job_order = 10
  bq_parent_project_name = 'thestreet-analytics'
  bq_project_name = 'thestreet-analytics'
  dataset_id = [13210892, 184077347, 67977450]
  today = datetime.date.today() - datetime.timedelta(days=2)
  today = str(today).replace('-','')
  table = f"ga_sessions_{today}"

  onb.hydrate_bigquery_bronze(project_name=project_name,system_name=system_name,stage_name=stage_name,job_name = f'Arena Ingest Bronze Table {dataset_id[0]}',system_secret_scope=system_secret_scope,system_order=system_order,stage_order=stage_order,job_order=job_order,bq_parent_project_name=bq_parent_project_name, bq_project_name=bq_project_name, schema_name=dataset_id[0],table_name=table,num_partitions=200) 
  onb.hydrate_bigquery_bronze(project_name=project_name,system_name=system_name,stage_name=stage_name,job_name = f'Arena Ingest Bronze Table {dataset_id[1]}',system_secret_scope=system_secret_scope,system_order=system_order,stage_order=stage_order,job_order=job_order,bq_parent_project_name=bq_parent_project_name, bq_project_name=bq_project_name, schema_name=dataset_id[1],table_name=table,num_partitions=900)
  onb.hydrate_bigquery_bronze(project_name=project_name,system_name=system_name,stage_name=stage_name,job_name = f'Arena Ingest Bronze Table {dataset_id[2]}',system_secret_scope=system_secret_scope,system_order=system_order,stage_order=stage_order,job_order=job_order,bq_parent_project_name=bq_parent_project_name, bq_project_name=bq_project_name, schema_name=dataset_id[2],table_name=table,num_partitions=8)
  onb.update_stage_threads(project_name=project_name, stage_name=stage_name, threads=3)

  stage_name = 'Arena Ingest Silver'
  stage_order = 20

  onb.hydrate_bigquery_silver(project_name=project_name,system_name=system_name,stage_name=stage_name,job_name = f'Arena Ingest Silver Adswerve {dataset_id[0]}',system_secret_scope=system_secret_scope,system_order=system_order,stage_order=stage_order,job_order=job_order,bq_parent_project_name=bq_parent_project_name, bq_project_name=bq_project_name, schema_name=dataset_id[0],table_name=table,num_partitions=200,explode_and_flatten=False,adswerve_table_name='hitsPage',silver_transformations_path="./Adswerve Transformation Functions") 
  onb.hydrate_bigquery_silver(project_name=project_name,system_name=system_name,stage_name=stage_name,job_name = f'Arena Ingest Silver Adswerve {dataset_id[0]}',system_secret_scope=system_secret_scope,system_order=system_order,stage_order=stage_order,job_order=job_order,bq_parent_project_name=bq_parent_project_name, bq_project_name=bq_project_name, schema_name=dataset_id[0],table_name=table,num_partitions=200,explode_and_flatten=False,adswerve_table_name='sessions',silver_transformations_path="./Adswerve Transformation Functions")

  onb.update_stage_threads(project_name=project_name, stage_name=stage_name, threads=2)

  stage_name = 'Arena Ingest Gold'
  stage_order = 30

  onb.hydrate_bigquery_gold(project_name=project_name,system_name=system_name,stage_name=stage_name,job_name = f'Arena Ingest Gold Custom',system_secret_scope=system_secret_scope,system_order=system_order,stage_order=stage_order,job_order=job_order,schema_name=dataset_id[0],source_table_name=table,source_destination="silvergeneral", destinatation_table_name=f"custom_table_{str(today)}",adswerve_table_names="hitsPage, sessions") 
  onb.update_stage_threads(project_name=project_name, stage_name=stage_name, threads=1)

  system_name = 'Arena DQ'
  stage_name = 'Arena Ingest Bronze'
  system_order = 20
  stage_order = 10
  notebook_path = '../Data Quality Rules Engine/Data Quality Profiling and Rules Development'
  onb.hydrate_data_quality_assessment(project_name=project_name,system_name=system_name,system_secret_scope=system_secret_scope,system_order=system_order,stage_name=stage_name,stage_order=stage_order,job_name=f'UAT_BigQuery_{dataset_id[0]}',job_is_active=True,job_order=10,table_name=f'bronze.arena_big_query_{dataset_id[0]}_{table}_staging',delta_history_minutes=-1,notebook_path=notebook_path)
  onb.hydrate_data_quality_assessment(project_name=project_name,system_name=system_name,system_secret_scope=system_secret_scope,system_order=system_order,stage_name=stage_name,stage_order=stage_order,job_name=f'UAT_BigQuery_{dataset_id[1]}',job_is_active=True,job_order=10,table_name=f'bronze.arena_big_query_{dataset_id[1]}_{table}_staging',delta_history_minutes=-1,notebook_path=notebook_path)
  onb.hydrate_data_quality_assessment(project_name=project_name,system_name=system_name,system_secret_scope=system_secret_scope,system_order=system_order,stage_name=stage_name,stage_order=stage_order,job_name=f'UAT_BigQuery_{dataset_id[2]}',job_is_active=True,job_order=10,table_name=f'bronze.arena_big_query_{dataset_id[2]}_{table}_staging',delta_history_minutes=-1,notebook_path=notebook_path)
  onb.hydrate_data_quality_assessment(project_name=project_name,system_name=system_name,system_secret_scope=system_secret_scope,system_order=system_order,stage_name=stage_name,stage_order=stage_order,job_name=f'UAT_BigQuery_{dataset_id[0]}',job_is_active=True,job_order=10,table_name=f'silvergeneral.arena_big_query_{dataset_id[0]}_sessions_{table}',delta_history_minutes=-1,notebook_path=notebook_path)
  onb.hydrate_data_quality_assessment(project_name=project_name,system_name=system_name,system_secret_scope=system_secret_scope,system_order=system_order,stage_name=stage_name,stage_order=stage_order,job_name=f'UAT_BigQuery_{dataset_id[0]}',job_is_active=True,job_order=10,table_name=f'silvergeneral.arena_big_query_{dataset_id[0]}_hitsPage_{table}',delta_history_minutes=-1,notebook_path=notebook_path)
  onb.hydrate_data_quality_assessment(project_name=project_name,system_name=system_name,system_secret_scope=system_secret_scope,system_order=system_order,stage_name=stage_name,stage_order=stage_order,job_name=f'UAT_BigQuery_{dataset_id[0]}',job_is_active=True,job_order=10,table_name=f'goldgeneral.custom_table_{str(today)}',delta_history_minutes=-1,notebook_path=notebook_path)
  onb.update_stage_threads(project_name=project_name, stage_name=stage_name, threads=6)

#   #hydrate bigquery (rerun)
#   stage_name = 'UAT_BigQuery_ingest_daily_rerun'
#   stage_order = 20
#   onb.hydrate_batch_sql(project_name=project_name,system_name=system_name,stage_name=stage_name,job_name = 'UAT_SQL_adventureworkslt_SalesLT_SalesOrderHeader_rerun',system_secret_scope=system_secret_scope,system_order=system_order,stage_order=stage_order,job_order=job_order,schema_name='SalesLT',table_name='SalesOrderHeader',primary_key_columns='SalesOrderID',timestamp_columns='DueDate,ModifiedDate,OrderDate,ShipDate')
#   onb.hydrate_batch_sql(project_name=project_name,system_name=system_name,stage_name=stage_name,job_name = 'UAT_SQL_adventureworkslt_dbo_BuildVersion_rerun',system_secret_scope=system_secret_scope,system_order=system_order,stage_order=stage_order,job_order=job_order,schema_name='dbo',table_name='BuildVersion',primary_key_columns='SystemInformationID',timestamp_columns='VersionDate,ModifiedDate',load_type='Append',pushdown_query = 'SELECT * FROM dbo.BuildVersion WHERE 1=1 OR ModifiedDate >= DATEADD(DAY, -30, SYSDATETIME())')
#   onb.hydrate_batch_sql(project_name=project_name,system_name=system_name,stage_name=stage_name,job_name = 'UAT_SQL_adventureworkslt_SalesLT_ProductModel_rerun',system_secret_scope=system_secret_scope,system_order=system_order,stage_order=stage_order,job_order=job_order,schema_name='SalesLT',table_name='ProductModel',primary_key_columns='ProductModelID',timestamp_columns='ModifiedDate', use_windowed_extraction = True, windowing_column = 'ModifiedDate', windowed_extraction_begin_date = '2002-01-01', windowed_extraction_end_date='2021-01-01', windowed_extraction_interval = 'Y', windowed_extraction_process_latest_window_first = False)
#   onb.hydrate_batch_sql(project_name=project_name,system_name=system_name,stage_name=stage_name,job_name = 'UAT_SQL_adventureworkslt_SalesLT_Address_rerun',system_secret_scope=system_secret_scope,system_order=system_order,stage_order=stage_order,job_order=job_order,schema_name='SalesLT',table_name='Address',primary_key_columns='AddressID',timestamp_columns='ModifiedDate', silver_zone_partition_column = 'StateProvince', encrypt_columns = 'AddressLine1,AddressLine2', load_type = 'Overwrite', destination = 'silverprotected')
#   onb.update_stage_threads(project_name=project_name, stage_name=stage_name, threads=4)

#   #gold zone object creation
#   system_name = 'UAT_SQL_adventureworkslt_enrich'
#   system_order = 30
#   stage_name = 'UAT_SQL_adventureworkslt_prep_daily'
#   stage_order = 10
#   system_secret_scope = 'internal'
#   onb.hydrate_generic_notebook(project_name=project_name,system_name=system_name,stage_name='UAT_SQL_adventureworkslt_prep_daily',job_name='UAT_SQL_Gold_Load_Views',system_secret_scope=system_secret_scope,parameters = {},notebook_path = '../Data Quality Rules Engine/UAT_SQL',system_is_active = True,stage_is_active = True,job_is_active = True,system_order = system_order,stage_order = 10,job_order = 10)

#   #hydrate gold zone (empty)
#   stage_name = 'UAT_SQL_adventureworkslt_enrich_daily'
#   stage_order = 20
#   onb.hydrate_gold_zone(project_name=project_name,system_name=system_name,system_secret_scope=system_secret_scope,system_order=system_order,stage_name=stage_name,stage_order=stage_order,job_name='UAT_SQL_SalesLT_SalesOrderHeader',job_order=10,view_name='silvergeneral.vadventureworkslt_saleslt_salesorderheader',table_name='goldgeneral.adventureworkslt_saleslt_salesorderheader',purpose='uatsql',primary_key_columns='AccountNumber',partition_column='',cluster_column='AccountNumber',cluster_buckets='8',optimize_where= '',optimize_z_order_by='',vacuum_retention_hours='168',load_type='MergeType2',delete_not_in_source='true',destination='goldgeneral',gold_zone_notebook_path='../Data Engineering/Gold Zone/Gold Load');
#   onb.hydrate_gold_zone(project_name=project_name,system_name=system_name,system_secret_scope=system_secret_scope,system_order=system_order,stage_name=stage_name,stage_order=stage_order,job_name='UAT_SQL_sales',job_order=10,view_name='silvergeneral.UAT_SQL_sales',table_name='goldgeneral.UAT_SQL_sales',purpose='uatsql',primary_key_columns='AccountNumber',partition_column='',cluster_column='AccountNumber',cluster_buckets='8',optimize_where= '',optimize_z_order_by='',vacuum_retention_hours='168',load_type='Overwrite',delete_not_in_source='true',destination='goldgeneral',gold_zone_notebook_path='../Data Engineering/Gold Zone/Gold Load');
#   onb.update_stage_threads(project_name=project_name, stage_name=stage_name, threads=2)

#   #hydrate gold zone (rerun)
#   stage_name = 'UAT_SQL_adventureworkslt_enrich_daily_rerun'
#   stage_order = 30
#   onb.hydrate_gold_zone(project_name=project_name,system_name=system_name,system_secret_scope=system_secret_scope,system_order=system_order,stage_name=stage_name,stage_order=stage_order,job_name='UAT_SQL_SalesLT_SalesOrderHeader_rerun',job_order=10,view_name='silvergeneral.vadventureworkslt_saleslt_salesorderheader',table_name='goldgeneral.adventureworkslt_saleslt_salesorderheader',purpose='uatsql',primary_key_columns='AccountNumber',partition_column='',cluster_column='AccountNumber',cluster_buckets='8',optimize_where= '',optimize_z_order_by='',vacuum_retention_hours='168',load_type='MergeType2',delete_not_in_source='true',destination='goldgeneral',gold_zone_notebook_path='../Data Engineering/Gold Zone/Gold Load');
#   onb.hydrate_gold_zone(project_name=project_name,system_name=system_name,system_secret_scope=system_secret_scope,system_order=system_order,stage_name=stage_name,stage_order=stage_order,job_name='UAT_SQL_sales_rerun',job_order=10,view_name='silvergeneral.UAT_SQL_sales',table_name='goldgeneral.UAT_SQL_sales',purpose='uatsql',primary_key_columns='AccountNumber',partition_column='',cluster_column='AccountNumber',cluster_buckets='8',optimize_where= '',optimize_z_order_by='',vacuum_retention_hours='168',load_type='Overwrite',delete_not_in_source='true',destination='goldgeneral',gold_zone_notebook_path='../Data Engineering/Gold Zone/Gold Load');
#   onb.update_stage_threads(project_name=project_name, stage_name=stage_name, threads=2)

#   #hydrate presentation zone
#   system_name = 'UAT_SQL_Load'
#   system_order = 40
#   stage_name = 'UAT_SQL_Load_DW_Daily'
#   stage_order = 10
#   onb.hydrate_presentation_zone(project_name=project_name,system_name=system_name,system_secret_scope=system_secret_scope,system_order=40,stage_name=stage_name,stage_order=10,job_name='UAT_SQL_adventureworkslt_SalesLT_ProductModel_upsert',job_order=10,table_name='goldgeneral.UAT_SQL_sales',destination_schema_name='dbo', stored_procedure_name='',save_mode='overwrite',presentation_zone_notebook_path='../Data Engineering/Presentation Zone/SQL JDBC')
#   onb.hydrate_platinum_zone(project_name=project_name,system_name=system_name,system_secret_scope=system_secret_scope,system_order=40,stage_name=stage_name,stage_order=10,job_name='UAT_SQL_adventureworkslt_saleslt_salesorderheader_upsert',job_order=10,table_name='goldgeneral.adventureworkslt_saleslt_salesorderheader',destination_schema_name='dbo',destination_table_name='UAT_SQL_adventureworkslt_saleslt_salesorderheader',stored_procedure_name='',num_partitions='8',ignore_date_to_process_filter='true',load_type='overwrite',truncate_table_instead_of_drop_and_replace='true',isolation_level='READ_COMMITTED',presentation_zone_notebook_path='../Data Engineering/Presentation Zone/SQL Spark Connector 3', job_is_active=False)
#   onb.update_stage_threads(project_name=project_name, stage_name=stage_name, threads=1)

#   #hydrate data quality assessment
#   #bronze
#   system_name = 'UAT_SQL_DataQuality'
#   system_order = 50
#   stage_name = 'UAT_SQL_Bronze_Zone_QC'
#   stage_order = 10
#   notebook_path = '../Data Quality Rules Engine/Data Quality Assessment'
#   onb.hydrate_data_quality_assessment(project_name=project_name,system_name=system_name,system_secret_scope=system_secret_scope,system_order=system_order,stage_name=stage_name,stage_order=stage_order,job_name='UAT_SQL_Bronze_Zone_QC_dataqualityassessment_bronze_adventureworkslt_saleslt_address_staging',job_is_active=True,job_order=10,table_name='bronze.adventureworkslt_saleslt_address_staging',delta_history_minutes=-1,notebook_path=notebook_path)
#   onb.hydrate_data_quality_assessment(project_name=project_name,system_name=system_name,system_secret_scope=system_secret_scope,system_order=system_order,stage_name=stage_name,stage_order=stage_order,job_name='UAT_SQL_Bronze_Zone_QC_dataqualityassessment_bronze_adventureworkslt_saleslt_address_productmodel_staging',job_is_active=True,job_order=10,table_name='bronze.adventureworkslt_saleslt_productmodel_staging',delta_history_minutes=-1,notebook_path=notebook_path)
#   onb.hydrate_data_quality_assessment(project_name=project_name,system_name=system_name,system_secret_scope=system_secret_scope,system_order=system_order,stage_name=stage_name,stage_order=stage_order,job_name='UAT_SQL_Bronze_Zone_QC_dataqualityassessment_bronze_adventureworkslt_saleslt_salesorderheader_staging',job_is_active=True,job_order=10,table_name='bronze.adventureworkslt_saleslt_salesorderheader_staging',delta_history_minutes=-1,notebook_path=notebook_path)
#   onb.hydrate_data_quality_assessment(project_name=project_name,system_name=system_name,system_secret_scope=system_secret_scope,system_order=system_order,stage_name=stage_name,stage_order=stage_order,job_name='UAT_SQL_Bronze_Zone_QC_dataqualityassessment_bronze_adventureworkslt_dbo_buildversion_staging',job_is_active=True,job_order=10,table_name='bronze.adventureworkslt_dbo_buildversion_staging',delta_history_minutes=-1,notebook_path=notebook_path)
#   onb.update_stage_threads(project_name=project_name, stage_name=stage_name, threads=4)
#   #silver
#   stage_name = 'UAT_SQL_Silver_Zone_QC'
#   stage_order = 20
#   onb.hydrate_data_quality_assessment(project_name=project_name,system_name=system_name,system_secret_scope=system_secret_scope,system_order=system_order,stage_name=stage_name,stage_order=stage_order,job_name='UAT_SQL_Silver_Zone_QC_dataqualityassessment_silver_adventureworkslt_saleslt_salesorderheader',job_is_active=True,job_order=10,table_name='silvergeneral.adventureworkslt_saleslt_salesorderheader',delta_history_minutes=-1,notebook_path=notebook_path)
#   onb.hydrate_data_quality_assessment(project_name=project_name,system_name=system_name,system_secret_scope=system_secret_scope,system_order=system_order,stage_name=stage_name,stage_order=stage_order,job_name='UAT_SQL_Silver_Zone_QC_dataqualityassessment_silver_adventureworkslt_saleslt_productmodel',job_is_active=True,job_order=10,table_name='silvergeneral.adventureworkslt_saleslt_productmodel',delta_history_minutes=-1,notebook_path=notebook_path)
#   onb.hydrate_data_quality_assessment(project_name=project_name,system_name=system_name,system_secret_scope=system_secret_scope,system_order=system_order,stage_name=stage_name,stage_order=stage_order,job_name='UAT_SQL_Silver_Zone_QC_dataqualityassessment_silver_adventureworkslt_dbo_buildversion',job_is_active=True,job_order=10,table_name='silvergeneral.adventureworkslt_dbo_buildversion',delta_history_minutes=-1,notebook_path=notebook_path)
#   onb.hydrate_data_quality_assessment(project_name=project_name,system_name=system_name,system_secret_scope=system_secret_scope,system_order=system_order,stage_name=stage_name,stage_order=stage_order,job_name='UAT_SQL_Silver_Zone_QC_dataqualityassessment_silver_adventureworkslt_saleslt_address',job_is_active=True,job_order=10,table_name='silverprotected.adventureworkslt_saleslt_address',delta_history_minutes=-1,notebook_path=notebook_path)
#   onb.update_stage_threads(project_name=project_name, stage_name=stage_name, threads=4)
#   #gold
#   stage_name = 'UAT_SQL_Gold_Zone_QC'
#   stage_order = 30
#   onb.hydrate_data_quality_assessment(project_name=project_name,system_name=system_name,system_secret_scope=system_secret_scope,system_order=system_order,stage_name=stage_name,stage_order=stage_order,job_name='UAT_SQL_Gold_Zone_QC_dataqualityassessment_goldgeneral_adventureworkslt_saleslt_salesorderheader',job_is_active=True,job_order=10,table_name='goldgeneral.adventureworkslt_saleslt_salesorderheader',delta_history_minutes=-1,notebook_path=notebook_path)
#   onb.hydrate_data_quality_assessment(project_name=project_name,system_name=system_name,system_secret_scope=system_secret_scope,system_order=system_order,stage_name=stage_name,stage_order=stage_order,job_name='UAT_SQL_Gold_Zone_QC_dataqualityassessment_goldgeneral_UAT_SQL_sales',job_is_active=True,job_order=10,table_name='goldgeneral.UAT_SQL_sales',delta_history_minutes=-1,notebook_path=notebook_path)
#   onb.update_stage_threads(project_name=project_name, stage_name=stage_name, threads=2)    

  #hydrate platinum zone
  #system_name = 'UAT_SQL_DataQuality Load'
  #system_order = 60
  #system_secret_scope = 'metadatadb'
  #stage_name = 'UAT_SQL Data Quality Assessment Load to Metadata DB'
  #stage_order = 10
  #notebook_path = '../Data Engineering/Presentation Zone/SQL Spark Connector 3'
  #onb.hydrate_platinum_zone(project_name=project_name,system_name=system_name,system_secret_scope=system_secret_scope,system_order=system_order,stage_name=stage_name,stage_order=stage_order,
  #                          job_name='UAT_SQL Data Quality Assessment Load to Metadata DB DQ Validation Result',job_order=10,table_name='goldprotected.dataqualityvalidationresult',destination_schema_name='staging',
  #                          destination_table_name='DataQualityValidationResult',stored_procedure_name='staging.LoadDataQualityValidationResult',num_partitions='8',ignore_date_to_process_filter='true',
  #                          load_type='overwrite',truncate_table_instead_of_drop_and_replace='false',isolation_level='READ_COMMITTED',presentation_zone_notebook_path=notebook_path)
  
  #onb.hydrate_platinum_zone(project_name=project_name,system_name=system_name,system_secret_scope=system_secret_scope,system_order=system_order,stage_name=stage_name,stage_order=stage_order,
  #                          job_name='UAT_SQL Data Quality Assessment Load to Metadata DB DQ Validation Result Detail',job_order=20,table_name='goldprotected.vdataqualityvalidationresultdetail',
  #                          destination_schema_name='staging',
  #                          destination_table_name='DataQualityValidationResultDetail',stored_procedure_name='staging.LoadDataQualityValidationResultDetail',num_partitions='8',ignore_date_to_process_filter='true',
  #                          load_type='overwrite',truncate_table_instead_of_drop_and_replace='false',isolation_level='READ_COMMITTED',presentation_zone_notebook_path=notebook_path)

  onb.load_orchestration_tables(project_name="UAT_BigQuery")
