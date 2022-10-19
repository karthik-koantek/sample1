def hydrate(project_name):
  import ktk
  from pyspark.sql.functions import col, create_map, lit, col, crc32, concat
  from itertools import chain
  import json
  
  onb = ktk.OrchestrationDeltaNotebook()
  onb.create_orchestration_staging_tables(project_name)

  #teardown
  onb.hydrate_generic_notebook(project_name=project_name, system_name='UAT_Cosmos_Teardown', system_secret_scope='internal', system_is_active=True, system_order=10, stage_name='UAT_Cosmos_Teardown_daily', stage_is_active = True, stage_order = 10, job_name='UAT_Cosmos_Teardown', job_is_active=True, job_order=10, parameters={}, notebook_path = '../Data Quality Rules Engine/UAT_Cosmos_Teardown')
  
  #hydrate cosmos and presentation zone
  stage_name = 'UAT_Cosmos_Daily'
  stage_order = 10
  job_order = 10
  system_order = 20
  system_name = 'UAT_Cosmos'
  system_secret_scope ='CosmosDB'
  
  onb.hydrate_cosmos(project_name=project_name, system_name='UAT_Cosmos', system_secret_scope='CosmosDB', system_is_active=True, system_order=system_order, stage_name=stage_name, stage_is_active = True, stage_order = stage_order, job_name='UAT_Cosmos_cosmosdbingest_usecases', job_is_active=True, job_order=job_order, collection='usecases', partition_column='/usecaseId', pushdown_predicate='', schema_name='cosmosdbingest', table_name='usecases', silver_zobe_partition_column='', silver_zone_cluster_column = 'pk', silver_zone_cluster_buckets = '8', optmize_where='', optimize_z_order_by='', vaccum_retention_hours = 168, num_partitions = 8, primary_key_columns='messages_message_messageId,anonymizedMethods_method,name', explode_and_flatten = 'True', cleanse_column_names = 'True', timestamp_columns = '', timestamp_format = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", encrypt_columns = '', load_type = 'Merge', destination = 'silvergeneral', bronze_zone_notebook_path = '../Data Engineering/Bronze Zone/Batch CosmosDB', silver_zone_notebook_path = '../Data Engineering/Silver Zone/Delta Load')
  onb.hydrate_presentation_zone(project_name=project_name, system_name='UAT_Cosmos', system_secret_scope='CosmosDB', system_is_active=True, system_order=system_order, stage_name=stage_name, stage_is_active = True, stage_order = stage_order, job_name='UAT_Cosmos_adventureworkslt_SalesLT_Address_cdb', job_is_active=True, job_order=job_order, collection='addresses', table_name='silverprotected.adventureworkslt_SalesLT_Address', id_column='AddressID', presentation_zone_notebook_path = '../Data Engineering/Presentation Zone/SQL Spark Connector')
  onb.update_stage_threads(project_name=project_name, stage_name=stage_name, threads=2)
  
  stage_name = 'UAT_Cosmos_Daily_Rerun'
  stage_order = 20
  
  onb.hydrate_cosmos(project_name=project_name, system_name='UAT_Cosmos', system_secret_scope='CosmosDB', system_is_active=True, system_order=system_order, stage_name=stage_name, stage_is_active = True, stage_order = stage_order, job_name='UAT_Cosmos_cosmosdbingest_usecases_rerun', job_is_active=True, job_order=job_order, collection='usecases', partition_column='/usecaseId', pushdown_predicate='', schema_name='cosmosdbingest', table_name='usecases', silver_zobe_partition_column='', silver_zone_cluster_column = 'pk', silver_zone_cluster_buckets = '8', optmize_where='', optimize_z_order_by='', vaccum_retention_hours = 168, num_partitions = 8, primary_key_columns='messages_message_messageId,anonymizedMethods_method,name', explode_and_flatten = 'True', cleanse_column_names = 'True', timestamp_columns = '', timestamp_format = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", encrypt_columns = '', load_type = 'Merge', destination = 'silvergeneral', bronze_zone_notebook_path = '../Data Engineering/Bronze Zone/Batch CosmosDB', silver_zone_notebook_path = '../Data Engineering/Silver Zone/Delta Load')
  onb.hydrate_presentation_zone(project_name=project_name, system_name='UAT_Cosmos', system_secret_scope='CosmosDB', system_is_active=True, system_order=system_order, stage_name=stage_name, stage_is_active = True, stage_order = stage_order, job_name='UAT_Cosmos_adventureworkslt_SalesLT_Address_cdb_rerun', job_is_active=True, job_order=job_order, collection='addresses', table_name='silverprotected.adventureworkslt_SalesLT_Address', id_column='AddressID', presentation_zone_notebook_path = '../Data Engineering/Presentation Zone/SQL Spark Connector')
  onb.update_stage_threads(project_name=project_name, stage_name=stage_name, threads=2)
  
  #hydrate data quality assessment
  system_name = 'UAT_Cosmos_DataQuality'
  system_secret_scope ='internal'
  system_order = 30
  stage_name = 'UAT_Cosmos_DataQuality'
  stage_order = 10
  job_order = 10
  system_is_active = True
  stage_is_active = True
  job_is_active = True
  delta_history_minutes = -1
  notebook_path = '../Data Quality Rules Engine/Data Quality Assessment'
  
  onb.hydrate_data_quality_assessment(project_name=project_name, system_name=system_name, system_secret_scope=system_secret_scope, system_is_active=system_is_active, system_order=system_order, stage_name=stage_name, stage_is_active=stage_is_active, stage_order=stage_order, job_name='UAT_Cosmos_DataQuality_dataqualityassessment_silverprotected_cosmosdb_cosmosdbingest_usecases', job_is_active=job_is_active, job_order=job_order, table_name='silverprotected.cosmosdb_cosmosdbingest_usecases', delta_history_minutes=delta_history_minutes, notebook_path=notebook_path)
    onb.hydrate_data_quality_assessment(project_name=project_name, system_name=system_name, system_secret_scope=system_secret_scope, system_is_active=system_is_active, system_order=system_order, stage_name=stage_name, stage_is_active=stage_is_active, stage_order=stage_order, job_name='UAT_Cosmos_DataQuality_dataqualityassessment_bronze_cosmosdb_cosmosdbingest_usecases_staging', job_is_active=job_is_active, job_order=job_order, table_name='silverprotected.cosmosdb_cosmosdbingest_usecases', delta_history_minutes=delta_history_minutes, notebook_path=notebook_path)
    onb.update_stage_threads(project_name=project_name, stage_name=stage_name, threads=2)
    
    #hydrate platinum zone
    system_name = 'UAT_Cosmos_DataQuality Load'
    system_secret_scope = 'metadatadb'
    system_order = 40
    stage_name = 'UAT_SQL Data Quality Assessment Load to Metadata DB'
    stage_is_active = True
    stage_order = 10
    destination_schema_name='staging'
    num_partitions = '8'
    ignore_date_to_process_filter='true'
    load_type='overwrite'
    truncate_table_instead_of_drop_and_replace = 'false'
    isolation_level = 'READ_COMMITTED'
    presentation_zone_notebook_path = '../Data Engineering/Presentation Zone/SQL Spark Connector 3'

    onb.hydrate_platinum_zone(project_name=project_name, system_name=system_name, system_secret_scope=system_secret_scope, system_is_active=system_is_active, system_order=system_order, stage_name=stage_name, stage_is_active=stage_is_active, stage_order=stage_order, job_name='UAT_ML_dataqualityassessment_loadmetadatadb_dataqualityvalidationresult', job_is_active=job_is_active, job_order=10, table_name='goldprotected.dataqualityvalidationresult', destination_schema_name=destination_schema_name, destination_table_name='DataQualityValidationResult', stored_procedure_name='staging.LoadDataQualityValidationResult', num_partitions=num_partitions, ignore_date_to_process_filter=ignore_date_to_process_filter, load_type=load_type, truncate_table_instead_of_drop_and_replace=truncate_table_instead_of_drop_and_replace, isolation_level=isolation_level, presentation_zone_notebook_path=presentation_zone_notebook_path)
    onb.hydrate_platinum_zone(project_name=project_name, system_name=system_name, system_secret_scope=system_secret_scope, system_is_active=system_is_active, system_order=system_order, stage_name=stage_name, stage_is_active=stage_is_active, stage_order=stage_order, job_name='UAT_ML_dataqualityassessment_loadmetadatadb_dataqualityvalidationresultdetail', job_is_active=job_is_active, job_order=20, table_name='goldprotected.vdataqualityvalidationresultdetail', destination_schema_name=destination_schema_name, destination_table_name='DataQualityValidationResultDetail', stored_procedure_name='staging.LoadDataQualityValidationResultDetail', num_partitions=num_partitions, ignore_date_to_process_filter=ignore_date_to_process_filter, load_type=load_type, truncate_table_instead_of_drop_and_replace=truncate_table_instead_of_drop_and_replace, isolation_level=isolation_level, presentation_zone_notebook_path=presentation_zone_notebook_path)
    onb.update_stage_threads(project_name=project_name, stage_name=stage_name, threads=2)