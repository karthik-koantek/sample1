#from multiprocessing.dummy import JoinableQueue
#from os import system
#from random import SystemRandom
#import sys
#from matplotlib.pyplot import table

#import numpy
#from yaml import load


def hydrate(project_name):
    import ktk
    from pyspark.sql.functions import col, create_map, lit, col, crc32, concat
    from itertools import chain
    import json

    onb = ktk.OrchestrationDeltaNotebook()
    onb.create_orchestration_staging_tables(project_name)

    #teardown
    onb.hydrate_generic_notebook(project_name=project_name, system_name='UAT_ExternalFiles_Teardwon', system_secret_scope='internal', system_is_active=True, system_order=10, stage_name='UAT_ExternalFiles_Teardown_daily', stage_is_active = True, stage_order = 10, job_name='UAT_ExternalFiles_Teardown', job_is_active=True, job_order=10, parameters={}, notebook_path = '../Data Quality Rules Engine/UAT_ExternalFiles_Teardown')

    #hydrate batch file (empty)
    system_name = 'UAT_ExternalFiles_externalfilesystem'
    stage_name = 'UAT_ExternalFiles_externalfilesystem_Daily'
    system_secret_scope = 'ExternalBlobStore'
    system_is_active = True
    stage_is_active = True
    job_is_active = True
    system_order = 20
    stage_order = 10
    job_order = 10
    is_date_partitioned = 'False'
    silver_zone_partition_column = ''
    silver_zone_cluster_column = 'pk'
    silver_zone_cluster_buckets = '8'
    optimize_z_order_by = ''
    vacuum_retention_hours = 168
    partition_column = ''
    num_partitions = '8'
    timestamp_format = 'yyyy-MM-dd''T''HH:mm:ss.SSS''Z'''
    destination = 'silvergeneral'
    silver_notebook_path = '../Data Engineering/Silver Zone/Delta Load'
    
    onb.hydrate_batch_file(project_name=project_name, system_name=system_name, system_secret_scope=system_secret_scope, system_is_active=system_is_active, system_order=system_order, stage_name=stage_name, stage_is_active=stage_is_active, stage_order=stage_order, job_name='UAT_ExternalFiles_dvcpmdpcvsa_dvcatalystexternaldata_csv_noaa_noaaapi', job_is_active=job_is_active, job_order=job_order, external_data_path='csv/noaa', schema_name='noaa', table_name='noaaapi', file_extension='', delimiter=',', header='True', is_date_partitioned=is_date_partitioned, silver_zone_partition_column=silver_zone_partition_column, silver_zone_cluster_column=silver_zone_cluster_column, silver_zone_cluster_buckets=silver_zone_cluster_buckets, optimize_z_order_by=optimize_z_order_by, vacuum_retention_hours=vacuum_retention_hours, partition_column=partition_column, num_partitions=num_partitions, primary_key_columns='Date,ABC,DEF', explode_and_flatten='False', cleanse_column_names='False', timestamp_columns='Date', timestamp_format=timestamp_format, encrypt_columns='', load_type='Merge', destination=destination, bronze_notebook_path='../Data Engineering/Bronze Zone/Batch File CSV', silver_notebook_path=silver_notebook_path)
    onb.hydrate_batch_file(project_name=project_name, system_name=system_name, system_secret_scope=system_secret_scope, system_is_active=system_is_active, system_order=system_order, stage_name=stage_name, stage_is_active=stage_is_active, stage_order=stage_order, job_name='UAT_ExternalFiles_dvcpmdpcvsa_dvcatalystexternaldata_json_youtube_youtube', job_is_active=job_is_active, job_order=job_order, external_data_path='json/youtube', schema_name='youtube', table_name='youtube', file_extension='json', multi_line='True', delimiter=',', is_date_partitioned=is_date_partitioned, silver_zone_partition_column=silver_zone_partition_column, silver_zone_cluster_column=silver_zone_cluster_column, silver_zone_cluster_buckets=silver_zone_cluster_buckets, optimize_z_order_by=optimize_z_order_by, vacuum_retention_hours=vacuum_retention_hours, partition_column=partition_column, num_partitions=num_partitions, primary_key_columns='items_etag', explode_and_flatten='True', cleanse_column_names='True', timestamp_columns='', timestamp_format=timestamp_format, encrypt_columns='', load_type='Merge', destination=destination, bronze_notebook_path='../Data Engineering/Bronze Zone/Batch File JSON', silver_notebook_path=silver_notebook_path)
    onb.hydrate_batch_file(project_name=project_name, system_name=system_name, system_secret_scope=system_secret_scope, system_is_active=system_is_active, system_order=system_order, stage_name=stage_name, stage_is_active=stage_is_active, stage_order=stage_order, job_name='UAT_ExternalFiles_dvcpmdpcvsa_dvcatalystexternaldata_parquet_userdata_userdata1', job_is_active=job_is_active, job_order=job_order, external_data_path='parquet/userdata', schema_name='userdata', table_name='userdata1', file_extension='parquet', is_date_partitioned=is_date_partitioned, silver_zone_partition_column=silver_zone_partition_column, silver_zone_cluster_column=silver_zone_cluster_column, silver_zone_cluster_buckets=silver_zone_cluster_buckets, optimize_z_order_by=optimize_z_order_by, vacuum_retention_hours=vacuum_retention_hours, partition_column=partition_column, num_partitions=num_partitions, primary_key_columns='id', explode_and_flatten='False', cleanse_column_names='False', timestamp_columns='registration_dttm', timestamp_format=timestamp_format, encrypt_columns='cc,birthdate,email,first_name,ip_address,last_name,salary', load_type='Merge', destination=destination, bronze_notebook_path='../Data Engineering/Bronze Zone/Batch File Parquet', silver_notebook_path=silver_notebook_path)
    onb.hydrate_batch_file(project_name=project_name, system_name=system_name, system_secret_scope=system_secret_scope, system_is_active=system_is_active, system_order=system_order, stage_name=stage_name, stage_is_active=stage_is_active, stage_order=stage_order, job_name='UAT_ExternalFiles_dvcpmdpcvsa_dvcatalystexternaldata_xml_simple', job_is_active=job_is_active, job_order=job_order, external_data_path='xml/simple', schema_name='menu', table_name='breakfast', file_extension='xml', is_date_partitioned=is_date_partitioned, row_tag='food', root_tag='breakfast_menu', silver_zone_partition_column=silver_zone_partition_column, silver_zone_cluster_column=silver_zone_cluster_column, silver_zone_cluster_buckets=silver_zone_cluster_buckets, optimize_z_order_by=optimize_z_order_by, vacuum_retention_hours=vacuum_retention_hours, partition_column=partition_column, num_partitions=num_partitions, primary_key_columns='', explode_and_flatten='True', cleanse_column_names='True', timestamp_columns='', timestamp_format=timestamp_format, encrypt_columns='', load_type='Overwrite', destination=destination, bronze_notebook_path='../Data Engineering/Bronze Zone/Batch File XML', silver_notebook_path=silver_notebook_path)

    #hydrate batch file (rerun)
    stage_name = 'UAT_ExternalFiles_externalfilesystem_Daily_rerun'
    stage_order = 20
    onb.hydrate_batch_file(project_name=project_name, system_name=system_name, system_secret_scope=system_secret_scope, system_is_active=system_is_active, system_order=system_order, stage_name=stage_name, stage_is_active=stage_is_active, stage_order=stage_order, job_name='UAT_ExternalFiles_dvcpmdpcvsa_dvcatalystexternaldata_csv_noaa_noaaapi_rerun', job_is_active=job_is_active, job_order=job_order, external_data_path='csv/noaa', schema_name='noaa', table_name='noaaapi', file_extension='', delimiter=',', header='True', is_date_partitioned=is_date_partitioned, silver_zone_partition_column=silver_zone_partition_column, silver_zone_cluster_column=silver_zone_cluster_column, silver_zone_cluster_buckets=silver_zone_cluster_buckets, optimize_z_order_by=optimize_z_order_by, vacuum_retention_hours=vacuum_retention_hours, partition_column=partition_column, num_partitions=num_partitions, primary_key_columns='Date,ABC,DEF', explode_and_flatten='False', cleanse_column_names='False', timestamp_columns='Date', timestamp_format=timestamp_format, encrypt_columns='', load_type='Merge', destination=destination, bronze_notebook_path='../Data Engineering/Bronze Zone/Batch File CSV', silver_notebook_path=silver_notebook_path)
    onb.hydrate_batch_file(project_name=project_name, system_name=system_name, system_secret_scope=system_secret_scope, system_is_active=system_is_active, system_order=system_order, stage_name=stage_name, stage_is_active=stage_is_active, stage_order=stage_order, job_name='UAT_ExternalFiles_dvcpmdpcvsa_dvcatalystexternaldata_json_youtube_youtube_rerun', job_is_active=job_is_active, job_order=job_order, external_data_path='json/youtube', schema_name='youtube', table_name='youtube', file_extension='json', multi_line='True', delimiter=',', is_date_partitioned=is_date_partitioned, silver_zone_partition_column=silver_zone_partition_column, silver_zone_cluster_column=silver_zone_cluster_column, silver_zone_cluster_buckets=silver_zone_cluster_buckets, optimize_z_order_by=optimize_z_order_by, vacuum_retention_hours=vacuum_retention_hours, partition_column=partition_column, num_partitions=num_partitions, primary_key_columns='items_etag', explode_and_flatten='True', cleanse_column_names='True', timestamp_columns='', timestamp_format=timestamp_format, encrypt_columns='', load_type='Merge', destination=destination, bronze_notebook_path='../Data Engineering/Bronze Zone/Batch File JSON', silver_notebook_path=silver_notebook_path)
    onb.hydrate_batch_file(project_name=project_name, system_name=system_name, system_secret_scope=system_secret_scope, system_is_active=system_is_active, system_order=system_order, stage_name=stage_name, stage_is_active=stage_is_active, stage_order=stage_order, job_name='UAT_ExternalFiles_dvcpmdpcvsa_dvcatalystexternaldata_parquet_userdata_userdata1_rerun', job_is_active=job_is_active, job_order=job_order, external_data_path='parquet/userdata', schema_name='userdata', table_name='userdata1', file_extension='parquet', is_date_partitioned=is_date_partitioned, silver_zone_partition_column=silver_zone_partition_column, silver_zone_cluster_column=silver_zone_cluster_column, silver_zone_cluster_buckets=silver_zone_cluster_buckets, optimize_z_order_by=optimize_z_order_by, vacuum_retention_hours=vacuum_retention_hours, partition_column=partition_column, num_partitions=num_partitions, primary_key_columns='id', explode_and_flatten='False', cleanse_column_names='False', timestamp_columns='registration_dttm', timestamp_format=timestamp_format, encrypt_columns='cc,birthdate,email,first_name,ip_address,last_name,salary', load_type='Merge', destination=destination, bronze_notebook_path='../Data Engineering/Bronze Zone/Batch File Parquet', silver_notebook_path=silver_notebook_path)
    onb.hydrate_batch_file(project_name=project_name, system_name=system_name, system_secret_scope=system_secret_scope, system_is_active=system_is_active, system_order=system_order, stage_name=stage_name, stage_is_active=stage_is_active, stage_order=stage_order, job_name='UAT_ExternalFiles_dvcpmdpcvsa_dvcatalystexternaldata_xml_simple_rerun', job_is_active=job_is_active, job_order=job_order, external_data_path='xml/simple', schema_name='menu', table_name='breakfast', file_extension='xml', is_date_partitioned=is_date_partitioned, row_tag='food', root_tag='breakfast_menu', silver_zone_partition_column=silver_zone_partition_column, silver_zone_cluster_column=silver_zone_cluster_column, silver_zone_cluster_buckets=silver_zone_cluster_buckets, optimize_z_order_by=optimize_z_order_by, vacuum_retention_hours=vacuum_retention_hours, partition_column=partition_column, num_partitions=num_partitions, primary_key_columns='', explode_and_flatten='True', cleanse_column_names='True', timestamp_columns='', timestamp_format=timestamp_format, encrypt_columns='', load_type='Overwrite', destination=destination, bronze_notebook_path='../Data Engineering/Bronze Zone/Batch File XML', silver_notebook_path=silver_notebook_path)

    #hydrate gold zone PowerBI
    system_name = 'UAT_GoldZone'
    system_secret_scope = 'internal'
    system_is_active = True
    system_order = 30
    stage_name = 'UAT_GoldZone'
    stage_is_active = True
    stage_order = 10
    job_name = 'UAT_GoldZone_externalblobstore_youtube_youtube'
    job_is_active = True
    job_order = 10
    table_name = 'silvergeneral.externalblobstore_youtube_youtube'
    cleanse_path = True
    rename_file = True
    gold_zone_notebook_path = '../Data Engineering/Gold Zone/Power BI File'

    onb.hydrate_gold_zone_power_bi(project_name=project_name, system_name=system_name, system_secret_scope=system_secret_scope, system_is_active=system_is_active, system_order=system_order, stage_name=stage_name, stage_is_active=stage_is_active, stage_order=stage_order, job_name=job_name, job_is_active=job_is_active, job_order=job_order, table_name=table_name, cleanse_path=cleanse_path, rename_file=rename_file, gold_zone_notebook_path=gold_zone_notebook_path)

    #hydrate clone
    destination = 'sandbox'    
    destination_relative_path = 'uatgoldzone'
    append_todays_date_to_table_name = 'False'
    overwrite_table='True'
    destination_table_comment='uat gold zone testing clone'
    time_travel_timestamp_expression=''
    time_travel_version_expression=''
    log_retention_duration_days='7'
    deleted_file_retention_duration_days='7'
    notebook_path = '../Data Engineering/Sandbox Zone/Clone Table'

    onb.hydrate_clone(project_name=project_name, system_name=system_name, system_secret_scope=system_secret_scope, system_is_active=system_is_active, system_order=system_order, stage_name=stage_name, stage_is_active=stage_is_active, stage_order=stage_order, job_name='UAT_GoldZone_externalblobstore_userdata_userdata1', job_is_active=job_is_active, job_order=job_order, source_table_name='silvergeneral.externalblobstore_userdata_userdata1', destination=destination, destination_relative_path=destination_relative_path, destination_table_name='userdata_userdata1', append_todays_date_to_table_name=append_todays_date_to_table_name, overwrite_table=overwrite_table, destination_table_comment=destination_table_comment, clone_type='SHALLOW', time_travel_timestamp_expression=time_travel_timestamp_expression, time_travel_version_expression=time_travel_version_expression, log_retention_duration_days = log_retention_duration_days, deleted_file_retention_duration_days=deleted_file_retention_duration_days, notebook_path=notebook_path)
    onb.hydrate_clone(project_name=project_name, system_name=system_name, system_secret_scope=system_secret_scope, system_is_active=system_is_active, system_order=system_order, stage_name=stage_name, stage_is_active=stage_is_active, stage_order=stage_order, job_name='UAT_GoldZone_externalblobstore_noaa_noaaapi', job_order=job_order, source_table_name='silvergeneral.externalblobstore_noaa_noaaapi', destination=destination, destination_relative_path=destination_relative_path, destination_table_name='noaa_noaaapi', append_todays_date_to_table_name=append_todays_date_to_table_name, overwrite_table=overwrite_table, destination_table_comment=destination_table_comment, clone_type='DEEP', time_travel_timestamp_expression=time_travel_timestamp_expression, time_travel_version_expression=time_travel_version_expression, log_retention_duration_days = log_retention_duration_days, deleted_file_retention_duration_days=deleted_file_retention_duration_days, notebook_path=notebook_path)

    #hydrate data quality assessment
    system_name='UAT_ExternalFiles_DataQuality'
    system_order = 40
    stage_order = 10
    notebook_path='../Data Quality Rules Engine/Data Quality Assessment'
    delta_history_minutes=-1
    
    #bronze
    stage_name = 'UAT_ExternalFiles_Bronze_Zone_QC'
    onb.hydrate_data_quality_assessment(project_name=project_name, system_name=system_name, system_secret_scope=system_secret_scope, system_is_active=system_is_active, system_order=system_order, stage_name=stage_name, stage_is_active=stage_is_active, stage_order=stage_order, job_name='UAT_ExternalFiles_Bronze_Zone_QC_dataqualityassessment_bronze_UAT_ExternalFiles', job_is_active=job_is_active, job_order=job_order, table_name='bronze.externalblobstore_noaa_noaaapi_staging', delta_history_minutes=delta_history_minutes, notebook_path=notebook_path)
    onb.hydrate_data_quality_assessment(project_name=project_name, system_name=system_name, system_secret_scope=system_secret_scope, system_is_active=system_is_active, system_order=system_order, stage_name=stage_name, stage_is_active=stage_is_active, stage_order=stage_order, job_name='UAT_ExternalFiles_Bronze_Zone_QC_dataqualityassessment_bronze_externalblobstore_userdata_userdata1_staging', job_is_active=job_is_active, job_order=job_order, table_name='bronze.externalblobstore_userdata_userdata1_staging', delta_history_minutes=delta_history_minutes, notebook_path=notebook_path)
    onb.hydrate_data_quality_assessment(project_name=project_name, system_name=system_name, system_secret_scope=system_secret_scope, system_is_active=system_is_active, system_order=system_order, stage_name=stage_name, stage_is_active=stage_is_active, stage_order=stage_order, job_name='UAT_ExternalFiles_Bronze_Zone_QC_dataqualityassessment_bronze_externalblobstore_youtube_youtube_staging', job_is_active=job_is_active, job_order=job_order, table_name='bronze.externalblobstore_youtube_youtube_staging', delta_history_minutes=delta_history_minutes, notebook_path=notebook_path)
    onb.hydrate_data_quality_assessment(project_name=project_name, system_name=system_name, system_secret_scope=system_secret_scope, system_is_active=system_is_active, system_order=system_order, stage_name=stage_name, stage_is_active=stage_is_active, stage_order=stage_order, job_name='UAT_ExternalFiles_Bronze_Zone_QC_dataqualityassessment_bronze_externalblobstore_menu_breakfast_staging', job_is_active=job_is_active, job_order=job_order, table_name='bronze.externalblobstore_menu_breakfast_staging', delta_history_minutes=delta_history_minutes, notebook_path=notebook_path)

    #silver
    stage_name = 'UAT_ExternalFiles_Silver_Zone_QC'
    stage_order = 20
    
    onb.hydrate_data_quality_assessment(project_name=project_name, system_name=system_name, system_secret_scope=system_secret_scope, system_is_active=system_is_active, system_order=system_order, stage_name=stage_name, stage_is_active=stage_is_active, stage_order=stage_order, job_name='UAT_ExternalFiles_Silver_Zone_QC_dataqualityassessment_silver_externalblobstore_noaa_noaaapi', job_is_active=job_is_active, job_order=job_order, table_name='silverprotected.externalblobstore_noaa_noaaapi', delta_history_minutes=delta_history_minutes, notebook_path=notebook_path)
    onb.hydrate_data_quality_assessment(project_name=project_name, system_name=system_name, system_secret_scope=system_secret_scope, system_is_active=system_is_active, system_order=system_order, stage_name=stage_name, stage_is_active=stage_is_active, stage_order=stage_order, job_name='UAT_ExternalFiles_Silver_Zone_QC_dataqualityassessment_silver_externalblobstore_userdata_userdata1', job_is_active=job_is_active, job_order=job_order, table_name='silverprotected.externalblobstore_userdata_userdata1', delta_history_minutes=delta_history_minutes, notebook_path=notebook_path)
    onb.hydrate_data_quality_assessment(project_name=project_name, system_name=system_name, system_secret_scope=system_secret_scope, system_is_active=system_is_active, system_order=system_order, stage_name=stage_name, stage_is_active=stage_is_active, stage_order=stage_order, job_name='UAT_ExternalFiles_Silver_Zone_QC_dataqualityassessment_silver_externalblobstore_youtube_youtube', job_is_active=job_is_active, job_order=job_order, table_name='silverprotected.externalblobstore_youtube_youtube', delta_history_minutes=delta_history_minutes, notebook_path=notebook_path)
    onb.hydrate_data_quality_assessment(project_name=project_name, system_name=system_name, system_secret_scope=system_secret_scope, system_is_active=system_is_active, system_order=system_order, stage_name=stage_name, stage_is_active=stage_is_active, stage_order=stage_order, job_name='UAT_ExternalFiles_Silver_Zone_QC_dataqualityassessment_silver_externalblobstore_menu_breakfast', job_is_active=job_is_active, job_order=job_order, table_name='silverprotected.externalblobstore_menu_breakfast', delta_history_minutes=delta_history_minutes, notebook_path=notebook_path)
  
    #gold
    stage_name='UAT_ExternalFiles_Gold_Zone_QC'
    stage_order = 30
  
    onb.hydrate_data_quality_assessment(project_name=project_name, system_name=system_name, system_secret_scope=system_secret_scope, system_is_active=system_is_active, system_order=system_order, stage_name=stage_name, stage_is_active=stage_is_active, stage_order=stage_order, job_name='UAT_ExternalFiles_Gold_Zone_QC_dataqualityassessment_sandbox_userdata_userdata1', job_is_active=job_is_active, job_order=job_order, table_name='sandbox.userdata_userdata1', delta_history_minutes=delta_history_minutes, notebook_path=notebook_path)
    onb.hydrate_data_quality_assessment(project_name=project_name, system_name=system_name, system_secret_scope=system_secret_scope, system_is_active=system_is_active, system_order=system_order, stage_name=stage_name, stage_is_active=stage_is_active, stage_order=stage_order, job_name='UAT_ExternalFiles_Gold_Zone_QC_dataqualityassessment_sandbox_noaa_noaaapi', job_is_active=job_is_active, job_order=job_order, table_name='sandbox.noaa_noaaapi', delta_history_minutes=delta_history_minutes, notebook_path=notebook_path)

    #hydrate platinum zone
    system_name = 'UAT_ExternalFiles_DataQuality Load'
    system_secret_scope = 'metadatadb'
    system_order = 60
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

    onb.hydrate_platinum_zone(project_name=project_name, system_name=system_name, system_secret_scope=system_secret_scope, system_is_active=system_is_active, system_order=system_order, stage_name=stage_name, stage_is_active=stage_is_active, stage_order=10, job_name='dataqualityassessment_loadmetadatadb_dataqualityvalidationresult', job_is_active=False, job_order=10, table_name='goldprotected.dataqualityvalidationresult', destination_schema_name='staging', destination_table_name='DataQualityValidationResult', stored_procedure_name='staging.LoadDataQualityValidationResult', num_partitions=num_partitions, ignore_date_to_process_filter=ignore_date_to_process_filter, load_type=load_type, truncate_table_instead_of_drop_and_replace=truncate_table_instead_of_drop_and_replace, isolation_level=isolation_level, presentation_zone_notebook_path=presentation_zone_notebook_path)
    onb.hydrate_platinum_zone(project_name=project_name, system_name=system_name, system_secret_scope=system_secret_scope, system_is_active=system_is_active, system_order=system_order, stage_name=stage_name, stage_is_active=stage_is_active, stage_order=10, job_name='dataqualityassessment_loadmetadatadb_dataqualityvalidationresultdetail', job_is_active=False, job_order=20, table_name='goldprotected.vdataqualityvalidationresultdetail', destination_schema_name='staging', destination_table_name='DataQualityValidationResultDetail', stored_procedure_name='staging.LoadDataQualityValidationResultDetail', num_partitions=num_partitions, ignore_date_to_process_filter=ignore_date_to_process_filter, load_type=load_type, truncate_table_instead_of_drop_and_replace=truncate_table_instead_of_drop_and_replace, isolation_level=isolation_level, presentation_zone_notebook_path=presentation_zone_notebook_path)

    onb.load_orchestration_tables(project_name="UAT_ExternalFiles")

