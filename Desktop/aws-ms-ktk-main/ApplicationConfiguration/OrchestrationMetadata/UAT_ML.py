
#from multiprocessing.dummy import JoinableQueue
#from os import system
#from pickletools import optimize
#from sys import set_asyncgen_hooks
#from turtle import st
#from itsdangerous import JSONWebSignatureSerializer, json
#from matplotlib.pyplot import table
#import numpy


def hydrate(project_name):
    import ktk
    from pyspark.sql.functions import col, create_map, lit, col, crc32, concat
    from itertools import chain
    import json

    onb = ktk.OrchestrationDeltaNotebook()
    onb.create_orchestration_staging_tables(project_name)

    #teardown
    onb.hydrate_generic_notebook(project_name=project_name, system_name='UAT_ML_Teardown', system_secret_scope='internal', system_is_active=True, system_order=10, stage_name='UAT_ML_Teardown_daily', stage_is_active=True, stage_order=10, job_name='UAT_ML_Teardown', job_is_active=True, job_order=10, parameters={}, notebook_path='../Data Quality Rules Engine/UAT_ML_Teardown')
    
    #hydrate eda notebook
    system_name = 'UAT_ML_EDA'
    system_secret_scope = 'internal'
    system_order = 20
    system_is_active = True
    stage_name = 'UAT_ML_EDA_Daily'
    stage_is_active = True
    stage_order = 10
    job_is_active = True
    job_order = 10
    delta_history_minutes = '-1'
    sample_percent = '-1'

    onb.hydrate_eda_notebook(project_name=project_name, system_name=system_name, system_secret_scope=system_secret_scope, system_order=system_order, system_is_active=system_is_active, stage_name=stage_name, stage_is_active=stage_is_active, stage_order=10, job_name='UAT_ML_EDA_sandbox_bikesharingml_dataprofiling_custom', job_is_active=job_is_active, job_order=job_order, table_name='sandbox.bikesharingml', delta_history_minutes='-1', sample_percent=sample_percent, column_to_analyze='', column_to_analyze2='', notebook_path='../Exploratory Data Analysis/Data Profiling - Custom')
    onb.hydrate_eda_notebook(project_name=project_name, system_name=system_name, system_secret_scope=system_secret_scope, system_order=system_order, system_is_active=system_is_active, stage_name=stage_name, stage_is_active=stage_is_active, stage_order=10, job_name='UAT_ML_EDA_sandbox_bikesharingml_dataprofiling_custom_univariate', job_is_active=job_is_active, job_order=job_order, table_name='sandbox.bikesharingml', delta_history_minutes='-1', sample_percent=sample_percent, column_to_analyze='temp', column_to_analyze2='', notebook_path='../Exploratory Data Analysis/Data Profiling - Custom Univariate')
    onb.hydrate_eda_notebook(project_name=project_name, system_name=system_name, system_secret_scope=system_secret_scope, system_order=system_order, system_is_active=system_is_active, stage_name=stage_name, stage_is_active=stage_is_active, stage_order=10, job_name='UAT_ML_EDA_sandbox_bikesharingml_dataprofiling_custom_bivariate', job_is_active=job_is_active, job_order=job_order, table_name='sandbox.bikesharingml', delta_history_minutes='-1', sample_percent=sample_percent, column_to_analyze='temp', column_to_analyze2='cnt', notebook_path='../Exploratory Data Analysis/Data Profiling - Custom Bivariate')
    onb.hydrate_eda_notebook(project_name=project_name, system_name=system_name, system_secret_scope=system_secret_scope, system_order=system_order, system_is_active=system_is_active, stage_name=stage_name, stage_is_active=stage_is_active, stage_order=10, job_name='UAT_ML_EDA_sandbox_bikesharingml_dataprofiling_dataprep', job_is_active=job_is_active, job_order=job_order, table_name='sandbox.bikesharingml', delta_history_minutes='-1', sample_percent=sample_percent, column_to_analyze='', column_to_analyze2='', notebook_path='../Exploratory Data Analysis/Data Profiling - DataPrep')
    onb.hydrate_eda_notebook(project_name=project_name, system_name=system_name, system_secret_scope=system_secret_scope, system_order=system_order, system_is_active=system_is_active, stage_name=stage_name, stage_is_active=stage_is_active, stage_order=10, job_name='UAT_ML_EDA_sandbox_bikesharingml_dataprofiling_dataprep_univariate', job_is_active=job_is_active, job_order=job_order, table_name='sandbox.bikesharingml', delta_history_minutes='-1', sample_percent=sample_percent, column_to_analyze='temp', column_to_analyze2='', notebook_path='../Exploratory Data Analysis/Data Profiling - DataPrep Univariate')
    onb.hydrate_eda_notebook(project_name=project_name, system_name=system_name, system_secret_scope=system_secret_scope, system_order=system_order, system_is_active=system_is_active, stage_name=stage_name, stage_is_active=stage_is_active, stage_order=10, job_name='UAT_ML_EDA_sandbox_bikesharingml_dataprofiling_dataprep_bivariate', job_is_active=job_is_active, job_order=job_order, table_name='sandbox.bikesharingml', delta_history_minutes='-1', sample_percent=sample_percent, column_to_analyze='temp', column_to_analyze2='cnt', notebook_path='../Exploratory Data Analysis/Data Profiling - DataPrep Bivariate')
    onb.hydrate_eda_notebook(project_name=project_name, system_name=system_name, system_secret_scope=system_secret_scope, system_order=system_order, system_is_active=system_is_active, stage_name=stage_name, stage_is_active=stage_is_active, stage_order=10, job_name='UAT_ML_EDA_sandbox_adult_dataprofiling_pandas_profiling', job_is_active=job_is_active, job_order=job_order, table_name='sandbox.adult', delta_history_minutes='-1', sample_percent=sample_percent, column_to_analyze='', column_to_analyze2='', notebook_path='../Exploratory Data Analysis/Data Profiling - Pandas Profiling')

    #hydrate experiment training
    system_name='UAT_ML_Train'
    system_secret_scope='internal'
    system_order = 30
    system_is_active = True
    stage_name = 'UAT_ML_Train_Daily'
    stage_is_active = True
    stage_order = 10
    job_order = 10
    experiment_id = ''
    continuous_columns = ''
    regularization_parameters = '0.1, 0.01'
    prediction_column = 'prediction'
    folds = '2'
    train_test_split = '0.7, 0.3'

    onb.hydrate_ML_experiment_training(project_name=project_name, system_name=system_name, system_secret_scope=system_secret_scope, system_order=system_order, system_is_active=system_is_active, stage_name=stage_name, stage_is_active=stage_is_active, stage_order=stage_order, job_name='UAT_ML_Train_sandbox_bikesharingml_linear_regression', job_order=job_order, job_is_active=True, table_name='sandbox.bikesharingml', experiment_name= 'UAT_ML Linear Regression for sandbox_bikesharingml', experiment_id=experiment_id, excluded_columns='instant,dteday,temp,casual,registered', label='cnt', continuous_columns=continuous_columns, regularization_parameters=regularization_parameters, prediction_column=prediction_column, folds=folds, train_test_split=train_test_split, notebook_path='../ML/spark/Train Linear Regression')
    onb.hydrate_ML_experiment_training(project_name=project_name, system_name=system_name, system_secret_scope=system_secret_scope, system_order=system_order, system_is_active=system_is_active, stage_name=stage_name, stage_is_active=stage_is_active, stage_order=stage_order, job_name='UAT_ML_Train_sandbox_bikesharingml_random_forest_regressor', job_order=job_order, job_is_active=False, table_name='sandbox.bikesharingml', experiment_name= 'UAT_ML Random Forest Regressor for sandbox_bikesharingml', experiment_id=experiment_id, excluded_columns='instant,dteday,temp,casual,registered', label='cnt', continuous_columns=continuous_columns, regularization_parameters=regularization_parameters, prediction_column=prediction_column, folds=folds, train_test_split=train_test_split, notebook_path='../ML/spark/Train Random Forest Regressor')
    onb.hydrate_ML_experiment_training(project_name=project_name, system_name=system_name, system_secret_scope=system_secret_scope, system_order=system_order, system_is_active=system_is_active, stage_name=stage_name, stage_is_active=stage_is_active, stage_order=stage_order, job_name='UAT_ML_Train_sandbox_bikesharingml_xgboost_regressor', job_order=job_order, job_is_active=False, table_name='sandbox.bikesharingml', experiment_name= 'UAT_ML XGBoost Regressor for sandbox_bikesharingml', experiment_id=experiment_id, excluded_columns='instant,dteday,temp,casual,registered', label='cnt', continuous_columns=continuous_columns, regularization_parameters=regularization_parameters, prediction_column=prediction_column, folds=folds, train_test_split=train_test_split, notebook_path='../ML/spark/Train XGBoost Regressor')
    onb.hydrate_ML_experiment_training(project_name=project_name, system_name=system_name, system_secret_scope=system_secret_scope, system_order=system_order, system_is_active=system_is_active, stage_name=stage_name, stage_is_active=stage_is_active, stage_order=stage_order, job_name='UAT_ML_Train_sandbox_adult_logistic_regression', job_order=job_order, job_is_active=True, table_name='sandbox.adult', experiment_name= 'UAT_ML Logistic Regression for sandbox_adult', experiment_id=experiment_id, excluded_columns='', label='income', continuous_columns=continuous_columns, regularization_parameters=regularization_parameters, prediction_column=prediction_column, folds=folds, train_test_split=train_test_split, notebook_path='../ML/spark/Train Logistic Regression')

    #hydrate ML Batch Inference
    system_name = 'UAT_ML_Predict'
    system_secret_scope = 'internal'
    system_order = 40
    system_is_active = True
    stage_name = 'UAT_ML_Predict_Daily'
    stage_is_active = True
    stage_order = 10
    job_is_active = True
    experiment_notebook_path = '/Framework/ML/spark/Train Linear Regression'
    experiment_id = ''
    metric_clause = 'metrics.avg_rmse DESC'
    table_name = 'sandbox.bikesharingml'
    excluded_columns = 'instant,dteday,temp,casual,registered'
    destination = 'silvergeneral'

    #ML batch inference
    onb.hydrate_ML_batch_inference(project_name=project_name, system_name=system_name, system_secret_scope=system_secret_scope, system_order=system_order, system_is_active=system_is_active, stage_name=stage_name, stage_is_active=stage_is_active, stage_order=stage_order, job_name='UAT_ML_Predict_sandbox_bikesharingml_batch_inference', job_order=30, job_is_active=job_is_active, experiment_notebook_path=experiment_notebook_path, experiment_id=experiment_id, metric_clause=metric_clause, table_name=table_name, delta_history_minutes=-1, excluded_columns=excluded_columns, partitions='8', primary_columns='', destination_table_name='uat_bikesharingpredictions', partition_col='', cluster_col='pk', optimize_where='', optimize_z_order_by='', vacuum_retention_hours=168, load_type='Overwrite', destination=destination, notebook_path='../ML/Batch Inference')
    #ML streaming inference
    onb.hydrate_ML_streaming_inference(project_name=project_name, system_name=system_name, system_secret_scope=system_secret_scope, system_order=system_order, system_is_active=system_is_active, stage_name=stage_name, stage_is_active=stage_is_active, stage_order=stage_order, job_name='UAT_ML_Predict_sandbox_bikesharingml_streaming_inference', job_order=40, job_is_active=job_is_active, experiment_notebook_path=experiment_notebook_path, experiment_id=experiment_id, metric_clause=metric_clause, table_name=table_name, excluded_columns=excluded_columns, destination_table_name='uat_bikesharingpredictionsstreaming', destination=destination, notebook_path='../ML/Streaming Inference')

    #hydrate data quality assessment
    system_name = 'UAT_ML_DataQuality'
    system_secret_scope = 'internal'
    system_is_active = True
    system_order = 50
    stage_name = 'UAT_ML_DataQuality_Silver_Zone_QC'
    stage_is_active = True
    stage_order = 10
    job_is_active = True
    job_order = 10
    table_name = 'silvergeneral.uat_bikesharingpredictions'
    delta_history_minutes = -1
    notebook_path = '../Data Quality Rules Engine/Data Quality Assessment'

    onb.hydrate_data_quality_assessment(project_name=project_name, system_name=system_name, system_secret_scope=system_secret_scope, system_is_active=system_is_active, system_order=system_order, stage_name=stage_name, stage_is_active=stage_is_active, stage_order=stage_order, job_name='UAT_ML_Silver_Zone_QC_dataqualityassessment_silver_uat_bikesharingpredictions', job_is_active=job_is_active, job_order=job_order, table_name='silvergeneral.uat_bikesharingpredictions', delta_history_minutes=delta_history_minutes, notebook_path=notebook_path)
    onb.hydrate_data_quality_assessment(project_name=project_name, system_name=system_name, system_secret_scope=system_secret_scope, system_is_active=system_is_active, system_order=system_order, stage_name=stage_name, stage_is_active=stage_is_active, stage_order=stage_order, job_name='UAT_ML_Silver_Zone_QC_dataqualityassessment_silver_uat_bikesharingpredictionsstreaming', job_is_active=job_is_active, job_order=job_order, table_name='silvergeneral.uat_bikesharingpredictionsstreaming', delta_history_minutes=delta_history_minutes, notebook_path=notebook_path)

    #hyrdate platinum zone
    system_name = 'UAT_ML_DataQuality Load'
    system_secret_scope = 'metadatadb'
    system_is_active = True
    system_order = 60
    stage_name = 'UAT_ML Data Quality Assessment Load to Metadata DB'
    stage_is_active = True
    stage_order = 10
    job_is_active = True
    destination_schema_name = 'staging'
    num_partitions = '8'
    ignore_date_to_process_filter = 'true'
    load_type = 'overwrite'
    truncate_table_instead_of_drop_and_replace = 'false'
    isolation_level = 'READ_COMMITTED'
    presentation_zone_notebook_path = '../Data Engineering/Presentation Zone/SQL Spark Connector 3'

   # onb.hydrate_platinum_zone(project_name=project_name, system_name=system_name, system_secret_scope=system_secret_scope, system_is_active=system_is_active, system_order=system_order, stage_name=stage_name, stage_is_active=stage_is_active, stage_order=stage_order, job_name='UAT_ML_dataqualityassessment_loadmetadatadb_dataqualityvalidationresult', job_is_active=job_is_active, job_order=10, table_name='goldprotected.dataqualityvalidationresult', destination_schema_name='staging', destination_table_name='DataQualityValidationResult', stored_procedure_name='staging.LoadDataQualityValidationResult', num_partitions=num_partitions, ignore_date_to_process_filter=ignore_date_to_process_filter, load_type=load_type, truncate_table_instead_of_drop_and_replace=truncate_table_instead_of_drop_and_replace, isolation_level=isolation_level, presentation_zone_notebook_path=presentation_zone_notebook_path)
   # onb.hydrate_platinum_zone(project_name=project_name, system_name=system_name, system_secret_scope=system_secret_scope, system_is_active=system_is_active, system_order=system_order, stage_name=stage_name, stage_is_active=stage_is_active, stage_order=stage_order, job_name='UAT_ML_dataqualityassessment_loadmetadatadb_dataqualityvalidationresultdetail', job_is_active=job_is_active, job_order=20, table_name='goldprotected.vdataqualityvalidationresultdetail', destination_schema_name='staging', destination_table_name='DataQualityValidationResultDetail', stored_procedure_name='staging.LoadDataQualityValidationResultDetail', num_partitions=num_partitions, ignore_date_to_process_filter=ignore_date_to_process_filter, load_type=load_type, truncate_table_instead_of_drop_and_replace=truncate_table_instead_of_drop_and_replace, isolation_level=isolation_level, presentation_zone_notebook_path=presentation_zone_notebook_path)
    onb.load_orchestration_tables(project_name="UAT_ML")

