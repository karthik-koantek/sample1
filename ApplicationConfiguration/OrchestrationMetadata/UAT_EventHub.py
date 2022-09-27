def hydrate(project_name):
  import ktk
  from pyspark.sql.functions import col, create_map, lit, col, crc32, concat
  from itertools import chain
  import json
   
  onb = ktk.OrchestrationDeltaNotebook()
  onb.create_orchestration_staging_tables(project_name)

  #teardown
  parameters = {
    "dataSource" : "test",
    "hashtag" : "Databricks",
    "eventHubExternalSystem" : "EventHubTwitterIngestion",
    "twitterExternalSystem" : "TwitterAPI",
    "timeoutSeconds" : "60"
  }
  onb.hydrate_generic_notebook(project_name=project_name, system_name='UAT_EventHub_Teardown', system_secret_scope='internal', system_is_active=True, system_order=10, stage_name='UAT_EventHub_Twitter', stage_is_active = True, stage_order = 10, job_name='UAT_EventHub_twitter_databricks_hashtag_to_eventhub', job_is_active=True, job_order=10, parameters=parameters, notebook_path = '../Development/Send Tweets To Event Hub')
    
  #hydrate streaming notebook
  system_name = 'UAT_EventHub'
  stage_name = 'UAT_EventHub_Twitter'
  system_secret_scope = 'internal'
  system_is_active = True
  stage_is_active = True
  job_is_active = True
  system_order = 10
  stage_order = 10
  job_order = 20
  external_system = 'EventHubTwitterIngestion'
  trigger = 'Once'
  max_events_per_trigger = 10000
  offset = 0
  interval = 60
  stop_seconds = 120
  event_hub_starting_position = 'fromStartOfStream'
  table_name = 'databrickstweets'
  destination = 'silvergeneral'
  notebook_path = '../Data Engineering/Streaming/Ingest Event Hub'
   
  onb.hydrate_streaming_notebook(project_name = project_name, system_name = system_name, system_secret_scope = system_secret_scope, system_order = system_order, system_is_active = system_is_active, stage_name = stage_name, stage_is_active = stage_is_active, stage_order = stage_order, job_name = 'UAT_EventHub_twitter_databricks_eventhub_to_datalake', job_order = job_order, job_is_active = job_is_active, external_system = external_system, trigger = trigger, max_events_per_trigger = max_events_per_trigger, offset = offset, interval = interval, event_hub_starting_position = event_hub_starting_position,  table_name = table_name, destination = destination, stop_seconds = stop_seconds, notebook_path = notebook_path)
    
  job_order = 30
  external_system = 'CognitiveServices'
  notebook_path = '../Data Engineering/Streaming/Sentiment Analysis using Azure Cognitive Services'
  column_name = 'Body'
  output_table_name = 'databrickstweetsscored'
    
  onb.hydrate_streaming_notebook(project_name = project_name,  system_name = system_name, system_secret_scope = system_secret_scope, system_order = system_order, system_is_active = system_is_active, stage_name = stage_name, stage_is_active = stage_is_active, stage_order = stage_order, job_name = 'UAT_EventHub_twitter_databricks_sentiment_analysis', job_order = job_order, job_is_active = job_is_active, external_system = external_system, trigger = trigger, max_events_per_trigger = max_events_per_trigger, interval = interval, table_name = table_name, destination = destination, stop_seconds = stop_seconds, column_name = column_name, output_table_name = output_table_name, notebook_path = notebook_path)
  onb.update_stage_threads(project_name=project_name, stage_name=stage_name, threads=2)
    
  parameters = {
    "eventHubExternalSystem" : "EventHubCVIngestion",
    "messageCount" : "100",
    "sleepSecondsBetweenMessages" : ".5"
  }
  onb.hydrate_generic_notebook(project_name=project_name, system_name='UAT_EventHub_Teardown', system_secret_scope='internal', system_is_active=True, system_order=10, stage_name='UAT_EventHub_CV', stage_is_active = True, stage_order = 20, job_name='UAT_EventHub_CV_to_eventhub', job_is_active=True, job_order=10, parameters=parameters, notebook_path = '../Development/Send JSON Payload to Event Hub')
  onb.update_stage_threads(project_name=project_name, stage_name=stage_name, threads=2)
    
  #hydrate streaming notebook
  system_name = 'UAT_EventHub'
  stage_name = 'UAT_EventHub_CV'
  system_secret_scope = 'internal'
  system_is_active = True
  stage_is_active = True
  job_is_active = True
  system_order = 10
  stage_order = 20
  job_order = 20
  external_system = 'EventHubCVIngestion'
  trigger = 'Microbatch'
  max_events_per_trigger = 10000
  offset = -1
  interval = 10
  stop_seconds = 120
  event_hub_starting_position = 'fromStartOfStream'
  table_name = 'cvpayload'
  destination = 'silvergeneral'
  notebook_path = '../Data Engineering/Streaming/Ingest Event Hub'
   
  onb.hydrate_streaming_notebook(project_name = project_name, system_name = system_name, system_secret_scope = system_secret_scope, system_order = system_order, system_is_active = system_is_active, stage_name = stage_name, stage_is_active = stage_is_active, stage_order = stage_order, job_name = 'UAT_EventHub_CV_ingest_eventhub', job_order = job_order, job_is_active = job_is_active, external_system = external_system, trigger = trigger, max_events_per_trigger = max_events_per_trigger, offset = offset, interval = interval, event_hub_starting_position = event_hub_starting_position, table_name = table_name, destination = destination, stop_seconds = stop_seconds, output_table_name = output_table_name, notebook_path = notebook_path)
    
  job_order = 30
  offset = 0
  external_system = 'internal'
  notebook_path = '../Data Engineering/Streaming/Flatten JSON'
  column_name = 'Body'
  output_table_name = 'cvflattened'
    
  onb.hydrate_streaming_notebook(project_name = project_name,  system_name = system_name, system_secret_scope = system_secret_scope, system_order = system_order, system_is_active = system_is_active, stage_name = stage_name, stage_is_active = stage_is_active, stage_order = stage_order, job_name = 'UAT_EventHub_CV_flatten', job_order = job_order, job_is_active = job_is_active, external_system = external_system, trigger = trigger, max_events_per_trigger = max_events_per_trigger, offset = offset, interval = interval, table_name = table_name, destination = destination, stop_seconds = stop_seconds, column_name = column_name, output_table_name = output_table_name, notebook_path = notebook_path)
    
  job_order = 40
  external_system = 'EventHubCVDistribution'
  table_name = 'cvflattened'
  notebook_path = '../Data Engineering/Streaming/Output to Event Hub'
    
  onb.hydrate_streaming_notebook(project_name = project_name,  system_name = system_name, system_secret_scope = system_secret_scope, system_order = system_order, system_is_active = system_is_active, stage_name = stage_name, stage_is_active = stage_is_active, stage_order = stage_order, job_name = 'UAT_EventHub_CV_distribution_eventhub', job_order = job_order, job_is_active = job_is_active, external_system = external_system, trigger = trigger, interval = interval, table_name = table_name, destination = destination, stop_seconds = stop_seconds, notebook_path = notebook_path)
  onb.update_stage_threads(project_name=project_name, stage_name=stage_name, threads=3)