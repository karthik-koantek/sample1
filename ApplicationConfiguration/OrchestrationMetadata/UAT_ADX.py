from multiprocessing.dummy import JoinableQueue
from os import system
from random import SystemRandom
import sys
from matplotlib.pyplot import table

import numpy
from yaml import load


def hydrate(project_name):
    import ktk
    from pyspark.sql.functions import col, create_map, lit, col, crc32, concat
    from itertools import chain
    import json

    onb = ktk.OrchestrationDeltaNotebook()
    onb.create_orchestration_staging_tables(project_name)
    
    #cvflattened
    onb.hydrate_streaming_notebook(project_name='UAT_ADX', system_name='UAT_ADX_ConnectedVehicle', system_secret_scope='internal', system_is_active=True, system_order=10, stage_name='UAT_ADX_CV', stage_is_active = True, stage_order = 10, job_name='UAT_ADX_cvflattened', job_is_active=True, job_order=10, external_system = 'AzureDataExplorer', trigger = 'Microbatch', interval = 10, stop_seconds = 120, database_name = 'connectedvehicle', table_name = 'cvflattened', output_table_name = 'cvflattened', notebook_path = '../Data Engineering/Streaming/Output to Azure Data Explorer')
    
    #noaa_noaaapi
    onb.hydrate_presentation_zone(project_name='UAT_ADX', system_name='UAT_ADX_ConnectedVehicle', system_secret_scope='AzureDataExplorer', system_is_active=True, system_order=10, stage_name='UAT_ADX_CV', stage_is_active = True, stage_order = 10, job_name='UAT_ADX_noaa_noaaapi', job_is_active=True, job_order=20, database_name = 'connectedvehicle', table_name = 'noaa_noaaapi', output_table_name = 'cvflattened', notebook_path = '../Data Engineering/Presentation Zone/Azure Data Explorer')
    
