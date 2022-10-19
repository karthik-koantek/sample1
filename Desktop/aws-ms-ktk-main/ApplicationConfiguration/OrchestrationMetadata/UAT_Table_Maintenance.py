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
    
    #UAT_Table_Maintenance_cvpayload
    onb.hydrate_spark_table_maintenance(project_name='UAT_Table_Maintenance', system_name='UAT_Table_Maintenance', system_secret_scope='internal', system_is_active=True, system_order=10, stage_name='UAT_Table_Maintenance', stage_is_active = True, stage_order = 10, job_name='UAT_Table_Maintenance_cvpayload', job_is_active=True, job_order=10, table_name = 'cvpayload', run_optimize = '1', run_vacuum = '1', optimize_where = '', optimize_z_order_by = '', vacuum_retention_hours = 168, notebook_path = '../Orchestration/Spark Table Maintenance')
