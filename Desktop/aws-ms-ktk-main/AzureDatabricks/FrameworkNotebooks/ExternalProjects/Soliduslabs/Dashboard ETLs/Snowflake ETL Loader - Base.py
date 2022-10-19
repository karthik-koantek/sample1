# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ## Utilities for BI load ETLs

# COMMAND ----------

# MAGIC %run "/Shared/Soliduslabs/Utils/env_utils"

# COMMAND ----------

options = {
  "sfUrl": get_sf_url(),
  "sfUser": snowflake_user,
  "sfPassword": snowflake_password,
  "sfDatabase": get_sf_database(),
  "sfWarehouse": w_sf_warehouse,
  "sfRole": 'DEV_READER',
}

print(options)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Getting Distinct Solidus Clients (From Snowflake)
# MAGIC This is because of how our table partitioning / clustering works and it's much more efficient to query the tables on our solidus_clients one-by-one

# COMMAND ----------

distinct_client_ids_orders_query = f"""
    SELECT DISTINCT solidus_client
    FROM (
        SELECT DISTINCT solidus_client 
        FROM {get_sf_database()}.MS.PRIVATE_ORDERS 
        WHERE solidus_client NOT IN ('INTEGRATION_CLIENT', 'DEFAULT_CLIENT', 'TEST_CLIENT')

          UNION ALL

        SELECT DISTINCT solidus_client 
        FROM {get_sf_database()}.MS.PRIVATE_EXECUTIONS
        WHERE solidus_client NOT IN ('INTEGRATION_CLIENT', 'DEFAULT_CLIENT', 'TEST_CLIENT')

          UNION ALL

        SELECT DISTINCT solidus_client 
        FROM {get_sf_database()}.TM.TRANSACTIONS
        WHERE solidus_client NOT IN ('INTEGRATION_CLIENT', 'DEFAULT_CLIENT', 'TEST_CLIENT')
      )
"""

print(distinct_client_ids_orders_query)

# COMMAND ----------

from pyspark import StorageLevel

distinct_solidus_clients_df = spark.read\
    .format("snowflake") \
    .options(**options) \
    .option("query", distinct_client_ids_orders_query) \
    .load()

distinct_solidus_clients_df.createOrReplaceTempView('distinct_solidus_clients')
# orders_usage_df.persist(StorageLevel.DISK_ONLY)

# print(f'Orders usage count is: {str(orders_usage_df.count())}')

distinct_solidus_clients_df.cache()
display(distinct_solidus_clients_df)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Utility functions

# COMMAND ----------

def get_latest_datestr_to_query_dbsql_tbl(tbl_name: str) -> str:
    latest_date_df = spark.sql(f"""
        SELECT MAX(date) FROM {tbl_name}
    """).collect()

    # Even if result is empty, response will not be null, looking like this: [Row(max(date)=None)]
    latest_date = str(latest_date_df[0][0]) if latest_date_df[0][0] else '2000-01-01'

    print(f'latest_date in table {tbl_name}: {latest_date}')
    
    return latest_date

# COMMAND ----------

from datetime import datetime

def get_latest_datestr_and_hour_to_query_dbsql_tbl(tbl_name: str, date_ts_col: str) -> str:
    latest_date_df = spark.sql(f"""
        SELECT MAX({date_ts_col}) max_date
        FROM {tbl_name}
    """).collect()

    # Even if result is empty, response will not be null, looking like this: [Row(max(date)=None)]
    latest_date_and_hour_str = str(latest_date_df[0][0]) if latest_date_df[0][0] else '2000-01-01 00:00:00.000'
    lastest_date_and_hour = datetime.strptime(latest_date_and_hour_str, '%Y-%m-%d %H:%M:%S.%f')

    print(f'latest_date in table {tbl_name}: {lastest_date_and_hour}')
    
    # Breaking the date into date and hour
    return datetime.strftime(lastest_date_and_hour, '%Y-%m-%d'), int(datetime.strftime(lastest_date_and_hour, '%H'))

# COMMAND ----------

from datetime import datetime, timedelta
from typing import Tuple

# 
def get_incremental_date_range(latest_date: str) -> Tuple[str, str]: 
    # Adding a day as to start querying from where we DON'T yet have gold/aggregated data
    next_day_from_latest_dt = datetime.strptime(latest_date, '%Y-%m-%d') + timedelta(days=1)
    next_day_from_latest_datestr = datetime.strftime(next_day_from_latest_dt, '%Y-%m-%d')
    
    yesterday_datestr = datetime.strftime(datetime.now() - timedelta(days=1), '%Y-%m-%d')
    
    return next_day_from_latest_datestr, yesterday_datestr

# COMMAND ----------


