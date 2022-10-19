# Databricks notebook source
# MAGIC %run "/Shared/Soliduslabs/Utils/env_utils_just_consts"

# COMMAND ----------

dbutils.widgets.removeAll()

dbutils.widgets.dropdown(name='w_environment', defaultValue='rnd', label='Environment', choices=ALL_ENVS)
dbutils.widgets.text(name="w_sf_warehouse", defaultValue="MULTI_C_RND_LOAD", label="Warehouse")
dbutils.widgets.text(name="w_scope", defaultValue="Soliduslabs_RND", label="Scope")

widgets = [ 'w_environment', 'w_sf_warehouse', 'w_scope' ]
secrets = [ 'snowflake_user', 'snowflake_password' ]

# COMMAND ----------

for widget in widgets:
    exec("{0} = dbutils.widgets.get('{0}')".format(widget))
  
for secret in secrets:
    exec("{0} = dbutils.secrets.get(scope=\'{1}\', key=\'{0}\')".format(secret, w_scope))
    

# COMMAND ----------

# MAGIC %run "/Shared/Soliduslabs/Utils/env_utils"

# COMMAND ----------

# MAGIC %run "/Shared/Soliduslabs/Dashboard ETLs/Snowflake ETL Loader - Base"

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Errors

# COMMAND ----------

errors_sf_table = f'{get_sf_database()}.SYSTEM.ERRORS'
print(f'errors_sf_table: {errors_sf_table}')
print('---')
errors_temp_view = 'errors_usage_df'
print(f'errors_temp_view: {errors_temp_view}')
print('---')
dbsql_errors_table = '{env}.errors'.format(env=compose_dbsql_name())
print(f'dbsql_errors_table: {dbsql_errors_table}')

# COMMAND ----------

print(f'Making sure DB "{compose_dbsql_name()}" exists...')

spark.sql("""
    CREATE DATABASE IF NOT EXISTS {env};
""".format(env=compose_dbsql_name()))

print(f'Making sure table {dbsql_errors_table} exits...')
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {dbsql_errors_table} (
        solidus_client STRING,
        date_hour TIMESTAMP,
        error_code STRING,
        topic STRING,
        source STRING,
        symbol STRING,
        system_name STRING,
        message_type STRING,
        counter INT
    )
""")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Checking the latest date for which we have BI/"Gold" data for Errors

# COMMAND ----------

latest_error_date, latest_error_hour = get_latest_datestr_and_hour_to_query_dbsql_tbl(dbsql_errors_table, 'date_hour')

# COMMAND ----------

from datetime import datetime, timedelta


def compose_errors_query(latest_date: str, latest_date_hour: int) -> str:    
    # And ending in the previous hour, just so we won't get a half-filled hour when it comes to errors
    todays_date = datetime.strftime(datetime.now(), '%Y-%m-%d')
    last_hour_ts = datetime.strftime(datetime.now() - timedelta(hours=1), '%Y-%m-%d %H:00:00')
    
    print(f'Querying form date: {latest_date} & hour: {latest_date_hour} to date: {last_hour_ts}')
    
    return f"""
    SELECT  solidus_client,
            timestamp_from_parts(YEAR(date), MONTH(date), DAY(date), hour, 0, 0) date_hour,
            error_code, 
            topic, 
            source, 
            symbol, 
            system_name, 
            message_type, 
            counter
    FROM (
        /* First query / subquery filters coarsely, we'll refine the filter using the date_hour field */ 
        SELECT PARSE_JSON(PARSE_JSON(record_metadata):"key"):"solidusClient"::TEXT solidus_client,
               DATE(PARSE_JSON(record_metadata):"CreateTime") date, 
               HOUR(TO_TIMESTAMP(PARSE_JSON(record_metadata):"CreateTime")::TIMESTAMP_NTZ) hour,
               PARSE_JSON(PARSE_JSON(record_metadata):"key"):"errorCode"::TEXT error_code,
               PARSE_JSON(record_metadata):"topic"::TEXT topic,
               PARSE_JSON(record_content):"source"::TEXT source,
               PARSE_JSON(record_content):"symbol"::TEXT symbol,
               PARSE_JSON(record_content):"systemName"::TEXT system_name, 
               PARSE_JSON(record_content):"messageType"::TEXT message_type,
               COUNT(*) counter
        FROM {errors_sf_table}
        WHERE DATE(PARSE_JSON(record_metadata):"CreateTime") >= '{latest_date}'
          AND HOUR(TO_TIMESTAMP(PARSE_JSON(record_metadata):"CreateTime")::TIMESTAMP_NTZ) >= ({latest_date_hour} + 1)
          AND DATE(PARSE_JSON(record_metadata):"CreateTime") <= '{todays_date}'  
        GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9
        ORDER BY solidus_client, date DESC, HOUR DESC  
    )
    WHERE timestamp_from_parts(YEAR(date), MONTH(date), DAY(date), hour, 0, 0) <= '{last_hour_ts}'
    """

print(compose_errors_query(latest_error_date, latest_error_hour))

# COMMAND ----------

from pyspark import StorageLevel
from pyspark.sql import DataFrame

print('Running errors usage for all solidus_clients.')
    
q_errors = compose_errors_query(latest_error_date, latest_error_hour)

errors_usage_df = spark.read \
    .format("snowflake") \
    .options(**options) \
    .option("query", q_errors) \
    .load()

errors_usage_df.persist(StorageLevel.DISK_ONLY)

print(f'Errors usage count is: {str(errors_usage_df.count())}')
errors_usage_df.createOrReplaceTempView(errors_temp_view)

display(errors_usage_df)

# COMMAND ----------

insert_errors_query = f"""
    INSERT INTO {dbsql_errors_table} 
    SELECT solidus_client, 
           date_hour,
           error_code, 
           topic, 
           source, 
           symbol, 
           system_name, 
           message_type, 
           counter
    FROM {errors_temp_view};
""" 

print(insert_errors_query)

spark.sql(insert_errors_query)

# COMMAND ----------

spark.sql(f"""
    OPTIMIZE {dbsql_errors_table} zorder BY (solidus_client, date_hour)
""")
