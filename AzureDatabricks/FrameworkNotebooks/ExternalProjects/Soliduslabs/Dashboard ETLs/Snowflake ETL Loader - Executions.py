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

# MAGIC %run "/Shared/Soliduslabs/Dashboard ETLs/Snowflake ETL Loader - Base"

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Executions

# COMMAND ----------

execs_sf_table = f'{get_sf_database()}.MS.PRIVATE_EXECUTIONS'
print(f'execs_sf_table: {execs_sf_table}')
print('---')
orders_sf_table = f'{get_sf_database()}.MS.PRIVATE_ORDERS'
print(f'orders_sf_table (for joins): {orders_sf_table}')
print('---')
execs_temp_view = 'execs_usage_df'
print(f'execs_temp_view: {execs_temp_view}')
print('---')
dbsql_execs_table = '{env}.private_executions_usage'.format(env=compose_dbsql_name())
print(f'dbsql_execs_table: {dbsql_execs_table}')

# COMMAND ----------

print(f'Making sure DB "{compose_dbsql_name()}" exists...')

spark.sql("""
    CREATE DATABASE IF NOT EXISTS {env};
""".format(env=compose_dbsql_name()))

print(f'Making sure table {dbsql_execs_table} exits...')
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {dbsql_execs_table} (
        solidus_client STRING,
        date DATE,
        sell_client_id STRING,
        sell_account STRING,
        sell_actor_id STRING,
        buy_client_id STRING,
        buy_account STRING,
        buy_actor_id STRING,
        execution_type STRING,
        status STRING,
        symbol STRING,
        ex_venue STRING,
        counter INT,
        min_exec_price DECIMAL(28, 12), 
        max_exec_price DECIMAL(28, 12), 
        avg_exec_price DECIMAL(28, 12), 
        min_exec_qty DECIMAL(28, 12), 
        max_exec_qty DECIMAL(28, 12), 
        avg_exec_qty DECIMAL(28, 12), 
        sum_exec_qty DECIMAL(28, 12), 
        min_usd_notional DECIMAL(28, 12), 
        max_usd_notional DECIMAL(28, 12), 
        avg_usd_notional DECIMAL(28, 12),
        sum_usd_notional DECIMAL(28, 12), 
        min_sell_price DECIMAL(28, 12),
        max_sell_price DECIMAL(28, 12),
        avg_sell_price DECIMAL(28, 12),
        min_buy_price DECIMAL(28, 12),
        max_buy_price DECIMAL(28, 12),
        avg_buy_price DECIMAL(28, 12)
    )
""")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Checking the latest date for which we have BI/"Gold" data for Executions

# COMMAND ----------

latest_execs_date = get_latest_datestr_to_query_dbsql_tbl(dbsql_execs_table)

# COMMAND ----------

from datetime import datetime, timedelta

def compose_executions_usage_query(solidus_client: str, latest_date: str) -> str:
    # Adding a day as to start querying from where we DON'T yet have gold/aggregated data
    next_day_from_latest_datestr, yesterday_datestr = get_incremental_date_range(latest_date)
    print(f'Composed SF query for dates {next_day_from_latest_datestr} - {yesterday_datestr}')
    
    return f"""
    SELECT e.solidus_client,
           DATE(e.transact_time) date,
           o1.client_id sell_client_id,
           o1.account sell_account,
           o1.actor_id sell_actor_id,
           o2.client_id buy_client_id,
           o2.account buy_account,
           o2.actor_id buy_actor_id,
           e.execution_type,
           e.status,
           e.symbol,
           e.ex_venue,
           COUNT(*) counter,
           MIN(e.price) min_exec_price,
           MAX(e.price) max_exec_price,
           AVG(e.price) avg_exec_price,
           MIN(e.quantity) min_exec_qty,
           MAX(e.quantity) max_exec_qty,
           AVG(e.quantity) avg_exec_qty,
           SUM(e.quantity) sum_exec_qty,
           MIN(e.usd_notional) min_usd_notional,
           MAX(e.usd_notional) max_usd_notional,
           AVG(e.usd_notional) avg_usd_notional,
           SUM(e.usd_notional) sum_usd_notional,
           MIN(o1.price) min_sell_price,
           MAX(o1.price) max_sell_price,
           AVG(o1.price) avg_sell_price,
           MIN(o2.price) min_buy_price,
           MAX(o2.price) max_buy_price,
           AVG(o2.price) avg_buy_price
    FROM {execs_sf_table} e
    LEFT OUTER JOIN {orders_sf_table} o1
    ON  e.order_id = o1.id
    AND e.order_version = o1.version
    AND o1.solidus_client = '{solidus_client}'
    AND DATE(o1.transact_time) >= '{next_day_from_latest_datestr}'
    AND DATE(o1.transact_time) <= '{yesterday_datestr}'
    AND o1.side = 'Sell'
    LEFT OUTER JOIN {orders_sf_table} o2
    ON  e.matching_order_id = o2.id
    AND e.matching_order_version = o2.version
    AND o2.solidus_client = '{solidus_client}'
    AND DATE(o2.transact_time) >= '{next_day_from_latest_datestr}'
    AND DATE(o2.transact_time) <= '{yesterday_datestr}'
    AND (o2.side = 'Buy' OR e.execution_type <> 'EXCHANGE')
    WHERE e.solidus_client = '{solidus_client}'
      AND DATE(e.transact_time) >= '{next_day_from_latest_datestr}'
      AND DATE(e.transact_time) <= '{yesterday_datestr}'
    GROUP BY e.solidus_client,
             DATE(e.transact_time),
             o1.client_id,
             o1.account,
             o1.actor_id,
             o2.client_id,
             o2.account,
             o2.actor_id,
             e.execution_type,
             e.status,
             e.symbol,
             e.ex_venue;
    """

# print(orders_usage_query)

# COMMAND ----------

from pyspark import StorageLevel
from pyspark.sql import DataFrame

# Appending all solidus_client data by querying it one-by-one
# This is due to the query optimization heirarchy defined in our Snowflake tables
execs_usage_df: DataFrame = None

for solidus_client in [x[0] for x in distinct_solidus_clients_df.collect()]:
    print(f'Running execs usage for solidus_client: {solidus_client}')
    
    q_execs = compose_executions_usage_query(solidus_client, latest_execs_date)

    client_specific_execs_usage_df = spark.read \
        .format("snowflake") \
        .options(**options) \
        .option("query", q_execs) \
        .load()
    
    client_specific_execs_usage_df.cache()
    print(f'{solidus_client} execs got {str(client_specific_execs_usage_df.count())} rows')

    if not execs_usage_df:
        execs_usage_df = client_specific_execs_usage_df
    else:
        execs_usage_df = execs_usage_df.union(client_specific_execs_usage_df)
        
    execs_usage_df.persist(StorageLevel.DISK_ONLY)

print(f'Executions usage count is: {str(execs_usage_df.count())}')
execs_usage_df.createOrReplaceTempView(execs_temp_view)

display(execs_usage_df)

# COMMAND ----------

insert_execs_query = f"""
    INSERT INTO {dbsql_execs_table} 
    SELECT solidus_client, 
        CAST(date AS DATE),
        sell_client_id,
        sell_account,
        sell_actor_id,
        buy_client_id,
        buy_account,
        buy_actor_id,
        execution_type,
        status,
        symbol,
        ex_venue,
        CAST(counter AS INT),
        CAST(min_exec_price AS DECIMAL(28, 12)), 
        CAST(max_exec_price AS DECIMAL(28, 12)), 
        CAST(avg_exec_price AS DECIMAL(28, 12)), 
        CAST(min_exec_qty AS DECIMAL(28, 12)), 
        CAST(max_exec_qty AS DECIMAL(28, 12)), 
        CAST(sum_exec_qty AS DECIMAL(28, 12)), 
        CAST(min_usd_notional AS DECIMAL(28, 12)), 
        CAST(min_exec_price AS DECIMAL(28, 12)), 
        CAST(max_usd_notional AS DECIMAL(28, 12)), 
        CAST(avg_usd_notional AS DECIMAL(28, 12)),
        CAST(sum_usd_notional AS DECIMAL(28, 12)), 
        CAST(min_sell_price AS DECIMAL(28, 12)),
        CAST(max_sell_price AS DECIMAL(28, 12)),
        CAST(avg_sell_price AS DECIMAL(28, 12)),
        CAST(min_buy_price AS DECIMAL(28, 12)),
        CAST(max_buy_price AS DECIMAL(28, 12)),
        CAST(avg_buy_price AS DECIMAL(28, 12))        
    FROM {execs_temp_view};
""" 
print(insert_execs_query)

spark.sql(insert_execs_query)

# COMMAND ----------

spark.sql(f"""
    OPTIMIZE {dbsql_execs_table} zorder BY (solidus_client, date)
""")
