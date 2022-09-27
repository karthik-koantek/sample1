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
# MAGIC ### Orders

# COMMAND ----------

orders_sf_table = f'{get_sf_database()}.MS.PRIVATE_ORDERS'
print(f'orders_sf_table: {orders_sf_table}')
print('---')
orders_temp_view = 'orders_usage_df'
print(f'orders_temp_view: {orders_temp_view}')
print('---')
dbsql_orders_table = '{env}.private_orders_usage'.format(env=compose_dbsql_name())
print(f'dbsql_orders_table: {dbsql_orders_table}')

# COMMAND ----------

print(f'Making sure DB "{compose_dbsql_name()}" exists...')

spark.sql("""
    CREATE DATABASE IF NOT EXISTS {env};
""".format(env=compose_dbsql_name()))

orders_tbl_name = '{env}.private_orders_usage'.format(env=compose_dbsql_name())
print(f'Making sure table {orders_tbl_name} exits...')
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {orders_tbl_name} (
        solidus_client STRING, 
        date DATE,
        client_id STRING,
        account STRING, 
        actor_id STRING, 
        side STRING, 
        symbol STRING, 
        order_status STRING, 
        order_type STRING,
        counter INT,
        min_price DECIMAL(28, 12), 
        max_price DECIMAL(28, 12),
        avg_price DECIMAL(28, 12),
        sum_price DECIMAL(28, 12),
        min_qty DECIMAL(28, 12), 
        max_qty DECIMAL(28, 12), 
        avg_qty DECIMAL(28, 12),
        sum_qty DECIMAL(28, 12), 
        min_usd_notional DECIMAL(28, 12),
        max_usd_notional DECIMAL(28, 12), 
        avg_usd_notional DECIMAL(28, 12),
        sum_usd_notional DECIMAL(28, 12),
        distinct_order_ids INT, 
        min_version INT,
        max_version INT
    )
""")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Checking the latest date for which we have BI/"Gold" data for Orders

# COMMAND ----------

latest_orders_date = get_latest_datestr_to_query_dbsql_tbl(dbsql_orders_table)

# COMMAND ----------

from datetime import datetime, timedelta


def compose_orders_usage_query(solidus_client: str, latest_date: str) -> str:
    # Adding a day as to start querying from where we DON'T yet have gold/aggregated data
    next_day_from_latest_datestr, yesterday_datestr = get_incremental_date_range(latest_date)
    print(f'Composed SF query for dates {next_day_from_latest_datestr} - {yesterday_datestr}')
    
    return f"""
    SELECT  solidus_client, 
                client_id, 
                DATE(transact_time) date,
                account, 
                actor_id, 
                side,
                symbol,
                order_status,
                order_type,
                COUNT(*) counter,
                MIN(price) min_price, 
                MAX(price) max_price, 
                AVG(price) avg_price, 
                SUM(price) sum_price, 
                MIN(order_qty) min_qty, 
                MAX(order_qty) max_qty, 
                AVG(order_qty) avg_qty, 
                SUM(order_qty) sum_qty,
                MIN(usd_notional) min_usd_notional, 
                MAX(usd_notional) max_usd_notional, 
                AVG(usd_notional) avg_usd_notional, 
                SUM(usd_notional) sum_usd_notional,
                COUNT(DISTINCT id) distinct_order_ids,
                MIN(version) min_version,
                MAX(version) max_version
        FROM {orders_sf_table}
        WHERE solidus_client = '{solidus_client}'
          AND DATE(transact_time) >= '{next_day_from_latest_datestr}'
          AND DATE(transact_time) <= '{yesterday_datestr}'
        GROUP BY solidus_client, 
                 DATE(transact_time),
                 client_id, 
                 account, 
                 actor_id, 
                 side,
                 symbol,
                 order_status,
                 order_type
    """

# print(compose_orders_usage_query('NDAX', '2022-07-01'))
# print(orders_usage_query)

# COMMAND ----------

from pyspark import StorageLevel
from pyspark.sql import DataFrame

# Appending all solidus_client data by querying it one-by-one
# This is due to the query optimization heirarchy defined in our Snowflake tables
orders_usage_df: DataFrame = None

for solidus_client in [x[0] for x in distinct_solidus_clients_df.collect()]:
    print(f'Running orders usage for solidus_client: {solidus_client}')
    
    q_orders = compose_orders_usage_query(solidus_client, latest_orders_date)

    client_specific_orders_usage_df = spark.read \
        .format("snowflake") \
        .options(**options) \
        .option("query", q_orders) \
        .load()
    
    client_specific_orders_usage_df.cache()
    print(f'{solidus_client} orders got {str(client_specific_orders_usage_df.count())} rows')

    if not orders_usage_df:
        orders_usage_df = client_specific_orders_usage_df
    else:
        orders_usage_df = orders_usage_df.union(client_specific_orders_usage_df)
        
    orders_usage_df.persist(StorageLevel.DISK_ONLY)

print(f'Orders usage count is: {str(orders_usage_df.count())}')
orders_usage_df.createOrReplaceTempView(orders_temp_view)

display(orders_usage_df)

# COMMAND ----------

insert_orders_query = f"""
    INSERT INTO {orders_tbl_name} 
    SELECT solidus_client, 
        CAST(date AS DATE),
        client_id,
        account, 
        actor_id, 
        side, 
        symbol, 
        order_status, 
        order_type,
        CAST(counter AS INT),
        CAST(min_price AS DECIMAL(28, 12)), 
        CAST(max_price AS DECIMAL(28, 12)),
        CAST(avg_price AS DECIMAL(28, 12)),
        CAST(sum_price AS DECIMAL(28, 12)),
        CAST(min_qty AS DECIMAL(28, 12)), 
        CAST(max_qty AS DECIMAL(28, 12)), 
        CAST(avg_qty AS DECIMAL(28, 12)),
        CAST(sum_qty AS DECIMAL(28, 12)), 
        CAST(min_usd_notional AS DECIMAL(28, 12)),
        CAST(max_usd_notional AS DECIMAL(28, 12)), 
        CAST(avg_usd_notional AS DECIMAL(28, 12)),
        CAST(sum_usd_notional AS DECIMAL(28, 12)),
        CAST(distinct_order_ids AS INT), 
        CAST(min_version AS INT),
        CAST(max_version AS INT)
    FROM {orders_temp_view};
""" 
print(insert_orders_query)

spark.sql(insert_orders_query)

# COMMAND ----------

spark.sql(f"""
    OPTIMIZE {orders_tbl_name} zorder BY (solidus_client, date)
""")
