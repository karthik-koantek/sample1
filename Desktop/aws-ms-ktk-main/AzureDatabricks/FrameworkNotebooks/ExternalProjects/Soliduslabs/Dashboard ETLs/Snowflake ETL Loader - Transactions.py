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
# MAGIC ### Transactions

# COMMAND ----------

txns_sf_table = f'{get_sf_database()}.TM.TRANSACTIONS'
print(f'txns_sf_table: {txns_sf_table}')
print('---')
txns_temp_view = 'transactions_usage_df'
print(f'transactions_usage_df: {txns_temp_view}')
print('---')
dbsql_txns_table = '{env}.transactions_usage'.format(env=compose_dbsql_name())
print(f'dbsql_transactions_table: {dbsql_txns_table}')

# COMMAND ----------

print(f'Making sure DB "{compose_dbsql_name()}" exists...')

spark.sql("""
    CREATE DATABASE IF NOT EXISTS {env};
""".format(env=compose_dbsql_name()))

print(f'Making sure table {dbsql_txns_table} exits...')
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {dbsql_txns_table} (
        solidus_client STRING,
        date DATE,
        client_id STRING,
        external_account_id STRING,
        internal_account_id STRING,
        actor_id STRING,
        external_account_type STRING,
        version INT,
        status STRING,
        type STRING,
        currency STRING,
        ex_venue STRING,
        counter INT,
        min_qty DECIMAL(28, 12), 
        max_qty DECIMAL(28, 12), 
        avg_qty DECIMAL(28, 12), 
        sum_qty DECIMAL(28, 12), 
        min_usd_notional DECIMAL(28, 12), 
        max_usd_notional DECIMAL(28, 12), 
        avg_usd_notional DECIMAL(28, 12), 
        sum_usd_notional DECIMAL(28, 12)
    )
""")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Checking the latest date for which we have BI/"Gold" data for Transactions

# COMMAND ----------

latest_txns_date = get_latest_datestr_to_query_dbsql_tbl(dbsql_txns_table)

# COMMAND ----------

from datetime import datetime, timedelta

def compose_txns_usage_query(solidus_client: str, latest_date: str) -> str:
    next_day_from_latest_datestr, yesterday_datestr = get_incremental_date_range(latest_date)
    print(f'Composed SF query for dates {next_day_from_latest_datestr} - {yesterday_datestr}')
    
    return f"""
    SELECT t.solidus_client,
           DATE(t.transact_time) date,
           t.client_id,
           t.external_account_id,
           t.internal_account_id,
           t.actor_id,
           t.external_account_type,
           t.version,
           t.status,
           t.type,
           t.currency,
           t.ex_venue,
           COUNT(*) counter,
           MIN(t.quantity) min_qty,
           MAX(t.quantity) max_qty,
           AVG(t.quantity) avg_qty,
           SUM(t.quantity) sum_qty,
           MIN(t.usd_notional) min_usd_notional,
           MAX(t.usd_notional) max_usd_notional,
           AVG(t.usd_notional) avg_usd_notional,
           SUM(t.usd_notional) sum_usd_notional
    FROM {txns_sf_table} t
    WHERE solidus_client = '{solidus_client}'
      AND DATE(TRANSACT_TIME) >= '{next_day_from_latest_datestr}'
      AND DATE(TRANSACT_TIME) <= '{yesterday_datestr}'
    GROUP BY t.solidus_client,
             DATE(t.transact_time),
             t.client_id,
             t.external_account_id,
             t.internal_account_id,
             t.actor_id,
             t.external_account_type,
             t.version,
             t.status,
             t.type,
             t.currency,
             t.ex_venue;
    """



# COMMAND ----------

from pyspark import StorageLevel
from pyspark.sql import DataFrame

# Appending all solidus_client data by querying it one-by-one
# This is due to the query optimization heirarchy defined in our Snowflake tables
txns_usage_df: DataFrame = None

for solidus_client in [x[0] for x in distinct_solidus_clients_df.collect()]:
    print(f'Running txns usage for solidus_client: {solidus_client}')
    
    q_execs = compose_txns_usage_query(solidus_client, latest_txns_date)

    client_specific_txns_usage_df = spark.read \
        .format("snowflake") \
        .options(**options) \
        .option("query", q_execs) \
        .load()
    
    client_specific_txns_usage_df.cache()
    print(f'{solidus_client} txns got {str(client_specific_txns_usage_df.count())} rows')

    if not txns_usage_df:
        txns_usage_df = client_specific_txns_usage_df
    else:
        txns_usage_df = txns_usage_df.union(client_specific_txns_usage_df)
        
    txns_usage_df.persist(StorageLevel.DISK_ONLY)

print(f'TXNs usage count is: {str(txns_usage_df.count())}')
txns_usage_df.createOrReplaceTempView(txns_temp_view)

display(txns_usage_df)

# COMMAND ----------

insert_txns_query = f"""
    INSERT INTO {dbsql_txns_table} 
    SELECT solidus_client, 
        CAST(date AS DATE),
        client_id,
        external_account_id,
        internal_account_id,
        actor_id,
        external_account_type,
        version,
        status,
        type,
        currency,
        ex_venue,
        CAST(counter AS INT), 
        CAST(min_qty AS DECIMAL(28, 12)), 
        CAST(max_qty AS DECIMAL(28, 12)), 
        CAST(avg_qty AS DECIMAL(28, 12)), 
        CAST(sum_qty AS DECIMAL(28, 12)), 
        CAST(min_usd_notional AS DECIMAL(28, 12)), 
        CAST(max_usd_notional AS DECIMAL(28, 12)), 
        CAST(avg_usd_notional AS DECIMAL(28, 12)), 
        CAST(sum_usd_notional AS DECIMAL(28, 12))
    FROM {txns_temp_view};
""" 
print(insert_txns_query)

spark.sql(insert_txns_query)

# COMMAND ----------

spark.sql(f"""
    OPTIMIZE {dbsql_txns_table} zorder BY (solidus_client, date)
""")
