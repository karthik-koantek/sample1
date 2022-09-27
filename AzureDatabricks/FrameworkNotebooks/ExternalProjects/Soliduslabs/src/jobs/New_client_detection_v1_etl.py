# Databricks notebook source
# MAGIC %md 
# MAGIC ####Creating Parameters Widgets

# COMMAND ----------

dbutils.widgets.text(name="solidus_client", defaultValue="DEFAULT_CLIENT", label="solidus_client")
dbutils.widgets.text(name="env", defaultValue="rnd", label="env")
dbutils.widgets.text(name="job_exec_name", defaultValue="TEST_CLIENT_rmf", label="job_exec_name")
dbutils.widgets.text(name="job_args", defaultValue="", label="job_args")
dbutils.widgets.text(name="externalSystem", defaultValue="", label="External System")
dbutils.widgets.dropdown(name="destination", defaultValue="silvergeneral", choices=["silverprotected", "silvergeneral"], label="Destination")
dbutils.widgets.text(name="schemaName", defaultValue="", label="Table Schema Name")
dbutils.widgets.text(name="tableName", defaultValue="", label="Table Name")

# COMMAND ----------

widgets = ["solidus_client","env","job_exec_name","job_args","externalSystem","destination","schemaName","tableName"]
for widget in widgets:
    exec("{0} = dbutils.widgets.get('{0}')".format(widget))

# COMMAND ----------

job_args=job_args[1:-1]
job_args_dict = dict(map(lambda x: x.split('='), job_args.split('", "')))

# COMMAND ----------

# MAGIC %md 
# MAGIC ####Importing libraries and other dependancies

# COMMAND ----------

#from pyspark.sql import SparkSession, DataFrame
from pyspark.storagelevel import StorageLevel
from pyspark.sql import functions as f
from pyspark.sql import types as t
from typing import Dict, List, Any
import math

# COMMAND ----------

# MAGIC %run ../jobs/import_library

# COMMAND ----------

# MAGIC %md 
# MAGIC ####Setting up dictionary parameters keys

# COMMAND ----------

# input parameter keys
EXEC_TIME = 'exec_time_millis'
DAYS_BACK = 'days_back'
UCV_BATCH_SIZE = 'ucv_batch_size'
AUTH_SERVICE_MASTER_PASSWORD = 'auth_service_master_password'
AUTH_SERVICE_URL = 'auth_service_url'
CLIENT_STORE_SERVICE_URL = 'client_store_service_url'

# consts
CLIENT_ID_CS = 'client_id_cs'
CLIENT_ID_SF = 'client_id_sf'
CLIENT_ID = 'client_id'

ACCOUNT_IDS_CS = 'account_ids_cs'
ACCOUNT_IDS_SF = 'account_ids_sf'
ACCOUNT_IDS = 'account_ids'

# COMMAND ----------

# MAGIC %md
# MAGIC ####Arguement Validation

# COMMAND ----------

# Validating the job arguements.
def validate_job_args( **kwargs: Dict[str, Any]) -> None:
    assert kwargs.get(AUTH_SERVICE_MASTER_PASSWORD), 'error - Master password is null'
    verify_number(kwargs.get(EXEC_TIME))
    verify_number(kwargs.get(DAYS_BACK))
    verify_number(kwargs.get(UCV_BATCH_SIZE))
    verify_url(kwargs.get(AUTH_SERVICE_URL))
    verify_url(kwargs.get(CLIENT_STORE_SERVICE_URL))     

# COMMAND ----------

# MAGIC %md 
# MAGIC ####Fetching pagewise data from API endpoint and return integrated dataframe

# COMMAND ----------

# Create a query and get the data from snowflake 

    
def compose_existing_client_df_across_pages( sys_token: str,
                                         number_of_pages_to_fetch: int, ucv_batch_size: int,
                                         ucv_client_account_fetcher: UcvClientAccountFetcher):

    existing_client_account_pairs = None
    
    # Getting batch of data from every page.
    
    for page_num in range(number_of_pages_to_fetch):
        response: DataFrame = ucv_client_account_fetcher.fetch(
            sys_token, page_num, ucv_batch_size, as_spark_df=True, spark=spark
        )
        
        if not existing_client_account_pairs:
            existing_client_account_pairs = response
        else:
            existing_client_account_pairs = existing_client_account_pairs.union(response)

    # If response is None, construct empty DataFrame to avoid NullPointers
    if not existing_client_account_pairs or existing_client_account_pairs.count() == 0:
        existing_client_account_pairs = spark.createDataFrame([], get_client_account_df_schema())

    existing_client_account_pairs.persist(StorageLevel.DISK_ONLY)

    return existing_client_account_pairs

# COMMAND ----------

# MAGIC %md
# MAGIC ####Detecting new Identifiers by comparing snowflake and client-store data

# COMMAND ----------

    
def detect_new_identifiers( existing_client_account_pairs, identifier_detection_df) -> DataFrame:
    """
    :param existing_client_account_pairs:
    Dataframe from client-store containing client_id [str] and account_ids [List[str]]
    :param identifier_detection_df:
    Dataframe from Snowflake containing client_id [str] and account_ids [List[str]]
    :return:
    DataFrame with the same fields (client_id & account_ids) however this dataframe only contains the data
    that is required to be updated within the client-store
    """
    client_store_df = existing_client_account_pairs\
        .withColumnRenamed(CLIENT_ID, CLIENT_ID_CS)\
        .withColumnRenamed(ACCOUNT_IDS, ACCOUNT_IDS_CS)

    snowflake_df = identifier_detection_df \
        .withColumnRenamed(CLIENT_ID, CLIENT_ID_SF) \
        .withColumnRenamed(ACCOUNT_IDS, ACCOUNT_IDS_SF)

    joined_df = client_store_df\
        .join(snowflake_df, client_store_df[CLIENT_ID_CS] == snowflake_df[CLIENT_ID_SF], "full")\
        .withColumn(CLIENT_ID, f.coalesce(CLIENT_ID_CS, CLIENT_ID_SF)) \
        .withColumn('exists_cs', ~f.isnull(CLIENT_ID_CS)) \
        .withColumn('exists_sf', ~f.isnull(CLIENT_ID_SF)) \
        .drop(CLIENT_ID_CS, CLIENT_ID_SF)

    to_insert = joined_df\
        .withColumn(ACCOUNT_IDS_SF, f.coalesce(f.col(ACCOUNT_IDS_SF), f.array()))\
        .withColumn(ACCOUNT_IDS_CS, f.coalesce(f.col(ACCOUNT_IDS_CS), f.array()))\
        .withColumn(ACCOUNT_IDS, f.array_except(f.col(ACCOUNT_IDS_SF), f.col(ACCOUNT_IDS_CS)))\
        .withColumn(ACCOUNT_IDS, f.coalesce(f.col(ACCOUNT_IDS), f.array()))

    to_insert = to_insert.drop(ACCOUNT_IDS_CS, ACCOUNT_IDS_SF)\
        .filter(
            (f.col('exists_sf') & ~f.col('exists_cs'))
            | (f.size(f.col(ACCOUNT_IDS)) > f.lit(0))
        )\
        .select(CLIENT_ID, ACCOUNT_IDS)

    return to_insert

# COMMAND ----------

# MAGIC %md
# MAGIC ####Return account_df_schema

# COMMAND ----------

# Returns struct schema in order to create empty existing_client_account_pairs dataframe to avoid null pointer exception.
def get_client_account_df_schema():
    
    return t.StructType([
        t.StructField('client_id', t.StringType(), False),
        t.StructField('account_ids', t.ArrayType(f.StringType()), True),
    ])

# COMMAND ----------

# MAGIC %md
# MAGIC ####Fetch Auth Token using api

# COMMAND ----------


# Fetching sys token for client store using fetch_master_password method from master_password_fetcher class
def fetch_sys_token( master_password_fetcher: AuthMasterPasswordFetcher, master_password: str) -> str:
    return master_password_fetcher.fetch_master_password(
        solidus_client, master_password)

# COMMAND ----------

validate_job_args(**job_args_dict)

# COMMAND ----------

# laying out the params
auth_url: str = job_args_dict.get(AUTH_SERVICE_URL)
client_store_url: str = job_args_dict.get(CLIENT_STORE_SERVICE_URL)
exec_time_millis: float = job_args_dict[EXEC_TIME]
days_back: int = int(job_args_dict[DAYS_BACK])
ucv_batch_size: int = int(job_args_dict[UCV_BATCH_SIZE])

# COMMAND ----------

# MAGIC %md 
# MAGIC ####Object Instantiation for all api classes

# COMMAND ----------

# Setting up auth-service's token fetcher (will be used multiple times) as well as other data fetchers
print("Setting up auth-service's token fetcher (will be used multiple times) as well as other data fetchers")
master_password_fetcher = AuthMasterPasswordFetcher(auth_url)
auth_me_info = AuthMeEndpointFetcher(auth_url)

# COMMAND ----------

# client-store endpoints
print("client-store endpoints")
ucv_application_counter = UcvApplicationCounter(client_store_url)
ucv_client_account_fetcher = UcvClientAccountFetcher(client_store_url)
ucv_client_account_updater = UcvClientAccountUpdater(client_store_url)

# COMMAND ----------

# Getting Snowflake configs based on env
print("Getting Snowflake configs based on env")
sf_config: SnowflakeConfig = sf_env_configs[env]

# COMMAND ----------

# Generating token for using endpoint
print("Generating token for using endpoint")
sys_token = fetch_sys_token(master_password_fetcher, str(job_args_dict.get(AUTH_SERVICE_MASTER_PASSWORD)))

# COMMAND ----------

# Step #1 - Get EXISTING client_ids / account_ids count
print("Step #1 - Get EXISTING client_ids / account_ids count")
existing_ucv_entries_count = ucv_application_counter.count_applications(sys_token)

# Getting number of pages to fetch by dividing total new entries by batch size
print("Getting number of pages to fetch by dividing total new entries by batch size")
number_of_pages_to_fetch = math.ceil(existing_ucv_entries_count / int(ucv_batch_size))

#Getting values of existing client IDs/ Account IDs 
print("Getting values of existing client IDs/ Account IDs ")
existing_client_account_pairs = compose_existing_client_df_across_pages(sys_token, number_of_pages_to_fetch, ucv_batch_size, ucv_client_account_fetcher)

# COMMAND ----------

# Query Snowflake 
# Step #2 -Creating a query and fetching the data from snowflake data warehouse for NEW client_ids / account_ids

silverZoneTableName = "{0}.{1}_{2}_{3}".format(destination, externalSystem, schemaName, tableName)

identifier_detection_df = spark.sql(f"select CLIENT_ID,ACCOUNTS from {silverZoneTableName}").withColumn('ACCOUNTS', f.split(f.regexp_replace(f.col('ACCOUNTS'), '\n| |"|\\[|\\]', ""), ',')).withColumnRenamed('CLIENT_ID', 'client_id').withColumnRenamed('ACCOUNTS', 'account_ids')



identifier_detection_df.persist(StorageLevel.DISK_ONLY)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Pushing the data using api on client store https://client-store.env.soliduslabs.app

# COMMAND ----------


    
# Step #3 - Join the existing data from our client-store with data from Snowflake
#   Data that's ONLY seen in Snowflake => new client_ids/scv_applications that need to be inserted
#   Existing client_ids with never before seen account_ids => The account IDs need to be introduced
#   Result is all the data that needs to be sent to the client-store, whether new client_ids, or existing
#   client_ids with new account_ids

data_to_insert = detect_new_identifiers(existing_client_account_pairs, identifier_detection_df)

# If there is no data to insert simply return
if data_to_insert.count() == 0:
    pass
    
sys_user_id = auth_me_info.fetch(sys_token)['id']

ucv_client_account_updater.bulk_insert_customers(auth_token=sys_token, customer_client_accounts=data_to_insert,created_by=sys_user_id, ucv_batch_size=ucv_batch_size)
