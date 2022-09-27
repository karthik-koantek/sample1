# Databricks notebook source
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

print(job_args)

# COMMAND ----------

job_args=job_args[1:-1]
#print(lambda x: x.split('='), job_args.split('", "'))
job_args_dict = dict(map(lambda x: x.split('='), job_args.split('", "')))
print(type(job_args_dict))

# COMMAND ----------

from pyspark.sql import SparkSession, DataFrame
from pyspark.storagelevel import StorageLevel
from pyspark.ml.feature import StandardScaler, StandardScalerModel
from pyspark.ml.functions import vector_to_array
from pyspark.sql import functions as f
from pyspark.sql import types as t

# COMMAND ----------

# MAGIC %run ../jobs/import_library

# COMMAND ----------

from typing import Dict, List, Any
import numpy as np
import json

# input parameter keys
EXEC_TIME = 'exec_time_millis'
DAYS_BACK = 'days_back'
AUTH_SERVICE_MASTER_PASSWORD = 'auth_service_master_password'
AUTH_SERVICE_URL = 'auth_service_url'
CLIENT_STORE_SERVICE_URL = 'client_store_service_url'
MODEL_SERVING_SERVICE_URL = 'model_serving_service_url'
CONFIG_MANAGER_SERVICE_URL = 'config_manager_service_url'

INTERNAL_ACCOUNT_ID = 'internal_account_id'
EXTERNAL_ACCOUNT_ID = 'external_account_id'
CLIENT_ID = 'client_id'
ACTOR_ID = 'actor_id'

ALL_FIELDS = [CLIENT_ID, EXTERNAL_ACCOUNT_ID, INTERNAL_ACCOUNT_ID, ACTOR_ID]

FEATURE_COLUMN_NAME_LIST = [
    'DAILY_HOURLY_AVG',
    'DAILY_FREQ_AVG',
    'DAILY_USD_SIZE_AVG',
    'DEPOSIT_WITHDRAWAL_RATIO',
]

# beneath this value, we will not run K-Means, (which start at 2, and in our particular case, even 3 clusters)
# Because of how we choose our K (using the derivative of the improvement of the loss function) ("elbow method").
# This means we have to have a substantial number of data-points for this.
# (We can still run our data as a single cluster if it is larger than MINIMUM_DATA_POINTS_REQUIRED_SINGLE_CLUSTER)
MINIMUM_DATA_POINTS_REQUIRED_FOR_CLUSTERING = 200

# If K-Means IS run, we will filter out clusters smaller than this value
# (usually a lot of very small "junk" clusters are created)
MINIMUM_DATA_POINTS_FILTER_FOR_PRODUCED_CLUSTER = 25

# Beneath this value, even a single cluster should not be accounted for, as it would produce too many false-positives
# This means that CPD will not even run for segments smaller than this value
MINIMUM_DATA_POINTS_REQUIRED_SINGLE_CLUSTER = 50

TM_ACTIVITY_COLS = ['IDENTIFIER', 'ID_TYPE', 'SOLIDUS_CLIENT', 'EX_VENUE'] + FEATURE_COLUMN_NAME_LIST


# COMMAND ----------

def segmented_customer_params_df_to_temp_sf_table(self,
                                                   segmented_customer_params_df: DataFrame, temp_tbl_name: str,
                                                   sf_user: str, sf_pass: str, sf_role: str, sf_account: str,
                                                   sf_region: str, sf_config: SnowflakeConfig) -> None:

    conn_details = sf_jdbc_conn_details(
        sf_username=sf_user, sf_password=sf_pass, sf_account=sf_account, sf_region=sf_region,
        sf_database=sf_config.database, sf_warehouse=sf_config.warehouse, sf_schema='TM', sf_role=sf_role,
        sf_include_cloud_provider=sf_config.include_cloud_provider)

    sf_conn = open_sf_jdbc_connection(conn_details)

    run_command(query=f'USE DATABASE {sf_config.database}', sf_conn=sf_conn)

    self.log_info(f'Creating temp table: {temp_tbl_name}')
    create_temp_sf_tbl(sf_conn, temp_tbl_name, [
        ('customer_id', 'text'),
        ('segment_id', 'int'),
        ('days_back', 'int'),
    ])

    # Creating a temp table with UCV/client-store/segment data to get the right "days back" param value for querying
    write_df_to_table(segmented_customer_params_df, temp_tbl_name, conn_details)


# COMMAND ----------

# def query_tm_activity( sf_config: SnowflakeConfig, exec_time_millis: float, days_back: int,
#                           snowflake_user: str, snowflake_password: str, snowflake_role: str,
#                           snowflake_account: str, snowflake_region: str) -> DataFrame:

# #         print(f"""sql_query = compose_cpd_clustering_query({solidus_client}, str({exec_time_millis}), {days_back}, {sf_config.database})""")  
            
#          sql_query = compose_cpd_clustering_query(solidus_client, str(exec_time_millis), days_back, sf_config.database)
        
#          url=_compose_sf_connection_string(snowflake_account, snowflake_region, sf_config.include_cloud_provider)
        
        
# # Calling Snowflake connector notebook to get the data from snowflake and storing it into delta lake in JSON format.
         
  
#          sql_query_run= dbutils.notebook.run('/Shared/Soliduslabs/Snowflake connector/Batch Snowflake'
#         ,0,{'BronzeTableName':BronzeTableName,'Scope':sf_config.scope,'sfWarehouse':sf_config.warehouse,'sfUrl':url,'sfDatabase':sf_config.database,'Query':sql_query})

#     # Bronze Zone data stored.
#          sf_df=spark.read.format("delta").option("inferschema",True).load(f"dbfs:/mnt/solidus-databricks-rnd/nvirginia-prod/BronzeData/{BronzeTableName}/")
    
#     # The Snowflake query uses ARRAY_AGG which returns result as StringType rather than ArrayType when converted
#     # to Spark
#          return sf_df

# COMMAND ----------

def extract_default_segment_info( segment_info_df: DataFrame) -> (str, str):
    default_segment_row = segment_info_df.filter('isDefaultSegment').first()
    assert default_segment_row, f'default segment was not found for solidusClient: {solidus_client}, ' \
        f'segments are: {segment_info_df.select("name").distinct().collect()}'
    print(default_segment_row, type(default_segment_row))

    return default_segment_row['id'], default_segment_row['name']

# COMMAND ----------

def enriching_segment_data(
         segment_info_df: DataFrame, customer_segments_df: DataFrame, default_segment_id: str) -> DataFrame:

    # Adding segment data to customer-segment mapping, and filtering out the default segment
    # (the default segment will be executed anyway, and on the entire dataset)
    # This table will be used to filter the tm_activity dataset based on other segments
    segment_df = customer_segments_df\
        .filter(f.col('segment_id') != f.lit(default_segment_id))\
        .join(segment_info_df, on=segment_info_df.id == customer_segments_df.segment_id, how='left') \
        .drop('id') \
        .withColumn('name', f.when(f.isnull(f.col('name')), f.lit('Unknown')).otherwise(f.col('name')))

    segment_df.show()


    return segment_df

# COMMAND ----------

def run_clustering_for_segment(
         activity_df: DataFrame, id_type: str,
        segment_id: int, segment_name: str, is_default_segment: bool) -> ClusteringSegmentResults:

    df_count = activity_df.count()
    
    if df_count < MINIMUM_DATA_POINTS_REQUIRED_SINGLE_CLUSTER:

        return None

    df_features = create_features_column(
        df=activity_df, feature_col_names=FEATURE_COLUMN_NAME_LIST, output_col_name='features')


    df_features.show(20, False)

    # We standardize our features before introducing them to clustering.
    # It's important to send the mean & std values to be stored with our clusters, since the SAME
    # standardizing transformation will need to happen for every other data-point we use with our clusters
    standardizer_model, df_standard_features = standardize_features(
        df_features, input_col='features', output_col='features_standard')


    df_standard_features.show(20, False)

    # Depending on segment size, either go with clustering, or simply treat the entire data-set as a single cluster
    # K == 1
    if df_count < MINIMUM_DATA_POINTS_REQUIRED_FOR_CLUSTERING:

        # Adding the predictions column where every row has prediction = 0 (meaning, a single cluster)
        cluster_results = df_standard_features.withColumn('predictions', f.lit(0))

        # Computing the feature centers for our single cluster and wrapping with numpy array
        # and an external list to have it in the same format as if it came out of PySpark's K-Means clusterer
        # The result is a list of a single array, where each value is the mean of a different feature
        cluster_centers = calculate_single_cluster_feature_means(cluster_results, 'features_standard')

    # K > 1
    else:

        clusterer = KmeansAutoKClusterer(
             max_k=50, loss_derivative_cutoff=0.75, skip_k_step_size=2)

        kmeans_model, cluster_results = clusterer.execute(df_standard_features, 'features_standard', 'predictions')

        # Plot results in Jupyter notebook
#         if is_debug_mode:
#             standardized_cluster_results = .extract_feature_columns_from_vec(
#                     cluster_results, 'features_standard'
#             )
#             scatter_plots_2d_from_df(
#                 standardized_cluster_results, FEATURE_COLUMN_NAME_LIST, 'predictions',
#                 title_prefix=f'{id_type} - segment: {segment_name}',
#                 is_debug_mode=.is_debug_mode
#             )

        cluster_centers = kmeans_model.clusterCenters()


    cluster_results.sort(f.col('predictions')).show(100, False)


    cluster_sizes = compute_cluster_sizes(cluster_results)

    # Filter "junk" small cluster often created by K-Means
    filtered_cluster_results = filter_small_df_partitions(
        cluster_results,
        ['id_type', 'solidus_client', 'ex_venue', 'predictions'],
        MINIMUM_DATA_POINTS_FILTER_FOR_PRODUCED_CLUSTER
    )


    # calculate std for each cluster (mean we already have as those are the clusters' centroids)
    # cluster_results columns comming in:
    # (identifier, id_type, solidus_client, ex_venue. features_standard, predictions)
    cluster_stddevs_collected = calculate_cluster_stddevs(filtered_cluster_results, 'features_standard')

    # Compose results into relevant objects
    cluster_infos: List[ClusterInfo] = []
    for row in cluster_stddevs_collected:
        current_cluster = row.predictions
        cluster_center = cluster_centers[current_cluster]

        feature1 = ClusterFeatureStats(FEATURE_COLUMN_NAME_LIST[0], row[FEATURE_COLUMN_NAME_LIST[0] + '_stddev'],
                                       cluster_center[0])
        feature2 = ClusterFeatureStats(FEATURE_COLUMN_NAME_LIST[1], row[FEATURE_COLUMN_NAME_LIST[1] + '_stddev'],
                                       cluster_center[1])
        feature3 = ClusterFeatureStats(FEATURE_COLUMN_NAME_LIST[2], row[FEATURE_COLUMN_NAME_LIST[2] + '_stddev'],
                                       cluster_center[2])
        feature4 = ClusterFeatureStats(FEATURE_COLUMN_NAME_LIST[3], row[FEATURE_COLUMN_NAME_LIST[3] + '_stddev'],
                                       cluster_center[3])

        cluster_infos.append(
            ClusterInfo(
                str(current_cluster),
                int(cluster_sizes[current_cluster]),
                [feature1, feature2, feature3, feature4]
            )
        )

    # package segment-level stats
    standarization_values: List[ClusterFeatureStats] = [
        ClusterFeatureStats(FEATURE_COLUMN_NAME_LIST[idx], mean, std)
        for idx, (mean, std) in enumerate(zip(standardizer_model.mean, standardizer_model.std))
    ]

    segment_results = ClusteringSegmentResults(
        id_type, solidus_client, segment_id, is_default_segment, standarization_values, cluster_infos)

    return segment_results

# COMMAND ----------

def calculate_cluster_stddevs( cluster_results: DataFrame, feature_vec_colname: str) -> List[t.Row]:
    # Breaking the vector column into 4 distinct columns
    cluster_results_features_df = extract_feature_columns_from_vec(cluster_results, feature_vec_colname)


    cluster_results_features_df.show()

    f0_stddev = FEATURE_COLUMN_NAME_LIST[0] + '_stddev'
    f1_stddev = FEATURE_COLUMN_NAME_LIST[1] + '_stddev'
    f2_stddev = FEATURE_COLUMN_NAME_LIST[2] + '_stddev'
    f3_stddev = FEATURE_COLUMN_NAME_LIST[3] + '_stddev'

    # isnull check is for division by zero - rare edge-case
    cluster_agg = cluster_results_features_df \
        .groupBy('predictions') \
        .agg(
            f.stddev(FEATURE_COLUMN_NAME_LIST[0]).alias(f0_stddev),
            f.stddev(FEATURE_COLUMN_NAME_LIST[1]).alias(f1_stddev),
            f.stddev(FEATURE_COLUMN_NAME_LIST[2]).alias(f2_stddev),
            f.stddev(FEATURE_COLUMN_NAME_LIST[3]).alias(f3_stddev),
        ) \
        .withColumn(f0_stddev, f.when(f.isnull(f.col(f0_stddev)), f.lit(0)).otherwise(f.col(f0_stddev))) \
        .withColumn(f1_stddev, f.when(f.isnull(f.col(f1_stddev)), f.lit(0)).otherwise(f.col(f1_stddev))) \
        .withColumn(f2_stddev, f.when(f.isnull(f.col(f2_stddev)), f.lit(0)).otherwise(f.col(f2_stddev))) \
        .withColumn(f3_stddev, f.when(f.isnull(f.col(f3_stddev)), f.lit(0)).otherwise(f.col(f3_stddev)))


    cluster_agg.show()

    # sorting results by cluster-lables, just for convenience
    cluster_agg_local = cluster_agg.sort('predictions').collect()

    return cluster_agg_local

# COMMAND ----------

def extract_feature_columns_from_vec( df: DataFrame, feature_vec_colname: str):
    # Breaking the vector column into 4 distinct columns
    return df \
        .withColumn('features_array', vector_to_array(f.col(feature_vec_colname))) \
        .withColumn(FEATURE_COLUMN_NAME_LIST[0], f.col('features_array')[0]) \
        .withColumn(FEATURE_COLUMN_NAME_LIST[1], f.col('features_array')[1]) \
        .withColumn(FEATURE_COLUMN_NAME_LIST[2], f.col('features_array')[2]) \
        .withColumn(FEATURE_COLUMN_NAME_LIST[3], f.col('features_array')[3]) \
        .drop('features_array')

# COMMAND ----------

def filter_activity_by_segment( activity_df: DataFrame, segment_df: DataFrame, segment_id: str) -> DataFrame:
    segmented_customer_df: DataFrame = segment_df.filter(f.col('segment_id') == f.lit(segment_id))

    return activity_df.join(
        segmented_customer_df, on=activity_df['identifier'] == segmented_customer_df['customer_id'], how='inner')\
        .select(TM_ACTIVITY_COLS)

# COMMAND ----------

def standardize_features( df: DataFrame, input_col: str, output_col: str) -> (StandardScalerModel, DataFrame):
    standardizer = StandardScaler(withMean=True, withStd=True, inputCol=input_col, outputCol=output_col)

    standardizer_model = standardizer.fit(df)

    standard_df = standardizer_model.transform(df)

    return standardizer_model, standard_df

# COMMAND ----------

def compute_cluster_sizes( cluster_results) -> Dict:
    counter_df_collected = cluster_results\
        .groupBy(f.col('predictions'))\
        .agg(f.count(f.col('identifier')).alias('size'))\
        .collect()

    return {row['predictions']: row['size'] for row in counter_df_collected}

# COMMAND ----------

def composing_clustering_stats( id_type_results: List[ClusterIdentifierResults]) -> str:
    num_of_id_types = len(id_type_results)

    if num_of_id_types == 0:
        return f'CLUSTER_PUBLISHING - Execution for solidus_client: {solidus_client} returned no results'

    result_stats = {result.id_type: str(len(result.segments)) + ' segments' for result in id_type_results}

    return f'CLUSTER_PUBLISHING - Publishing results for {num_of_id_types} id_types. {result_stats}'

# COMMAND ----------

def calculate_single_cluster_feature_means( df: DataFrame, features_colname: str):
    num_of_features = len(FEATURE_COLUMN_NAME_LIST)
    cluster_centers = df.agg(
        f.array(*[f.mean(vector_to_array(features_colname)[i])
                  for i in range(num_of_features)]).alias("cluster_center")).collect()

    # "collect" wraps result in "Row" object. We're taking that out while wrapping the result in a numpy-array
    # and an external list in order to conform with the format returned by a clusterer (in case of K > 1)
    if cluster_centers:
        return [np.array(cluster_centers[0][0])]

    return None

# COMMAND ----------

def log_distinct_cluster_count( df: DataFrame, df_name: str, predictions_colname: str, stage: str) -> int:
    try:
        cluster_count = df.agg(f.countDistinct(predictions_colname)).collect()[0][0]
    except Exception:
        pass

# COMMAND ----------

def fetch_sys_token( master_password_fetcher: AuthMasterPasswordFetcher, master_password: str) -> str:
    return master_password_fetcher.fetch_master_password(
        solidus_client, master_password)

# COMMAND ----------

def validate_job_args( **job_args_dict: Dict[str, Any]) -> None:



    assert job_args_dict.get(AUTH_SERVICE_MASTER_PASSWORD), 'error - Master password is null'
    verify_number(job_args_dict.get(EXEC_TIME))
    verify_number(job_args_dict.get(DAYS_BACK))
    verify_url(job_args_dict.get(AUTH_SERVICE_URL))
    verify_url(job_args_dict.get(CLIENT_STORE_SERVICE_URL))
    verify_url(job_args_dict.get(MODEL_SERVING_SERVICE_URL))
    verify_url(job_args_dict.get(CONFIG_MANAGER_SERVICE_URL))

# COMMAND ----------

# # Validating inputs
validate_job_args(**job_args_dict)

# COMMAND ----------

# laying out the params
auth_url: str = job_args_dict.get(AUTH_SERVICE_URL)
client_store_url: str = job_args_dict.get(CLIENT_STORE_SERVICE_URL)
config_manager_url: str = job_args_dict.get(CONFIG_MANAGER_SERVICE_URL)
model_serving_url: str = job_args_dict.get(MODEL_SERVING_SERVICE_URL)
exec_time_millis: float = float(job_args_dict[EXEC_TIME])
# days_back: int = job_args_dict[DAYS_BACK]


# COMMAND ----------

# Setting up auth-service's token fetcher (will be used multiple times) as well as other data fetchers
master_password_fetcher = AuthMasterPasswordFetcher(auth_url)
client_store_fetcher = UcvCustomerSegmentFetcher(client_store_url)
#ignore_list_fetcher = UcvIgnoreListFetcher(client_store_url)
segment_config_fetcher = ConfigManagerSegmentFetcher(config_manager_url)
algo_config_fetcher = ConfigManagerAlgoFetcher(config_manager_url)

# COMMAND ----------

# TODO: Support ex_venue (different clusters for different ex_venues)
sys_token = fetch_sys_token(master_password_fetcher, str(job_args_dict.get(AUTH_SERVICE_MASTER_PASSWORD)))


# COMMAND ----------

# Getting Snowflake configs based on env
# sf_config: SnowflakeConfig = sf_env_configs[env]

# COMMAND ----------

print(sys_token)

# COMMAND ----------

# TODO: Use fetched "time-back" param in CPD algo
days_back: int = 60

# COMMAND ----------

# Getting the "time-back" parameter for every segment. Assumption is that it will always exist.
# (no need to fallback on the "default" segment)
algo_info = algo_config_fetcher.fetch_segmented_models(
    auth_token=sys_token)
print(type(algo_info))

segment_time_back = convert_model_info_to_cpd_time_back(solidus_client, algo_info)  # TODO: use
longest_time_back = max([x for x in segment_time_back.values()])

# COMMAND ----------

# MAGIC %run "/Shared/Soliduslabs/src/sqls/cpd_clustering"

# COMMAND ----------

print(ALL_FIELDS)

# COMMAND ----------

# TODO: Clean up users with little activity that will skew results

# TODO: why are 215 rows turining to 125!?!?!
silverZoneTableName = "{0}.{1}_{2}_{3}".format(destination, externalSystem, schemaName, tableName)
tm_activity_df = spark.sql(f"select * from {silverZoneTableName}")


# COMMAND ----------

# TODO: REMOVE REMOVE REMOVE
tm_activity_df.show(1000, False)

# COMMAND ----------

# Removing outliers (for better clustering)
iqr_df = tm_activity_df
for feature_col in FEATURE_COLUMN_NAME_LIST:
    iqr_df = calculate_feature_iqr(iqr_df, feature_col, partition_by=['id_type'])
    


tm_activity_df_no_outliers = filter_iqr_outliers(
    iqr_df, FEATURE_COLUMN_NAME_LIST, outlier_feature_threshold=2, drop_iqr_cols=True)

# COMMAND ----------

# Splitting the dataframe into 4 DFs: One for client_id, actor_id and internal/external account_ids
id_specific_activity_dfs: Dict[str, DataFrame] = split_dataframe_by_identifiers(
    tm_activity_df_no_outliers,
    'id_type',
    [CLIENT_ID, EXTERNAL_ACCOUNT_ID, INTERNAL_ACCOUNT_ID, ACTOR_ID]
)


# COMMAND ----------

# tm_activity_df_no_outliers.show()

# get users and segments from the client-store
customer_segments_df = client_store_fetcher.fetch_ucv_list(
    auth_token=sys_token, as_spark_df=True
)
display(customer_segments_df)
customer_segments_df.persist(StorageLevel.DISK_ONLY)

# COMMAND ----------

 # get segment-info from config-manager
segment_info_df = segment_config_fetcher.fetch_slim_segments(
     auth_token=sys_token, as_spark_df=True
 )
display(segment_info_df)
segment_info_df.persist(StorageLevel.DISK_ONLY)


# COMMAND ----------



default_segment_id, default_segment_name = extract_default_segment_info(segment_info_df)

segment_info_no_default_df = segment_info_df.filter(f.col('id') != f.lit(default_segment_id))
segment_info_no_default_collected = segment_info_no_default_df.collect()
display(segment_info_no_default_df)
display(customer_segments_df)
print(default_segment_id)

segment_df = enriching_segment_data(segment_info_no_default_df, customer_segments_df, default_segment_id)

# COMMAND ----------

# customer_segments_df.show()

id_type_results: List[ClusterIdentifierResults] = []
for id_type, activity_df in id_specific_activity_dfs.items():

    # Running clustering for the default segment (entire dataset)
    # TODO: Decide what to do when None is returned
    default_segment_results: ClusteringSegmentResults = run_clustering_for_segment(
        activity_df, id_type, default_segment_id, default_segment_name, True)

    if not default_segment_results:
        continue


# COMMAND ----------

# And running it again for each segment
segment_results: List[ClusteringSegmentResults] = []
for segment_info_row in segment_info_no_default_collected:
    curr_segment_id: str = segment_info_row['id']
    curr_segment_name: str = segment_info_row['name']
    segmented_activity_df = filter_activity_by_segment(activity_df, segment_df, curr_segment_id)

    result: ClusteringSegmentResults = run_clustering_for_segment(
        segmented_activity_df, id_type, curr_segment_id, curr_segment_name, False)

    segment_results.append(result)

# TODO: Filters out "None" segments... might want to do something else
processed_segment_results = [x for x in [default_segment_results] + segment_results if x is not None]

id_type_result = ClusterIdentifierResults(solidus_client=solidus_client,
                                          id_type=id_type,
                                          days_back=days_back,
                                          segment_data=processed_segment_results)

id_type_results.append(id_type_result)

# COMMAND ----------

# MAGIC %run /Shared/Soliduslabs/src/shared/io/rest/rest_lib

# COMMAND ----------

# TODO: Send to model serving...

clustering_stats_msg: str = composing_clustering_stats(id_type_results)

ModelServingCpdClusterSender(model_serving_url, is_debug_mode).publish(sys_token, id_type_results)


