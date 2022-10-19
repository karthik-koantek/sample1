# Databricks notebook source
#import snowflake.connector as sf_connector
from py4j.java_gateway import java_import

SNOWFLAKE_SOURCE_NAME = 'net.snowflake.spark.snowflake'


def read_table_as_df(spark, query, sf_username, sf_password,
                     sf_account, sf_region, sf_database, sf_schema, sf_warehouse,
                     sf_role, sf_include_cloud_provider, aws_access_key_id, aws_secret_access_key, pushdown=False):
    spark._jsc.hadoopConfiguration().set("fs.s3n.awsAccessKeyId", aws_access_key_id)
    spark._jsc.hadoopConfiguration().set("fs.s3n.awsSecretAccessKey", aws_secret_access_key)

    if pushdown:
        java_import(spark._sc._jvm, "org.apache.spark.sql.api.python.*")
        enable_snowflake_spark_pushdown(spark)

    sf_options = {
        'sfUrl': _compose_sf_connection_string(sf_account, sf_region, sf_include_cloud_provider),
        'sfUser': sf_username,
        'sfPassword': sf_password,
        'sfDatabase': sf_database,
        'sfSchema': sf_schema,
        'sfWarehouse': sf_warehouse,
        'sfRole': sf_role,
    }

    df = spark.read.format(SNOWFLAKE_SOURCE_NAME) \
        .options(**sf_options) \
        .option('query', query) \
        .load()

    return df


# def run_command(query, sf_username, sf_password, sf_account, sf_region,
#                 sf_database, sf_schema, sf_warehouse, sf_role):
#     con = sf_connector.connect(
#         user=sf_username,
#         password=sf_password,
#         account=sf_account,
#         region=sf_region,
#         warehouse=sf_warehouse,
#         database=sf_database,
#         schema=sf_schema,
#         role=sf_role,
#     )

#     cur = con.cursor()
#     try:
#         cur.execute(query)
#     finally:
#         cur.close()


def enable_snowflake_spark_pushdown(spark):
    spark.sparkContext._jvm.net.snowflake.spark.snowflake.SnowflakeConnectorUtils.enablePushdownSession(
        spark.sparkContext._jvm.org.apache.spark.sql.SparkSession.builder().getOrCreate()
    )


def disable_snowflake_spark_pushdown(spark):
    spark.sparkContext._jvm.net.snowflake.spark.snowflake.SnowflakeConnectorUtils.disablePushdownSession(
        spark.sparkContext._jvm.org.apache.spark.sql.SparkSession.builder().getOrCreate()
    )


def _compose_sf_connection_string(sf_account, sf_region, sf_include_cloud_provider):
    provider = f'.{sf_include_cloud_provider}' if sf_include_cloud_provider else ''
    return f'{sf_account}.{sf_region}{provider}.snowflakecomputing.com'
