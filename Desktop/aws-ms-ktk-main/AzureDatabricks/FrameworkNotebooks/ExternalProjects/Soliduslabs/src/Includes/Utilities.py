# Databricks notebook source
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as f
from pyspark.storagelevel import StorageLevel
from typing import Dict, List


def split_dataframe_by_identifiers(
        df: DataFrame, id_type_col_name: str, id_type_values: List[str]) -> Dict[str, DataFrame]:

    return {
        id_type_value: df.filter(f.col(id_type_col_name) == f.lit(id_type_value)).persist(StorageLevel.DISK_ONLY)
        for id_type_value in id_type_values
    }


def filter_small_df_partitions(df: DataFrame, fields_to_partition_by: List[str], min_size_threshold: int) -> DataFrame:
    window_cntr_fieldname = 'tmp_win_cnt'
    return df.withColumn(window_cntr_fieldname, f.count(f.lit(1)).over(Window.partitionBy(fields_to_partition_by)))\
        .filter(f.col(window_cntr_fieldname) >= f.lit(min_size_threshold))\
        .drop(window_cntr_fieldname)

# COMMAND ----------

import json


# Use within json.dumps(obj, cls=JSONEncoder) to recursively jsonify nested objects
class JSONEncoder(json.JSONEncoder):
    def default(self, obj):
        return obj.__dict__

# COMMAND ----------

from pyspark.sql import DataFrame
from pyspark.sql import functions as f

from typing import List
import random

import matplotlib.pyplot as plt
import seaborn as sns
plt.style.use('seaborn-whitegrid')


def scatter_plots_2d_from_df(
        df: DataFrame, feature_cols: List[str], color_colname: str, limit: int = 1000,
        title_prefix: str = None, is_debug_mode=False):

    assert len(feature_cols), '2d plot cannot have one dimension'

    # Casting the "predictions"/color_colname column as a string so that Pandas/Seaborn doesn't include it as
    # a feature in the pairplot
    pandas_df = df.select(feature_cols + [color_colname]).sort(f.lit(random.random())).limit(limit).toPandas()
    pandas_df[color_colname] = pandas_df[color_colname].astype(str)

    # The "vars" parameter is meant to make sure that the added "predictions"/"color_colname" column
    # is not used as a feature column in the pairplot
    g = sns.pairplot(pandas_df, vars=pandas_df.columns[:-1], diag_kind='hist', hue=color_colname)
    g.fig.suptitle(title_prefix, y=0.3)  # y= some height>1
    if is_debug_mode:
        plt.show()

    return pandas_df

# COMMAND ----------

from typing import Dict


# sf_include_cloud_provider is there simply because Snowflake are inconsistent with handing out their URLs.
# Some account-based SF URLs look like this:
#     https://mn41405.ap-southeast-1.snowflakecomputing.com/
# while others do have the cloud provider:
#     https://nc21661.us-east-2.aws.snowflakecomputing.com/
class SnowflakeConfig:
    def __init__(self, env, role, database, warehouse, include_cloud_provider=None, scope="Soliduslabs_RND",BronzeTableName="RND/clientid-detection-etl"):
        self.env = env
        self.role = role
        self.database = database
        self.warehouse = warehouse
        self.include_cloud_provider = include_cloud_provider
        self.scope=scope
        self.BronzeTableName=BronzeTableName

    def __dict__(self):
        return f'env: {self.env}, role: {self.role}, database: {self.database}, ' \
            f'warehouse: {self.warehouse}, include_cloud_provider: {self.include_cloud_provider},scope:{self.scope},BronzeTableName:{self.BronzeTableName}'


# key: env_name
##Need to add scope and bronzeTableName in all the calls
sf_env_configs: Dict[str, SnowflakeConfig] = {
    'testing': SnowflakeConfig('testing', 'DEV_READER', 'SOLIDUS_PERFORM', 'MULTI_C_PERFORM_LOAD', None, 'Soliduslabs_RND',"RND/clientid-detection-etl"),
    'rnd': SnowflakeConfig('rnd', 'DEV_READER', 'SOLIDUS_RND', 'RND_WH', None),
    'staging': SnowflakeConfig('staging', 'DEV_READER', 'SOLIDUS_STAGING', 'STAGING_WH', None),
    'demo': SnowflakeConfig('demo', 'DEV_READER', 'SOLIDUS_DEMO', 'DEMO_WH', None),
    'perform': SnowflakeConfig('perform', 'DEV_READER', 'SOLIDUS_PERFORM', 'PERFORM_WH', None),
    'uat_us': SnowflakeConfig('uat', 'DEV_READER', 'SOLIDUS_UAT_US', 'UAT_US_WH', None),
    'uat_asia': SnowflakeConfig('uat-asia', 'DEV_READER', 'SOLIDUS_UAT_ASIA', 'UAT_ASIA_WH', None),
    'uat_eu': SnowflakeConfig('uat-eu', 'DEV_READER', 'SOLIDUS_UAT_EU', 'UAT_EU_WH', None),
    'prod_us': SnowflakeConfig('prod-us', 'DEV_READER', 'SOLIDUS_PROD_US', 'PROD_US_WH', None),
    'prod_asia': SnowflakeConfig('prod-asia', 'DEV_READER', 'SOLIDUS_PROD_ASIA', 'PROD_ASIA_WH', None),
    'prod_eu': SnowflakeConfig('prod-eu', 'DEV_READER', 'SOLIDUS_PROD_EU', 'PROD_EU_WH', None),
    'fidelity': SnowflakeConfig('fidelity', 'FIDELITY_READER', 'SOLIDUS_FIDELITY', 'FIDELITY_WH', 'aws'),
}

# COMMAND ----------

from pyspark.ml.feature import VectorAssembler
from pyspark.sql import functions as f
from pyspark.sql import DataFrame, Window

from functools import reduce

from typing import List


# Spark ML functions require a vector of features in order to work
def create_features_column(df: DataFrame, feature_col_names: List[str], output_col_name='features') -> DataFrame:
    assembler = VectorAssembler(
        inputCols=[x for x in feature_col_names], outputCol=output_col_name
    )

    return assembler.transform(df)


def calculate_feature_iqr(df: DataFrame, feature_colname: str, partition_by: List[str] = None) -> DataFrame:
    """
    Calculating the "interquartile range" - a useful method for detecting outliers in datasets.
    Essentially, it works by describing the middle 50% of values within the dataset (Q1 to Q3) and using those values
    to find data-point that deviate greatly.

    :param df:
    :param feature_colname: The column name containing the values we want to asses
    :param partition_by: Optional. Column names to partition by
    (for example, if we want to run the calculation separately for differnt id_types (client_id, account_id, actor_id)
    :return: Input DF with additional {feature_colname}_iqr & {feature_colname}_outlier columns
    """
    percentile_colname = f'{feature_colname}_percentils'
    iqr_colname = _compose_iqr_suffix(feature_colname)
    outlier_colname = _compose_iqr_outlier(feature_colname)
    partition_cols = [f.col(colname) for colname in partition_by] if partition_by else f.lit(1)

    perc_25_colname, perc_75_colname = _compose_perc_25(feature_colname), _compose_perc_75(feature_colname)

    print(f'Calculating IQR for column: {feature_colname}')

    df = df.withColumn(percentile_colname, f.expr(f'percentile_approx({feature_colname}, array(0.25, 0.75))').over(
        Window.partitionBy(partition_cols))) \
        .withColumn(perc_25_colname, f.col(percentile_colname)[0]) \
        .withColumn(perc_75_colname, f.col(percentile_colname)[1]) \
        .drop(percentile_colname) \
        .withColumn(iqr_colname, f.col(perc_75_colname) - f.col(perc_25_colname)) \
        .withColumn(outlier_colname,
                    f.when(
                        (f.col(feature_colname) < f.col(perc_25_colname) - (f.col(iqr_colname) * f.lit(1.5))) |
                        (f.col(feature_colname) > f.col(perc_75_colname) + (f.col(iqr_colname) * f.lit(1.5))),
                        f.lit(True)
                    ).otherwise(False))

    return df


def filter_iqr_outliers(df: DataFrame, feature_names: List[str],
                        outlier_feature_threshold: int = 1, drop_iqr_cols: bool = False) -> DataFrame:
    """
    Used in tandem with the "calculate_feature_iqr".
    It accepts a dataframe with the {feature_colname}_iqr & {feature_colname}_outlier columns and filters accordingly.

    :param df: DF with the {feature_colname}_iqr & {feature_colname}_outlier columns, (possibly more multiple features)
    :param feature_names: feature names to filter by
    :param outlier_feature_threshold: Optional. Only filter rows where X or more features have been deemed as outliers.
    :param drop_iqr_cols: Optional. If set to true, the IQR related fields will be dropped.

    :return: A DF without the rows deemed as outliers (in any of the features).
    """
    # Create 4 columns where a "true" value equals 1 and a False equals 0
    filter_outliers_sum = [
        (f.when(f.col(_compose_iqr_outlier(feature_col)), f.lit(1)).otherwise(f.lit(0)))
        for feature_col in feature_names
    ]

    # And we filter to keep rows where that number of outlier features was not met
    num_of_outlier_features_col = 'OUTLIERS_COUNT'
    filtered_df = df.withColumn(
        num_of_outlier_features_col, reduce(lambda col1, col2: col1 + col2, filter_outliers_sum, f.lit(0))
    )\
        .filter(f.col(num_of_outlier_features_col) < f.lit(outlier_feature_threshold))\
        .drop(num_of_outlier_features_col)

    if drop_iqr_cols:
        cols_to_drop = [_compose_iqr_suffix(colname) for colname in feature_names] + \
                       [_compose_iqr_outlier(colname) for colname in feature_names] + \
                       [_compose_perc_25(colname) for colname in feature_names] + \
                       [_compose_perc_75(colname) for colname in feature_names]

        return filtered_df.drop(*cols_to_drop)

    return filtered_df


def _compose_iqr_suffix(colname: str) -> str:
    return f'{colname}_IQR'


def _compose_iqr_outlier(colname: str) -> str:
    return f'{colname}_OUTLIER'


def _compose_perc_25(colname: str) -> str:
    return f'{colname}_PERC_25'


def _compose_perc_75(colname: str) -> str:
    return f'{colname}_PERC_75'

# COMMAND ----------

from urllib.parse import urlparse


def verify_url(url: str) -> bool:
    assert url, 'url missing from validation'

    parsed = urlparse(url)
    assert parsed.scheme and parsed.netloc \
        and (':' in parsed.netloc or '.' in parsed.netloc), \
        f'error: URL {url} is badly formatted'


def verify_number(num_string: str) -> bool:
    assert num_string, 'input missing from validation'

    try:
        float(num_string)
        return True
    except RuntimeError:
        return False
