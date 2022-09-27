# Databricks notebook source
# MAGIC %run "/Shared/Soliduslabs/src/shared/io/rest/rest_lib"

# COMMAND ----------

from typing import Dict
from jsonschema import validate
import requests

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import types as t

#from src.shared.io.rest.api_endpoint_verifier_base import EndpointVerifierBase

SCV_APPLICATION_COUNT_ENDPOINT = '/api/v2/internal/scv/application-count'
SCV_CLIENT_ACCOUNT_ENDPOINT = '/api/v2/internal/scv/client-account-pairings'

CLIENT_ID_FIELD = 'client_id'
ACCOUNT_IDS_FIELD = 'account_ids'


class UcvApplicationCounter(EndpointVerifierBase):
    def __init__(self, base_url: str):
        self.base_url = base_url

    def count_applications(self, auth_token: str) -> int:
        headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json',
            'X-Auth-Token': auth_token,
            'User-Agent': 'Spark Job - UCV Client/Account Manager',
        }

        response = requests.get(f'{self.base_url}{SCV_APPLICATION_COUNT_ENDPOINT}', headers=headers)
        assert response.status_code == 200, \
            f'Bad response received from client-store service: {response.status_code}: {response.text}'

        return response.json()['count']

    def _verify_api_contract(self, instance: Dict, schema: Dict) -> None:
        validate(instance=instance, schema=schema)

    def _get_schema(self) -> Dict:
        return {
            "type": "object",
            "description": "Count the number of ScvApplications for that solidus_client in order to know how many"
                           "paginated pages to fetch from the backend",
            "properties": {
                "count": {
                    "description": "Num of ScvApplications",
                    "type": "integer"
                }
            },
        }

    def _convert_response_to_spark_df(self, spark: SparkSession, json_string: str) -> DataFrame:
        raise Exception('Not Implemented - POST call with empty response requires no processing of response')


class UcvClientAccountFetcher(EndpointVerifierBase):
    def __init__(self, base_url: str):
        self.base_url = base_url

    def fetch(self, auth_token: str, page_num: int, ucv_batch_size: int,
              as_spark_df: bool = False, spark: SparkSession = None) -> int:
        headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json',
            'X-Auth-Token': auth_token,
            'User-Agent': 'Spark Job - UCV Client/Account Manager',
        }

        response = requests.get(
            f'{self.base_url}{SCV_CLIENT_ACCOUNT_ENDPOINT}?page={page_num}&pageSize={ucv_batch_size}', headers=headers)

        assert response.status_code == 200, \
            f'Bad response received from client-store service: {response.status_code}: {response.text}'

        self._verify_api_contract(response.json(), self._get_schema())

        parsed_response = response.json()['items']

        if as_spark_df:
            return self._convert_response_to_spark_df(spark, parsed_response)

        return parsed_response

    def _verify_api_contract(self, instance: Dict, schema: Dict) -> None:
        validate(instance=instance, schema=schema)

    def _get_schema(self) -> Dict:
        return {
            "type": "object",
            "description": "Page info with Client/Account items array - holding paginated identifier list",
            "$defs": {
                "client-account-info": {
                    "type": "array",
                    "properties": {
                        "clientId": {
                            "type": "string",
                        },
                        "accountIds": {
                            "type": ["string"]
                        }
                    },
                },
            },
            "properties": {
                "items": {
                    "$ref": "#/$defs/client-account-info"
                },
                "pageNumber": {
                    "type": "integer",
                    "description": "pagingated page number, starting with zero",
                },
                "pageSize": {
                    "type": "integer",
                    "description": "Page size for each pagination iteration",
                },
                "total": {
                    "type": "integer",
                    "description": "Total number of items, across all pages",
                }
            }
        }

    def _convert_response_to_spark_df(self, spark: SparkSession, json_string: str) -> DataFrame:
        incoming_client_account_spark_schema = t.StructType([
            t.StructField("clientId", t.StringType(), nullable=False),
            t.StructField("accountIds", t.ArrayType(t.StringType(), False), nullable=False),
        ])

        return spark.read.json(
            spark.sparkContext.parallelize([json_string]),
            incoming_client_account_spark_schema
        ) \
            .withColumnRenamed("clientId", CLIENT_ID_FIELD) \
            .withColumnRenamed("accountIds", ACCOUNT_IDS_FIELD)

# COMMAND ----------

from pyspark.sql import DataFrame, Window, Row
from pyspark.sql import functions as f
from pyspark import StorageLevel

from typing import List
from datetime import datetime
import requests
import json

SCV_BULK_INSERT_ENDPOINT = '/api/v2/internal/scv/bulk-insert'


class UcvClientAccountUpdater:
    def __init__(self, base_url: str):
        self.base_url = base_url

    def bulk_insert_customers(
            self, auth_token: str, customer_client_accounts: DataFrame, created_by: int,
            ucv_batch_size: int, timeout_sec: int = 30) -> None:
        """
        :param auth_token: Auth-Token with INTERNAL_ACCESS privileges, in order to access "internal" controllers
        :param customer_client_accounts: A Spark dataframe with the columns: client_id & account_ids (array)
        :param created_by: The user_account ID of the account creating the UCV entries
        :param ucv_batch_size: The size of updates that are done to the client-store per iteration
        :param timeout_sec: Timeout for request, in seconds
        """
        headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json',
            'X-Auth-Token': auth_token,
            'User-Agent': 'Spark Job - UCV Client/Account Manager',
        }

        if customer_client_accounts.count() == 0:
            print('BULK_INSERT - Num of results is zero. Skipping...')
            return

        num_of_batches = int(customer_client_accounts.count() / ucv_batch_size) + 1

        print(f'BULK_INSERT - Preparing to update {str(num_of_batches)} pages.')

        numbered_ids = self._add_row_num(customer_client_accounts, 'row_num')
        numbered_ids.persist(StorageLevel.DISK_ONLY)

        failures_counter = 0
        for i in range(num_of_batches):
            # 0 to 499, 500 to 999, etc..
            start_idx = i * ucv_batch_size
            end_idx = (i + 1) * ucv_batch_size

            try:
                print(f'UCV_POSTING - Executing UCV batch between index {start_idx} & {end_idx - 1}')

                local_results = numbered_ids.filter(
                    (f.col('row_num') >= f.lit(start_idx)) & (f.col('row_num') < f.lit(end_idx))
                ).collect()

                local_payloads = self._convert_ids_to_payloads(local_results, created_by)

                print(f'Sending {len(local_results)} entries to UCV')

                response = requests.post(
                    f'{self.base_url}{SCV_BULK_INSERT_ENDPOINT}',
                    headers=headers, data=json.dumps(local_payloads), timeout=timeout_sec
                )

                assert response.status_code == 200, \
                    f'Bad response received from client-store service: {response.status_code}: {response.text}'

            except Exception as ex:
                failures_counter += 1
                print(ex)

                assert failures_counter <= 3, \
                    "Too many failed attempts occurred while posting results to client-store. Aborting..."

        assert failures_counter == 0, f"Encountered {failures_counter} failure(s) while posting to client-store. " \
            f"Returning failed status for future retry."

    def _add_row_num(self, df: DataFrame, colname: str) -> DataFrame:
        return df.withColumn(colname, f.row_number().over(Window.orderBy(f.col('client_id'))))

    def _convert_ids_to_payloads(self, local_id_df_entries: List[Row], created_by: int):
        """
        Turning list of client_ids and account_ids to payloads for UCV (bulk-insertion process).
        Example:
        From: { "client_id": "hiShahar12345", "account_ids": ["acc2"] }
        To:
        {
            "userId": "hiShahar12345",
            "type": "UNKNOWN",
            "risk": "MEDIUM",
            "status": "ACTIVE",
            "approvedAt": "1647617637000",
            "approvedBy": "90",
            "updatedAt": "1647617637000",
            "updatedBy": "90",
            "values": {
                "RISK": "MEDIUM",
                "STATUS": "ACTIVE"
            },
            "subAccounts": [
                {
                    "subAccountId": "acc2",
                    "risk": "HIGH",
                    "onboardedAt": "1647617637000"
                }
            ]
        }
        :return:
        """
        current_time_millis = round(datetime.now().timestamp() * 1000)
        return [
            {
                'userId': row['client_id'],
                'type': 'UNKNOWN',
                'risk': 'MEDIUM',
                'status': 'ACTIVE',
                'approvedAt': current_time_millis,
                'approvedBy': created_by,
                'updatedAt': current_time_millis,
                'updatedBy': created_by,
                'values': {
                    'RISK': 'MEDIUM',
                    'STATUS': 'ACTIVE',
                },
                'subAccounts': [
                    {
                        'subAccountId': acc,
                        'risk': 'MEDIUM',
                        'onboardedAt': current_time_millis
                    }
                    for acc in row['account_ids'] if acc and len(acc) > 0
                ]
            }
            for row in local_id_df_entries
        ]

# COMMAND ----------

from jsonschema import validate
from typing import Dict
import requests

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import types as t

#from src.shared.io.rest.api_endpoint_verifier_base import EndpointVerifierBase

SCV_LIST_ENDPOINT = '/api/v2/scv/customer-segments'

CUSTOMER_ID_FIELD = 'customer_id'
SEGMENT_ID_FIELD = 'segment_id'


class UcvCustomerSegmentFetcher(EndpointVerifierBase):
    def __init__(self, base_url: str):
        self.base_url = base_url

    def fetch_ucv_list(self, auth_token: str, as_spark_df: bool = False):
        headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json',
            'X-Auth-Token': auth_token,
            'User-Agent': 'Spark Job - UCV Manager',
        }

        response = requests.get(f'{self.base_url}{SCV_LIST_ENDPOINT}', headers=headers)
        print(self.base_url)
        print(SCV_LIST_ENDPOINT)
        assert response.status_code == 200, \
            f'Bad response received from client-store service: {response.status_code}: {response.text}'
        self._verify_api_contract(response.json(), self._get_schema())
        print(response.status_code)
        
        
            
        
        
        if as_spark_df :
            #if response.text=="[]":
            #   return spark.createDataFrame([], None)
            #else:
               return self._convert_response_to_spark_df(spark, response.text)


        return response.json()

    def _verify_api_contract(self, instance: Dict, schema: Dict) -> None:
        validate(instance=instance, schema=schema)

    def _get_schema(self) -> Dict:
        return {
            "type": "array",
            "description": "A slim object mapping between a customer identifier (currently only client_id) "
                           "and segment_id",
            "items": {
                "$ref": "#/$defs/customer_segment"
            },
            "$defs": {
                "customer_segment": {
                    "type": "object",
                    "properties": {
                        "customerId": {
                            "type": "string",
                            "description": "The value of customer identifier (e.g, client_id)",
                        },
                        "segmentId": {
                            "type": "number",
                            "description": "A running number (sequence) representing segment_id",
                        },
                    },
                    "required": ["customerId", "segmentId"],
                }
            }
        }

    def _convert_response_to_spark_df(self, spark: SparkSession, json_string: str) -> DataFrame:
        incoming_customer_segment_spark_schema = t.StructType([
            t.StructField("customerId", t.StringType(), nullable=False),
            t.StructField("segmentId", t.StringType(), nullable=False),
        ])

        return spark.read.json(
            spark.sparkContext.parallelize([json_string]),
            incoming_customer_segment_spark_schema
        )\
            .withColumnRenamed("customerId", CUSTOMER_ID_FIELD)\
            .withColumnRenamed("segmentId", SEGMENT_ID_FIELD)
