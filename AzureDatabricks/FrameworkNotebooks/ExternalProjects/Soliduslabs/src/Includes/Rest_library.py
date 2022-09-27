# Databricks notebook source
# MAGIC %run "/Shared/Soliduslabs/src/algologic/cpd_clustering/clustering_result_dto"

# COMMAND ----------



# COMMAND ----------

# MAGIC %run "/Shared/Soliduslabs/src/shared/utils/utils_lib"

# COMMAND ----------

from abc import ABC, abstractmethod
from typing import Dict

from pyspark.sql import SparkSession
from pyspark.sql import types as t


class EndpointVerifierBase(ABC):
    @abstractmethod
    def _verify_api_contract(self, instance: Dict, schema: Dict) -> None:
        pass

    @abstractmethod
    def _get_schema(self) -> Dict:
        pass

    @abstractmethod
    def _convert_response_to_spark_df(self, spark: SparkSession, json_string: str) -> t.StructType:
        pass

# COMMAND ----------

from jsonschema import validate
from typing import Dict
import requests

from pyspark.sql import SparkSession
from pyspark.sql import types as t

#from src.shared.io.rest.api_endpoint_verifier_base import EndpointVerifierBase

AUTH_MASTER_PASSWORD_ENDPOINT = '/api/v1/internal/auth/system-login'
AUTH_ME_ENDPOINT = '/api/v1/users/me'


class AuthMasterPasswordFetcher(EndpointVerifierBase):
    def __init__(self, base_url: str):
        self.base_url = base_url

    def fetch_master_password(self, solidus_client: str, master_password: str) -> str:
        headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json',
            'User-Agent': 'Spark Job - Auth Master'
        }

        payload = {
            'solidusClientId': solidus_client,
            'masterPassword': master_password,
        }

        response = requests.post(f'{self.base_url}{AUTH_MASTER_PASSWORD_ENDPOINT}', headers=headers, json=payload)
        assert response.status_code == 200, \
            f'Bad response received from auth-service while fetching sys token: {response.status_code}: {response.text}'
        self._verify_api_contract(response.json(), self._get_schema())

        return response.json()['token']
    
    def _verify_api_contract(self, instance: Dict, schema: Dict) -> None:
        validate(instance=instance, schema=schema)

    def _get_schema(self) -> Dict:
        return {
            "type": "object",
            "properties": {
                "token": {
                    "type": "string",
                    "description": "Expirable auth-token for system-user",
                    "min": 20
                }
            },
            "required": ["token"]
        }

    def _convert_response_to_spark_df(self, spark: SparkSession, json_string: str) -> t.StructType:
        raise Exception('Master-password as DF is not implemented')


class AuthMeEndpointFetcher(EndpointVerifierBase):
    def __init__(self, base_url: str):
        self.base_url = base_url

    def fetch(self, auth_token: str) -> None:
        headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json',
            'User-Agent': 'Spark Job - Auth Master',
            'X-Auth-Token': auth_token,
        }

        response = requests.get(f'{self.base_url}{AUTH_ME_ENDPOINT}', headers=headers)

        assert response.status_code == 200,\
            'Bad response received from auth-service while calling "me" endpoint: '\
            f'{response.status_code}: {response.text}'
        self._verify_api_contract(response.json(), self._get_schema())

        return response.json()

    def _verify_api_contract(self, instance: Dict, schema: Dict) -> None:
        validate(instance=instance, schema=schema)

    def _get_schema(self) -> Dict:
        return {
            "type": "object",
            "properties": {
                "id": {
                    "type": "number",
                    "description": "id of user_account object",
                },
                "firstName": {
                    "type": "string",
                    "description": "First name of user",
                },
                "lastName": {
                    "type": "string",
                    "description": "Last name of user",
                },
                "email": {
                    "type": "string",
                    "description": "Email of user",
                },
                "name": {
                    "type": "string",
                    "description": "Full name of user, usually concatanation of first & last name",
                },
                "teamIds": {
                    "type": "array",
                    "description": "Ids of all teams that the user is a part of",
                    "items": {
                        "type": "number"
                    }
                },
                "permissions": {
                    "type": "array",
                    "description": "Permissions that user is privy to",
                    "items": {
                        "type": "string"
                    }
                }
            },
            "required": ["id", "email"]
        }

    def _convert_response_to_spark_df(self, spark: SparkSession, json_string: str) -> t.StructType:
        raise Exception('Master-password as DF is not implemented')

# COMMAND ----------

from jsonschema import validate
from typing import Dict
import requests

from pyspark.sql import SparkSession, DataFrame

#from src.shared.io.rest.api_endpoint_verifier_base import EndpointVerifierBase

SEGMENTED_MODELS_ENDPOINT = '/api/v1/segments'


class ConfigManagerAlgoFetcher(EndpointVerifierBase):
    def __init__(self, base_url: str):
        self.base_url = base_url

    def fetch_segmented_models(self, auth_token: str, as_spark_df: bool = False):
        headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json',
            'X-Auth-Token': auth_token,
            'User-Agent': 'Spark Job - Config Segments',
        }

        req_params = {
            'mechanismType': 'RT_EXECUTOR'
        }
        response = requests.get(f'{self.base_url}{SEGMENTED_MODELS_ENDPOINT}', headers=headers, params=req_params)

        assert response.status_code == 200, \
            f'Bad response received from config-manager service: {response.status_code}: {response.text}'
        self._verify_api_contract(response.json(), self._get_schema())

        if as_spark_df:
            self._convert_response_to_spark_df(spark, response.text)

        return response.json()

    def _verify_api_contract(self, instance: Dict, schema: Dict) -> None:
        validate(instance=instance, schema=schema)

    def _get_schema(self) -> Dict:
        return {
            "type": "array",
            "description": "An array representing different solidus_client segments, and their models and configs",
            "items": {
                "$ref": "#/$defs/segmented_models"
            },
            "$defs": {
                "segmented_models": {
                    "type": "object",
                    "properties": {
                        "id": {
                            "type": "number",
                            "description": "ID of the segment",
                        },
                        "name": {
                            "type": "string",
                            "description": "name of the segment",
                        },
                        "models": {
                            "type": "array",
                            "description": "The segment's models and configurations",
                            "items": {
                                "$ref": "#/$defs/model_info"
                            }
                        }
                    },
                    "required": ["id", "name", "models"],
                },
                "model_info": {
                    "type": "object",
                    "properties": {
                        "id": {
                            "type": "string",
                            "description": "ID of the model_dto",
                        },
                        "name": {
                            "type": "string",
                            "description": "name of the model",
                        },
                        "params": {
                            "type": "array",
                            "description": "Model parameters",
                            "items": {
                                "$ref": "#/$defs/model_params"
                            }
                        }
                    },
                    "required": ["id", "name", "params"],
                },
                "model_params": {
                    "type": "object",
                    "properties": {
                        "name": {
                            "type": "string",
                            "description": "name of the param",
                        },
                        "values": {
                            "type": "array",
                            "description": "values of a param/threshold",
                            "items": {
                                "$ref": "#/$defs/param_values"
                            }
                        },
                    },
                    "required": ["name", "values"],
                },
                "param_values": {
                    "type": "object"
                }
            }
        }

    def _convert_response_to_spark_df(self, spark: SparkSession, json_string: str) -> DataFrame:
        raise NotImplementedError('model_dto data as Spark dataframe is not implemented')

# COMMAND ----------

from jsonschema import validate
from typing import Dict
import requests

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import types as t

#from src.shared.io.rest.api_endpoint_verifier_base import EndpointVerifierBase

SLIM_SEGMENTS_ENDPOINT = '/api/v1/segments/slim'

SEGMENT_ID_FIELD_config = 'id'
SEGMENT_NAME_FIELD = 'name'
SEGMENT_IS_DEFAULT_FIELD = 'is_default_segment'


class ConfigManagerSegmentFetcher(EndpointVerifierBase):
    def __init__(self, base_url: str):
        self.base_url = base_url

    def fetch_slim_segments(self, auth_token: str, as_spark_df: bool = False):
        headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json',
            'X-Auth-Token': auth_token,
            'User-Agent': 'Spark Job - Config Segments',
        }
        print(self.base_url,SLIM_SEGMENTS_ENDPOINT)
  
        response = requests.get(f'{self.base_url}{SLIM_SEGMENTS_ENDPOINT}', headers=headers)
        assert response.status_code == 200, \
            f'Bad response received from config-manager service: {response.status_code}: {response.text}'
        self._verify_api_contract(response.json(), self._get_schema())
        
        #return response
        if as_spark_df:
            return self._convert_response_to_spark_df(spark, response.text)

        return response.json()

    def _verify_api_contract(self, instance: Dict, schema: Dict) -> None:
        validate(instance=instance, schema=schema)

    def _get_schema(self) -> Dict:
        return {
            "type": "array",
            "description": "A slim object describing a segment's properties (currently only applies to client_id) ",
            "items": {
                "$ref": "#/$defs/slim_segment"
            },
            "$defs": {
                "slim_segment": {
                    "type": "object",
                    "properties": {
                        "id": {
                            "type": "number",
                            "description": "ID of the segment",
                        },
                        "name": {
                            "type": "string",
                            "description": "name of the segment",
                        },
                        "isDefaultSegment": {
                            "type": "boolean",
                            "description": "is this the client's default segment? Should be 1 and only 1 of those"
                        }
                    },
                    "required": ["id", "name", "isDefaultSegment"],
                }
            }
        }

    def _convert_response_to_spark_df(self, spark: SparkSession, json_string: str) -> DataFrame:
        incoming_customer_segment_spark_schema = t.StructType([
            t.StructField('id', t.StringType(), nullable=False),
            t.StructField('name', t.StringType(), nullable=False),
            t.StructField('isDefaultSegment', t.BooleanType(), nullable=False),
        ])

        return spark.read.json(
            spark.sparkContext.parallelize([json_string]),
            incoming_customer_segment_spark_schema
        )\
            .withColumnRenamed('id', SEGMENT_ID_FIELD_config)\
            .withColumnRenamed('name', SEGMENT_NAME_FIELD)\
            .withColumnRenamed('isDefaultSegment', SEGMENT_IS_DEFAULT_FIELD)

# COMMAND ----------

#from src.algologic.cpd_clustering.clustering_result_dto import ClusterIdentifierResults
#from src.shared.utils.json_utils import JSONEncoder
from typing import List
import requests
import json

PUBLISH_CPD_CLUSTERS_ENDPOINT = '/api/v1/internal/cpd/clusters'


class ModelServingCpdClusterSender:
    def __init__(self, base_url: str, is_debug_mode: bool = False):
        self.base_url = base_url
        self.is_debug_mode = is_debug_mode

    def publish(self, auth_token: str, payload: List[ClusterIdentifierResults]):
        headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json',
            'X-Auth-Token': auth_token,
            'User-Agent': 'Spark Job - Model Serving CPD',
        }
        #print(payload)
        print('JSONifying data...')
        jsonified_data = json.dumps(payload, cls=JSONEncoder)
        print(jsonified_data)
        print(type(jsonified_data))
        if self.is_debug_mode:
            print(jsonified_data)

        print('Sending...')
        print(self.base_url,PUBLISH_CPD_CLUSTERS_ENDPOINT)
        response = requests.post(
            f'{self.base_url}{PUBLISH_CPD_CLUSTERS_ENDPOINT}',
            headers=headers,
            json=jsonified_data
        )
        print(response.text)
      
        assert response.status_code == 200, \
            f'Bad response received from model-serving service when attempting to publish: ' \
            f'{response.status_code}: {response.text}'
