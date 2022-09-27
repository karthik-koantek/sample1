from io import StringIO
import requests
import sys
import json
import datetime
import pytz
import time
import math
import pandas as pd
from loguru import logger
from databricks_api import DatabricksAPI
from pyspark.sql import SparkSession
from pyspark.dbutils import DBUtils
from pyspark.sql.functions import col
from dateutil.relativedelta import relativedelta
from datetime import timedelta

# Initialize logger
logger.add(sink=f"./logs/marketo/mkt_{datetime.datetime.today().strftime('%d-%m-%Y')}.log", rotation="00:00", retention="30 days")

spark = SparkSession.builder.appName('DatabricksFramework').getOrCreate()
dbutils = DBUtils(spark)

BASE_URI = "https://272-JVS-996.mktorest.com"

MARKETO_API_ENDPOINTS = {
    'IDENTITY' : {
        '_get_token' : '/identity/oauth/token'
    },
    'ACTIVITIES' : {
        'get_activity_types': '/rest/v1/activities/types.json',
        'get_paging_token': '/rest/v1/activities/pagingtoken.json',
        'get_lead_activities': '/rest/v1/activities.json',
        'get_deleted_leads' : '/rest/v1/activities/deletedleads.json',
        'get_custom_activity_types' : '/rest/v1/activities/external/types.json'
    },
    'CAMPAIGNS' : {
        'get_campaigns' : '/rest/v1/campaigns.json'
    },
    'PROGRAMS' : {
        'get_programs' : '/rest/asset/v1/programs.json'
    },
    'LEADS' : {
        'describe_lead2' : '/rest/v1/leads/describe2.json',
        'describe_lead' : '/rest/v1/leads/describe.json'
    },
    'CHANNELS' : {
        'get_channels' : '/rest/asset/v1/channels.json'
    },
    'SMART_CAMPAIGNS' : {
        'get_smart_campaigns' : '/rest/asset/v1/smartCampaigns.json'
    },
    'TAGS' : {
        'get_tag_types' : '/rest/asset/v1/tagTypes.json'
    },
    'SMART_LISTS' : {
        'get_smart_lists' : '/rest/asset/v1/smartLists.json'
    },
    'STATIC_LISTS' : {
        'get_static_lists' : '/rest/asset/v1/staticLists.json'
    },
    'LEADS_STATIC_LISTS' : {
        'get_lists' : '/rest/v1/lists.json',
        'get_leads_by_list_id' : '/rest/v1/lists/{listId}/leads.json'
    },
    'BULK_EXPORT_ACTIVITIES' : {
        'create_export_activity_job' : '/bulk/v1/activities/export/create.json',
        'enqueue_export_activity_job' : '/bulk/v1/activities/export/{exportId}/enqueue.json',
        'get_export_activity_job_status' : '/bulk/v1/activities/export/{exportId}/status.json',
        'get_export_activity_file' : '/bulk/v1/activities/export/{exportId}/file.json'
    },
    'BULK_EXPORT_LEADS' : {
        'create_export_lead_job' : '/bulk/v1/leads/export/create.json',
        'enqueue_export_lead_job' : '/bulk/v1/leads/export/{exportId}/enqueue.json',
        'get_export_lead_job_status' : '/bulk/v1/leads/export/{exportId}/status.json',
        'get_export_lead_file' : '/bulk/v1/leads/export/{exportId}/file.json'
    }
}

# These activities are supported by Bulk API
BULK_ACTIVITY_TYPES = ['Click Email', 'Click Link', 'Email Delivered', 'Fill Out Form', 'Open Email', 'Send Email', 'Visit Webpage', \
    'Unsubscribe Email', 'Email Bounced', 'Email Bounced Soft', 'Add to Nurture', 'Change Score', 'Change Segment', 'Change Owner', \
    'Interesting Moment', 'Request Campaign', 'Change Data Value', 'Add to SFDC Campaign', 'Call Webhook', 'Change Nurture Cadence', \
    'Change Nurture Track', 'Change Program Member Data', 'Change Status in Progression', 'Change Status in SFDC Campaign', \
    'Convert Lead', 'Delete Lead from SFDC', 'Fill Out LinkedIn Lead Gen Form', 'Remove from SFDC Campaign', 'Send Alert', 'Sync Lead to SFDC'] 

# This activity is not supported by Bulk API. Can be fetched using 'get_deleted_leads' endpoint
ENDPOINT_ACTIVITY_TYPE = ['Delete Lead']

class MarketoException:
    '''This class is being used to handle errors encountered while making API calls.'''
    def __init__(self, source_name, error):
        self.err = {
            'sourceName': source_name,
            'errorCode': error['code'],
            'errorDescription': error['message']
        }
        
    def return_error(self):
        return self.err


class MarketoClient:

    def __init__(self, client_id=None, client_secret=None, access_token=None):
        self.base_uri = BASE_URI
        assert((client_id and client_secret) or access_token), logger.critical('Provide either client_id and client_secret or access_token')
        self.client_id = client_id
        self.client_secret = client_secret
        self.access_token = access_token
        if not self.access_token:
            self.get_token()
        self.api_calls = 0
        self.filesize = 0
        self.datasize = 0
        self.api_calls_threshold = 10000
        self.filesize_threshold = 9500000000
        self.current_time = pytz.utc.localize(datetime.datetime.utcnow()).astimezone(pytz.timezone("US/Central"))
        self.current_date = str(datetime.datetime.fromisoformat(str(self.current_time)).strftime("%Y-%m-%d"))
        self.initialize()


    def initialize(self):
        if (spark.sql("show databases").filter(col("databaseName") == "metadb_marketo").count() == 0):
            spark.sql("create database metadb_marketo")
            spark.sql("create table metadb_marketo.api_limits (date DATE, calls INT, filesize BIGINT)")
            spark.sql("create table metadb_marketo.leads_meta (startDate DATE, endDate DATE, status VARCHAR(100))")
            spark.sql("create table metadb_marketo.activities_meta (startDate DATE, endDate DATE, status VARCHAR(100))")
        else:
            if(spark.sql("show tables in metadb_marketo").filter(col("tableName") == "api_limits").count() == 0):
                spark.sql("create table metadb_marketo.api_limits (date DATE, calls INT, filesize BIGINT)")
            if(spark.sql("show tables in metadb_marketo").filter(col("tableName") == "leads_meta").count() == 0):
                spark.sql("create table metadb_marketo.leads_meta (startDate DATE, endDate DATE, status VARCHAR(100))")
            if(spark.sql("show tables in metadb_marketo").filter(col("tableName") == "activities_meta").count() == 0):
                spark.sql("create table metadb_marketo.activities_meta (startDate DATE, endDate DATE, status VARCHAR(100))")
        if (spark.sql(f"""SELECT * FROM metadb_marketo.api_limits WHERE date = cast('{self.current_date}' as DATE)""").count() == 0):
            spark.sql(f"""INSERT INTO metadb_marketo.api_limits VALUES (cast('{self.current_date}' as DATE), 0, 0)""")
        if (spark.sql(f"""SELECT * FROM metadb_marketo.leads_meta""").count() == 0):
            startDate = datetime.datetime.strptime('2021-01-01', '%Y-%m-%d').date()
            endDate = datetime.datetime.strptime('2021-01-31', '%Y-%m-%d').date()
            spark.sql(f"""INSERT INTO metadb_marketo.leads_meta VALUES (cast('{startDate}' as DATE), cast('{endDate}' as DATE), 'waiting')""")
        if (spark.sql(f"""SELECT * FROM metadb_marketo.activities_meta""").count() == 0):
            startDate = datetime.datetime.strptime('2021-01-01', '%Y-%m-%d').date()
            endDate = datetime.datetime.strptime('2021-01-07', '%Y-%m-%d').date()
            spark.sql(f"""INSERT INTO metadb_marketo.activities_meta VALUES (cast('{startDate}' as DATE), cast('{endDate}' as DATE), 'waiting')""")
            
    def allow_request(self, url):
        data = spark.sql(f"SELECT * FROM metadb_marketo.api_limits WHERE date = cast('{self.current_date}' as DATE)").collect()
        for row in data:
            self.api_calls = row['calls']
            self.filesize = row['filesize']
        if self.api_calls < self.api_calls_threshold:
            if 'export' in url:
                if self.filesize < self.filesize_threshold:
                    return True
                else:
                    return False
            return True
        else:
            return False

        # if 'export' not in url:
        #     if self.api_calls < self.api_calls_threshold:
        #         return True
        #     else:
        #         return False
        # elif 'export' in url:
        #     if self.filesize < self.filesize_threshold:
        #         return True
        #     else:
        #         return False


    def get_token (self):
        '''
        Authentication :- Identity : Identity Controller
        https://developers.marketo.com/rest-api/endpoint-reference/authentication-endpoint-reference/#!/Identity/identityUsingGET
        '''
        auth_server_url = f"{self.base_uri}{MARKETO_API_ENDPOINTS['IDENTITY']['_get_token']}"
        token_req_payload = {'client_id': self.client_id, 'client_secret': self.client_secret,'grant_type': 'client_credentials'}
        token_response = requests.get(auth_server_url,params=token_req_payload)

        if token_response.status_code == 200:
            self.access_token = json.loads(token_response.text)['access_token']
            logger.success('Token generated successfully')
            self._create_auth_secret(string_value=self.access_token)
            logger.info(f'access_token: {self.access_token}')
        else:
            raise ValueError(MarketoException(self.get_token.__name__, json.loads(token_response.text)['errors'][0]).return_error())

    
    def _make_request(self, url, params={}, headers={}, data={}, verb=None):
        api_auth_token = self._get_auth_secret()
        headers["Authorization"] = f"Bearer {api_auth_token}"
        if verb is None:
            verb = "GET"
        allow = self.allow_request(url)
        if allow:
            spark.sql(f"UPDATE metadb_marketo.api_limits SET calls = calls + 1, filesize = filesize + {self.datasize} where date = cast('{self.current_date}' as DATE)")
            resp = requests.request(verb, url=url, params=params, headers=headers, json=data)
            try:
                response = json.loads(resp.text)
            except Exception as e:
                if resp.status_code in [200, 206]:
                    DF = pd.read_csv(StringIO(resp.text), dtype='str', encoding='utf-8', engine='python')
                    DF = spark.createDataFrame(DF)
                    return DF
                else:
                    raise Exception(f"Request Failed: {str(e)}")
            logger.success(f"Request triggered {url}")
            logger.debug(response)
            if response["success"] == False:
                if response["errors"][0]["code"] in ("601", "602"):
                    logger.info("Access token is invalid/ expired.")
                    self.get_token()
                    response = self._make_request(url, params, headers, data, verb)
                    return response
                else:
                    raise Exception(MarketoException(self._make_request.__name__, response['errors'][0]).return_error())
            elif response["success"]:
                logger.success(f"Request {url} successfully completed.")
                return response
            else:
                raise Exception(f"Empty response: {response}")
        else:
            raise Exception(f"API Limits threshold reached for {self.current_date}. Modify threshold in __init__ to continue")
        

    def _create_auth_secret(self, scope='marketo', key='accessToken', string_value=None):
        workspace_url = dbutils.secrets.get(scope='internal', key='WorkspaceUrl')
        dbx_token = dbutils.secrets.get(scope='internal', key='BearerToken')
        db = DatabricksAPI(host=workspace_url, token=dbx_token)
        try:
            db.secret.put_secret(scope=scope, key=key, string_value=string_value)
        except Exception as e:
            error = {
                'code': None,
                'message': str(e)
            }
            logger.error(f"Failed to create secret '{key}' within secret scope '{scope}'")
            raise Exception(MarketoException(self._create_auth_secret.__name__, error.return_error()))
                             
    def _get_auth_secret(self):
        auth_token = dbutils.secrets.get(scope='marketo', key='accessToken')
        return auth_token

    def _prepare_args(self, args):
        if len(args) > 0:    
            prepped_args = {}
            for k, v in args.items():
                if v is not None:
                    prepped_args[k] = v
            return prepped_args
        else:
            return args

    
    def _process_offset(self, url, args, result_list=[]):
        while True:
            result = self._make_request(url, args)
            if result is None:
                raise Exception("Empty Response")
            if 'result' in result:
                result_list.extend(result['result'])
                if len(result['result']) < args['maxReturn']:
                    break
            else:
                break
            args['offset'] += args['maxReturn']
        return result_list

    # ---------- ACTIVITIES ----------

    def _process_activity_batch(self, url, args, result_list=[]):
        while True:
            result = self._make_request(url, params=args)
            if result is None:
                raise Exception("Empty Response")
            if 'result' in result and result['result']:
                result_list.extend(result['result'])
            if result['moreResult'] is False:
                break
            args['nextPageToken'] = result['nextPageToken']
        return result_list


    def get_activity_types(self):
        '''
        Lead Database :- Activities: Activity Controller
        https://developers.marketo.com/rest-api/endpoint-reference/lead-database-endpoint-reference/#!/Activities/getAllActivityTypesUsingGET
        '''
        url = f"{self.base_uri}{MARKETO_API_ENDPOINTS['ACTIVITIES']['get_activity_types']}"
        return self._make_request(url)['result']

    
    def _get_activity_ids(self, activity_types=BULK_ACTIVITY_TYPES):
        types = self.get_activity_types()
        types = {i["name"]: i["id"] for i  in types}
        activity_ids = [types[i] for i in activity_types]
        return activity_ids
        
    
    def get_paging_token(self, sinceDatetime):
        '''
        Lead Database -> Activities: Activity Controller -> Get Paging Token
        https://developers.marketo.com/rest-api/endpoint-reference/lead-database-endpoint-reference/#!/Activities/getActivitiesPagingTokenUsingGET
        '''
        if sinceDatetime is None:
            raise ValueError("Invalid argument: required argument sinceDatetime is none.")
        else:
            args = {'sinceDatetime': sinceDatetime}
            url = f"{self.base_uri}{MARKETO_API_ENDPOINTS['ACTIVITIES']['get_paging_token']}"
            return self._make_request(url, params=args)['nextPageToken']


    def get_lead_activities(self, activityTypeIds, nextPageToken=None, sinceDatetime=None, assetIds=None, listId=None, leadIds=None, batchSize=None):
        '''
        Lead Database -> Activities: Activity Controller -> Get Lead Activities
        https://developers.marketo.com/rest-api/endpoint-reference/lead-database-endpoint-reference/#!/Activities/getLeadActivitiesUsingGET
        '''
        if activityTypeIds is None:
            raise ValueError("Invalid argument: activityTypeIds needs to be specified")
        if nextPageToken is None and sinceDatetime is None:
            raise ValueError("Invalid argument: Either nextPageToken or sinceDatetime needs to be specified")
        else:
            if nextPageToken is None:
                nextPageToken = self.get_paging_token(sinceDatetime=sinceDatetime)
            keys = ['nextPageToken', 'activityTypeIds', 'assetIds', 'listId', 'leadIds', 'batchSize']
            values = [nextPageToken, activityTypeIds, assetIds, listId, leadIds, batchSize]
            args = self._prepare_args(dict(zip(keys, values)))

            url = f"{self.base_uri}{MARKETO_API_ENDPOINTS['ACTIVITIES']['get_lead_activities']}"
            result_list = self._process_activity_batch(url, args)
            return result_list


    def get_deleted_leads(self, nextPageToken=None, sinceDatetime=None, batchSize=None):
        '''
        Lead Database -> Activities: Activity Controller -> Get Deleted Leads 
        https://developers.marketo.com/rest-api/endpoint-reference/lead-database-endpoint-reference/#!/Activities/getDeletedLeadsUsingGET
        '''
        if nextPageToken is None and sinceDatetime is None:
            raise ValueError("Invalid argument: Either nextPageToken or sinceDatetime needs to be specified")
        else:
            if nextPageToken is None:
                nextPageToken = self.get_paging_token(sinceDatetime=sinceDatetime)

            keys = ['nextPageToken', 'batchSize']
            values = [nextPageToken, batchSize]
            args = self._prepare_args(dict(zip(keys, values)))
            
            url = f"{self.base_uri}{MARKETO_API_ENDPOINTS['ACTIVITIES']['get_deleted_leads']}"
            result_list = self._process_activity_batch(url, args)
            return result_list


    def get_custom_activity_types(self):
        '''
        Lead Database -> Activities: Activity Controller -> Get Custom Activity Types 
        https://developers.marketo.com/rest-api/endpoint-reference/lead-database-endpoint-reference/#!/Activities/getCustomActivityTypeUsingGET
        '''
        url = f"{self.base_uri}{MARKETO_API_ENDPOINTS['ACTIVITIES']['get_custom_activity_types']}"
        return self._make_request(url)

    # ---------- CAMPAIGNS ----------

    def get_campaigns(self, id=None, name=None, programName=None, workspaceName=None, batchSize=None, nextPageToken=None, isTriggerable=None):
        '''
        Lead Database -> Campaigns : Campaigns Controller -> Get Campaigns 
        https://developers.marketo.com/rest-api/endpoint-reference/lead-database-endpoint-reference/#!/Campaigns/getCampaignsUsingGET
        '''
        keys = ['id', 'name', 'programName', 'workspaceName', 'batchSize', 'nextPageToken', 'isTriggerable']
        values = [id, name, programName, workspaceName, batchSize, nextPageToken, isTriggerable]
        args = self._prepare_args(dict(zip(keys, values)))

        url = f"{self.base_uri}{MARKETO_API_ENDPOINTS['CAMPAIGNS']['get_campaigns']}" 

        result_list = []
        while True:
            result = self._make_request(url, params=args)
            if result is None:
                raise Exception("Empty Response")
            if 'result' in result and result['result']:
                result_list.extend(result['result'])
            if len(result['result']) == 0 or 'nextPageToken' not in result:
                break
            args['nextPageToken'] = result['nextPageToken']
        return result_list

    # ---------- PROGRAMS ----------

    def get_programs(self, maxReturn=200, offset=0, filterType=None, earliestUpdatedAt=None, latestUpdatedAt=None):
        '''
        Asset -> Programs : Program Controller -> Get Programs
        https://developers.marketo.com/rest-api/endpoint-reference/asset-endpoint-reference/#!/Programs/browseProgramsUsingGET
        '''
        keys = ['maxReturn', 'offset', 'filterType', 'earliestUpdatedAt', 'latestUpdatedAt']
        values = [maxReturn, offset, filterType, earliestUpdatedAt, latestUpdatedAt]
        args = self._prepare_args(dict(zip(keys, values)))

        url = f"{self.base_uri}{MARKETO_API_ENDPOINTS['PROGRAMS']['get_programs']}"
        return self._process_offset(url, args)

    # ---------- LEADS ----------

    def describe_lead2(self):
        '''
        Lead Database -> Leads : Leads Controller -> Describe Lead2
        https://developers.marketo.com/rest-api/endpoint-reference/lead-database-endpoint-reference/#!/Leads/describeUsingGET_6
        '''
        url = f"{self.base_uri}{MARKETO_API_ENDPOINTS['LEADS']['describe_lead2']}"
        return self._make_request(url)['result']

    
    def describe_lead(self):
        '''
        Lead Database -> Leads : Leads Controller -> Describe Lead
        https://developers.marketo.com/rest-api/endpoint-reference/lead-database-endpoint-reference/#!/Leads/describeUsingGET_2
        '''
        url = f"{self.base_uri}{MARKETO_API_ENDPOINTS['LEADS']['describe_lead']}"
        return self._make_request(url)['result']

    # ---------- CHANNELS ----------

    def get_channels(self, maxReturn=200, offset=0):
        '''
        Asset -> Channels : Channels Controller -> Get Channels
        https://developers.marketo.com/rest-api/endpoint-reference/asset-endpoint-reference/#!/Channels/getAllChannelsUsingGET
        '''
        keys = ['maxReturn', 'offset']
        values = [maxReturn, offset]
        args = self._prepare_args(dict(zip(keys, values)))
        
        url = f"{self.base_uri}{MARKETO_API_ENDPOINTS['CHANNELS']['get_channels']}"
        return self._process_offset(url, args)

    # ---------- SMART CAMPAIGNS ----------

    def get_smart_campaigns(self, maxReturn=200, offset=0, folder=None, earliestUpdatedAt=None, latestUpdatedAt=None, isActive=None):
        '''
        Asset -> Smart Campaigns : Smart Campaign Controller -> Get Smart Campaigns
        https://developers.marketo.com/rest-api/endpoint-reference/asset-endpoint-reference/#!/Smart_Campaigns/getAllSmartCampaignsGET
        '''
        keys = ['maxReturn', 'offset', 'folder', 'earliestUpdatedAt', 'latestUpdatedAt', 'isActive']
        values = [maxReturn, offset, folder, earliestUpdatedAt, latestUpdatedAt, isActive]
        args = self._prepare_args(dict(zip(keys, values)))

        url = f"{self.base_uri}{MARKETO_API_ENDPOINTS['SMART_CAMPAIGNS']['get_smart_campaigns']}"
        return self._process_offset(url, args)

    # ---------- TAGS ----------

    def get_tag_types(self, maxReturn=200, offset=0):
        '''
        Asset -> Tags : Tag Controller -> Get Tag Types
        https://developers.marketo.com/rest-api/endpoint-reference/asset-endpoint-reference/#!/Tags/getTagTypesUsingGET
        '''
        keys = ['maxReturn', 'offset']
        values = [maxReturn, offset]
        args = self._prepare_args(dict(zip(keys, values)))

        url = f"{self.base_uri}{MARKETO_API_ENDPOINTS['TAGS']['get_tag_types']}"
        return self._process_offset(url, args)

    # --------- SMART LISTS ---------------

    def get_smart_lists(self, folder=None, offset=0, maxReturn=200, earliestUpdatedAt=None, latestUpdatedAt=None):
        '''
        Asset -> Smart Lists : Smart List Controller -> Get Smart Lists
        https://developers.marketo.com/rest-api/endpoint-reference/asset-endpoint-reference/#!/Smart_Lists/getSmartListsUsingGET        '''
        keys = ['folder', 'offset', 'maxReturn', 'earliestUpdatedAt', 'latestUpdatedAt']
        values = [folder, offset, maxReturn, earliestUpdatedAt, latestUpdatedAt]
        args = self._prepare_args(dict(zip(keys, values)))

        url = f"{self.base_uri}{MARKETO_API_ENDPOINTS['SMART_LISTS']['get_smart_lists']}" 
        return self._process_offset(url, args)
    
    # ---------------- STATIC LISTS -------------

    def get_static_lists(self, folder=None, offset=0, maxReturn=200, earliestUpdatedAt=None, latestUpdatedAt=None):
        '''
        Asset -> Static Lists : Static List Controller -> Get Static Lists
        https://developers.marketo.com/rest-api/endpoint-reference/asset-endpoint-reference/#!/Static_Lists/getStaticListsUsingGET   ''' 
         
        keys = ['folder', 'offset', 'maxReturn', 'earliestUpdatedAt', 'latestUpdatedAt']
        values = [folder, offset, maxReturn, earliestUpdatedAt, latestUpdatedAt]
        args = self._prepare_args(dict(zip(keys, values)))

        url = f"{self.base_uri}{MARKETO_API_ENDPOINTS['STATIC_LISTS']['get_static_lists']}" 
        return self._process_offset(url, args)
    
    # ---------------- LEADS STATIC LISTS -------------

    def get_leads_by_list_id(self, listId, fields=None, batchSize=None, nextPageToken=None):
        '''
        LeadDatabase -> Static Lists : Lists Controller -> Get Leads By List Id
        https://developers.marketo.com/rest-api/endpoint-reference/lead-database-endpoint-reference/#!/Static_Lists/getLeadsByListIdUsingGET '''
        
        if listId is None:
            raise Exception("Invalid Arguments: Required listId cannot be None")
        
        keys = ['listId', 'fields', 'batchSize', 'nextPageToken']
        values = [listId, fields, batchSize, nextPageToken]
        args = self._prepare_args(dict(zip(keys, values)))

        url = f"{self.base_uri}{MARKETO_API_ENDPOINTS['LEADS_STATIC_LISTS']['get_leads_by_list_id']}" 
        return self._process_offset(url, args)

    # ---------- Get_Lists ----------
    def get_lists(self, id=None, name=None, programName=None, workspaceName=None, batchSize=None, nextPageToken=None):
        keys = ['id', 'name', 'programName', 'workspaceName','batchSize','nextPageToken']
        values = [id, name, programName, workspaceName, batchSize, nextPageToken]
        args = self._prepare_args(dict(zip(keys, values)))
        url = f"{self.base_uri}{MARKETO_API_ENDPOINTS['LEADS_STATIC_LISTS']['get_lists']}"
        result_list = []
        while True:
            result = self._make_request(url, params=args)
            if result is None:
                raise Exception("Empty Response")
            if 'result' in result and result['result']:
                result_list.extend(result['result'])
            if len(result['result']) == 0 or 'nextPageToken' not in result:
                break
            args['nextPageToken'] = result['nextPageToken']
        return result_list

# ----------- Bulk Export Activities-------------

    def _validate_format_date(self, date_text):
        try:
            date = datetime.datetime.strptime(date_text, '%Y-%m-%d').isoformat()
            return date
        except ValueError:
            raise ValueError("Incorrect date format, should be YYYY-MM-DD")


    def bulk_export_activities(self, destination=None, incrementalFlag=False):
        if incrementalFlag==False:
            startAt = pytz.utc.localize(datetime.datetime.utcnow() - relativedelta(days=2)).astimezone(pytz.timezone("US/Central"))
            startAt = datetime.datetime.fromisoformat(str(startAt)).strftime("%Y-%m-%d")
            endAt = self.current_date
        else:
            if(spark.sql(f"""SELECT status FROM metadb_marketo.activities_meta""").first()["status"]=='finished'):
                raise Exception(f"Jobs finished fetching data till current datetime. Schedule daily jobs for future data")
            startAt = spark.sql(f"""SELECT startDate FROM metadb_marketo.activities_meta WHERE status='waiting'""").first()["startDate"]
            endAt = spark.sql(f"""SELECT endDate FROM metadb_marketo.activities_meta WHERE status='waiting'""").first()["endDate"]
            startAt = self._validate_format_date(str(startAt))
            endAt = self._validate_format_date(str(endAt))
        dbutils.fs.rm(destination, True)
        logger.info(f"######### Data being fetched for timeframe: {startAt} to {endAt} ##########")
        activity_ids = self._get_activity_ids()
        create_url = f"{self.base_uri}{MARKETO_API_ENDPOINTS['BULK_EXPORT_ACTIVITIES']['create_export_activity_job']}"
        data = {"format": "CSV",
                    "filter": { 
                        "createdAt": { 
                            "startAt": startAt,
                            "endAt": endAt
                        },
                        "activityTypeIds": activity_ids
                    }
                    }
        response = self._make_request(create_url, data=data, headers={"Content-type" : "application/json; charset=utf-8"}, verb="POST")
        
        if response['success']:
            exportId = response['result'][0]['exportId']
            if exportId is None:
                raise Exception("ExportId is none.")
        else:
            raise Exception(f"Failed to create export activity job: {str(response.text)}")

        enqueue_url = (f"{self.base_uri}{MARKETO_API_ENDPOINTS['BULK_EXPORT_ACTIVITIES']['enqueue_export_activity_job']}").replace('{exportId}', exportId)
        response = self._make_request(enqueue_url, headers={"Content-type" : "application/json; charset=utf-8"}, verb="POST")
        
        if response['success']:
            exportId = response['result'][0]['exportId']
        else:
            raise Exception(f"Failed to enqueue export activity job: {str(response.text)}")

        status_url = (f"{self.base_uri}{MARKETO_API_ENDPOINTS['BULK_EXPORT_ACTIVITIES']['get_export_activity_job_status']}").replace('{exportId}', exportId)

        status = "Processing"
        wait_time = 0
        wait = 120
        while status != "Completed":
            if wait_time > 1500:
                raise Exception(f"Marketo didn't process activitiy data in {wait_time} seconds for export id {exportId}. Change wait_time to wait for longer.")
            time.sleep(wait)
            response = self._make_request(status_url)
            if response['success']:
                status = response['result'][0]['status']
            else:
                raise Exception(f"Error in getting status of export activity job: {str(response.text)}")
            wait_time += wait
        wait_time = 0

        self.datasize = response['result'][0]['fileSize']
        
#         if destination:
#             i = 0
#             step = 100000000
#             l = 0
#             bytes_extracted = 0
#             for _ in range(0, math.ceil(total_filesize/step)):
#                 if total_filesize - bytes_extracted >= step:
#                     l = l + step
#                     headers = {"Range": f"bytes={i}-{l}"}
#                     logger.debug(headers)
#                     self.datasize = l - i
#                     bytes_extracted += self.datasize
#                     get_file_url = (f"{self.base_uri}{MARKETO_API_ENDPOINTS['BULK_EXPORT_ACTIVITIES']['get_export_activity_file']}").replace('{exportId}', exportId)
#                     result = self._make_request(get_file_url, headers=headers)
#                     result.write.mode("append").format("json").save(destination)
#                     i = l
#                 else:
#                     l = total_filesize - 1
#                     headers = {"Range": f"bytes={i}-{l}"}
#                     logger.debug(headers)
#                     self.datasize = l - i
#                     get_file_url = (f"{self.base_uri}{MARKETO_API_ENDPOINTS['BULK_EXPORT_ACTIVITIES']['get_export_activity_file']}").replace('{exportId}', exportId)
#                     result = self._make_request(get_file_url, headers=headers)
#                     result.write.mode("append").format("json").save(destination)

        get_file_url = (f"{self.base_uri}{MARKETO_API_ENDPOINTS['BULK_EXPORT_ACTIVITIES']['get_export_activity_file']}").replace('{exportId}', exportId)
        result = self._make_request(get_file_url)
        result.write.mode("append").format("json").save(destination)
        # return result

        # ----------- Bulk Export Leads -------------

    def bulk_export_leads(self, destination=None, incrementalFlag=False):
        if incrementalFlag==False:
            startAt = pytz.utc.localize(datetime.datetime.utcnow() - relativedelta(days=2)).astimezone(pytz.timezone("US/Central"))
            startAt = datetime.datetime.fromisoformat(str(startAt)).strftime("%Y-%m-%d")
            endAt = self.current_date
        else:
            if(spark.sql(f"""SELECT status FROM metadb_marketo.leads_meta""").first()["status"]=='finished'):
                raise Exception(f"Jobs finished fetching data till current datetime. Schedule daily jobs for future data")
            startAt = spark.sql(f"""SELECT startDate FROM metadb_marketo.leads_meta WHERE status='waiting'""").first()["startDate"]
            endAt = spark.sql(f"""SELECT endDate FROM metadb_marketo.leads_meta WHERE status='waiting'""").first()["endDate"]
            startAt = self._validate_format_date(str(startAt))
            endAt = self._validate_format_date(str(endAt))
        dbutils.fs.rm(destination, True)
        logger.info(f"######### Data being fetched for timeframe: {startAt} to {endAt} ##########")
        fields = [i['rest']['name'] for i in self.describe_lead()]
        
        # if export_type in ("createdAt", "updatedAt"):
        data = {
            "fields": fields,
            "format": "CSV",
            "filter": {
                "createdAt": {
                    "startAt": startAt,
                    "endAt": endAt
                }
            }
            }
        # elif export_type == "both":
        #     data = {
        #         "fields": fields,
        #         "format": "CSV",
        #         "filter": {
        #             "createdAt": {
        #                 "startAt": "2017-01-01T00:00:00Z",
        #                 "endAt": "2017-01-31T00:00:00Z"
        #             },
        #             "updatedAt": {
        #                 "startAt": "2017-01-01T00:00:00Z",
        #                 "endAt": "2017-01-31T00:00:00Z"
        #             }
        #         }
        #         }

        create_url = f"{self.base_uri}{MARKETO_API_ENDPOINTS['BULK_EXPORT_LEADS']['create_export_lead_job']}"
        response = self._make_request(create_url, data=data, headers={"Content-type" : "application/json; charset=utf-8"}, verb="POST")
        
        if response['success']:
            exportId = response['result'][0]['exportId']
            if exportId is None:
                raise Exception("ExportId is none.")
        else:
            raise Exception(f"Failed to create export activity job: {str(response.text)}")

        enqueue_url = (f"{self.base_uri}{MARKETO_API_ENDPOINTS['BULK_EXPORT_LEADS']['enqueue_export_lead_job']}").replace('{exportId}', exportId)
        response = self._make_request(enqueue_url, headers={"Content-type" : "application/json; charset=utf-8"}, verb="POST")
        
        if response['success']:
            exportId = response['result'][0]['exportId']
        else:
            raise Exception(f"Failed to enqueue export activity job: {str(response.text)}")

        status_url = (f"{self.base_uri}{MARKETO_API_ENDPOINTS['BULK_EXPORT_LEADS']['get_export_lead_job_status']}").replace('{exportId}', exportId)

        status = "Processing"
        wait_time = 0
        wait = 120
        while status != "Completed":
            time.sleep(wait)
            if wait_time > 600:
                raise Exception(f"Marketo didn't process leads data in {wait_time} seconds for export id {exportId}. Change wait_time to wait for longer.")
            response = self._make_request(status_url)
            if response['success']:
                status = response['result'][0]['status']
            else:
                raise Exception(f"Error in getting status of export activity job: {str(response.text)}")
            wait_time += wait
        wait_time = 0
        
        self.datasize = response['result'][0]['fileSize']

#         if destination:
#             i = 0
#             step = 100000000
#             l = 0
#             bytes_extracted = 0
#             for _ in range(0, math.ceil(total_filesize/step)):
#                 if total_filesize - bytes_extracted >= step:
#                     l = l + step
#                     headers = {"Range": f"bytes={i}-{l}"}
#                     self.datasize = l - i
#                     bytes_extracted += self.datasize
#                     get_file_url = (f"{self.base_uri}{MARKETO_API_ENDPOINTS['BULK_EXPORT_LEADS']['get_export_lead_file']}").replace('{exportId}', exportId)
#                     result = self._make_request(get_file_url, headers=headers)
#                     result.write.mode("append").format("json").save(destination)
#                     i = l
#                 else:
#                     l = total_filesize - 1
#                     headers = {"Range": f"bytes={i}-{l}"}
#                     self.datasize = l - i
#                     get_file_url = (f"{self.base_uri}{MARKETO_API_ENDPOINTS['BULK_EXPORT_LEADS']['get_export_lead_file']}").replace('{exportId}', exportId)
#                     result = self._make_request(get_file_url, headers=headers)
#                     result.write.mode("append").format("json").save(destination)
  
        get_file_url = (f"{self.base_uri}{MARKETO_API_ENDPOINTS['BULK_EXPORT_LEADS']['get_export_lead_file']}").replace('{exportId}', exportId)
        result = self._make_request(get_file_url)
        result.write.mode("append").format("json").save(destination)
        # return result