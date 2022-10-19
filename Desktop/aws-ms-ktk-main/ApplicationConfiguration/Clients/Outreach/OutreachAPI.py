import requests
import sys
import json
import time,math
import datetime
from loguru import logger
from databricks_api import DatabricksAPI
from pyspark.sql import SparkSession
from pyspark.dbutils import DBUtils
from dateutil.relativedelta import relativedelta
from pyspark import SparkContext

logger.add(sink=f"./logs/outreach/out_{datetime.datetime.today().strftime('%d-%m-%Y')}.log", rotation="00:00", retention="30 days")

spark = SparkSession.builder.appName('DatabricksFramework').getOrCreate()
dbutils = DBUtils(spark)
sc = SparkContext.getOrCreate()

class OutreachException:
    '''This class is being used to handle errors encountered while making API calls.'''

    def __init__(self, source_name, error):
        if 'getToken' in source_name:
            self.err = {
                'sourceName': source_name,
                'errorCode': error['error'],
                'errorDescription': error['error_description']
            }
        else:
            self.err = error

    def return_error(self):
        return self.err

class OutreachAPI:
    def __init__(self, client_id=None, client_secret=None, access_token=None, refresh_token=None, authorization_code=None, urlDateTime=None, isInitialLoad=None):
        assert ((client_id and client_secret and authorization_code) or (client_id and client_secret and access_token and refresh_token and isInitialLoad)), logger.critical('Provide either client_id, client_secret and authroization_code or access_token and refresh_token along with creds and load criteria')
        self.authorization_code = authorization_code
        self.client_id = client_id
        self.client_secret = client_secret
        self.access_token = access_token
        self.refresh_token = refresh_token
        self.isInitialLoad = isInitialLoad
        self.urlDateTime = urlDateTime
        if not self.access_token and not self.refresh_token:
            self.getToken()
        if self.isInitialLoad=='True':
            self.urlDateTime='2021-01-01T00:00:00.000Z'
        else:
            self.urlDateTime=str((datetime.datetime.now() - relativedelta(days=2)).strftime('%Y-%m-%dT00:00:00.000Z'))
        

    def getToken(self):
        token_url = "https://api.outreach.io/oauth/token"
        redirect_uri = "https://nintex.com/oauth/outreach"
        data = {'grant_type': 'authorization_code', 'code': self.authorization_code, 'redirect_uri': redirect_uri}
        access_token_response = requests.post(token_url, data=data, verify=False, allow_redirects=False,
                                              auth=(self.client_id, self.client_secret))
        if access_token_response.status_code == 200:
            self.access_token = json.loads(access_token_response.text)['access_token']
            self.refresh_token = json.loads(access_token_response.text)['refresh_token']
            logger.success('Token generated successfully')
            logger.info(f'access_token: {self.access_token}')
            logger.info(f'refresh_token: {self.refresh_token}')
            self.create_auth_secret(key='accessToken', string_value=self.access_token)
            self.create_auth_secret(key='refreshToken', string_value=self.refresh_token)
            self.create_auth_secret(key='refreshFlag', string_value='none')
            logger.info('Tokens added to Secrets successfully')
        else:
            raise ValueError(OutreachException(self.getToken.__name__,json.loads(access_token_response.text)).return_error())

    def refreshToken(self):
        refreshFlag = dbutils.secrets.get(scope='outreach', key='refreshFlag')
        if refreshFlag=='hold':
            time.sleep(5)
            return
        else:
            self.create_auth_secret(key='refreshFlag', string_value='hold')
            token_url = "https://api.outreach.io/oauth/token"
            redirect_uri = "https://nintex.com/oauth/outreach"
            data = {'grant_type': 'refresh_token', 'refresh_token': self.refresh_token, 'redirect_uri': redirect_uri}
            refresh_token_response = requests.post(token_url, data=data, verify=False, allow_redirects=False,
                                                   auth=(self.client_id, self.client_secret))
            if refresh_token_response.status_code == 200:
                self.access_token = json.loads(refresh_token_response.text)['access_token']
                self.refresh_token = json.loads(refresh_token_response.text)['refresh_token']
                logger.success('Token generated successfully')
                logger.info(f'access_token: {self.access_token}')
                logger.info(f'refresh_token: {self.refresh_token}')
                self.create_auth_secret(key='accessToken', string_value=self.access_token)
                self.create_auth_secret(key='refreshToken', string_value=self.refresh_token)
                logger.info(f'access_token: {self.access_token}')
                self.create_auth_secret(key='refreshFlag', string_value='none')
            else:
                raise ValueError(OutreachException(self.refresh_token.__name__,json.loads(refresh_token_response.text)).return_error())
    
    def create_auth_secret(self, key , scope='outreach', string_value=None):
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
            raise Exception(OutreachException(self.create_auth_secret.__name__, error.return_error()))
    
    def epochconverter(self, epochtime):
        current_datetime=int(datetime.datetime.now().strftime("%s"))
        reset_datetime=int(epochtime)
        logger.info(f"Current time in epoch is: {current_datetime}")
        logger.info(f"Limit time in epoch is: {reset_datetime}")
        return((reset_datetime-current_datetime)+10)
    
    def make_recursive_request(self, recursiveurl, transientPath, flag, objectName, apiCallCounter, processedRecords, retryCounter, writeCounter): 
        if processedRecords==None:
            processedRecords=[]
        if retryCounter==None:
            retryCounter=0
        if writeCounter==None:
            writeCounter=0
        while(flag!=0):
            apiCallCounter-=1
            writeCounter+=1
            logger.info(apiCallCounter+1)
            logger.info(writeCounter)
            bearer_token = f"Bearer {self.access_token}"
            payload = {}
            headers = {
                'Authorization': bearer_token
            }
            logger.info('###Awaiting Repsonse###')
            r = requests.request("GET", recursiveurl, headers=headers, data=payload)
            logger.info('###Response Recieved###')
            if r.status_code!=200:
                if r.status_code==401:
                    self.refreshToken()
                    apiCallCounter+=1
                    writeCounter-=1
                    continue
                elif r.status_code==429:
                    sleepduration=self.epochconverter(r.headers['x-ratelimit-reset'])
                    logger.info(f'Hourly API Limit exhaustion: Pipeline on sleep for: {sleepduration} seconds')
                    time.sleep(sleepduration)
                    apiCallCounter+=1
                    writeCounter-=1
                    continue
                elif r.status_code==500 or r.status_code==503:
                    if retryCounter>=5:
                         raise Exception(f"Server not responding since 5 consecutive tries with status code: {r.status_code}")
                    retryCounter+=1
                    logger.info(f'Encountered Server Error: {r.status_code} Retrying after 30 seconds, RetryCounter: {retryCounter}')
                    time.sleep(30)
                    apiCallCounter+=1
                    writeCounter-=1
                    continue
                else:  
                    raise Exception(f"Corrupt response with status code: {r.status_code}")
                    #raise Exception(OutreachException(self.make_recursive_request.__name__, response).return_error())
            elif r.status_code == 200:
                retryCounter=0
                response = json.loads(r.text)
                logger.success(f"Request {recursiveurl} successfully completed.")
                fetchedRecords=response['data']
                if 'next' not in response['links'].keys():
                    flag=0
                    processedRecords.extend(fetchedRecords)
                    jsondata=json.dumps(processedRecords)
                    df = spark.read.json(sc.parallelize([jsondata]))
                    df.write.format('json').mode("append").save(transientPath)
                    logger.info('###Records written after last API Call###')
                    break
                else:
                    recursiveurl=response['links']['next']
                    processedRecords.extend(fetchedRecords)
                    if(writeCounter==100):
                        jsondata=json.dumps(processedRecords)
                        df = spark.read.json(sc.parallelize([jsondata]))
                        df.write.format('json').mode("append").save(transientPath)
                        processedRecords=[]
                        writeCounter=0
                        logger.info('###Records written after 100 consecutive API Calls###')
            else:
                raise Exception(f"Empty response with status code: {r.status_code}")
            
    def make_request(self, url, transientPath, objectName, retryCounter=None):
        if retryCounter==None:
                retryCounter=0
        bearer_token = f"Bearer {self.access_token}"
        payload = {}
        headers = {
            'Authorization': bearer_token
        }
        logger.info('###Awaiting Repsonse###')
        r = requests.request("GET", url, headers=headers, data=payload)
        logger.info('###Response Recieved###')
        #print(f"Timeout Value in header is: {self.epochconverter(r.headers['x-ratelimit-reset'])}")
        if r.status_code!=200:
            if r.status_code==401:
                self.refreshToken()
                self.make_request(url, transientPath, objectName)
            elif r.status_code==429:
                sleepduration=self.epochconverter(r.headers['x-ratelimit-reset'])
                logger.info(f'Hourly API Limit exhaustion: Pipeline on sleep for: {sleepduration} seconds')
                time.sleep(sleepduration)
                self.make_request(url, transientPath, objectName)
            elif r.status_code==500 or r.status_code==503:
                if retryCounter>=5:
                     raise Exception(f"Server not responding since 5 consecutive tries with status code: {r.status_code}")
                retryCounter+=1
                logger.info(f'Encountered Server Error: {r.status_code} Retrying after 30 seconds, RetryCounter: {retryCounter}')
                time.sleep(30)
                self.make_request(url, transientPath, objectName, retryCounter)
            else:
                raise Exception(f"Corrupt response with status code: {r.status_code}")
                #raise Exception(OutreachException(self.make_request.__name__, response).return_error())
        elif r.status_code == 200:
            response = json.loads(r.text)
            logger.success(f"Request {url} successfully completed.")
            processedrecords=response['data']
            if 'next' in response['links'].keys():
                recursiveurl=response['links']['next']
                recursiveurl=recursiveurl.replace('count=true','count=false')
                logger.info(f"Total count of records: {int(response['meta']['count'])}")
                apiCallCounter=math.ceil(int(response['meta']['count'])/250)-1
                flag=1
                jsondata=json.dumps(processedrecords)
                df = spark.read.json(sc.parallelize([jsondata]))
                df.write.format('json').mode("overwrite").save(transientPath)
                logger.info('###Records written for first API Call###')
                self.make_recursive_request(recursiveurl, transientPath, flag, objectName, apiCallCounter, None, None, None)
            else:
                if processedrecords==[]:
                    logger.info('###No records found in current timeframe###')
                    dbutils.fs.rm(transientPath,True)
                    return
                jsondata=json.dumps(processedrecords)
                df = spark.read.json(sc.parallelize([jsondata]))
                df.write.format('json').mode("overwrite").save(transientPath)
                logger.info('###Records written for last API Call###')
                return
        else:
            raise Exception(f"Empty response with status code: {r.status_code}")

            
    def getAccounts(self,transientPath):
        url = "https://api.outreach.io/api/v2/accounts?newFilterSyntax=true&filter[createdAt][gte]="+self.urlDateTime+"&page[size]=250&count=true&sort=createdAt"
        return self.make_request(url,transientPath,'Accounts')

    def getUsers(self,transientPath):
        url = "https://api.outreach.io/api/v2/users?newFilterSyntax=true&filter[createdAt][gte]="+self.urlDateTime+"&page[size]=250&count=true&sort=createdAt"
        return self.make_request(url,transientPath,'Users')

    def getTeams(self,transientPath):
        url = "https://api.outreach.io/api/v2/teams?newFilterSyntax=true&filter[createdAt][gte]="+self.urlDateTime+"&page[size]=250&count=true&sort=createdAt"
        return self.make_request(url,transientPath,'Teams')

    def getProspects(self,transientPath):
        url = "https://api.outreach.io/api/v2/prospects?newFilterSyntax=true&filter[createdAt][gte]="+self.urlDateTime+"&page[size]=250&count=true&sort=createdAt"
        return self.make_request(url,transientPath,'Prospects')

    def getCalls(self,transientPath):
        url = "https://api.outreach.io/api/v2/calls?newFilterSyntax=true&filter[createdAt][gte]="+self.urlDateTime+"&page[size]=250&count=true&sort=createdAt"
        return self.make_request(url,transientPath,'Calls')

    def getEvents(self,transientPath):
        url = "https://api.outreach.io/api/v2/events?newFilterSyntax=true&filter[createdAt][gte]="+self.urlDateTime+"&page[size]=250&count=true&sort=createdAt"
        return self.make_request(url,transientPath,'Events')

    def getMailings(self,transientPath):
        url = "https://api.outreach.io/api/v2/mailings?newFilterSyntax=true&filter[createdAt][gte]="+self.urlDateTime+"&page[size]=250&count=true&fields[mailing]=attributableSequenceId,attributableSequenceName,bouncedAt,bodyText,clickCount,clickedAt,createdAt,deliveredAt,followUpSequenceId,followUpSequenceName,followUpSequenceStartingDate,followUpTaskType,mailingType,markedAsSpamAt,meetingDescription,meetingDuration,meetingLocation,meetingTitle,openCount,openedAt,repliedAt,state,stateChangedAt,subject,trackLinks,trackOpens,unsubscribedAt,updatedAt,calendar,followUpSequence,mailbox,opportunity,prospect,schedule,sequence,sequenceState,sequenceStep,task,tasks,template,user&sort=createdAt"
        return self.make_request(url,transientPath,'Mailings')

    def getTasks(self,transientPath):
        url = "https://api.outreach.io/api/v2/tasks?newFilterSyntax=true&filter[createdAt][gte]="+self.urlDateTime+"&page[size]=250&count=true&sort=createdAt"
        return self.make_request(url,transientPath,'Tasks')

    def getSequence_states(self,transientPath):
        url = "https://api.outreach.io/api/v2/sequenceStates?newFilterSyntax=true&filter[createdAt][gte]="+self.urlDateTime+"&page[size]=250&count=true&sort=createdAt"
        return self.make_request(url,transientPath,'Sequence_states')

    def getSequence_steps(self,transientPath):
        url = "https://api.outreach.io/api/v2/sequenceSteps?newFilterSyntax=true&filter[createdAt][gte]="+self.urlDateTime+"&page[size]=250&count=true&sort=createdAt"
        return self.make_request(url,transientPath,'Sequence_steps')

    def getSequences(self,transientPath):
        url = "https://api.outreach.io/api/v2/sequences?newFilterSyntax=true&filter[createdAt][gte]="+self.urlDateTime+"&page[size]=250&count=true&sort=createdAt"
        return self.make_request(url,transientPath,'Sequences')