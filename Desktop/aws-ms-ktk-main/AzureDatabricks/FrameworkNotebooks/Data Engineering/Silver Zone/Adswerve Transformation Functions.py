# Databricks notebook source
# pip install oauth2client

# COMMAND ----------

# pip install --upgrade google-api-python-client

# COMMAND ----------

from apiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials

# COMMAND ----------

# DBTITLE 1,To RUN
# # DB to store results & etc.
# databricksDbName = "Adswerve_Silver_Dummy"
# spark.sql("CREATE DATABASE IF NOT EXISTS "+databricksDbName)

# COMMAND ----------

# DBTITLE 1,Big Query Load Directly
# # Big Query Database & Table
# dbNameBigQuery = '13210892'
# tableNameBigQuery = 'ga_sessions_20220716'

# # Loading that table into a Dataframe & creating a view for SQL to work on
# temp = spark.read.format("bigquery").option("parentProject", "thestreet-analytics").option("table", "thestreet-analytics.{db}.{table}".format(db=dbNameBigQuery, table=tableNameBigQuery)).load()
# print(temp.count(), len(temp.columns))
# temp.createOrReplaceTempView(tableNameBigQuery)
# spark.sql("SELECT * FROM "+tableNameBigQuery).createOrReplaceTempView("ga_sessions_limit")  

# display(temp)

# COMMAND ----------

# Creating a folder to store .json for Management API code
dbutils.fs.mkdirs("FileStore/Adswerve")

# COMMAND ----------

dbutils.fs.ls("FileStore/Adswerve")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Management API Stuff

# COMMAND ----------

"""A simple example of how to access the Google Analytics API."""

from apiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials
from google.oauth2 import service_account


def get_service(api_name, api_version, scopes, key_file_location):
    """Get a service that communicates to a Google API.

    Args:
        api_name: The name of the api to connect to.
        api_version: The api version to connect to.
        scopes: A list auth scopes to authorize for the application.
        key_file_location: The path to a valid service account JSON key file.

    Returns:
        A service that is connected to the specified API.
    """

    # Creds need to be changed Would come from : https://pypi.org/project/google-auth/
    
    #credentials = ServiceAccountCredentials.from_json_keyfile_name(key_file_location, scopes=scopes)
    #credentials = service_account.Credentials.from_service_account_file( key_file_location, scopes=scopes)
    
    credentials = service_account.Credentials.from_service_account_file(
    key_file_location,
    scopes=scopes,
    subject='gapythonapi@ga-python-api-356213.iam.gserviceaccount.com')

    '''
    Ref : https://developers.google.com/identity/protocols/oauth2/service-account#python    
    
    credentials = service_account.Credentials.from_service_account_file(
    'service-account.json',
    scopes=['email'],
    subject='user@example.com')
    '''

    # Build the service object.
    service = build(api_name, api_version, credentials=credentials)

    return service


def get_first_profile_id(service):
    # Use the Analytics service object to get the first profile id.

    # Get a list of all Google Analytics accounts for this user
    accounts = service.management().accounts().list().execute()

    if accounts.get('items'):
        # Get the first Google Analytics account.
        account = accounts.get('items')[0].get('id')

        # Get a list of all the properties for the first account.
        properties = service.management().webproperties().list(
                accountId=account).execute()

        if properties.get('items'):
            # Get the first property id.
            property = properties.get('items')[0].get('id')

            # Get a list of all views (profiles) for the first property.
            profiles = service.management().profiles().list(
                    accountId=account,
                    webPropertyId=property).execute()

            if profiles.get('items'):
                # return the first view (profile) id.
                return profiles.get('items')[0].get('id')

    return None


def get_results(service, profile_id):
    # Use the Analytics Service Object to query the Core Reporting API
    # for the number of sessions within the past seven days.
    return service.data().ga().get(
            ids='ga:' + profile_id,
            start_date='7daysAgo',
            end_date='today',
            metrics='ga:sessions').execute()


def print_results(results):
    # Print data nicely for the user.
    if results:
        print ('View (Profile):', results.get('profileInfo').get('profileName'))
        print ('Total Sessions:', results.get('rows')[0][0])

    else:
        print ('No results found')


def main():
    # Define the auth scopes to request.
    scope = 'https://www.googleapis.com/auth/analytics.readonly'
    key_file_location = '/dbfs/FileStore/Adswerve/ga_python_api_356213_7d9ef9942faf.json' #Path to the .json file

    # Authenticate and construct service.
    service = get_service(
            api_name='analytics',
            api_version='v3',
            scopes=[scope],
            key_file_location=key_file_location)

    profile_id = get_first_profile_id(service)
    print_results(get_results(service, profile_id))

    print(type(service), type(profile_id))
    return service

# COMMAND ----------

service = main()
service

# COMMAND ----------

# Ref : https://developers.google.com/analytics/devguides/config/mgmt/v3/mgmtReference/management/customDimensions/list?apix_params=%7B%22accountId%22%3A%22126130613%22%2C%22webPropertyId%22%3A%22UA-6534317-1%22%7D#python

# Note: This code assumes you have an authorized Analytics service object.

# This request lists all custom dimensions for the authorized user.

import requests

try:
    dimensions = service.management().customDimensions().list(
      accountId='126130613',
      webPropertyId='UA-6534317-1',
  ).execute()

except TypeError as error:
  # Handle errors in constructing a query.
  print ('There was an error in constructing your query : %s' % error)

except requests.exceptions.HTTPError as error:
  # Handle API errors.
  print ('There was an API error : %s : %s' %
         (error.resp.status, error.resp.reason))


# COMMAND ----------

sessionCDs = ""
hitCDs = ""
productCDs = ""
cds = dimensions.get('items', []) #Analytics.Management.CustomDimensions.list(gaAccountId, gaPropertyId).items

excludedCDList = [] # include the exact indexes of CDs to exclude, example [74, 78, 115]

#tempInd

for i in range(0, len(cds)):
    if not cds[i]['active']:
        # ignore CD if not active
        continue
    
    ''' # Giving an error cause trying to access an empty list with an index val (ValueError: 1 is not in list)
    if excludedCDList.index(i+1) > -1:
        # ignore CD if in exclusion list
        continue
    '''
    tempInd = int(cds[i]['index'])
    if(cds[i]['scope'] == "SESSION" or cds[i]['scope'] == "USER"):
        #sessionCDs += "IF(customDimensions.index=" + tempInd + ",customDimensions.value,NULL) AS cd" + tempInd + ","
        sessionCDs += "(SELECT MAX(value) FROM exp_hits_cds  WHERE index = " +str(tempInd)+ ") AS cd" +str(tempInd)+ ","

    #Todo: Make sure that we can add session scoped to hit
    elif cds[i]['scope'] == "HIT" :
        #hitCDs += "MAX(IF(hits.customDimensions.index=" + tempInd + ",hits.customDimensions.value,NULL)) WITHIN RECORD AS cd" + tempInd + ","
        if i == 0:
        #special case because cd1 must be converted to NUMERIC
            hitCDs += "(SELECT TRY_CAST(MAX(value) AS INT) FROM exp_hits_cds  WHERE index = 1) AS cd1,"
        elif i == 11:
        #special case because cd12 is the contentID to the CMS and it was missing for some time off mobile.  This piece of regex grabs it from the pagepath if it isn't set already
            hitCDs += "case when (SELECT MAX(value) FROM exp_hits_cds AS customDimensions WHERE index = 12) is null then REGEXP_EXTRACT(hits.page.pagepath, r'\/.*(?:\/|\-)(\d{5,})(?:\.html|\/|)') else (SELECT MAX(value) FROM exp_hits_cds AS customDimensions WHERE index = 12) end AS cd12,"
        else:
            hitCDs += "(SELECT MAX(value) FROM exp_hits_cds  WHERE index = " +str(tempInd)+ ") AS cd" +str(tempInd)+ ","
        
    
    elif(cds[i]['scope'] == "PRODUCT"):
        productCDs += "(SELECT MAX(value) FROM exp_hits_cds WHERE index = " +str(tempInd)+ ") AS cd" +str(tempInd)+ ","


# COMMAND ----------

sessionCDs = sessionCDs.replace("\s*$/", "")[:-1]
hitCDs = hitCDs.replace("\s*$/", "")[:-1]
productCDs = productCDs.replace("\s*$/", "")[:-1]

print(productCDs)

# COMMAND ----------

# Don't know what goalX varibale is 
goalX = 'True'

# COMMAND ----------

# MAGIC %md
# MAGIC #### Queries.gs Line : 21 (sessions)

# COMMAND ----------

def sessions_transformation(df):
    df.createOrReplaceTempView("ga_sessions_limit")
    sessions = spark.sql(""" 
WITH hits_exp AS
(
    SELECT fullVIsitorId, visitNumber, visitorId, visitId, visitStartTime, date, userId, EXPLODE(hits) AS hits FROM ga_sessions_limit
),
exp_hits_cds AS
(
 SELECT exp_hits_cd.value, exp_hits_cd.index FROM (SELECT EXPLODE(hits.customDimensions) AS exp_hits_cd FROM hits_exp)
)

SELECT CONCAT(fullVisitorId, "-", CAST(visitNumber as STRING)) as sessionId,visitorId,visitNumber, visitId, visitStartTime, date, fullVisitorId,  userId
         , totals.visits
         , totals.hits
         , totals.pageviews
         , totals.timeonsite
         , totals.bounces
         , totals.transactions
         , totals.transactionrevenue
         , totals.newvisits
         , totals.screenviews
         , totals.uniquescreenviews
         , totals.timeonscreen
         , totals.totaltransactionrevenue
         , trafficsource.referralpath
         , trafficsource.campaign
         , trafficsource.source
         , trafficsource.medium
         , REGEXP_REPLACE(trafficsource.keyword,"[^\x00-\x7F]+", "") as keyword_nonenglish_chars_removed
         , trafficsource.adcontent
         , trafficsource.adwordsclickinfo.campaignid
         , trafficsource.adwordsclickinfo.adgroupid
         , trafficsource.adwordsclickinfo.creativeid
         , trafficsource.adwordsclickinfo.criteriaid
         , trafficsource.adwordsclickinfo.page
         , trafficsource.adwordsclickinfo.slot
         , trafficsource.adwordsclickinfo.criteriaparameters
         , trafficsource.adwordsclickinfo.gclid
         , trafficsource.adwordsclickinfo.customerid
         , trafficsource.adwordsclickinfo.adnetworktype
         , trafficsource.adwordsclickinfo.targetingcriteria.boomuserlistid
         , trafficsource.adwordsclickinfo.isvideoad
         , device.browser
         , device.browserversion
         , device.operatingsystem
         , device.operatingsystemversion
         , device.ismobile
         , device.mobiledevicebranding
         , device.flashversion
         , device.javaenabled
         , device.language
         , device.screencolors
         , device.screenresolution
         , device.devicecategory
         , geonetwork.continent
         , geonetwork.subcontinent
         , geonetwork.country
         , geonetwork.region
         , geonetwork.metro
         , trafficsource.istruedirect
         , trafficsource.campaigncode
         , device.mobiledevicemodel
         , {sessionCDs}
         -- FROM `' + projectId + '.' + datasetTableId + qDate + '`'
--          FROM ga_sessions_limit 
         FROM ga_sessions_limit""".format(sessionCDs=sessionCDs))
    
    return sessions

#     print( sessions.count(), len(sessions.columns) ) # BigQuery : 459024 [13210892_flattened.flattened_sessions_20220716]
#     display(sessions)


    # Save the result to DB
#     tableName = 'sessions_' + tableNameBigQuery.replace('ga_sessions_', '')
#     print("{dbName}.{tabName}".format(dbName=databricksDbName, tabName=tableName))
#     sessions.write.mode("Overwrite").saveAsTable("{dbName}.{tabName}".format(dbName=databricksDbName, tabName=tableName))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Queries.gs Line : 86 (hits)

# COMMAND ----------

#adswerve param = "hits"
def hits_transformation(df):
    df.createOrReplaceTempView("ga_sessions_limit")
    hits = spark.sql("""

    WITH hits_exp AS
    (
       SELECT fullVIsitorId, visitNumber, visitorId, visitId, visitStartTime, date, userId, EXPLODE(hits) AS hits FROM ga_sessions_limit
    ),
    exp_hits_cds AS
    (
     SELECT exp_hits_cd.value, exp_hits_cd.index FROM (SELECT EXPLODE(hits.customDimensions) AS exp_hits_cd FROM hits_exp)
    )

    SELECT CONCAT(fullVisitorId, "-", CAST(visitNumber as STRING), "-", CAST(hits.hitNumber as STRING)) as hitId, CONCAT(fullVisitorId, "-", CAST(visitNumber as STRING)) as sessionId, visitorId, visitNumber, visitId, visitStartTime, date,
    fullVisitorId,  userId,
    hits.hitNumber, hits.time, hits.hour, hits.minute, hits.isSecure, hits.isInteraction, hits.isEntrance, hits.isExit, hits.referer, hits.type
           , hits.social.socialinteractionnetwork
           , hits.social.socialinteractionaction
           , REGEXP_REPLACE(hits.page.pagepath,"[^\x00-\x7F]+", "") as  pagepath_nonenglish_chars_removed
           , hits.page.hostname
           , hits.page.pagetitle
           , REGEXP_REPLACE(hits.page.searchkeyword,"[^\x00-\x7F]+", "") as  searchkeyword_nonenglish_chars_removed
           , hits.page.searchcategory
           , hits.transaction.transactionid
           , hits.transaction.transactionrevenue
           , hits.transaction.transactiontax
           , hits.transaction.transactionshipping
           , hits.transaction.affiliation
           , hits.transaction.currencycode
           , hits.transaction.localtransactionrevenue
           , hits.transaction.localtransactiontax
           , hits.transaction.localtransactionshipping
           , hits.transaction.transactioncoupon
           , hits.item.transactionid as item_transactionid
           , hits.item.productname
           , hits.item.productcategory
           , hits.item.productsku
           , hits.item.itemquantity
           , hits.item.itemrevenue
           , hits.item.currencycode item_currencycode
           , hits.item.localitemrevenue
           , hits.contentinfo.contentdescription
           , hits.ecommerceaction.action_type
           , hits.ecommerceaction.step
           , hits.ecommerceaction.option
           , hits.exceptioninfo.description
           , hits.exceptioninfo.isfatal
           , hits.eventinfo.eventcategory
           , hits.eventinfo.eventaction
           , hits.eventinfo.eventlabel
           , hits.eventinfo.eventvalue
           , hits.publisher.dfpclicks
           , hits.publisher.dfpimpressions
           , hits.publisher.dfpmatchedqueries
           , hits.publisher.dfpmeasurableimpressions
           , hits.publisher.dfpqueries
           , hits.publisher.dfprevenuecpm
           , hits.publisher.dfprevenuecpc
           , hits.publisher.dfpviewableimpressions
           , hits.publisher.dfppagesviewed
           , hits.publisher.adsensebackfilldfpclicks
           , hits.publisher.adsensebackfilldfpimpressions
           , hits.publisher.adsensebackfilldfpmatchedqueries
           , hits.publisher.adsensebackfilldfpmeasurableimpressions
           , hits.publisher.adsensebackfilldfpqueries
           , hits.publisher.adsensebackfilldfprevenuecpm
           , hits.publisher.adsensebackfilldfprevenuecpc
           , hits.publisher.adsensebackfilldfpviewableimpressions
           , hits.publisher.adsensebackfilldfppagesviewed
           , hits.publisher.adxbackfilldfpclicks
           , hits.publisher.adxbackfilldfpimpressions
           , hits.publisher.adxbackfilldfpmatchedqueries
           , hits.publisher.adxbackfilldfpmeasurableimpressions
           , hits.publisher.adxbackfilldfpqueries
           , hits.publisher.adxbackfilldfprevenuecpm
           , hits.publisher.adxbackfilldfprevenuecpc
           , hits.publisher.adxbackfilldfpviewableimpressions
           , hits.publisher.adxbackfilldfppagesviewed
           , hits.publisher.adxclicks
           , hits.publisher.adximpressions
           , hits.publisher.adxmatchedqueries
           , hits.publisher.adxmeasurableimpressions
           , hits.publisher.adxqueries
           , hits.publisher.adxrevenue
           , hits.publisher.adxviewableimpressions
           , hits.publisher.adxpagesviewed
           , hits.publisher.adsviewed
           , hits.publisher.adsunitsviewed
           , hits.publisher.adsunitsmatched
           , hits.publisher.viewableadsviewed
           , hits.publisher.measurableadsviewed
           , hits.publisher.adspagesviewed
           , hits.publisher.adsclicked
           , hits.publisher.adsrevenue
           , hits.publisher.dfpadgroup
           , hits.publisher.dfpadunits
           , hits.publisher.dfpnetworkid
           , REGEXP_REPLACE(hits.page.pagePathLevel1,"[^\x00-\x7F]+", "") as  pagePathLevel1_nonenglish_chars_removed
           , REGEXP_REPLACE(hits.page.pagePathLevel2,"[^\x00-\x7F]+", "") as  pagePathLevel2_nonenglish_chars_removed
           , REGEXP_REPLACE(hits.page.pagePathLevel3,"[^\x00-\x7F]+", "") as  pagePathLevel3_nonenglish_chars_removed
           , REGEXP_REPLACE(hits.page.pagePathLevel4,"[^\x00-\x7F]+", "") as  pagePathLevel4_nonenglish_chars_removed
           , hits.sourcePropertyInfo.sourcePropertyDisplayName
           , hits.sourcePropertyInfo.sourcePropertyTrackingid
           , {hitCDs}
     FROM hits_exp """.format(hitCDs=hitCDs))
    return hits

#     print( hits.count(), len(hits.columns) ) # BigQuery : 3580768 [13210892_flattened.flattened_hits_20220716]
#     display(hits)


#     # Save the result to DB
#     tableName = 'hits_' + tableNameBigQuery.replace('ga_sessions_', '')
#     print("{dbName}.{tabName}".format(dbName=databricksDbName, tabName=tableName))
#     hits.write.mode("Overwrite").saveAsTable("{dbName}.{tabName}".format(dbName=databricksDbName, tabName=tableName))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Queries.gs Line : 180 (hits_page)

# COMMAND ----------

#adswerve param = "hitsPage"
def hitsPage_transformation(df):
    df.createOrReplaceTempView("ga_sessions_limit")
    hitsPage = spark.sql("""

    WITH hits_exp AS
    (
       SELECT fullVIsitorId, visitNumber, visitorId, visitId, visitStartTime, date, userId, EXPLODE(hits) AS hits FROM ga_sessions_limit
    ),
    exp_hits_cds AS
    (
     SELECT exp_hits_cd.value, exp_hits_cd.index FROM (SELECT EXPLODE(hits.customDimensions) AS exp_hits_cd FROM hits_exp)
    )

    SELECT CONCAT(fullVisitorId, "-", CAST(visitNumber as STRING), "-", CAST(hits.hitNumber as STRING)) as hitId, CONCAT(fullVisitorId, "-", CAST(visitNumber as STRING)) as sessionId, visitorId, visitNumber, visitId, visitStartTime, date,
    fullVisitorId,  userId,
    hits.hitNumber, hits.time, hits.hour, hits.minute, hits.isSecure, hits.isInteraction, hits.isEntrance, hits.isExit, hits.referer, hits.type
           , hits.social.socialinteractionnetwork
           , hits.social.socialinteractionaction
           , REGEXP_REPLACE(hits.page.pagepath,"[^\x00-\x7F]+", "") as  pagepath_nonenglish_chars_removed
           , hits.page.hostname
           , hits.page.pagetitle
           , REGEXP_REPLACE(hits.page.searchkeyword,"[^\x00-\x7F]+", "") as  searchkeyword_nonenglish_chars_removed
           , hits.page.searchcategory
           , hits.transaction.transactionid
           , hits.transaction.transactionrevenue
           , hits.transaction.transactiontax
           , hits.transaction.transactionshipping
           , hits.transaction.affiliation
           , hits.transaction.currencycode
           , hits.transaction.localtransactionrevenue
           , hits.transaction.localtransactiontax
           , hits.transaction.localtransactionshipping
           , hits.transaction.transactioncoupon
           , hits.item.transactionid as item_transactionid
           , hits.item.productname
           , hits.item.productcategory
           , hits.item.productsku
           , hits.item.itemquantity
           , hits.item.itemrevenue
           , hits.item.currencycode item_currencycode
           , hits.item.localitemrevenue
           , hits.contentinfo.contentdescription
           , hits.ecommerceaction.action_type
           , hits.ecommerceaction.step
           , hits.ecommerceaction.option
           , hits.exceptioninfo.description
           , hits.exceptioninfo.isfatal
           , hits.eventinfo.eventcategory
           , hits.eventinfo.eventaction
           , hits.eventinfo.eventlabel
           , hits.eventinfo.eventvalue
           , hits.publisher.dfpclicks
           , hits.publisher.dfpimpressions
           , hits.publisher.dfpmatchedqueries
           , hits.publisher.dfpmeasurableimpressions
           , hits.publisher.dfpqueries
           , hits.publisher.dfprevenuecpm
           , hits.publisher.dfprevenuecpc
           , hits.publisher.dfpviewableimpressions
           , hits.publisher.dfppagesviewed
           , hits.publisher.adsensebackfilldfpclicks
           , hits.publisher.adsensebackfilldfpimpressions
           , hits.publisher.adsensebackfilldfpmatchedqueries
           , hits.publisher.adsensebackfilldfpmeasurableimpressions
           , hits.publisher.adsensebackfilldfpqueries
           , hits.publisher.adsensebackfilldfprevenuecpm
           , hits.publisher.adsensebackfilldfprevenuecpc
           , hits.publisher.adsensebackfilldfpviewableimpressions
           , hits.publisher.adsensebackfilldfppagesviewed
           , hits.publisher.adxbackfilldfpclicks
           , hits.publisher.adxbackfilldfpimpressions
           , hits.publisher.adxbackfilldfpmatchedqueries
           , hits.publisher.adxbackfilldfpmeasurableimpressions
           , hits.publisher.adxbackfilldfpqueries
           , hits.publisher.adxbackfilldfprevenuecpm
           , hits.publisher.adxbackfilldfprevenuecpc
           , hits.publisher.adxbackfilldfpviewableimpressions
           , hits.publisher.adxbackfilldfppagesviewed
           , hits.publisher.adxclicks
           , hits.publisher.adximpressions
           , hits.publisher.adxmatchedqueries
           , hits.publisher.adxmeasurableimpressions
           , hits.publisher.adxqueries
           , hits.publisher.adxrevenue
           , hits.publisher.adxviewableimpressions
           , hits.publisher.adxpagesviewed
           , hits.publisher.adsviewed
           , hits.publisher.adsunitsviewed
           , hits.publisher.adsunitsmatched
           , hits.publisher.viewableadsviewed
           , hits.publisher.measurableadsviewed
           , hits.publisher.adspagesviewed
           , hits.publisher.adsclicked
           , hits.publisher.adsrevenue
           , hits.publisher.dfpadgroup
           , hits.publisher.dfpadunits
           , hits.publisher.dfpnetworkid
           , REGEXP_REPLACE(hits.page.pagePathLevel1,"[^\x00-\x7F]+", "") as  pagePathLevel1_nonenglish_chars_removed
           , REGEXP_REPLACE(hits.page.pagePathLevel2,"[^\x00-\x7F]+", "") as  pagePathLevel2_nonenglish_chars_removed
           , REGEXP_REPLACE(hits.page.pagePathLevel3,"[^\x00-\x7F]+", "") as  pagePathLevel3_nonenglish_chars_removed
           , REGEXP_REPLACE(hits.page.pagePathLevel4,"[^\x00-\x7F]+", "") as  pagePathLevel4_nonenglish_chars_removed
           , hits.sourcePropertyInfo.sourcePropertyDisplayName
           , hits.sourcePropertyInfo.sourcePropertyTrackingid
           , {hitCDs}
     FROM hits_exp WHERE hits.type = 'PAGE' OR hits.type = 'APPVIEW' """.format(hitCDs=hitCDs))
    return hitsPage

#     print( hits.count(), len(hits.columns) ) # BigQuery : 3580768 [13210892_flattened.flattened_hits_20220716]
#     display(hits)


#     # Save the result to DB
#     tableName = 'hits_' + tableNameBigQuery.replace('ga_sessions_', '')
#     print("{dbName}.{tabName}".format(dbName=databricksDbName, tabName=tableName))
#     hits.write.mode("Overwrite").saveAsTable("{dbName}.{tabName}".format(dbName=databricksDbName, tabName=tableName))

# COMMAND ----------

#adswerve = hits_page
def hits_page_transformation(df):
    
    dummyHitsPage = df.filter(" type = 'PAGE' OR type = 'APPVIEW' ")
    return dummyHitsPage

#     print(dummyHitsPage.count(), len(dummyHitsPage.columns))

#     # Save the result to DB
#     tableName = 'hits_page_' + tableNameBigQuery.replace('ga_sessions_', '')
#     print("{dbName}.{tabName}".format(dbName=databricksDbName, tabName=tableName))
#     dummyHitsPage.write.mode("Overwrite").saveAsTable("{dbName}.{tabName}".format(dbName=databricksDbName, tabName=tableName))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Queries.gs Line : 184 (hits_event)

# COMMAND ----------

#adswerve  param: hits_event_
def hits_event_transformation(df):
    dummyHitsEvent = df.filter("type = 'EVENT' ")
    return dummyHitsEvent

# print(dummyHitsEvent.count(), len(dummyHitsEvent.columns))

# # Save the result to DB
# tableName = 'hits_event_' + tableNameBigQuery.replace('ga_sessions_', '')
# print("{dbName}.{tabName}".format(dbName=databricksDbName, tabName=tableName))
# dummyHitsEvent.write.mode("Overwrite").saveAsTable("{dbName}.{tabName}".format(dbName=databricksDbName, tabName=tableName))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Queries.gs Line : 189 (products)

# COMMAND ----------

# adswerve param= products

def products_transformation(df):
    df.createOrReplaceTempView('ga_sessions_limit')
    
    products = spark.sql("""

    WITH exp_hits_Product AS
    (
    SELECT fullVIsitorId, visitNumber, userId, EXPLODE(exp_hits.product) AS hitsProduct, exp_hits AS hits 
              FROM ( SELECT fullVIsitorId, visitNumber, userId, EXPLODE(hits) AS exp_hits FROM ga_sessions_limit)
    ),
    exp_hits_cds AS
    (
     SELECT exp_hits_cd.value, exp_hits_cd.index FROM (SELECT EXPLODE(hitsProduct.customDimensions) AS exp_hits_cd FROM exp_hits_Product)
    ),
    products AS (
      SELECT CONCAT(fullVisitorId, "-", CAST(visitNumber as STRING), "-", CAST(hits.hitNumber as STRING)) as hitId,
        CONCAT(fullVisitorId, "-", CAST(visitNumber as STRING)) as sessionId,
        fullVisitorId,
        userId,
        hitsProduct.productSKU,
        hitsProduct.v2ProductName,
        hitsProduct.v2ProductCategory,
        hitsProduct.productVariant,
        hitsProduct.productBrand,
        hitsProduct.productRevenue,
        hitsProduct.localProductRevenue,
        hitsProduct.productPrice,
        hitsProduct.localProductPrice,
        hitsProduct.productQuantity,
        hitsProduct.productRefundAmount,
        hitsProduct.localProductRefundAmount,
        hitsProduct.isImpression,
        hitsProduct.isClick,
        hitsProduct.productListName,
        hitsProduct.productListPosition,
        hitsProduct.productCouponCode 
      FROM exp_hits_Product
    )
      -- FROM ga_sessions_limit,
      -- EXPLODE(hits) as hits, -- UNNEST to EXPLODE
      -- EXPLODE(hits.product) as hitsProduct -- UNNEST to EXPLODE


    SELECT CONCAT(CONCAT(hitId), "-", CAST(ROW_NUMBER() OVER(PARTITION BY hitId ORDER BY hitId DESC) AS STRING)) AS productHitId, *
    FROM products """)
    return products

#     print( products.count(), len(products.columns) ) # BigQuery : ? (Don't know which table in BigQuery to look for)
#     display(products)


#     # Save the result to DB
#     tableName = 'products_' + tableNameBigQuery.replace('ga_sessions_', '')
#     print("{dbName}.{tabName}".format(dbName=databricksDbName, tabName=tableName))
#     products.write.mode("Overwrite").saveAsTable("{dbName}.{tabName}".format(dbName=databricksDbName, tabName=tableName))

# COMMAND ----------

''' Queries.gs Line : 189 (products)

Not sure where this table is pre
'''

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Queries.gs Line : 422 (hitssi)

# COMMAND ----------

# adswerve param= hitssi

def hitssi_transformation(df):
    df.createOrReplaceTempView('ga_sessions_limit')

    hitssi = spark.sql("""

    WITH hits_exp AS
    (
       SELECT fullVIsitorId, visitNumber, visitorId, visitId, visitStartTime, date, userId, EXPLODE(hits) AS hits FROM ga_sessions_limit
    ),
    exp_hits_cds AS
    (
     SELECT exp_hits_cd.value, exp_hits_cd.index FROM (SELECT EXPLODE(hits.customDimensions) AS exp_hits_cd FROM hits_exp)
    )

    SELECT CONCAT(fullVisitorId, "-", CAST(visitNumber as STRING), "-", CAST(hits.hitNumber as STRING)) as hitId, CONCAT(fullVisitorId, "-", CAST(visitNumber as STRING)) as sessionId, visitorId,visitNumber, visitId, visitStartTime, date,
    fullVisitorId,  userId,
    hits.hitNumber, hits.time, hits.hour, hits.minute, hits.isSecure, hits.isInteraction, hits.isEntrance, hits.isExit, hits.referer, hits.type
           , hits.social.socialinteractionnetwork
           , hits.social.socialinteractionaction
           , REGEXP_REPLACE(hits.page.pagepath,"[^\x00-\x7F]+", "") as  pagepath_nonenglish_chars_removed
           , hits.page.hostname
           , hits.page.pagetitle
           , REGEXP_REPLACE(hits.page.searchkeyword,"[^\x00-\x7F]+", "") as  searchkeyword_nonenglish_chars_removed
           , hits.page.searchcategory
           , hits.transaction.transactionid
           , hits.transaction.transactionrevenue
           , hits.transaction.transactiontax
           , hits.transaction.transactionshipping
           , hits.transaction.affiliation
           , hits.transaction.currencycode
           , hits.transaction.localtransactionrevenue
           , hits.transaction.localtransactiontax
           , hits.transaction.localtransactionshipping
           , hits.transaction.transactioncoupon
           , hits.item.transactionid as item_transactionid
           , hits.item.productname
           , hits.item.productcategory
           , hits.item.productsku
           , hits.item.itemquantity
           , hits.item.itemrevenue
           , hits.item.currencycode item_currencycode
           , hits.item.localitemrevenue
           , hits.contentinfo.contentdescription
           , hits.ecommerceaction.action_type
           , hits.ecommerceaction.step
           , hits.ecommerceaction.option
           , hits.exceptioninfo.description
           , hits.exceptioninfo.isfatal
           , hits.eventinfo.eventcategory
           , hits.eventinfo.eventaction
           , hits.eventinfo.eventlabel
           , hits.eventinfo.eventvalue
           , hits.publisher.dfpclicks
           , hits.publisher.dfpimpressions
           , hits.publisher.dfpmatchedqueries
           , hits.publisher.dfpmeasurableimpressions
           , hits.publisher.dfpqueries
           , hits.publisher.dfprevenuecpm
           , hits.publisher.dfprevenuecpc
           , hits.publisher.dfpviewableimpressions
           , hits.publisher.dfppagesviewed
           , hits.publisher.adsensebackfilldfpclicks
           , hits.publisher.adsensebackfilldfpimpressions
           , hits.publisher.adsensebackfilldfpmatchedqueries
           , hits.publisher.adsensebackfilldfpmeasurableimpressions
           , hits.publisher.adsensebackfilldfpqueries
           , hits.publisher.adsensebackfilldfprevenuecpm
           , hits.publisher.adsensebackfilldfprevenuecpc
           , hits.publisher.adsensebackfilldfpviewableimpressions
           , hits.publisher.adsensebackfilldfppagesviewed
           , hits.publisher.adxbackfilldfpclicks
           , hits.publisher.adxbackfilldfpimpressions
           , hits.publisher.adxbackfilldfpmatchedqueries
           , hits.publisher.adxbackfilldfpmeasurableimpressions
           , hits.publisher.adxbackfilldfpqueries
           , hits.publisher.adxbackfilldfprevenuecpm
           , hits.publisher.adxbackfilldfprevenuecpc
           , hits.publisher.adxbackfilldfpviewableimpressions
           , hits.publisher.adxbackfilldfppagesviewed
           , hits.publisher.adxclicks
           , hits.publisher.adximpressions
           , hits.publisher.adxmatchedqueries
           , hits.publisher.adxmeasurableimpressions
           , hits.publisher.adxqueries
           , hits.publisher.adxrevenue
           , hits.publisher.adxviewableimpressions
           , hits.publisher.adxpagesviewed
           , hits.publisher.adsviewed
           , hits.publisher.adsunitsviewed
           , hits.publisher.adsunitsmatched
           , hits.publisher.viewableadsviewed
           , hits.publisher.measurableadsviewed
           , hits.publisher.adspagesviewed
           , hits.publisher.adsclicked
           , hits.publisher.adsrevenue
           , hits.publisher.dfpadgroup
           , hits.publisher.dfpadunits
           , hits.publisher.dfpnetworkid
           , REGEXP_REPLACE(hits.page.pagePathLevel1,"[^\x00-\x7F]+", "") as  pagePathLevel1_nonenglish_chars_removed
           , REGEXP_REPLACE(hits.page.pagePathLevel2,"[^\x00-\x7F]+", "") as  pagePathLevel2_nonenglish_chars_removed
           , REGEXP_REPLACE(hits.page.pagePathLevel3,"[^\x00-\x7F]+", "") as  pagePathLevel3_nonenglish_chars_removed
           , REGEXP_REPLACE(hits.page.pagePathLevel4,"[^\x00-\x7F]+", "") as  pagePathLevel4_nonenglish_chars_removed
           , hits.sourcePropertyInfo.sourcePropertyDisplayName
           , hits.sourcePropertyInfo.sourcePropertyTrackingid
           , {hitCDs}

         FROM hits_exp
         --FROM ga_sessions_limit 
         -- LEFT JOIN EXPLODE(hits) as hits -- UNNEST to EXPLODE  -- Original
         -- LEFT JOIN ( SELECT EXPLODE(hits) FROM ga_sessions_limit ) AS hits -- Alternative ? 
         WHERE 
             (SELECT max(cd.value  LIKE "%si.com") FROM (SELECT EXPLODE(hits.customDimensions) AS cd, hits FROM hits_exp) WHERE cd.index = 31 ) 
             AND (hits.eventInfo.eventCategory IS NULL OR LOWER(hits.eventInfo.eventCategory) NOT LIKE "%video%") """.format(hitCDs=hitCDs))  
    # Stuff Inside WHERE clause is returning nothing
    return hitssi

# print( hitssi.count(), len(hitssi.columns) ) # BigQuery :  (Don't know which table in BigQuery to look for)
# display(hitssi)

# # Save the result to DB
# tableName = 'hitssi_' + tableNameBigQuery.replace('ga_sessions_', '')
# print("{dbName}.{tabName}".format(dbName=databricksDbName, tabName=tableName))
# hitssi.write.mode("Overwrite").saveAsTable("{dbName}.{tabName}".format(dbName=databricksDbName, tabName=tableName))

# COMMAND ----------

''' Queries.gs Line : 422 (hitssi)
Using the following Query we see that there's no data in the table post the date 2021-02-22 
SELECT date, COUNT(*) from googleanalytics.si_flattened_hits GROUP BY date

Not sure if the code (sql) was the same during 2021-02-22 for data validation 
'''

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Queries.gs Line : 518 (video_hits)

# COMMAND ----------

# adswerve param= hitsVideo

def hitsVideo_transformation(df):
    df.createOrReplaceTempView('ga_sessions_limit')

    hitsVideo = spark.sql(""" 

    WITH hits_exp AS
    (
       SELECT fullVIsitorId, visitNumber, visitorId, visitId, visitStartTime, date, userId, EXPLODE(hits) AS hits FROM ga_sessions_limit
    ),
    exp_hits_cds AS
    (
     SELECT exp_hits_cd.value, exp_hits_cd.index FROM (SELECT EXPLODE(hits.customDimensions) AS exp_hits_cd FROM hits_exp)
    )

    SELECT CONCAT(fullVisitorId, "-", CAST(visitNumber as STRING), "-", CAST(hits.hitNumber as STRING)) as hitId, CONCAT(fullVisitorId, "-", CAST(visitNumber as STRING)) as sessionId, visitorId, visitNumber, visitId, visitStartTime, date,
    fullVisitorId,  userId,
    hits.hitNumber, hits.time, hits.hour, hits.minute, hits.isSecure, hits.isInteraction, hits.isEntrance, hits.isExit, hits.referer, hits.type
           , hits.social.socialinteractionnetwork
           , hits.social.socialinteractionaction
           , REGEXP_REPLACE(hits.page.pagepath,"[^\x00-\x7F]+", "") as  pagepath_nonenglish_chars_removed
           , hits.page.hostname
           , hits.page.pagetitle
           , REGEXP_REPLACE(hits.page.searchkeyword,"[^\x00-\x7F]+", "") as  searchkeyword_nonenglish_chars_removed
           , hits.page.searchcategory
           , hits.transaction.transactionid
           , hits.transaction.transactionrevenue
           , hits.transaction.transactiontax
           , hits.transaction.transactionshipping
           , hits.transaction.affiliation
           , hits.transaction.currencycode
           , hits.transaction.localtransactionrevenue
           , hits.transaction.localtransactiontax
           , hits.transaction.localtransactionshipping
           , hits.transaction.transactioncoupon
           , hits.item.transactionid as item_transactionid
           , hits.item.productname
           , hits.item.productcategory
           , hits.item.productsku
           , hits.item.itemquantity
           , hits.item.itemrevenue
           , hits.item.currencycode item_currencycode
           , hits.item.localitemrevenue
           , hits.contentinfo.contentdescription
           , hits.ecommerceaction.action_type
           , hits.ecommerceaction.step
           , hits.ecommerceaction.option
           , hits.exceptioninfo.description
           , hits.exceptioninfo.isfatal
           , hits.eventinfo.eventcategory
           , hits.eventinfo.eventaction
           , hits.eventinfo.eventlabel
           , hits.eventinfo.eventvalue
           , hits.publisher.dfpclicks
           , hits.publisher.dfpimpressions
           , hits.publisher.dfpmatchedqueries
           , hits.publisher.dfpmeasurableimpressions
           , hits.publisher.dfpqueries
           , hits.publisher.dfprevenuecpm
           , hits.publisher.dfprevenuecpc
           , hits.publisher.dfpviewableimpressions
           , hits.publisher.dfppagesviewed
           , hits.publisher.adsensebackfilldfpclicks
           , hits.publisher.adsensebackfilldfpimpressions
           , hits.publisher.adsensebackfilldfpmatchedqueries
           , hits.publisher.adsensebackfilldfpmeasurableimpressions
           , hits.publisher.adsensebackfilldfpqueries
           , hits.publisher.adsensebackfilldfprevenuecpm
           , hits.publisher.adsensebackfilldfprevenuecpc
           , hits.publisher.adsensebackfilldfpviewableimpressions
           , hits.publisher.adsensebackfilldfppagesviewed
           , hits.publisher.adxbackfilldfpclicks
           , hits.publisher.adxbackfilldfpimpressions
           , hits.publisher.adxbackfilldfpmatchedqueries
           , hits.publisher.adxbackfilldfpmeasurableimpressions
           , hits.publisher.adxbackfilldfpqueries
           , hits.publisher.adxbackfilldfprevenuecpm
           , hits.publisher.adxbackfilldfprevenuecpc
           , hits.publisher.adxbackfilldfpviewableimpressions
           , hits.publisher.adxbackfilldfppagesviewed
           , hits.publisher.adxclicks
           , hits.publisher.adximpressions
           , hits.publisher.adxmatchedqueries
           , hits.publisher.adxmeasurableimpressions
           , hits.publisher.adxqueries
           , hits.publisher.adxrevenue
           , hits.publisher.adxviewableimpressions
           , hits.publisher.adxpagesviewed
           , hits.publisher.adsviewed
           , hits.publisher.adsunitsviewed
           , hits.publisher.adsunitsmatched
           , hits.publisher.viewableadsviewed
           , hits.publisher.measurableadsviewed
           , hits.publisher.adspagesviewed
           , hits.publisher.adsclicked
           , hits.publisher.adsrevenue
           , hits.publisher.dfpadgroup
           , hits.publisher.dfpadunits
           , hits.publisher.dfpnetworkid
           , REGEXP_REPLACE(hits.page.pagePathLevel1,"[^\x00-\x7F]+", "") as  pagePathLevel1_nonenglish_chars_removed
           , REGEXP_REPLACE(hits.page.pagePathLevel2,"[^\x00-\x7F]+", "") as  pagePathLevel2_nonenglish_chars_removed
           , REGEXP_REPLACE(hits.page.pagePathLevel3,"[^\x00-\x7F]+", "") as  pagePathLevel3_nonenglish_chars_removed
           , REGEXP_REPLACE(hits.page.pagePathLevel4,"[^\x00-\x7F]+", "") as  pagePathLevel4_nonenglish_chars_removed
           , hits.sourcePropertyInfo.sourcePropertyDisplayName
           , hits.sourcePropertyInfo.sourcePropertyTrackingid
           , {hitCDs}
           FROM hits_exp
           -- FROM `' + projectId + '.' + oldDatasetTableId + qDate + '` LEFT JOIN UNNEST(hits) as hits
           WHERE LOWER(hits.eventInfo.eventCategory) LIKE "%video%" """.format(hitCDs=hitCDs))
    return hitsVideo
# No Table for 18th June, so ran for 16th June, also no ga_sessions table for 16th June in Intraday

# print( hitsVideo.count(), len(hitsVideo.columns) ) # BigQuery : 5679035 (184077347_flattened.flattened_video_hits_20220716 Is it correct ?)
# display(hitsVideo)


# # Save the result to DB
# tableName = 'video_hits_' + tableNameBigQuery.replace('ga_sessions_', '')
# print("{dbName}.{tabName}".format(dbName=databricksDbName, tabName=tableName))
# hitsVideo.write.mode("Overwrite").saveAsTable("{dbName}.{tabName}".format(dbName=databricksDbName, tabName=tableName))

# COMMAND ----------

''' Queries.gs Line : 518 (video_hits)
The following query Returns Count as 5,679,035 for 2022-07-16 (columns 159)
    SELECT date, COUNT(*) from googleanalytics.flattened_video_hits GROUP BY date 
    
    USE host as Street Analytics (as it contains merged) in WHERE clause - Gord
    
    USING the below query query Returns Count as 788,797 for 2022-07-16 
    SELECT date, COUNT(*) FROM googleanalytics.flattened_video_hits WHERE hostname IN ('s.t.st', 'www.thestreet.com', 'aap.thestreet.com', 'papago.naver.net', 'players.brightcove.net') GROUP BY date
    
    USING SELECT date, COUNT(*) FROM googleanalytics.flattened_video_hits WHERE hostname ='www.thestreet.com' GROUP BY date
    We're getting 788,775
    
But in Spark SQL we're getting 794,441 when running for 13210892.ga_sessions_20220716 from Big Query (columns 107)

'''

# COMMAND ----------

# MAGIC %md
# MAGIC #### Queries.gs Line : 220 (goalX_articles)

# COMMAND ----------

# adswerve param= goalX_articles

def goalX_articles_transformation(df):
    df.createOrReplaceTempView('ga_sessions_limit')

    goalX_articles = spark.sql("""

    WITH hits_exp AS 
    (
      SELECT fullVIsitorId, visitNumber, visitorId, visitId, visitStartTime, trafficSource, device, date, userId, hits,
      CONCAT(fullVIsitorId, visitNumber, hits.hitnumber) as uniq
      FROM ( SELECT fullVIsitorId, visitNumber, visitorId, visitId, visitStartTime, trafficSource, device, date, userId, EXPLODE(hits) AS hits  FROM  ga_sessions_limit)
    ),
    exp_hits_cd AS
    (
      SELECT expHitsCD.index AS index, expHitsCD.value AS value, uniq
      FROM ( SELECT EXPLODE(hits.customDimensions) AS expHitsCD, CONCAT(fullVIsitorId, visitNumber, hits.hitnumber) as uniq  FROM hits_exp ) 
    ),
    uniCTE AS
    ( SELECT 
      CASE WHEN index=19 THEN value ELSE NULL END AS userid, 
      CASE WHEN index=1 THEN TRY_CAST(value AS NUMERIC) ELSE NULL END AS TSTmemberID,
      CASE WHEN index=5 THEN value ELSE NULL END AS TSToid,
      CASE WHEN index=12 THEN value ELSE NULL END AS articleId,
      CASE WHEN index=31 THEN value ELSE NULL END AS transactionId,
      CASE WHEN index=44 THEN value ELSE NULL END AS lastArticleIDFromCookie,
      CASE WHEN index=45 THEN value ELSE NULL END AS securePageType,
      CASE WHEN index=48 THEN value ELSE NULL END AS secureProductName, 
      uniq
    FROM exp_hits_cd
    ),

    goalX AS (
      SELECT DISTINCT date, fullVisitorId, visitNumber
      FROM hits_exp -- UNNEST to EXPLODE
      WHERE  {goalX} -- What is goalX (JS Variable) ?
      AND (hits.page.hostname IS NULL
        OR hits.page.hostname IN (
        "signup2.thestreet.com",
        "demosignup2.thestreet.com",
        "secure2.thestreet.com",
        "demosecure2.thestreet.com",
        "secure.thestreet.com"
        )
      )
    ),
    base AS (
      SELECT * EXCEPT(transactionId), 
      LAST_VALUE(transactionId) OVER (
            PARTITION BY date, fullVisitorId, visitNumber, userId, deviceCategory, source, medium, goalFlag
            ORDER BY timeOfHit
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
          ) AS transactionId
      FROM (
        SELECT * EXCEPT (goalFlag, transactionId, TSTmemberID, TSToid, lastArticleIDFromCookie),
          IF(goalFlag = MAX(goalFlag) OVER (PARTITION BY date, fullVisitorId, visitNumber, userId, deviceCategory, source, medium) AND goalFlag != 1, NULL, goalFlag
          ) AS goalFlag,
          FIRST_VALUE (TSTmemberID, True )--(TSTmemberID IGNORE NULLS)  
            OVER (
              PARTITION BY date, fullVisitorId, visitNumber, userId, deviceCategory, source, medium, goalFlag
              ORDER BY timeOfHit
              ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) AS TSTmemberID,
          FIRST_VALUE (TSToid, True) --(TSToid IGNORE NULLS) 
            OVER (
              PARTITION BY date, fullVisitorId, visitNumber, userId, deviceCategory, source, medium, goalFlag
              ORDER BY timeOfHit
              ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) AS TSToid,
          FIRST_VALUE (LastArticleIDFromCookie, True) --(LastArticleIDFromCookie IGNORE NULLS) 
            OVER (
              PARTITION BY date, fullVisitorId, visitNumber, userId, deviceCategory, source, medium, goalFlag
              ORDER BY timeOfHit
              ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) AS lastArticleIDFromCookie,
          LAST_VALUE (transactionId, True) --(transactionId IGNORE NULLS)
            OVER (
              PARTITION BY date, fullVisitorId, visitNumber, userId, deviceCategory, source, medium, goalFlag
              ORDER BY timeOfHit
              ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) AS transactionId
        FROM (
          SELECT date, fullVisitorId, visitNumber, device.deviceCategory, trafficSource.source, trafficSource.medium,
            hits.hitNumber, 
            TRY_CAST(hits.time/1000 AS INTEGER) as viewTime, --CAST(hits.time/1000 AS INT64) as viewTime, 
            hits.type, hits.page.hostname, hits.page.pagePath, 
            hits.page.pageTitle, 
            -- TIMESTAMP_MILLIS((visitStartTime*1000) + time) as timeOfHit, -- What is time here, Is it hits.time ?
            TIMESTAMP_MILLIS((visitStartTime*1000)) as timeOfHit,

            uniqueVals.userId,
            uniqueVals.TSTmemberID, 
            uniqueVals.TSToid, 
            uniqueVals.articleId, 
            uniqueVals.transactionId, 
            uniqueVals.lastArticleIDFromCookie, 
            uniqueVals.securePageType, 
            uniqueVals.secureProductName,
            IFNULL(
              1 + SUM(IF(  {goalX}          -- What is goalX (JS Variable) ?
                AND (hits.page.hostname IS NULL
                  OR hits.page.hostname IN (
                  "signup2.thestreet.com",
                  "demosignup2.thestreet.com",
                  "secure2.thestreet.com",
                  "demosecure2.thestreet.com",
                  "secure.thestreet.com"
                  )
                ),
                1, 0))
                OVER (
                  PARTITION BY date, fullVisitorId, visitNumber, hits.userId, device.deviceCategory, trafficSource.source, trafficSource.medium
                  ORDER BY hits.hitNumber
                  ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
                ),
              1
            ) AS goalFlag
          FROM hits_exp AS hits
          JOIN (SELECT * FROM uniCTE ) AS uniqueVals
          ON hits.uniq = uniqueVals.uniq
        )
      )
      WHERE goalFlag IS NOT NULL AND type = "PAGE"
    )

    SELECT
      to_date(date, 'yyyyMMdd') AS date, --DATE_FORMAT("%Y%m%d", date) AS date, -- PARSE_DATE to DATE_FORMAT
      CONCAT(fullVisitorId, "-", CAST(visitNumber as STRING)) as sessionId,
      fullVisitorId, visitNumber, userId, deviceCategory, source, medium, goalFlag, transactionId, securePageType, secureProductName, TSTmemberID, TSToid, lastArticleIDFromCookie,
      ROW_NUMBER()
        OVER (
          PARTITION BY date, fullVisitorId, visitNumber, userId, deviceCategory, source, medium, goalFlag, transactionId
          ORDER BY viewTime
        ) AS viewOrder,
      ROW_NUMBER()
        OVER (
          PARTITION BY date, fullVisitorId, visitNumber, userId, deviceCategory, source, medium, goalFlag, transactionId
          ORDER BY viewTime DESC
        ) AS viewOrderReversed,
      ROW_NUMBER()
        OVER (
          PARTITION BY date, fullVisitorId, visitNumber, userId, deviceCategory, source, medium
          ORDER BY viewTime
        ) AS fullViewOrder,
      ROW_NUMBER()
        OVER (
          PARTITION BY date, fullVisitorId, visitNumber, userId, deviceCategory, source, medium
          ORDER BY viewTime DESC
        ) AS fullViewOrderReversed,
      CASE WHEN articleId IS NOT NULL THEN ROW_NUMBER()
        OVER (
          PARTITION BY CASE WHEN articleId IS NOT NULL THEN 1 END, date, fullVisitorId, visitNumber, userId, deviceCategory, source, medium
          ORDER BY viewTime
        ) END AS viewArticleOrder,
      CASE WHEN articleId IS NOT NULL THEN ROW_NUMBER()
        OVER (
          PARTITION BY CASE WHEN articleId IS NOT NULL THEN 1 END, date, fullVisitorId, visitNumber, userId, deviceCategory, source, medium
          ORDER BY viewTime DESC
        ) END AS viewArticleOrderReversed,
      viewTime, articleId, pageTitle, hostname, pagePath
    FROM (
      SELECT
        *
      FROM base
      INNER JOIN goalX USING (date, fullVisitorId, visitNumber)
    )
    ORDER BY date, fullVisitorId, visitNumber, userId, goalFlag, viewOrder """.format(goalX=goalX))
    return goalX_articles

# print( goalX_articles.count(), len(goalX_articles.columns) ) # BigQuery : 
# display(goalX_articles)


# # Save the result to DB
# tableName = 'goalX_articles_' + tableNameBigQuery.replace('ga_sessions_', '')
# print("{dbName}.{tabName}".format(dbName=databricksDbName, tabName=tableName))
# goalX_articles.write.mode("Overwrite").saveAsTable("{dbName}.{tabName}".format(dbName=databricksDbName, tabName=tableName))

# COMMAND ----------

'''
No records for goalX_articles(Queries.gs Line : 220)
I think 13210892_flattened.flattened_goal1orderPlaced_articles is the corresponding table to which that 
output of that query goes, we observed that last table inserted with row_count > 1 is on 2022-05-31 with just 1 record & is somewhat same for older tables as well, seems some irregularity there (Not sure about if we're looking at the right table, also unsure about the insertion logic post processing of those queries)
'''

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Queries.gs Line : 613 (goalX_videos)

# COMMAND ----------

# adswerve param= goalX_videos

def goalX_videos_transformation(df):
    df.createOrReplaceTempView('ga_sessions_limit')

    goalX_videos = spark.sql("""

    WITH hits_exp AS 
    (
      SELECT fullVIsitorId, visitNumber, visitorId, visitId, visitStartTime, trafficSource, device, date, userId, hits,
      CONCAT(fullVIsitorId, visitNumber, hits.hitnumber) as uniq
      FROM ( SELECT fullVIsitorId, visitNumber, visitorId, visitId, visitStartTime, trafficSource, device, date, userId, EXPLODE(hits) AS hits  FROM  ga_sessions_limit)
    ),
    exp_hits_cd AS
    (
      SELECT expHitsCD.index AS index, expHitsCD.value AS value, uniq
      FROM ( SELECT EXPLODE(hits.customDimensions) AS expHitsCD, CONCAT(fullVIsitorId, visitNumber, hits.hitnumber) as uniq  FROM hits_exp ) 
    ),
    uniCTE AS
    ( SELECT 
      CASE WHEN index=19 THEN value ELSE NULL END AS userid, 
      CASE WHEN index=1 THEN TRY_CAST(value AS NUMERIC) ELSE NULL END AS TSTmemberID,
      CASE WHEN index=5 THEN value ELSE NULL END AS TSToid,
      CASE WHEN index=34 THEN value ELSE NULL END AS videoId,
      CASE WHEN index=31 THEN value ELSE NULL END AS transactionId,
      -- lastVideoIDFromCookie is hardcoded as NULL,
      CASE WHEN index=45 THEN value ELSE NULL END AS securePageType,
      CASE WHEN index=48 THEN value ELSE NULL END AS secureProductName, 
      uniq
    FROM exp_hits_cd
    ),

    goalX AS (
      SELECT DISTINCT date, fullVisitorId, visitNumber
      FROM hits_exp -- UNNEST to EXPLODE
      WHERE  {goalX} -- What is goalX (JS Variable) ?
      AND (hits.page.hostname IS NULL
        OR hits.page.hostname IN (
        "signup2.thestreet.com",
        "demosignup2.thestreet.com",
        "secure2.thestreet.com",
        "demosecure2.thestreet.com",
        "secure.thestreet.com"
        )
      )
    ),

    base AS ( 
      SELECT * EXCEPT (transactionId), 
        LAST_VALUE(transactionId) 
        OVER ( 
          PARTITION BY date, fullVisitorId, visitNumber, userId, deviceCategory, source, medium, goalFlag 
          ORDER BY timeOfHit 
          ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING 
        ) AS transactionId 
      FROM ( 
        SELECT * EXCEPT (goalFlag, transactionId, TSTmemberID, TSToid, lastVideoIDFromCookie, videoId), 
          IF(goalFlag = MAX(goalFlag) OVER (PARTITION BY date, fullVisitorId, visitNumber, userId, deviceCategory, source, medium) AND goalFlag != 1, 
            NULL, 
            goalFlag 
          ) AS goalFlag, 
          FIRST_VALUE(TSTmemberID, True) 
            OVER ( 
              PARTITION BY date, fullVisitorId, visitNumber, userId, deviceCategory, source, medium, goalFlag 
              ORDER BY timeOfHit 
              ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING 
            ) AS TSTmemberID, 
          FIRST_VALUE(TSToid, True) 
            OVER ( 
              PARTITION BY date, fullVisitorId, visitNumber, userId, deviceCategory, source, medium, goalFlag 
              ORDER BY timeOfHit 
              ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING 
            ) AS TSToid, 
          FIRST_VALUE(lastVideoIDFromCookie, True) 
            OVER ( 
              PARTITION BY date, fullVisitorId, visitNumber, userId, deviceCategory, source, medium, goalFlag
              ORDER BY timeOfHit
              ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) AS lastVideoIDFromCookie, 
          LAST_VALUE(transactionId, True) 
            OVER ( 
              PARTITION BY date, fullVisitorId, visitNumber, userId, deviceCategory, source, medium, goalFlag 
              ORDER BY timeOfHit 
              ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) AS transactionId, 
          FIRST_VALUE(videoId, True) 
            OVER ( 
              PARTITION BY date, fullVisitorId, visitNumber, userId, deviceCategory, source, medium, goalFlag, hostname, pageTitle 
              ORDER BY timeOfHit 
              ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) AS videoId 
        FROM ( 
          SELECT 
            date, 
            fullVisitorId, 
            visitNumber, 
            device.deviceCategory, 
            trafficSource.source, 
            trafficSource.medium, 
            hits.hitNumber, 
            --CAST(hits.time/1000 AS INT64) as viewTime, 
            TRY_CAST(hits.time/1000 AS INTEGER) as viewTime,
            hits.type, 
            hits.page.hostname, 
            hits.page.pagePath, 
            hits.page.pageTitle, 
            --TIMESTAMP_MILLIS((visitStartTime*1000) + time) as timeOfHit, -- What is time here, Is it hits.time ?
            TIMESTAMP_MILLIS((visitStartTime*1000)) as timeOfHit,

            /*(SELECT value FROM unnest(hits.customdimensions) where index=19) as userId, 
            (SELECT SAFE_CAST(value AS NUMERIC) FROM unnest(hits.customdimensions) where index=1) as TSTmemberID, 
            (SELECT value FROM unnest(hits.customdimensions) where index=5) as TSToid, 
            (SELECT value FROM unnest(hits.customdimensions) where index=34) as videoId, 
            (SELECT value FROM unnest(hits.customdimensions) where index=31) as transactionId, 
            NULL as lastVideoIDFromCookie, 
            (SELECT value FROM unnest(hits.customdimensions) where index=45) as securePageType, 
            (SELECT value FROM unnest(hits.customdimensions) where index=48) as secureProductName,*/

            uniqueVals.userId,
            uniqueVals.TSTmemberID, 
            uniqueVals.TSToid, 
            uniqueVals.videoId, 
            uniqueVals.transactionId, 
            NULL as lastVideoIDFromCookie,  
            uniqueVals.securePageType, 
            uniqueVals.secureProductName,

            IFNULL( 
              1 + SUM(IF( {goalX}          -- What is goalX (JS Variable) ? 
                AND (hits.page.hostname IS NULL 
                  OR hits.page.hostname IN ( 
                  "signup2.thestreet.com", 
                  "demosignup2.thestreet.com", 
                  "secure2.thestreet.com", 
                  "demosecure2.thestreet.com", 
                  "secure.thestreet.com" 
                  ) 
                ), 
                1, 0)) 
                OVER ( 
                  PARTITION BY date, fullVisitorId, visitNumber, hits.userId, device.deviceCategory, trafficSource.source, trafficSource.medium 
                  ORDER BY hits.hitNumber 
                  ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING 
                ), 
              1 
            ) AS goalFlag 
          FROM hits_exp AS hits
          JOIN (SELECT * FROM uniCTE ) AS uniqueVals
          ON hits.uniq = uniqueVals.uniq
          --FROM `' + projectId + '.' + datasetTableId + qDate + '`, unnest(hits) as hits 
        ) 
      ) 
      WHERE goalFlag IS NOT NULL AND type = "PAGE" 
    ) 
    SELECT 
      to_date(date, 'yyyyMMdd') AS date, --DATE_FORMAT("%Y%m%d", date) AS date, -- PARSE_DATE to DATE_FORMAT
      CONCAT(fullVisitorId, "-", CAST(visitNumber as STRING)) as sessionId, 
      fullVisitorId, visitNumber, userId, deviceCategory, source, medium, goalFlag, transactionId, securePageType, secureProductName, TSTmemberID, TSToid, lastVideoIDFromCookie, 
      ROW_NUMBER() 
        OVER ( 
          PARTITION BY date, fullVisitorId, visitNumber, userId, deviceCategory, source, medium, goalFlag, transactionId 
          ORDER BY viewTime 
        ) AS viewOrder, 
      ROW_NUMBER() 
        OVER ( 
          PARTITION BY date, fullVisitorId, visitNumber, userId, deviceCategory, source, medium, goalFlag, transactionId 
          ORDER BY viewTime DESC 
        ) AS viewOrderReversed, 
      ROW_NUMBER() 
        OVER ( 
          PARTITION BY date, fullVisitorId, visitNumber, userId, deviceCategory, source, medium 
          ORDER BY viewTime 
        ) AS fullViewOrder, 
      ROW_NUMBER() 
        OVER ( 
          PARTITION BY date, fullVisitorId, visitNumber, userId, deviceCategory, source, medium 
          ORDER BY viewTime DESC 
        ) AS fullViewOrderReversed, 
      CASE WHEN videoId IS NOT NULL THEN ROW_NUMBER() 
        OVER ( 
          PARTITION BY CASE WHEN videoId IS NOT NULL THEN 1 END, date, fullVisitorId, visitNumber, userId, deviceCategory, source, medium 
          ORDER BY viewTime 
        ) END AS viewVideoOrder, 
      CASE WHEN videoId IS NOT NULL THEN ROW_NUMBER() 
        OVER ( 
          PARTITION BY CASE WHEN videoId IS NOT NULL THEN 1 END, date, fullVisitorId, visitNumber, userId, deviceCategory, source, medium 
          ORDER BY viewTime DESC 
        ) END AS viewVideoOrderReversed, 
      viewTime, videoId, pageTitle, hostname, pagePath 
    FROM ( 
      SELECT 
        * 
      FROM base 
      INNER JOIN goalX USING (date, fullVisitorId, visitNumber) 
    ) 
    ORDER BY date, fullVisitorId, visitNumber, userId, goalFlag, viewOrder """.format(goalX=goalX))
    return goalX_videos

# print( goalX_videos.count(), len(goalX_videos.columns) ) # BigQuery : 
# display(goalX_videos)


# # Save the result to DB
# tableName = 'goalX_videos_' + tableNameBigQuery.replace('ga_sessions_', '')
# print("{dbName}.{tabName}".format(dbName=databricksDbName, tabName=tableName))
# goalX_videos.write.mode("Overwrite").saveAsTable("{dbName}.{tabName}".format(dbName=databricksDbName, tabName=tableName))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### The Street : Main Export, Line 111 & 135 onwards (contentQuery)

# COMMAND ----------

# adswerve param= contentQuery

def contentQuery_transformation(df):
    df.createOrReplaceTempView('ga_sessions_limit')

    contentQuery = spark.sql("""

    WITH exp_hits_cd AS
    (
      SELECT *
      FROM ( SELECT EXPLODE(hits.customDimensions) AS expHitsCD, hits, fullVIsitorId, visitNumber, date, channelGrouping, trafficSource, device, geoNetwork, totals
             FROM 
             ( SELECT EXPLODE(hits) AS hits, fullVIsitorId, visitNumber, date, channelGrouping, visitNumber, trafficSource, device, geoNetwork, totals FROM ga_sessions_limit))
    ),

    sessions AS 
    (
      SELECT COALESCE(t1.fullVisitorId, t2.fullVisitorId) as fullVisitorId, coalesce(t1.visitNumber, t2.visitNumber) as visitNumber, 
          sessionTransaction, sessionSignup 
          FROM( 
                SELECT fullVIsitorId, visitNumber, true as sessionTransaction 
                FROM exp_hits_cd
                WHERE RLIKE(hits.page.pagePath, r"^/signup/signup/(thankYou|postPaymentSell|hybridPaymentSell|crossSell|extraSell|success|upsell).html$") 
                GROUP BY 1,2
              ) as t1 
          FULL OUTER JOIN  
            ( SELECT fullVIsitorId, visitNumber, true as sessionSignup 
               FROM exp_hits_cd
              WHERE hits.page.pagePath = "/signup/signup/verify.html" 
              GROUP BY 1,2
              ) as t2 
          ON t1.fullVisitorId = t2.fullVIsitorId AND t1.visitNumber = t2.visitNumber 
    )

    SELECT date, exp_hits.fullVisitorId, 
      max(if (expHitsCD.index = 1, expHitsCD.value,null)) OVER(PARTITION BY exp_hits.fullVIsitorId) as userId,
      max(if( expHitsCD.index = 12, expHitsCD.value,null)) OVER(PARTITION BY exp_hits.fullVIsitorId) as articleId,
      MAX(if (expHitsCD.index = 2, expHitsCD.value,NULL)) OVER(PARTITION BY exp_hits.fullVIsitorId) AS adBlocker,
      MAX(if (expHitsCD.index = 44, expHitsCD.value,NULL)) OVER(PARTITION BY exp_hits.fullVIsitorId) AS lastArticleIdFromCookie,
      MAX(if (expHitsCD.index = 46, expHitsCD.value,NULL)) OVER(PARTITION BY exp_hits.fullVIsitorId) AS lastAuthorIdFromCookie,
      MAX(if (expHitsCD.index = 25, expHitsCD.value,NULL)) OVER(PARTITION BY exp_hits.fullVIsitorId) AS contentType,
      MAX(if (expHitsCD.index = 40, expHitsCD.value,NULL)) OVER(PARTITION BY exp_hits.fullVIsitorId) AS activeWindow,
      MAX(if (expHitsCD.index = 52, expHitsCD.value,NULL)) OVER(PARTITION BY exp_hits.fullVIsitorId) AS channels,
      hits.page.pageTitle as pageTitle, hits.page.hostname as host, hits.type as hitType, trafficSource.source AS source
      , channelGrouping, device.deviceCategory as deviceCategory, hits.isEntrance as isEntrance, hits.isExit as isExit, trafficSource.medium as medium, trafficSource.campaign as campaign, exp_hits.visitNumber, hits.hour as hour, hits.hitNumber as hitnumber, totals.hits as hits, hits.page.pagePath as pagePath, hits.page.pagePathLevel1 as pagePathLevel1, hits.page.pagePathLevel2 as pagePathLevel2, geoNetwork.country as country, geoNetwork.region as region, geoNetwork.city as
      city, hits.eventInfo.eventCategory eventCategory, hits.eventInfo.eventAction eventAction, hits.eventInfo.eventLabel eventLabel, hits.transaction.transactionId, 
      IF((hits.page.pagePath LIKE "%/k/" OR hits.page.hostname LIKE "%aap.thestreet.com"), true, false) as premium,
      IF(RLIKE(hits.page.pagePath, r"^/signup/signup/(thankYou|postPaymentSell|hybridPaymentSell|crossSell|extraSell|success|upsell)\.html$"), true, false) as hitTransaction,
      IF(hits.page.pagePath="/signup/signup/verify.html", true, false) as hitSignup
      -- FROM FLATTEN([thestreet-analytics:13210892.ga_sessions_' + qDate + '], hits)) as hits 

      FROM exp_hits_cd AS exp_hits
      LEFT OUTER JOIN sessions ON exp_hits.fullVisitorId = sessions.fullVIsitorId AND exp_hits.visitNumber=sessions.visitNumber """)
    return contentQuery

# print( contentQuery.count(), len(contentQuery.columns) ) # BigQuery : 
# display(contentQuery)

# # Save the result to DB
# tableName = 'contentQuery_' + tableNameBigQuery.replace('ga_sessions_', '')
# print("{dbName}.{tabName}".format(dbName=databricksDbName, tabName=tableName))
# contentQuery.write.mode("Overwrite").saveAsTable("{dbName}.{tabName}".format(dbName=databricksDbName, tabName=tableName))

# COMMAND ----------

# MAGIC %md
# MAGIC #### The Street : Main Export, Line 171  (AppContent Query)

# COMMAND ----------

# # Big Query Database & Table
# dbNameBigQuery = '67977450'
# tableNameBigQuery = 'ga_sessions_20220716'

# # Loading that table into a Dataframe & creating a view for SQL to work on
# temp = spark.read.format("bigquery").option("parentProject", "thestreet-analytics").option("table", "thestreet-analytics.{db}.{table}".format(db=dbNameBigQuery, table=tableNameBigQuery)).load()
# print(temp.count(), len(temp.columns))
# temp.createOrReplaceTempView(tableNameBigQuery)
# spark.sql("SELECT * FROM "+tableNameBigQuery).createOrReplaceTempView("ga_sessions_limit")  

# display(temp)

# COMMAND ----------

# Currently getting some issues on this
# adswerve param= appContentQuery

def appContentQuery_transformation(df):
    df.createOrReplaceTempView('ga_sessions_limit')

    appContentQuery = spark.sql("""

    WITH exp_hits_cd AS
    (
      SELECT expHitsCD, hits, fullVIsitorId, visitNumber, date, channelGrouping, trafficSource, device, geoNetwork, totals
      FROM ( SELECT EXPLODE(hits.customDimensions) AS expHitsCD, hits, fullVIsitorId, visitNumber, date, channelGrouping, trafficSource, device, geoNetwork, totals
             FROM 
             ( SELECT EXPLODE(hits) AS hits, fullVIsitorId, visitNumber, date, channelGrouping, visitNumber, trafficSource, device, geoNetwork, totals FROM ga_sessions_limit))
    ),

    sessionGoals AS 
    (
    SELECT COALESCE(t1.fullVisitorId, t2.fullVisitorId) AS fullVisitorId, COALESCE(t1.visitNumber, t2.visitNumber) AS visitNumber
      FROM (
           SELECT fullVIsitorId, visitNumber
          FROM exp_hits_cd
          GROUP BY 1, 2
            ) AS t1
      FULL OUTER JOIN (
        SELECT fullVIsitorId, visitNumber
        FROM exp_hits_cd
        GROUP BY 1, 2) AS t2
      ON t1.fullVisitorId = t2.fullVIsitorId AND t1.visitNumber = t2.visitNumber
    )

    SELECT date, fullVisitorId, visitNumber, userId, articleId, --LAST(SPLIT(adBlocker, "|")) adBlocker -- Getting an error so removed it
    adBlocker
    , screenName, hitType, source, medium, campaign, appName, deviceCategory, operatingSystem, operatingSystemVersion, mobileDeviceBranding, mobileDeviceModel, mobileDeviceInfo, screenResolution, language, appVersion, isEntrance, isExit, hour, hitNumber, hits, country, region, city, eventCategory, eventAction, eventLabel, --LAST(SPLIT(eventLabel, "|")) lastElementLabel -- Getting an error so removed it
    eventLabel AS lastElementLabel, transactionId 
    FROM ( 
      SELECT exp_hits.date, exp_hits.fullVisitorId, 
       MAX(if (expHitsCD.index = 1, expHitsCD.value, NULL)) OVER(PARTITION BY exp_hits.fullVIsitorId) AS userId, 
        MAX(IF( expHitsCD.index = 12, expHitsCD.value,NULL)) OVER(PARTITION BY exp_hits.fullVIsitorId) AS articleId, 
        MAX(if (expHitsCD.index = 2, expHitsCD.value, NULL)) OVER(PARTITION BY exp_hits.fullVIsitorId) AS adBlocker, 
        exp_hits.hits.appInfo.screenName AS screenName, exp_hits.hits.type AS hitType, trafficSource.source AS source, channelGrouping, device.deviceCategory AS deviceCategory, device.operatingSystem AS operatingSystem, device.operatingSystemVersion AS operatingSystemVersion, device.mobileDeviceBranding AS mobileDeviceBranding, device.mobileDeviceModel AS mobileDeviceModel, device.mobileDeviceInfo AS mobileDeviceInfo, device.screenResolution as screenResolution, exp_hits.hits.appInfo.appName as appName, device.language AS language, exp_hits.hits.appInfo.appVersion AS appVersion, exp_hits.hits.isEntrance AS isEntrance, exp_hits.hits.isExit AS isExit, trafficSource.medium AS medium, trafficSource.campaign AS campaign, exp_hits.visitNumber, exp_hits.hits.hour AS hour, exp_hits.hits.hitNumber AS hitnumber, totals.hits AS hits, geoNetwork.country AS country, geoNetwork.region AS region, geoNetwork.city AS city, exp_hits.hits.eventInfo.eventCategory eventCategory, exp_hits.hits.eventInfo.eventAction eventAction, exp_hits.hits.eventInfo.eventLabel eventLabel, exp_hits.hits.transaction.transactionId
    FROM exp_hits_cd AS exp_hits
      LEFT OUTER JOIN sessions ON exp_hits.fullVisitorId = sessions.fullVIsitorId AND exp_hits.visitNumber=sessions.visitNumber )
       """)
    return appContentQuery

# print( appContentQuery.count(), len(appContentQuery.columns) ) # BigQuery : 
# display(appContentQuery)

# # Save the result to DB
# tableName = 'appContentQuery_' + tableNameBigQuery.replace('ga_sessions_', '')
# print("{dbName}.{tabName}".format(dbName=databricksDbName, tabName=tableName))
# appContentQuery.write.mode("Overwrite").saveAsTable("{dbName}.{tabName}".format(dbName=databricksDbName, tabName=tableName))

# COMMAND ----------


