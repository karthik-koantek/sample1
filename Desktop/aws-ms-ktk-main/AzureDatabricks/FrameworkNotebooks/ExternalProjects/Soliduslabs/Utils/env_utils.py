# Databricks notebook source
# MAGIC %run "/Shared/Soliduslabs/Utils/env_utils_just_consts"

# COMMAND ----------

SF_US_URL = 'https://ua97033.us-east-1.snowflakecomputing.com/'
SF_ASIA_URL = 'https://solidus-solidus_asia.snowflakecomputing.com/'
SF_EU_URL = 'https://solidus-solidus_eur.snowflakecomputing.com/'
SF_FIDELITY_URL = 'https://solidus-solidus_us_east_2.snowflakecomputing.com/'

def get_sf_url():
    sf_urls = {
        RND: SF_US_URL,
        STAGING: SF_US_URL,
        PERFORM: SF_US_URL,
        DEMO: SF_US_URL,
        UAT_US: SF_US_URL,
        UAT_ASIA: SF_ASIA_URL,
        UAT_EU: SF_EU_URL,
        PROD_US: SF_US_URL,
        PROD_ASIA: SF_ASIA_URL,
        PROD_EU: SF_EU_URL,
        FIDELITY: SF_FIDELITY_URL
    }
    
    return sf_urls[dbutils.widgets.get(ENV_PARAM)]


# COMMAND ----------

def get_sf_database():
    sf_urls = {
        RND: 'SOLIDUS_RND',
        STAGING: 'SOLIDUS_STAGING',
        PERFORM: 'SOLIDUS_PERFORM',
        DEMO: 'SOLIDUS_DEMO',
        UAT_US: 'SOLIDUS_UAT_US',
        UAT_ASIA: 'SOLIDUS_UAT_ASIA',
        UAT_EU: 'SOLIDUS_UAT_EU',
        PROD_US: 'SOLIDUS_PROD_US',
        PROD_ASIA: 'SOLIDUS_PROD_ASIA',
        PROD_EU: 'SOLIDUS_PROD_EU',
        FIDELITY: 'SOLIDUS_FIDELITY',
    }
    
    return sf_urls[dbutils.widgets.get(ENV_PARAM)]

# COMMAND ----------

# DB convention used for our SQL dashboards (e.g, solidus_uat_us)
def compose_dbsql_name():
    return 'solidus_{env}'.format(env=dbutils.widgets.get(ENV_PARAM).replace('-', '_'))
