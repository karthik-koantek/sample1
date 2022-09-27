# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC Separated for cyclic-dependency reasons

# COMMAND ----------

RND, STAGING, PERFORM, DEMO= 'rnd', 'staging', 'perform', 'demo2022' 
UAT_US, UAT_ASIA, UAT_EU = 'uat-us', 'uat-asia', 'uat-eu'
PROD_US, PROD_ASIA, PROD_EU, FIDELITY = 'prod-us', 'prod-asia', 'prod-eu', 'fidelity'

ENV_PARAM = 'w_environment'

ALL_ENVS = [ RND, STAGING, PERFORM, DEMO, UAT_US, UAT_ASIA, UAT_EU, PROD_US, PROD_ASIA, PROD_EU ]
