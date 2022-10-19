# Databricks notebook source
# MAGIC %md
# MAGIC # Run ADF Project

# COMMAND ----------

# MAGIC %md
# MAGIC #### Initialize

# COMMAND ----------

# MAGIC %pip install azure-mgmt-resource
# MAGIC %pip install azure-mgmt-datafactory
# MAGIC %pip install azure-identity

# COMMAND ----------

from azure.identity import ClientSecretCredential
from azure.mgmt.resource import ResourceManagementClient
from azure.mgmt.datafactory import DataFactoryManagementClient
from azure.mgmt.datafactory.models import *
from datetime import datetime, timedelta
import time

dbutils.widgets.text(name="ADFProjectName", defaultValue="UAT_ADF_SQL", label="ADF Project Name")
dbutils.widgets.text(name="dateToProcess", defaultValue="", label="Date to Process")

ADFProjectName = dbutils.widgets.get("ADFProjectName")
dateToProcess = dbutils.widgets.get("dateToProcess")
subscriptionId = dbutils.secrets.get(scope="adf", key="SubscriptionId")
resourceGroupName = dbutils.secrets.get(scope="adf", key="ResourceGroupName")
dataFactoryName = dbutils.secrets.get(scope="adf", key="DataFactoryName")
clientId = dbutils.secrets.get(scope="adf", key="ClientId")
clientSecret = dbutils.secrets.get(scope="adf", key="ClientSecret")
tenantId = dbutils.secrets.get(scope="adf", key="TenantId")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create ADF Client

# COMMAND ----------

credentials = ClientSecretCredential(client_id=clientId, client_secret=clientSecret, tenant_id=tenantId)
ADFPipelineName = "Run Project"
#resourceClient = ResourceManagementClient(credentials, subscriptionId)
ADFClient = DataFactoryManagementClient(credentials, subscriptionId)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Run ADF Pipeline

# COMMAND ----------

runResponse = ADFClient.pipelines.create_run(resourceGroupName, dataFactoryName, ADFPipelineName, parameters={'projectName':ADFProjectName,'dateToProcess':dateToProcess})
runId = runResponse.run_id
print("Run Id: {0}".format(runId))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Get Run Status

# COMMAND ----------

time.sleep(30)
pipelineRun = ADFClient.pipeline_runs.get(resourceGroupName, dataFactoryName, runId)
pipelineRun.as_dict()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Wait for Completion

# COMMAND ----------

status = pipelineRun.status
while status == "InProgress":
  pipelineRun = ADFClient.pipeline_runs.get(resourceGroupName, dataFactoryName, runId)
  status = pipelineRun.status
  print("\n\tPipeline run status: {0} at {1}".format(pipelineRun.status, datetime.now()))
  time.sleep(30)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Final Status

# COMMAND ----------

pipelineRun = ADFClient.pipeline_runs.get(resourceGroupName, dataFactoryName, runId)
pipelineRun.as_dict()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Return Status

# COMMAND ----------

if pipelineRun.status == "Succeeded":
  dbutils.notebook.exit("Succeeded")
else:
  dbutils.notebook.exit("ADF Pipeline Failed")