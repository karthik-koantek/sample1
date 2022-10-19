# Databricks notebook source
# MAGIC %md
# MAGIC #### Initialize

# COMMAND ----------

# MAGIC %pip install databricks_api

# COMMAND ----------

import requests
import json

# COMMAND ----------

dbutils.widgets.text(name="PATtoken", defaultValue="", label="PAT token")
dbutils.widgets.dropdown(name="cloudProvider", defaultValue="Azure", choices=["Azure","GCP","AWS"], label="Cloud Provider")
dbutils.widgets.text(name="detailsProvided", defaultValue="", label="Provide Details")

access_token = dbutils.widgets.get("PATtoken")
cloudProvider = dbutils.widgets.get("cloudProvider")
detailsProvided = json.loads(dbutils.widgets.get("detailsProvided") or str({}))
headers = {"Authorization": f"Bearer {access_token}"}
context = json.loads(dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson())
databricks_endpoint = "https://" + context['tags']['browserHostName']

# Example dictionary to be provided in Provide Details Widget
# For Azure
#{"TransientStorageAccountName":"account-name",
#"TransientStorageAccountKey":"1234",
#"BronzeStorageAccountName":"account-name",
#"BronzeStorageAccountKey":"1234",
#"SilverGoldStorageAccountName":"account-name",
#"SilverGoldStorageAccountKey":"1234",
#"SandboxStorageAccountName":"account-name",
#"SandboxStorageAccountKey":"1234"}

# For AWS
#{"AWSBronzeBucketName":"koantek-ascend-s3",
#"AWSSilverGeneralBucketName":"koantek-ascend-s3",
#"AWSSilverProtectedBucketName":"koantek-ascend-s3",
#"AWSGoldGeneralBucketName":"koantek-ascend-s3",
#"AWSGoldProtectedBucketName":"koantek-ascend-s3"
#}

# COMMAND ----------


#access_token = 'dapib019f6bd96a1df8071caec3d62377d5d'
###### DO NOT MODIFY ######
from databricks_api import DatabricksAPI
def prerequisite(scope='internal', access_token=access_token):
    if access_token == 'Your access token':
        print("Please provide a valid access token if you don't have pre-requisite, otherwise ignore.")
    else:
        db = DatabricksAPI(host=spark.conf.get("spark.databricks.workspaceUrl"), token=access_token)
        try:
            db.secret.list_scopes()
            try:
                db.secret.create_scope(scope='internal')
                db.secret.put_secret(scope='internal', key='BearerToken', string_value=access_token)
            except Exception as e:
                print(e)
        except Exception as e:
            print(e)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Check if scope already exists

# COMMAND ----------

secret_scope_name = "internal"

if len([i.name for i in dbutils.secrets.listScopes() if i.name==secret_scope_name])==0:
  secret_scope_request_json = {
    "scope": secret_scope_name,
    "initial_manage_principal": "users"
  }

  endpoint = databricks_endpoint + "/api/2.0/secrets/scopes/create"
  response = requests.post(endpoint, json=secret_scope_request_json, headers=headers).json()
  print(f"Secret scope created with Name: {secret_scope_name}")
else:
  print(f"Secret scope named '{secret_scope_name}' already exists.")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Validating Details Provided

# COMMAND ----------

secrets=[]
if len(detailsProvided)!=0:
  if cloudProvider=='Azure':
    if 'TransientStorageAccountName' in detailsProvided:
      secrets.append({
        "scope": secret_scope_name,
        "key": "TransientStorageAccountName",
        "string_value": detailsProvided['TransientStorageAccountName']
      })
    if 'TransientStorageAccountKey' in detailsProvided:
      secrets.append({
        "scope": secret_scope_name,
        "key": "TransientStorageAccountKey",
        "string_value": detailsProvided['TransientStorageAccountKey']
      })
    if 'BronzeStorageAccountName' in detailsProvided:
      secrets.append({
        "scope": secret_scope_name,
        "key": "BronzeStorageAccountName",
        "string_value": detailsProvided['BronzeStorageAccountName']
      })
    if 'BronzeStorageAccountKey' in detailsProvided:
      secrets.append({
        "scope": secret_scope_name,
        "key": "BronzeStorageAccountKey",
        "string_value": detailsProvided['BronzeStorageAccountKey']
      })
    if 'SilverGoldStorageAccountName' in detailsProvided:
      secrets.append({
        "scope": secret_scope_name,
        "key": "SilverGoldStorageAccountName",
        "string_value": detailsProvided['SilverGoldStorageAccountName']
      })
    if 'SilverGoldStorageAccountKey' in detailsProvided:
      secrets.append({
        "scope": secret_scope_name,
        "key": "SilverGoldStorageAccountKey",
        "string_value": detailsProvided['SilverGoldStorageAccountKey']
      })
    if 'SandboxStorageAccountName' in detailsProvided:
      secrets.append({
        "scope": secret_scope_name,
        "key": "SandboxStorageAccountName",
        "string_value": detailsProvided['SandboxStorageAccountName']
      })
    if 'SandboxStorageAccountKey' in detailsProvided:
      secrets.append({
        "scope": secret_scope_name,
        "key": "SandboxStorageAccountKey",
        "string_value": detailsProvided['SandboxStorageAccountKey']
      })
    
  elif cloudProvider=='AWS':
    if 'AWSBronzeBucketName' in detailsProvided:
      secrets.append({
        "scope": secret_scope_name,
        "key": "AWSBronzeBucketName",
        "string_value": detailsProvided['AWSBronzeBucketName']
      })
    if 'AWSSilverGeneralBucketName' in detailsProvided:
      secrets.append({
        "scope": secret_scope_name,
        "key": "AWSSilverGeneralBucketName",
        "string_value": detailsProvided['AWSSilverGeneralBucketName']
      })
    if 'AWSSilverProtectedBucketName' in detailsProvided:
      secrets.append({
        "scope": secret_scope_name,
        "key": "AWSSilverProtectedBucketName",
        "string_value": detailsProvided['AWSSilverProtectedBucketName']
      })
    if 'AWSGoldGeneralBucketName' in detailsProvided:
      secrets.append({
        "scope": secret_scope_name,
        "key": "AWSGoldGeneralBucketName",
        "string_value": detailsProvided['AWSGoldGeneralBucketName']
      })
    if 'AWSGoldProtectedBucketName' in detailsProvided:
      secrets.append({
        "scope": secret_scope_name,
        "key": "AWSGoldProtectedBucketName",
        "string_value": detailsProvided['AWSGoldProtectedBucketName']
      })
    
else:
  print(f"Secret details are not provided for {cloudProvider}")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create secrets

# COMMAND ----------

endpoint = databricks_endpoint + "/api/2.0/secrets/put"

for secret in secrets:
  response = requests.post(endpoint, json=secret, headers=headers).json()
  print(f"Secret created with Key: {secret['key']} in Scope: {secret['scope']}")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Method for Dropping Internal Scope

# COMMAND ----------

def drop_internal_scope(drop_scope):
  if drop_scope == 'True':
    try:
        db.secret.delete_scope(secret_scope_name)
        print(f"'{secret_scope_name}' secret scope has been dropped successfully.")
    except Exception as e:
        print(e)
  else:
    print(f"Cannot drop '{secret_scope_name}' scope, as drop_scope is 'False'.")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Method for removing a particular secret

# COMMAND ----------

def remove_secret(secret_key):
  if secret_key != "":
      try:
          db.secret.delete_secret(secret_scope_name, secret_key)
          print(f"'{secret_key}' secret details have been removed from '{secret_scope_name}' secret scope.")
      except Exception as e:
          print(e)
  else:
      print(f"Secret key is empty. Please provide secret key to be removed from '{secret_scope_name}' scope.")
