# Databricks notebook source
# MAGIC %md
# MAGIC #### Initialize

# COMMAND ----------

# MAGIC %pip install databricks_api

# COMMAND ----------

from databricks_api import DatabricksAPI
secret_scope_name = 'external'
dbutils.widgets.text(name="PATtoken", defaultValue="", label="PAT token")
access_token = dbutils.widgets.get("PATtoken")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Check if scope already exists and create external secrets

# COMMAND ----------

#access_token = 'dapib019f6bd96a1df8071caec3d62377d5d'

def create_update_external_secrets(scope_name, secrets, access_token=None):
    db = DatabricksAPI(host=spark.conf.get("spark.databricks.workspaceUrl"), token=access_token)
    try:
        access_token = dbutils.secrets.get(scope='internal', key='BearerToken')
    except:
        if access_token is not None:
            if len([i.name for i in dbutils.secrets.listScopes() if i.name=='internal']) == 0:
                db.secret.create_scope(scope='internal', initial_manage_principal = 'users')
            db.secret.put_secret(scope = "internal", key='BearerToken', string_value='access_token')
        else:
            raise Exception("Please provide access_token (PAT_token).")
    if len([i.name for i in dbutils.secrets.listScopes() if i.name==scope_name])==0:
        db.secret.create_scope(scope=scope_name, initial_manage_principal = 'users')
    assert(type(secrets) == 'dict', "secrets object should be a dictionary containing keys, values for secrets to be created within specified scope.")
    for k, v in secrets.items():
        db.secret.put_secret(scope=scope_name, key=k, string_value=v)


# COMMAND ----------

scope_name = "arena_big_query"
secrets = {
    "credentials":
    "accountEmail": 
    "privateKey": 
    "privateKeyId": 
}

create_update_external_secrets(scope_name, secrets)

# COMMAND ----------

scope_name = "adventureworkslt"
secrets = {
    "DatabaseName": "ktkvdc",
    "Login": "armdeploymentadmin",
    "Pwd": "BiN3ReTNK3Z8rzw",
    "SQLServerName": "ktkvdc.database.windows.net"
}

create_update_external_secrets(scope_name, secrets, access_token)

# COMMAND ----------

dbutils.secrets.list(scope="ExternalBlobStore")

# COMMAND ----------

scope_name = "ExternalBlobStore"
secrets = {
    "BasePath": "wasbs://externaldata@ktkdexte16blobe.blob.core.windows.net",
    "ContainerOrFileSystemName": "externaldata",
    "StorageAccountKey": "TtDmuKLPb0qmcuwCpc1tx8znII0n0TIFw8gkphqShG8lqxz7scNvTXUl0GogxTVpZ/at1sDCg/6M7StdiNek7A==",
    "StorageAccountName": "fs.azure.account.key.ktkdexte16blobe.blob.core.windows.net"
}

create_update_external_secrets(scope_name, secrets, access_token)

# COMMAND ----------

scope_name = "EventHubTwitterIngestion"
secrets = {
    "EventHubConnectionString": 
    "EventHubConsumerGroup": 
    "EventHubName": 
}

#create_update_external_secrets(scope_name, secrets)

# COMMAND ----------

scope_name = "EventHubTwitterDistribution"
secrets = {
    "EventHubConnectionString": 
    "EventHubConsumerGroup": 
    "EventHubName": 
}

#create_update_external_secrets(scope_name, secrets)

# COMMAND ----------

scope_name = "TwitterAPI"
secrets = {
    "AccessToken": 
    "AccessTokenSecret": 
    "APIKey": 
    "SecretKey"
}

#create_update_external_secrets(scope_name, secrets)

# COMMAND ----------

scope_name = "CognitiveServices"
secrets = {
    "AccessKey": 
    "Endpoint": 
    "LanguagesPath": 
    "SentimentPath"
}

# create_update_external_secrets(scope_name, secrets)

# COMMAND ----------

scope_name = "EventHubCVIngestion"
secrets = {
    "EventHubConnectionString": 
    "EventHubConsumerGroup": 
    "EventHubName": 
}

# create_update_external_secrets(scope_name, secrets)

# COMMAND ----------

scope_name = "EventHubCVDistribution"
secrets = {
    "EventHubConnectionString": 
    "EventHubConsumerGroup": 
    "EventHubName": 
}

# create_update_external_secrets(scope_name, secrets)

# COMMAND ----------

scope_name = "AzureDataExplorer"
secrets = {
    "ADXApplicationAuthorityId": 
    "ADXApplicationId": 
    "ADXApplicationKey": 
    "ADXClusterName"
}

# create_update_external_secrets(scope_name, secrets)

# COMMAND ----------

scope_name = "CosmosDB"
secrets = {
    "Database": 
    "EndPoint": 
    "MasterKey": 
    "PreferredRegions"
}

# create_update_external_secrets(scope_name, secrets)

# COMMAND ----------

scope_name = "internal"
secrets = {
    "SanctionedSQLServerName": 'ktk-d-mdp-e16-sql.database.windows.net',
    "SanctionedDatabaseName": 'DWTemplate',
    "SanctionedSQLServerLogin": 'armdeploymentadmin',
    "SanctionedSQLServerPwd": 'BiN3ReTNK3Z8rzw'    
}
create_update_external_secrets(scope_name, secrets, access_token)

# COMMAND ----------

scope_name = "databricks_logs"
secrets = {
    "BasePath": "abfss://insights-logs-notebook@ktkdmdpe16blobt.dfs.core.windows.net",
    "ContainerOrFileSystemName": "insights-logs-notebook",
    "StorageAccountKey": "cK2Z8ddqrbUzuJcy/MOlqb2/ZikOwuE3TaX+DNR4mioBGWBEoZeJxz85W6JSGu0bgA5I+hCfyLh/6w/wGkuFEQ==",
    "StorageAccountName": "fs.azure.account.key.ktkdmdpe16blobt.dfs.core.windows.net" 
}
create_update_external_secrets(scope_name, secrets, access_token)

# COMMAND ----------

# #access_token = 'dapib019f6bd96a1df8071caec3d62377d5d'
# if access_token == "":
#   print("Please provide a valid access token.")
# else:
#   db = DatabricksAPI(host=spark.conf.get("spark.databricks.workspaceUrl"), token=access_token)

#   if len([i.name for i in dbutils.secrets.listScopes() if i.name==secret_scope_name])==0:
#       db.secret.create_scope(scope = secret_scope_name, initial_manage_principal = 'users')
#       print(f"Secret scope created with Name: {secret_scope_name}")
#   else:
#       print(f"Secret scope named '{secret_scope_name}' already exists.")
    
#   #create external secrets
#   try:
#         db.secret.put_secret(scope = secret_scope_name, key='ExternalContainerOrFileSystemName', string_value='externaldata')
#         db.secret.put_secret(scope = secret_scope_name, key='Login', string_value='armdeploymentadmin')
#         db.secret.put_secret(scope = secret_scope_name, key='DatabaseName', string_value='ktkvdc')
#         db.secret.put_secret(scope = secret_scope_name, key='ExternalDatabasePwd', string_value='BiN3ReTNK3Z8rzw')
#         db.secret.put_secret(scope = secret_scope_name, key='ExternalFileSystemHost', string_value='C:\ADFSampleFiles')
#         db.secret.put_secret(scope = secret_scope_name, key='ExternalFileSystemUserName', string_value='adf')
#         db.secret.put_secret(scope = secret_scope_name, key='ExternalFileSystemPassword', string_value='2s)Ng[8mxr4wCnvC')
#         db.secret.put_secret(scope = secret_scope_name, key='ExternalFileSystemPwd', string_value='2s)Ng[8mxr4wCnvC')
#         db.secret.put_secret(scope = secret_scope_name, key='ExternalSQLServerName', string_value='ktkvdc.database.windows.net')
#         db.secret.put_secret(scope = secret_scope_name, key='ExternalStorageAccountKey', string_value='TtDmuKLPb0qmcuwCpc1tx8znII0n0TIFw8gkphqShG8lqxz7scNvTXUl0GogxTVpZ/at1sDCg/6M7StdiNek7A==')
#         db.secret.put_secret(scope = secret_scope_name, key='ExternalStorageAccountName', string_value='ktkdexte16blobe')
#         db.secret.put_secret(scope = secret_scope_name, key='ExternalStorageAccountType', string_value='blob')
#   except Exception as e:
#     print(e)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Method for Dropping External Scope

# COMMAND ----------

def drop_external_scope(drop_scope):
  db = DatabricksAPI(host=spark.conf.get("spark.databricks.workspaceUrl"), token="dapib019f6bd96a1df8071caec3d62377d5d")
  if drop_scope == 'True':
      try:
          db.secret.delete_scope(drop_scope)
          print(f"'{drop_scope}' secret scope has been dropped successfully.")
      except Exception as e:
          print(e)
  else:
      print(f"Cannot drop '{drop_scope}' scope, as drop_scope is 'False'.")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Method for removing a particular secret

# COMMAND ----------

def remove_secret(secret_scope_name, secret_key):
  db = DatabricksAPI(host=spark.conf.get("spark.databricks.workspaceUrl"), token="dapib019f6bd96a1df8071caec3d62377d5d")
  if secret_key != "":
      try:
          db.secret.delete_secret(secret_scope_name, secret_key)
          print(f"'{secret_key}' secret details have been removed from '{secret_scope_name}' secret scope.")
      except Exception as e:
          print(e)
  else:
      print(f"Secret key is empty. Please provide secret key to be removed from '{secret_scope_name}' scope.")
