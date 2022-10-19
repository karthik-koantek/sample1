# Databricks notebook source
# MAGIC %md # Create Storage Mount Point

# COMMAND ----------

# MAGIC %md #### Initialize

# COMMAND ----------

import datetime, json

dbutils.widgets.text(name="mountPoint", defaultValue="", label="Mount Point")
dbutils.widgets.dropdown(name="storageAccountType", defaultValue="blob", choices=["blob", "adlsgen1", "adlsgen2"])
dbutils.widgets.text(name="storageAccountName", defaultValue="", label="Storage Account")
dbutils.widgets.text(name="storageAccountContainerName", defaultValue="", label="Container")
dbutils.widgets.dropdown(name="auth", defaultValue="storageAccountKey", choices=["storageAccountKey", "servicePrincipal"])
dbutils.widgets.text(name="storageAccountKey", defaultValue="", label="Storage Account Key")
dbutils.widgets.text(name="applicationId", defaultValue="", label="Service Principal Application Id")
dbutils.widgets.text(name="secret", defaultValue="", label="Service Principal Secret")
dbutils.widgets.text(name="tenantId", defaultValue="", label="Tenant Id")

mountPoint = dbutils.widgets.get("mountPoint")
storageAccountType = dbutils.widgets.get("storageAccountType")
storageAccountName = dbutils.widgets.get("storageAccountName")
storageAccountContainerName = dbutils.widgets.get("storageAccountContainerName")
auth = dbutils.widgets.get("auth")
storageAccountKey = dbutils.widgets.get("storageAccountKey")
applicationId = dbutils.widgets.get("applicationId")
secret = dbutils.widgets.get("secret")
tenantId = dbutils.widgets.get("tenantId")

context = dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson()
p = {
  "mountPoint": mountPoint,
  "storageAccountType": storageAccountType,
  "storageAccountName": storageAccountName,
  "storageAccountContainerName": storageAccountContainerName,
  "auth": auth,
  "storageAccountKey": storageAccountKey,
  "applicationId": applicationId,
  "secret": secret,
  "tenantId": tenantId
}
parameters = json.dumps(p)
print("Context: {0}".format(context))
print("Parameters: {0}".format(parameters))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Check if exists

# COMMAND ----------

try:
  files = dbutils.fs.ls(mountPoint)
  dbutils.notebook.exit("Mount Point already exists")
except:
  pass #we have to create a mount point

# COMMAND ----------

# MAGIC %md
# MAGIC #### Validate inputs and configure

# COMMAND ----------

if storageAccountType == "blob":
  if storageAccountKey == "":
    errorDescription = "Storage Account Key (or SAS Key, not currently supported here) must be supplied for a Blob Storage Mount Point."
    raise ValueError(errorDescription)
  source = "wasbs://{0}@{1}.blob.core.windows.net".format(storageAccountContainerName, storageAccountName)
  dfs = "fs.azure.account.key.{0}.blob.core.windows.net".format(storageAccountName)
  configs = {dfs: storageAccountKey}
elif storageAccountType == "adlsgen1":
  if applicationId == "" or secret == "" or tenantId == "":
    errorDescription = "Service Principal must be supplied for ADLS Gen1 Mount Point."
    raise ValueError(errorDescription)
  source = "adl://{0}.azuredatalakestore.net/{1}".format(storageAccountName, storageAccountContainerName)
  refreshUrl = "https://login.microsoftonline.com/{0}/oauth2/token".format(tenantId)
  configs = {
    "fs.adl.oauth2.access.token.provider.type": "ClientCredential",
    "fs.adl.oauth2.client.id": applicationId,
    "fs.adl.credential": secret,
    "fs.adl.oauth2.refresh.url": refreshUrl
  }
else: #adls gen2 supports both forms of authentication
  source = "abfss://{0}@{1}.dfs.core.windows.net".format(storageAccountContainerName, storageAccountName)
  if storageAccountKey != "":
    dfs = "fs.azure.account.key.{0}.dfs.core.windows.net".format(storageAccountName)
    configs = {dfs: storageAccountKey}
  elif applicationId != "" and secret != "" and tenantId != "":
    endpoint = "https://login.microsoftonline.com/{0}/oauth2/token".format(tenantId)
    configs = {
      "fs.azure.account.auth.type": "OAuth",
      "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.ClientCredsTokenProvider",
      "fs.azure.account.oauth2.client.id": applicationId,
      "fs.azure.account.oauth2.client.secret": secret,
      "fs.azure.account.oauth2.client.endpoint": endpoint
    }
  else:
    errorDescription = "Storage Account Key or Service Principal must be supplied for ADLS Gen2 Mount Point"

# COMMAND ----------

# MAGIC %md #### Create Mount Point

# COMMAND ----------

print("MountPoint: {0}".format(mountPoint))
print("source: {0}".format(source))
print("configs: {0}".format(configs))

# COMMAND ----------

try:
  print("Creating Mount Point {0}".format(mountPoint))
  dbutils.fs.mount(
  source = source,
  mount_point = mountPoint,
  extra_configs = configs
  )
except Exception as e:
  raise(e)

# COMMAND ----------

try:
  files = dbutils.fs.ls(mountPoint)
  dbutils.notebook.exit("Mount Point Creation Succeeded")
except Exception as e:
  raise(e)