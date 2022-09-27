# Databricks notebook source
# MAGIC %md # Data Catalog File

# COMMAND ----------

# MAGIC %md #### Initialize

# COMMAND ----------

# MAGIC %run "../Orchestration/Notebook Functions"

# COMMAND ----------

# MAGIC %run ../Development/Utilities

# COMMAND ----------

import json
import hashlib
import uuid
from multiprocessing.pool import ThreadPool
import sys, traceback
from functools import reduce
from pyspark.sql import DataFrame
from pyspark.sql.functions import unix_timestamp
from pyspark.sql.types import TimestampType, LongType
from azure.storage.filedatalake import DataLakeServiceClient

dbutils.widgets.text(name="stepLogGuid", defaultValue="00000000-0000-0000-0000-000000000000", label="stepLogGuid")
dbutils.widgets.text(name="stepKey", defaultValue="-1", label="stepKey")
dbutils.widgets.text(name="storageAccountSecretName", defaultValue="", label="Storage Account Secret Name")
dbutils.widgets.text(name="storageAccountKeySecretName", defaultValue="", label="Storage Account Key Secret Name")
dbutils.widgets.text(name="fileSystemName", defaultValue="", label="Storage Account File System Name")
dbutils.widgets.text(name="maxResultsPerPage", defaultValue="5000", label="Max Results Per Page")

stepLogGuid = dbutils.widgets.get("stepLogGuid")
stepKey = int(dbutils.widgets.get("stepKey"))
storageAccountSecretName = dbutils.widgets.get("storageAccountSecretName")
storageAccountKeySecretName = dbutils.widgets.get("storageAccountKeySecretName")
fileSystemName = dbutils.widgets.get("fileSystemName")
storageAccountName = dbutils.secrets.get(scope="internal", key=storageAccountSecretName)
storageAccountKey = dbutils.secrets.get(scope="internal", key=storageAccountKeySecretName)
accountUrl = "https://{0}.dfs.core.windows.net/".format(storageAccountName)
maxResultsPerPage = int(dbutils.widgets.get("maxResultsPerPage"))

context = dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson()
p = {
  "stepLogGuid": stepLogGuid,
  "stepKey": stepKey,
  "storageAccountName": storageAccountName,
  "storageAccountKey": storageAccountKey,
  "fileSystemName": fileSystemName,
  "accountUrl": accountUrl,
  "maxResultsPerPage": maxResultsPerPage
}
parameters = json.dumps(p)

notebookLogGuid = str(uuid.uuid4())
log_notebook_start(notebookLogGuid, stepLogGuid, stepKey, parameters, context, server, database, login, pwd)

print("Notebook Log Guid: {0}".format(notebookLogGuid))
print("Step Log Guid: {0}".format(stepLogGuid))
print("Context: {0}".format(context))
print("Parameters: {0}".format(parameters))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Get Paths

# COMMAND ----------

service = DataLakeServiceClient(account_url=accountUrl, credential=storageAccountKey)
fileSystem = service.get_file_system_client(fileSystemName)
paths = fileSystem.get_paths(max_results=maxResultsPerPage)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Iterate Paths and Generate Dataframe

# COMMAND ----------

def mergeFileDetail():
  sql = """
  MERGE INTO silverprotected.fileDetail AS tgt
  USING fileDetailStaging AS src ON tgt.storageAccountName = src.storageAccountName AND tgt.fileSystemName = src.fileSystemName AND tgt.fileName = src.fileName AND tgt.StorageAccountName = '{0}' AND tgt.fileSystemName = '{1}'
  WHEN MATCHED THEN UPDATE SET *
  WHEN NOT MATCHED THEN INSERT *
  """.format(storageAccountName, fileSystemName)
  print(sql)
  spark.sql(sql)

# COMMAND ----------

result = []
dfList = []
spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
for path in paths:
  print(path.name,path.content_length,path.last_modified,path.etag,path.group,path.owner,path.permissions)
  if path.is_directory == False:
    result.append([str(storageAccountName),str(fileSystemName),str(path.name),path.content_length,path.last_modified,str(path.etag),str(path.group),str(path.owner),str(path.permissions)])
if len(result) != 0:
  dfList.append(spark.createDataFrame(result,['storageAccountName','fileSystemName','fileName','sizeBytes','lastModified','eTag','group','owner','permissions']))
  dfList.append(dfList[-1].selectExpr(\
                            "storageAccountName"
                            , "fileSystemName"
                            , "fileName"
                            , "cast(sizeBytes AS bigint) as sizeBytes"
                            , "cast(unix_timestamp(lastModified, 'E, dd MMM yyyy HH:mm:ss') as timestamp) lastModified"
                            , "eTag"
                            , "group"
                            , "owner"
                            , "permissions"))
  dfList[-1].createOrReplaceTempView("fileDetailStaging")
  mergeFileDetail()

# COMMAND ----------

# MAGIC %md #### Log Completion

# COMMAND ----------

log_notebook_end(notebookLogGuid, 0, server, database, login, pwd)
dbutils.notebook.exit("Succeeded")