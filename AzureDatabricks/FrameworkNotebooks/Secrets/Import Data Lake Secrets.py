# Databricks notebook source
# MAGIC %md # Import Data Lake Secrets

# COMMAND ----------

# MAGIC %md
# MAGIC #### Initialize

# COMMAND ----------

def cacheSecrets(server, database, login, pwd, bronzeStorageAccountName, bronzeStorageAccountKey, bronzeBasePath, silverStorageAccountName, silverStorageAccountKey, silverGeneralBasePath, silverProtectedBasePath, archiveBasePath, goldStorageAccountName, goldStorageAccountKey, goldGeneralBasePath, goldProtectedBasePath, sandboxStorageAccountName, sandboxStorageAccountKey, sandboxBasePath, transientStorageAccountName, transientStorageAccountKey):
  from pyspark.sql.types import StructType, StructField, StringType

  schema = StructType([
    StructField("server", StringType(), True),
    StructField("database", StringType(), True),
    StructField("login", StringType(), True),
    StructField("pwd", StringType(), True),
    StructField("bronzeStorageAccountName", StringType(), True),
    StructField("bronzeStorageAccountKey", StringType(), True),
    StructField("bronzeBasePath", StringType(), True),
    StructField("silverStorageAccountName", StringType(), True),
    StructField("silverStorageAccountKey", StringType(), True),
    StructField("silverGeneralBasePath", StringType(), True),
    StructField("silverProtectedBasePath", StringType(), True),
    StructField("archiveBasePath", StringType(), True),
    StructField("goldStorageAccountName", StringType(), True),
    StructField("goldStorageAccountKey", StringType(), True),
    StructField("goldGeneralBasePath", StringType(), True),
    StructField("goldProtectedBasePath", StringType(), True),
    StructField("sandboxStorageAccountName", StringType(), True),
    StructField("sandboxStorageAccountKey", StringType(), True),
    StructField("sandboxBasePath", StringType(), True),
    StructField("transientStorageAccountName", StringType(), True),
    StructField("transientStorageAccountKey", StringType(), True)
  ])

  secrets = {
    "server":server,
    "database":database,
    "login":login,
    "pwd":pwd,
    "bronzeStorageAccountName":bronzeStorageAccountName,
    "bronzeStorageAccountKey": bronzeStorageAccountKey,
    "bronzeBasePath": bronzeBasePath,
    "silverStorageAccountName": silverStorageAccountName,
    "silverStorageAccountKey": silverStorageAccountKey,
    "silverGeneralBasePath": silverGeneralBasePath,
    "silverProtectedBasePath": silverProtectedBasePath,
    "archiveBasePath": archiveBasePath,
    "goldStorageAccountName": goldStorageAccountName,
    "goldStorageAccountKey": goldStorageAccountKey,
    "goldGeneralBasePath": goldGeneralBasePath,
    "goldProtectedBasePath": goldProtectedBasePath,
    "sandboxStorageAccountName": sandboxStorageAccountName,
    "sandboxStorageAccountKey": sandboxStorageAccountKey,
    "sandboxBasePath": sandboxBasePath,
    "transientStorageAccountName": transientStorageAccountName,
    "transientStorageAccountKey": transientStorageAccountKey
  }

  json_rdd=sc.parallelize([secrets])
  df = json_rdd.toDF(schema)
  df.createOrReplaceGlobalTempView("secrets")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Cache Secrets
# MAGIC Global temp table persists for the current Spark Session/Application

# COMMAND ----------

if len([s.name for s in spark.catalog.listTables("global_temp") if s.name=='secrets']) > 0:
  found = True
else:
  found = False

if (found == False):
  #metadata db
  server = dbutils.secrets.get(scope="internal", key="FrameworkDBServerName")
  database = dbutils.secrets.get(scope="internal", key="FrameworkDBName")
  login = dbutils.secrets.get(scope="internal", key="FrameworkDBAdministratorLogin")
  pwd = dbutils.secrets.get(scope="internal", key="FrameworkDBAdministratorPwd")
  #transient zone
  transientStorageAccountName = dbutils.secrets.get(scope="internal", key="TransientStorageAccountName")
  transientStorageAccountKey = dbutils.secrets.get(scope="internal", key="TransientStorageAccountKey")
  #bronze zone
  bronzeStorageAccountName = dbutils.secrets.get(scope="internal", key="BronzeStorageAccountName")
  bronzeStorageAccountKey = dbutils.secrets.get(scope="internal", key="BronzeStorageAccountKey")
  bronzeBasePath = "abfss://raw@{0}.dfs.core.windows.net".format(bronzeStorageAccountName)
  #silver zone
  silverStorageAccountName = dbutils.secrets.get(scope="internal", key="SilverGoldStorageAccountName")
  silverStorageAccountKey = dbutils.secrets.get(scope="internal", key="SilverGoldStorageAccountKey")
  silverGeneralBasePath = "abfss://silvergeneral@{0}.dfs.core.windows.net".format(silverStorageAccountName)
  silverProtectedBasePath = "abfss://silverprotected@{0}.dfs.core.windows.net".format(silverStorageAccountName)
  archiveBasePath = "abfss://archive@{0}.dfs.core.windows.net".format(silverStorageAccountName)
  #gold zone
  goldStorageAccountName = dbutils.secrets.get(scope="internal", key="SilverGoldStorageAccountName")
  goldStorageAccountKey = dbutils.secrets.get(scope="internal", key="SilverGoldStorageAccountKey")
  goldGeneralBasePath = "abfss://goldgeneral@{0}.dfs.core.windows.net".format(goldStorageAccountName)
  goldProtectedBasePath = "abfss://goldprotected@{0}.dfs.core.windows.net".format(goldStorageAccountName)
  #sandbox zone
  sandboxStorageAccountName = dbutils.secrets.get(scope="internal", key="SandboxStorageAccountName")
  sandboxStorageAccountKey = dbutils.secrets.get(scope="internal", key="SandboxStorageAccountKey")
  sandboxBasePath = "abfss://defaultsandbox@{0}.dfs.core.windows.net".format(sandboxStorageAccountName)
  cacheSecrets(server, database, login, pwd, bronzeStorageAccountName, bronzeStorageAccountKey, bronzeBasePath, silverStorageAccountName, silverStorageAccountKey, silverGeneralBasePath, silverProtectedBasePath, archiveBasePath, goldStorageAccountName, goldStorageAccountKey, goldGeneralBasePath, goldProtectedBasePath, sandboxStorageAccountName, sandboxStorageAccountKey, sandboxBasePath, transientStorageAccountName, transientStorageAccountKey)


# COMMAND ----------

# MAGIC %md
# MAGIC #### Read Secrets from Cache
# MAGIC For both Python and Scala

# COMMAND ----------

secrets = spark.sql("""
SELECT server, database, login, pwd, bronzeStorageAccountName, bronzeStorageAccountKey, bronzeBasePath, silverStorageAccountName, silverStorageAccountKey, silverGeneralBasePath, silverProtectedBasePath, archiveBasePath, goldStorageAccountName, goldStorageAccountKey, goldGeneralBasePath, goldProtectedBasePath, sandboxStorageAccountName, sandboxStorageAccountKey, sandboxBasePath, transientStorageAccountName, transientStorageAccountKey
FROM global_temp.secrets
""").collect()

server, database, login, pwd, bronzeStorageAccountName, bronzeStorageAccountKey, bronzeBasePath, silverStorageAccountName, silverStorageAccountKey, silverGeneralBasePath, silverProtectedBasePath, archiveBasePath, goldStorageAccountName, goldStorageAccountKey, goldGeneralBasePath, goldProtectedBasePath, sandboxStorageAccountName, sandboxStorageAccountKey, sandboxBasePath, transientStorageAccountName, transientStorageAccountKey = secrets[0]

# COMMAND ----------

# MAGIC %scala
# MAGIC val secrets = spark.sql("SELECT server, database, login, pwd, bronzeStorageAccountName, bronzeStorageAccountKey, bronzeBasePath, silverStorageAccountName, silverStorageAccountKey, silverGeneralBasePath, silverProtectedBasePath, archiveBasePath, goldStorageAccountName, goldStorageAccountKey, goldGeneralBasePath, goldProtectedBasePath, sandboxStorageAccountName, sandboxStorageAccountKey, sandboxBasePath, transientStorageAccountName, transientStorageAccountKey FROM global_temp.secrets").collect()
# MAGIC
# MAGIC val server:String = secrets(0)(0).toString()
# MAGIC val database:String = secrets(0)(1).toString()
# MAGIC val login:String = secrets(0)(2).toString()
# MAGIC val pwd:String = secrets(0)(3).toString()
# MAGIC val bronzeStorageAccountName:String = secrets(0)(4).toString()
# MAGIC val bronzeStorageAccountKey:String = secrets(0)(5).toString()
# MAGIC val bronzeBasePath:String = secrets(0)(6).toString()
# MAGIC val silverStorageAccountName:String = secrets(0)(7).toString()
# MAGIC val silverStorageAccountKey:String = secrets(0)(8).toString()
# MAGIC val silverGeneralBasePath:String = secrets(0)(9).toString()
# MAGIC val silverProtectedBasePath:String = secrets(0)(10).toString()
# MAGIC val archiveBasePath:String = secrets(0)(11).toString()
# MAGIC val goldStorageAccountName:String = secrets(0)(12).toString()
# MAGIC val goldStorageAccountKey:String = secrets(0)(13).toString()
# MAGIC val goldGeneralBasePath:String = secrets(0)(14).toString()
# MAGIC val goldProtectedBasePath:String = secrets(0)(15).toString()
# MAGIC val sandboxStorageAccountName:String = secrets(0)(16).toString()
# MAGIC val sandboxStorageAccountKey:String = secrets(0)(17).toString()
# MAGIC val sandboxBasePath:String = secrets(0)(18).toString()
# MAGIC val transientStorageAccountName:String = secrets(0)(19).toString()
# MAGIC val transientStorageAccountKey:String = secrets(0)(20).toString()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Set Spark Conf

# COMMAND ----------

bronzeDfs = "fs.azure.account.key.{0}.dfs.core.windows.net".format(bronzeStorageAccountName)
spark.conf.set(
  bronzeDfs,
  bronzeStorageAccountKey)

silverDfs = "fs.azure.account.key.{0}.dfs.core.windows.net".format(silverStorageAccountName)
spark.conf.set(
  silverDfs,
  silverStorageAccountKey)

goldDfs = "fs.azure.account.key.{0}.dfs.core.windows.net".format(goldStorageAccountName)
spark.conf.set(
  goldDfs,
  goldStorageAccountKey)

sandboxDfs = "fs.azure.account.key.{0}.dfs.core.windows.net".format(sandboxStorageAccountName)
spark.conf.set(
  sandboxDfs,
  sandboxStorageAccountKey)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create Mount Points
# MAGIC We would prefer not to create mount points for storage connections, but some Spark connectors (such as the XML Connector) require mount points to bypass the authentication/authorization as part of the connection.
# MAGIC Be aware that anyone with access to the databricks workspace has full control over the files in the connected mount point.  To mitigate, we will assume the Transient Zone is empty with the exception of immediate files to process.

# COMMAND ----------

def createMountPointHelper (mountPoint, storageAccountName, containerName, storageAccountKey):
  try:
    files = dbutils.fs.ls(mountPoint)
  except:
    pass
    args = {
      "mountPoint": mountPoint,
      "storageAccountType": "blob",
      "storageAccountName": storageAccountName,
      "storageAccountContainerName": containerName,
      "auth": storageAccountKey,
      "storageAccountKey": storageAccountKey
    }
    notebook = "../Secrets/Create Storage Mount Point"
    try:
      dbutils.notebook.run(notebook, 120, args)
    except Exception as e:
      raise e

# COMMAND ----------

createMountPointHelper("/mnt/azuredatafactory", transientStorageAccountName, "azuredatafactory", transientStorageAccountKey)
createMountPointHelper("/mnt/staging", transientStorageAccountName, "staging", transientStorageAccountKey)
createMountPointHelper("/mnt/polybase", transientStorageAccountName, "polybase", transientStorageAccountKey)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Validate Connectivity to all Paths

# COMMAND ----------

def testConnectivity(path):
  try:
    result = dbutils.fs.ls(path)
  except Exception as e:
    err = {
      "sourceName" : "Import Data Lake Secrets: testConnectivity",
      "errorCode" : "000",
      "errorDescription" : e.__class__.__name__
    }
    error = json.dumps(err)
    print(error)
    raise(e)

# COMMAND ----------

testConnectivity('/mnt/azuredatafactory')
testConnectivity('/mnt/staging')
testConnectivity('/mnt/polybase')
testConnectivity(bronzeBasePath)
testConnectivity(silverGeneralBasePath)
testConnectivity(silverProtectedBasePath)
testConnectivity(goldGeneralBasePath)
testConnectivity(goldProtectedBasePath)
testConnectivity(sandboxBasePath)