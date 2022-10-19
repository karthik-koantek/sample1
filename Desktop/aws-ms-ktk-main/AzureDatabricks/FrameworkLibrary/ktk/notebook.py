from datetime import datetime
import pyodbc
from pyspark.sql import SparkSession
from pyspark.dbutils import DBUtils
from pyspark.sql.functions import col, create_map, lit
from itertools import chain
import json
spark = SparkSession.builder.appName('DatabricksFramework').getOrCreate()

class Notebook():
  def __init__(self):
    self.setContext()
    self.setFrameworkDB("internal")
    self.setDataLakeStorage("internal")
    self.LogMethod = 3 # 0 = None, 1 = Azure SQL, 2 = DB, 3 = Both

  def get_dbutils(self):
    from pyspark.dbutils import DBUtils
    return DBUtils(spark)

  def setContext(self):
    dbutils = self.get_dbutils()
    try:
      exec("self.context = dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson()")
    except Exception as e:
      print(e.__class__.__name__)
      print(e)

  def setFromWidget(self, widgetName):
    dbutils = self.get_dbutils()
    try:
      exec("self.{0} = dbutils.widgets.get('{0}')".format(widgetName))
    except Exception as e:
      if e.__class__.__name__ == "Py4JJavaError":
        #Most likely Widget not found, fail gracefully and coalesce to None
        exec("self.{0} = None".format(widgetName))
      else:
        print(e.__class__.__name__)
        print(e)

  def setFromSecret(self, secretScope, secretKey, attributeName):
    dbutils = self.get_dbutils()
    try:
      setattr(self, attributeName, dbutils.secrets.get(scope=secretScope, key=secretKey))
    except Exception as e:
      print(e.__class__.__name__)
      print(e)
      exec("self.{0} = None".format(attributeName))

  def setDateToProcess(self):
    self.setFromWidget("dateToProcess")
    if self.dateToProcess == "" or self.dateToProcess is None:
      self.dateToProcess = datetime.utcnow().strftime('%Y/%m/%d')
    elif self.dateToProcess == "-1":
      self.dateToProcess = ""
    else:
      self.dateToProcess = self.dateToProcess

  def setFrameworkDB(self, secretScope):
    self.setFromSecret(secretScope, "FrameworkDBServerName", "FrameworkDBServerName")
    self.setFromSecret(secretScope, "FrameworkDBName", "FrameworkDBName")
    self.setFromSecret(secretScope, "FrameworkDBAdministratorLogin", "FrameworkDBAdministratorLogin")
    self.setFromSecret(secretScope, "FrameworkDBAdministratorPwd", "FrameworkDBAdministratorPwd")

  def setDataLakeStorage(self, secretScope):
    #Transient Zone
    self.setFromSecret(secretScope, "TransientStorageAccountName", "TransientStorageAccountName")
    self.setFromSecret(secretScope, "TransientStorageAccountKey", "TransientStorageAccountKey")
    #Bronze Zone
    self.setFromSecret(secretScope, "BronzeStorageAccountName", "BronzeStorageAccountName")
    self.setFromSecret(secretScope, "BronzeStorageAccountKey", "BronzeStorageAccountKey")
    #silver/Gold zone
    self.setFromSecret(secretScope, "SilverGoldStorageAccountName", "SilverGoldStorageAccountName")
    self.setFromSecret(secretScope, "SilverGoldStorageAccountKey", "SilverGoldStorageAccountKey")
    #sandbox zone
    self.setFromSecret(secretScope, "SandboxStorageAccountName", "SandboxStorageAccountName")
    self.setFromSecret(secretScope, "SandboxStorageAccountKey", "SandboxStorageAccountKey")
    #paths
    self.BronzeBasePath = "abfss://raw@{0}.dfs.core.windows.net".format(self.BronzeStorageAccountName)
    self.SilverGeneralBasePath = "abfss://silvergeneral@{0}.dfs.core.windows.net".format(self.SilverGoldStorageAccountName)
    self.SilverProtectedBasePath = "abfss://silverprotected@{0}.dfs.core.windows.net".format(self.SilverGoldStorageAccountName)
    self.ArchiveBasePath = "abfss://archive@{0}.dfs.core.windows.net".format(self.SilverGoldStorageAccountName)
    self.GoldGeneralBasePath = "abfss://goldgeneral@{0}.dfs.core.windows.net".format(self.SilverGoldStorageAccountName)
    self.GoldProtectedBasePath = "abfss://goldprotected@{0}.dfs.core.windows.net".format(self.SilverGoldStorageAccountName)
    self.SandboxBasePath = "abfss://defaultsandbox@{0}.dfs.core.windows.net".format(self.SandboxStorageAccountName)
    self.BronzeDfsPath = "fs.azure.account.key.{0}.dfs.core.windows.net".format(self.BronzeStorageAccountName)
    self.SilverDfsPath = "fs.azure.account.key.{0}.dfs.core.windows.net".format(self.SilverGoldStorageAccountName)
    self.GoldDfsPath = "fs.azure.account.key.{0}.dfs.core.windows.net".format(self.SilverGoldStorageAccountName)
    self.SandboxDfsPath = "fs.azure.account.key.{0}.dfs.core.windows.net".format(self.SandboxStorageAccountName)
    #spark.conf
    spark.conf.set(self.BronzeDfsPath,self.BronzeStorageAccountKey)
    spark.conf.set(self.SilverDfsPath,self.SilverGoldStorageAccountKey)
    spark.conf.set(self.GoldDfsPath,self.SilverGoldStorageAccountKey)
    spark.conf.set(self.SandboxDfsPath,self.SandboxStorageAccountKey)

  def displayAttributes(self, filterNones=True, excludeContext=True):
    if filterNones == True:
      if excludeContext == False:
        return {k: v for k,v in self.__dict__.items() if v is not None}
      else:
        return {k: v for k,v in self.__dict__.items() if v is not None and k != "context"}
    else:
      return self.__dict__

  def pyodbcConnectionString(self):
    connection = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+self.FrameworkDBServerName+';DATABASE='+self.FrameworkDBName+';UID='+self.FrameworkDBAdministratorLogin+';PWD='+ self.FrameworkDBAdministratorPwd)
    #self.pyodbcConnectionString = connection
    return connection

  def jdbcConnectionString(self):
    port = 1433
    url = "jdbc:sqlserver://{0}:{1};database={2}".format(self.FrameworkDBServerName, port, self.FrameworkDBName)
    properties = {
      "user" : self.FrameworkDBAdministratorLogin,
      "password" : self.FrameworkDBAdministratorPwd,
      "driver" : "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    }
    self.jdbcConnectionStringUrl = url
    self.jdbcConnectionStringProperties = properties
    return url, properties

  def pyodbcStoredProcedureResults(self, sp):
    try:
      connection = self.pyodbcConnectionString()
      cursor = connection.cursor()
      connection.autocommit = True
      cursor.execute(sp)
      results = cursor.fetchall()
      connection.close()
      return results
    except Exception as e:
      print("Failed to call stored procedure: {0}".format(sp))

  def pyodbcStoredProcedure(self, sp):
    try:
      connection = self.pyodbcConnectionString()
      connection.autocommit = True
      connection.execute(sp)
      connection.close()
    except Exception as e:
      print("Failed to call stored procedure: {0}".format(sp))
      raise e

  def runWithRetry(self, notebook, timeout, args = {}, max_retries = 0):
    dbutils = self.get_dbutils()
    num_retries = 0
    while True:
      try:
        print("\n{0}:running notebook {1} with args {2}\n".format(self.getCurrentTimestamp(), notebook, args))
        return dbutils.notebook.run(notebook, int(timeout), args)
      except Exception as e:
        if num_retries >= max_retries:
          raise e
          print("skipping notebook {0}".format(notebook))
          break
        else:
          print ("Retrying error", e)
          num_retries += 1

  def create_map_from_dictionary(self, dictionary_or_string):
    if isinstance(dictionary_or_string, str):
      dictionary_or_string = json.loads(dictionary_or_string)
    return create_map([lit(str(x)) for x in chain(*dictionary_or_string.items())])