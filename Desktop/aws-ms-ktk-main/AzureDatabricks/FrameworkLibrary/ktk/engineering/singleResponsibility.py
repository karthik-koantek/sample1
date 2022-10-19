from .notebook import Notebook
import uuid
import datetime 
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from pyspark.dbutils import DBUtils
spark = SparkSession.builder.appName('DatabricksFramework').getOrCreate()

class SingleResponsibilityNotebook(Notebook):
  def __init__(self, widgets, secrets):
    super().__init__()
    self.notebookLogGuid = str(uuid.uuid4())
    self.setFromWidgetList(widgets)
    self.setFromSecretList(secrets)
    self.setBasePath()
    self.setBronzeDataPath()
    self.setSilverDataPath()
    self.setBadRecordsPath()
    self.setfullExternalDataPath()

  def setFromWidgetList(self, widgets):
    for widget in widgets:
      self.setFromWidget(widget)

  def setFromSecretList(self, secrets):
    for secret in secrets:
      self.setFromSecret(self.externalSystem, secret, secret)

  def setBasePath(self):
    self.setFromWidget("destination")
    if not self.destination is None:
      if self.destination == "silverprotected":
        self.basePath = self.SilverProtectedBasePath
      elif self.destination == "silvergeneral":
        self.basePath = self.SilverGeneralBasePath
      elif self.destination == "goldgeneral":
        self.basePath = self.GoldGeneralBasePath
      elif self.destination == "goldprotected":
        self.basePath = self.GoldProtectedBasePath
      elif self.destination == "sandbox":
        self.basePath = self.SandboxBasePath
      elif self.destination == "archive":
        self.bsePath = self.ArchiveBasePath
      else:
        self.basePath = self.SilverGeneralBasePath
    else:
        self.basePath = None

  def setBronzeDataPath(self):
    try:
      self.bronzeDataPath = "{0}/{1}/{2}/{3}/{4}".format(self.BronzeBasePath, self.externalSystem, self.schemaName, self.tableName, self.dateToProcess)
    except Exception as e:
      self.bronzeDataPath = None

  def setSilverDataPath(self):
    try:
      self.silverDataPath = "{0}/{1}/{2}/{3}/".format(self.basePath, self.externalSystem, self.schemaName, self.tableName)
    except Exception as e:
      self.silverDataPath = None

  def setBadRecordsPath(self):
    try:
      self.badRecordsPath = "{0}/badRecords/{1}/{2}/{3}/".format(self.BronzeBasePath, self.externalSystem, self.schemaName, self.tableName)
    except Exception as e:
      self.badRecordsPath = None

  def setfullExternalDataPath(self):
    try:
      self.fullExternalDataPath = "{0}/{1}".format(self.BasePath, self.externalDataPath)
    except Exception as e:
      self.fullExternalDataPath = None

  def log_notebook_start (self, parameters):
    if self.LogMethod in (1, 3):
      query = """EXEC dbo.LogNotebookStart
      @NotebookLogGuid='{0}'
      ,@StepLogGuid='{1}'
      ,@StepKey={2}
      ,@Parameters='{3}'
      ,@Context='{4}';
      """.format(self.notebookLogGuid, self.stepLogGuid, self.stepKey, parameters.replace("'", "''"), self.context.replace("'", "''"))
      self.pyodbcStoredProcedure(query)

    if self.LogMethod in (2, 3):
      startDateTime = datetime.datetime.now()
      parametersMap = self.create_map_from_dictionary(parameters)
      dfList = []
      dfList.append(spark.createDataFrame([(self.notebookLogGuid)],"STRING"))
      dfList.append(dfList[-1].withColumnRenamed("value","notebookLogGuid"))
      dfList.append(dfList[-1].withColumn("stepLogGuid", lit(self.stepLogGuid)))
      dfList.append(dfList[-1].withColumn("stepKey", lit(self.stepKey)))
      dfList.append(dfList[-1].withColumn("startDateTime", lit(startDateTime)))
      dfList.append(dfList[-1].withColumn("Parameters", parametersMap))
      dfList.append(dfList[-1].withColumn("Context", lit(self.context.replace("'","''"))))
      dfList[-1].createOrReplaceTempView("startDF")
      query = """
      INSERT INTO silverprotected.notebooklog (NotebookLogGuid, StepLogGuid, StepKey, StartDateTime, EndDateTime, LogStatusKey, RowsAffected, Parameters, Context, Error)
      SELECT notebookLogGuid, stepLogGuid, stepKey, startDateTime, NULL, 1, NULL, Parameters, Context, NULL FROM startDF
      """
      spark.sql(query)

  def log_notebook_error (self, error):
    if self.LogMethod in (1,3):
      query = """EXEC dbo.LogNotebookError
      @NotebookLogGuid='{0}'
      ,@Error='{1}';
      """.format(self.notebookLogGuid, error)
      self.pyodbcStoredProcedure(query)

    if self.LogMethod in (2,3):
      errorMap = self.create_map_from_dictionary(error)
      endDateTime = datetime.datetime.now()
      dfList = []
      dfList.append(spark.createDataFrame([(self.notebookLogGuid)], "STRING"))
      dfList.append(dfList[-1].withColumnRenamed("value","notebookLogGuid"))
      dfList.append(dfList[-1].withColumn("Error", errorMap))
      dfList.append(dfList[-1].withColumn("endDateTime", lit(endDateTime)))
      dfList[-1].createOrReplaceTempView("errorDF")

      query = """
      INSERT INTO silverprotected.notebooklog (NotebookLogGuid, StepLogGuid, StepKey, StartDateTime, EndDateTime, LogStatusKey, RowsAffected, Parameters, Context, Error)
      SELECT notebookLogGuid, NULL, NULL, NULL, endDateTime, 3, NULL, NULL, NULL, Error FROM errorDF
      """
      spark.sql(query)

  def log_notebook_end (self, rows=0):
    if self.LogMethod in (1,3):
      query = """EXEC dbo.LogNotebookEnd
      @NotebookLogGuid='{0}'
      ,@RowsAffected={1}
      """.format(self.notebookLogGuid, rows)
      self.pyodbcStoredProcedure(query)

    if self.LogMethod in (2,3):
      endDateTime = datetime.datetime.now()
      dfList = []
      dfList.append(spark.createDataFrame([(self.notebookLogGuid)], "STRING"))
      dfList.append(dfList[-1].withColumnRenamed("value","notebookLogGuid"))
      dfList.append(dfList[-1].withColumn("endDateTime", lit(endDateTime)))
      dfList.append(dfList[-1].withColumn("Rows", lit(rows)))
      dfList[-1].createOrReplaceTempView("endDF")
      query = """
      INSERT INTO silverprotected.notebooklog (NotebookLogGuid, StepLogGuid, StepKey, StartDateTime, EndDateTime, LogStatusKey, RowsAffected, Parameters, Context, Error)
      SELECT notebookLogGuid, NULL, NULL, NULL, endDateTime, 2, Rows, NULL, NULL, NULL FROM endDF
      """
      spark.sql(query)

  def log_validationlog (self, validationLogGuid, stepLogGuid, validationKey, validationStatus, error, parameters):
    query = """EXEC dbo.LogValidationLog
      @ValidationLogGuid='{0}'
      ,@StepLogGuid='{1}'
      ,@ValidationKey={2}
      ,@ValidationStatus='{3}'
      ,@Error='{4}'
      ,@Parameters='{5}';
    """.format(validationLogGuid, stepLogGuid, validationKey, validationStatus, error, parameters)
    self.pyodbcStoredProcedure(query)

  def mergeAttributes(self, p):
    for key in p:
      setattr(self, key, p.get(key))
    return {**self.displayAttributes(), **p}