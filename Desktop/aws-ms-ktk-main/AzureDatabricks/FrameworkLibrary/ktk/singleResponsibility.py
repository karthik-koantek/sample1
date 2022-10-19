from .notebook import Notebook
import uuid

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
    query = """EXEC dbo.LogNotebookStart
    @NotebookLogGuid='{0}'
    ,@StepLogGuid='{1}'
    ,@StepKey={2}
    ,@Parameters='{3}'
    ,@Context='{4}';
    """.format(self.notebookLogGuid, self.stepLogGuid, int(self.stepKey), parameters.replace("'", "''"), self.context.replace("'", "''"))
    self.pyodbcStoredProcedure(query)

  def log_notebook_error (self, error):
    query = """EXEC dbo.LogNotebookError
    @NotebookLogGuid='{0}'
    ,@Error='{1}';
    """.format(self.notebookLogGuid, error)
    self.pyodbcStoredProcedure(query)

  def log_notebook_end (self, rows=0):
    query = """EXEC dbo.LogNotebookEnd
    @NotebookLogGuid='{0}'
    ,@RowsAffected={1}
    """.format(self.notebookLogGuid, rows)
    self.pyodbcStoredProcedure(query)

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