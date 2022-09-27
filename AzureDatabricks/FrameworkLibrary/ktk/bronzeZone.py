from .singleResponsibility import SingleResponsibilityNotebook

class BronzeZoneNotebook(SingleResponsibilityNotebook):
  def __init__(self):
    super().__init__()
    self.setFromWidget("externalDataPath")
    self.setFromWidget("fileExtension")
    self.setFromWidget("multiLine")
    self.setFromWidget("delimiter")
    self.setFromWidget("header")
    self.setFromWidget("numPartitions")
    self.setFromWidget("destination")
    self.setFromWidget("inferSchemaSampleBytes")
    self.setFromWidget("inferSchemaSampleFiles")
    self.setFromWidget("trigger")
    self.setFromWidget("maxEventsPerTrigger")
    self.setFromWidget("interval")
    self.setFromWidget("stopStreamsGracePeriodSeconds")
    self.setFromWidget("stopStreamsCheckInterval")
    self.setFromWidget("offset")
    self.setFromWidget("interval")
    self.setFromWidget("stopSeconds")
    self.setFromWidget("eventHubStartingPosition")
    self.setFromWidget("destination")
    self.setFromWidget("outputTableName")
    self.setFullExternalDataPath()
    self.setBronzeCheckpointPath()

  def setFullExternalDataPath(self):
    if self.externalSystem != "internal":
      self.setFromSecret("externalStorageAccountName", "StorageAccountName")
      self.setFromSecret("externalStorageAccountKey", "StorageAccountKey")
      self.setFromSecret("externalStorageAccountContainerName", "StorageContainerName")
      self.setFromSecret("externalStorageAccountBasePath", "StorageAccountBasePath")
      #spark.conf.set(self.externalStorageAccountName,self.externalStorageAccountKey)
    else:
      self.externalStorageAccountName = ""
      self.externalStorageAccountKey = ""
      self.externalStorageAccountBasePath = ""
    self.fullExternalDataPath = "{0}/{1}".format(self.externalStorageAccountBasePath, self.externalDataPath)

  def setBronzeCheckpointPath(self):
    self.bronzeCheckpointPath = "{0}/checkpoint/{1}/{2}/{3}/".format(self.bronzeBasePath, self.externalSystem, self.schemaName, self.tableName)

  def setSilverCheckpointPath(self):
    self.silverCheckpointPath = "{0}/checkpoint/{1}/{2}/{3}/".format(self.basePath, self.externalSystem, self.schemaName, self.tableName)