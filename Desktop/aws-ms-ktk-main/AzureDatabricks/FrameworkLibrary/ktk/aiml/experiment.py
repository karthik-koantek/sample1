'''
    EXPERIMENT.PY
'''
import mlflow 

import time
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('Databricks Framework').getOrCreate()
    
class Experiment():
  def __init__(self, snb, experimentName=""):
    self.tableName = snb.tableName
    self.excludedColumns = snb.excludedColumns
    self.experimentNotebookPath = snb.experimentNotebookPath
    
  def displayAttributes(self, filterNones=True, excludeContext=True):
    if filterNones == True:
      if excludeContext == False:
        return {k: v for k,v in self.__dict__.items() if v is not None}
      else:
        return {k: v for k,v in self.__dict__.items() if v is not None and k != "context"}
    else:
      return self.__dict__

  def displayExperimentDetails(self, experiment):
    print("Artifact Location: {0}".format(experiment.artifact_location))
    print("Lifecycle Stage: {0}".format(experiment.lifecycle_stage))
    print("Experiment Id: {0}".format(experiment.experiment_id))
    print("Experiment Name: {0}".format(experiment.name))

  def getMLFlowExperiment(self, experimentId = "", experimentNotebookPath = ""):
    if experimentId == "":
        if experimentNotebookPath == "":
            experiment = None
        else:
            experiment = mlflow.get_experiment_by_name(experimentNotebookPath)
    else:
        experiment = mlflow.get_experiment(experimentId)
    return experiment

  def selectModel(self, experiment, metricClause, modelName="model-file"):
    dfList = []
    dfList.append(spark.read.format("mlflow-experiment").load(experiment.experiment_id))
    dfList[-1].createOrReplaceTempView("runs")
    if metricClause == "":
        orderBy = "{0}".format(metricClause)
    else:
        orderBy = "end_time DESC"
    sql = "SELECT experiment_id, run_id, end_time, artifact_uri || '/{0}' AS model_uri FROM runs WHERE status == 'FINISHED' ORDER BY {1} LIMIT 1".format(modelName, orderBy)
    print(sql)
    dfList.append(spark.sql(sql))
    return dfList[-1], dfList[-1].first()[0], dfList[-1].first()[1], dfList[-1].first()[3]
      
  def excludeColumns(self, df, excludedColumns):
    columns = set(df.columns) - set(excludedColumns.split(","))
    return df.select(*columns)

  def refreshTable(self, tableName):
    try:
        spark.sql("REFRESH TABLE {0}".format(tableName))
        df = spark.table(tableName)
        return True
    except Exception as e:
            return False

  def getContinuousColumns(self, df):
    numericColumns = [c[0] for c in df.dtypes if c[1] in ["double", "float"]]
    return numericColumns

  def getElapsedTime(self, startTime):
    endTime = time.time()
    elapsedTime = endTime - startTime
    return elapsedTime

  