# Databricks notebook source
# MAGIC %md
# MAGIC # ML Functions

# COMMAND ----------

def getMLFlowExperiment(experimentId = "", experimentNotebookPath = ""):
  if experimentId == "":
    if experimentNotebookPath == "":
      experiment = None
    else:
      experiment = mlflow.get_experiment_by_name(experimentNotebookPath)
  else:
    experiment = mlflow.get_experiment(experimentId)
  return experiment

# COMMAND ----------

def selectModel(experiment, metricClause, modelName="model-file"):
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
  return dfList[-1]

# COMMAND ----------

def excludeColumns(df, excludedColumns):
  columns = set(df.columns) - set(excludedColumns.split(","))
  return df.select(*columns)

# COMMAND ----------

def refreshTable(tableName):
  try:
    spark.sql("REFRESH TABLE {0}".format(tableName))
    df = spark.table(tableName)
    return True
  except Exception as e:
    return False

# COMMAND ----------

def getContinuousColumns(df):
  numericColumns = [c[0] for c in df.dtypes if c[1] in ["double", "float"]]
  return numericColumns

# COMMAND ----------

def evaluateRegressionMetrics(predictionDF, labelCol="target", predictionCol="prediction"):
    evaluatorR2 = RegressionEvaluator(labelCol=labelCol
                                   ,predictionCol=predictionCol
                                   ,metricName="r2")

    evaluatorRMSE = RegressionEvaluator(labelCol=labelCol
                                   ,predictionCol=predictionCol
                                   ,metricName="rmse")

    evaluatorMAE = RegressionEvaluator(labelCol=labelCol
                                   ,predictionCol=predictionCol
                                   ,metricName="mae")

    r2 = evaluatorR2.evaluate(predictionDF)
    rmse = evaluatorRMSE.evaluate(predictionDF)
    mae = evaluatorMAE.evaluate(predictionDF)
    return r2, rmse, mae

# COMMAND ----------

def getElapsedTime(startTime):
  endTime = time.time()
  elapsedTime = endTime - startTime
  return elapsedTime

# COMMAND ----------

def createScatterPlot(df, label, predictionColumn, experimentName):
  pdPredictions = df.select(label, predictionColumn).toPandas()
  fig, ax = plt.subplots()
  sns.scatterplot(data=pdPredictions, x=label, y=predictionColumn)
  plt.xlabel("Label")
  plt.ylabel("Prediction")
  plt.title("Scatter Plot")
  temp = tempfile.NamedTemporaryFile(prefix="{0}_scatter".format(experimentName), suffix=".png")
  fig.savefig(temp)
  display(fig)
  return temp

# COMMAND ----------

def createResidualPlot(df, label, predictionColumn, experimentName):
  pdPredictions = df.select(label, predictionColumn).toPandas()
  fig, ax = plt.subplots()
  sns.residplot(x=pdPredictions[label], y=pdPredictions[predictionColumn], lowess=True)
  plt.xlabel("Predictor")
  plt.ylabel("Response")
  plt.title("Residual Plot")
  temp = tempfile.NamedTemporaryFile(prefix="{0}_residual".format(experimentName), suffix=".png")
  fig.savefig(temp)
  display(fig)
  return temp
