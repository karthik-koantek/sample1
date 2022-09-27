
from .experiment import Experiment
from pyspark.ml.evaluation import RegressionEvaluator
import matplotlib.pyplot as plt
import seaborn as sns
import tempfile

class RegressionExperiment(Experiment):
  def __init__(self, snb, experimentName=""):
    super().__init__(snb, experimentName)
    self.label = snb.label
    self.continuousColumns = snb.continuousColumns
    self.regularizationParameters = snb.regularizationParameters
    self.predictionColumn = snb.predictionColumn
    self.folds = snb.folds
    self.trainTestSplit = snb.trainTestSplit

  def evaluateRegressionMetrics(self, predictionDF, labelCol="target", predictionCol="prediction"):
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

  def createScatterPlot(self, df, label, predictionColumn, experimentName):
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

  def createResidualPlot(self, df, label, predictionColumn, experimentName):
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