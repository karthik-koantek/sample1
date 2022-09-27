
from .experiment import Experiment
from pyspark.ml.evaluation import MulticlassClassificationEvaluator, BinaryClassificationEvaluator
from pyspark.mllib.evaluation import BinaryClassificationMetrics
import matplotlib.pyplot as plt
import tempfile 

class ClassificationExperiment(Experiment):
  def __init__(self, snb, experimentName=""):
    super().__init__(snb, experimentName)
    self.label = snb.label
    self.continuousColumns = snb.continuousColumns
    self.regularizationParameters = snb.regularizationParameters
    self.predictionColumn = snb.predictionColumn
    self.folds = snb.folds
    self.trainTestSplit = snb.trainTestSplit

  def evaluateClassificationMetrics(self, predictionDF, labelCol="target", predictionCol="prediction"):
    evaluatorAccuracy = MulticlassClassificationEvaluator(labelCol=labelCol,
                                                          predictionCol=predictionCol,
                                                          metricName='accuracy'
                                                          )

    evaluatorPrecision = MulticlassClassificationEvaluator(labelCol=labelCol,
                                                          predictionCol=predictionCol,
                                                          metricName='weightedPrecision'
                                                          )

    evaluatorRecall = MulticlassClassificationEvaluator(labelCol=labelCol,
                                                          predictionCol=predictionCol,
                                                          metricName='weightedRecall'
                                                          )

    evaluatorAUC = BinaryClassificationEvaluator(labelCol=labelCol,
                                                  rawPredictionCol=predictionCol,
                                                  metricName='areaUnderROC'
                                                  )

    evaluatorF1 = MulticlassClassificationEvaluator(labelCol=labelCol,
                                                    predictionCol=predictionCol,
                                                    metricName='f1'
                                                    )


    accuracy = evaluatorAccuracy.evaluate(predictionDF)
    precision = evaluatorPrecision.evaluate(predictionDF)
    recall = evaluatorRecall.evaluate(predictionDF)
    auc = evaluatorAUC.evaluate(predictionDF)
    f1_score = evaluatorF1.evaluate(predictionDF)

    return accuracy, precision, recall, auc, f1_score 

