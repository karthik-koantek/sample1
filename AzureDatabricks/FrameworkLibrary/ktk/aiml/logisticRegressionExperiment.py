from cProfile import label
from .classificationExperiment import ClassificationExperiment
import mlflow
import time
import tempfile 
import matplotlib.pyplot as plt 
from pyspark.ml import Pipeline, param
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import VectorAssembler, StringIndexer, OneHotEncoder
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.mllib.evaluation import BinaryClassificationMetrics
from datetime import datetime 
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('Databricks Framework').getOrCreate()

class LogisticRegressionExperiment(ClassificationExperiment):
    def __init__(self, snb, experimentName=""):
        super().__init__(snb, experimentName)
        if experimentName == "":
            self.experimentName = "Train Logistic Regression for Table {0}".format(snb.tableName.replace(".","_"))
        else:
            self.experimentName = experimentName

    def trainingRun(self, df, runName=""):
        if runName == "":
            self.runName = "{0}".format(self.experimentName)
        else:
            self.runName = runName
        mlflow.set_experiment(self.experimentNotebookPath)
        with mlflow.start_run(run_name=runName) as run:
            overallStartTime = time.time()
            print("Label: {0}".format(self.label))
            mlflow.log_param("Label", self.label)
            print("Continuous Columns: {0}".format(self.continuousColumns))
            mlflow.log_param("Continuous Columns", self.continuousColumns)
            encodedColumns = [c for c in list(set(df.columns)-set(self.continuousColumns)) if c != self.label]
            print("Encoded Columns: {0}".format(encodedColumns))
            mlflow.log_param("Encoded Columns", encodedColumns)

            startTime = time.time()
            labelindexer = []
            labelindexer.append(StringIndexer(inputCol=self.label, outputCol="label"))
            indexers = [StringIndexer(handleInvalid='skip', inputCol=column, outputCol="{0}_indexed".format(column)) for column in encodedColumns]
            print("Step 1: Indexers: {0}".format(indexers))
            print("Step 1: Label Indexer: {0}".format(labelindexer))
            mlflow.set_tag("Indexers", indexers)
            mlflow.set_tag("Label Indexer", labelindexer)
            mlflow.log_metric("Step Elapsed Time", self.getElapsedTime(startTime), step=1)

            startTime = time.time()
            encoders = [OneHotEncoder(dropLast=False,inputCol=indexer.getOutputCol(),outputCol="{0}_encoded".format(indexer.getOutputCol())) for indexer in indexers]
            print("Step 2: Encoders: {0}".format(encoders))
            mlflow.set_tag("Encoders", encoders)
            mlflow.log_metric("Step Elapsed Time", self.getElapsedTime(startTime), step=2)

            startTime = time.time()
            assembler = VectorAssembler(inputCols=[encoder.getOutputCol() for encoder in encoders] + self.continuousColumns, outputCol="features")
            print("Step 3: Assembler: {0}".format(assembler))
            mlflow.set_tag("Assember", assembler)
            mlflow.log_metric("Step Elapsed Time", self.getElapsedTime(startTime), step=3)

            startTime = time.time()
            logit_r = LogisticRegression(featuresCol="features", labelCol="label")
            print("Step 4: Model: {0}".format(logit_r))
            mlflow.set_tag("Model", logit_r)
            mlflow.log_metric("Step Elapsed Time", self.getElapsedTime(startTime), step=4)

            startTime = time.time()
            pipeline = Pipeline(stages = indexers + labelindexer + encoders + [assembler, logit_r])
            print("Step 5: Pipeline: {0}".format(pipeline))
            mlflow.set_tag("Pipeline", pipeline)
            mlflow.log_metric("Step Elapsed Time", self.getElapsedTime(startTime), step=5)

            startTime = time.time()
            mlflow.set_tag("Regularization Parameters", self.regularizationParameters)
            paramGridList = [float(p) for p in self.regularizationParameters.split(",")]
            print("Param Grid List: {0}".format(paramGridList))
            mlflow.log_param("Param Grid List", paramGridList)
            paramGrid = ParamGridBuilder().addGrid(logit_r.regParam, paramGridList).build()
            print("Step 6: Param Grid: {0}".format(paramGrid))
            mlflow.set_tag("Param Grid", paramGrid)
            mlflow.log_metric("Step Elapsed Time", self.getElapsedTime(startTime), step=6)

            startTime = time.time()
            mlflow.set_tag("Folds", self.folds)
            crossval = CrossValidator(estimator=pipeline,estimatorParamMaps=paramGrid,evaluator=BinaryClassificationEvaluator(labelCol="label",rawPredictionCol=self.predictionColumn),numFolds=int(self.folds))
            print("Step 7: Cross Validator: {0}".format(crossval))
            mlflow.set_tag("Cross Validator", crossval)
            mlflow.log_metric("Step Elapsed Time", self.getElapsedTime(startTime), step=7)

            startTime = time.time()
            trainTestSplitList = [float(t) for t in self.trainTestSplit.split(",")]
            mlflow.log_param("trainTestSplit", self.trainTestSplit)
            print("Step 8: Train/Test Split: {0}".format(self.trainTestSplit))
            (trainingData, testData) = df.randomSplit(trainTestSplitList)
            mlflow.log_metric("Training Data Rows", trainingData.count())
            mlflow.log_metric("Test Data Rows", testData.count())
            mlflow.log_metric("Step Elapsed Time", self.getElapsedTime(startTime), step=8)

            startTime = time.time()
            print("Step 9: Fit and Save Model")
            model = crossval.fit(trainingData)
            mlflow.spark.log_model(spark_model=model.bestModel, artifact_path="model-file", registered_model_name=self.experimentName)
            mlflow.log_metric("Step Elapsed Time", self.getElapsedTime(startTime), step=9)

            startTime = time.time()
            print("Step 10: Predictions")
            predictions = model.transform(testData)
            try:
                temp = tempfile.NamedTemporaryFile(prefix="predictions-", suffix=".csv")
                temp_name = temp.name
                predictionArtifactColumns = [c[0] for c in predictions.dtypes if c[1] != "vector"]
                predictionsCSV = predictions.select(predictionArtifactColumns).toPandas().to_csv(temp_name, index=False)
                mlflow.log_artifact(temp_name, "predictions.csv")
            finally:
                temp.close() # Delete the temp file

            mlflow.log_metric("Step Elapsed Time", self.getElapsedTime(startTime), step=10)

            startTime = time.time()
            print("Step 11: Evaluate")
            (accuracy, precision, recall, auc, f1_score) = self.evaluateClassificationMetrics(predictions, "label", self.predictionColumn)
            print("Accuracy: {0}".format(accuracy))
            mlflow.log_metric("Accuracy", accuracy)
            print("Precision: {0}".format(precision))
            mlflow.log_metric("Precision", precision)
            print("Recall: {0}".format(recall))
            mlflow.log_metric("Recall", recall)
            print("AUC: {0}".format(auc))
            mlflow.log_metric("AUC", auc)
            print("F1_Score: {0}".format(f1_score))
            mlflow.log_metric("F1_Score", f1_score)
            bestPipeline = model.bestModel
            print("Best Pipeline: {0}".format(bestPipeline))
            p = {
                "Assembler": assembler,
                "Cross Validator": crossval,
                "Encoders": encoders,
                "Folds": self.folds,
                "Indexers": indexers,
                "Label Indexer": labelindexer,
                "Model": model,
                "Param Grid": paramGrid,
                "Pipeline": pipeline,
                "Regularization Parameters": self.regularizationParameters,
                "experimentNotebookPath": self.experimentNotebookPath
            }
            mlflow.set_tags(p)
            mlflow.log_metric("Step Elapsed Time", self.getElapsedTime(startTime), step=11)

            #startTime = time.time()
            #print("Step 12: Generate Artifacts")

            mlflow.log_metric("Overall Elapsed Time", self.getElapsedTime(overallStartTime))

            return run.info.run_uuid, predictions
