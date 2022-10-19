from tokenize import Single
from .singleResponsibility import SingleResponsibilityNotebook
from pyspark.sql import SparkSession
#from pyspark.sql.functions import col, lit, explode
from pyspark.dbutils import DBUtils
spark = SparkSession.builder.appName('DatabricksFramework').getOrCreate()
import great_expectations as ge

class DataQualityRulesNotebook(SingleResponsibilityNotebook):
    def __init__(self, widgets, secrets):
        super().__init__(widgets, secrets)
        self.createDataQualityRulesSchema()

    def convertValidationResultsToDataFrame(self, validation_results):
        dbutils = self.get_dbutils()
        tempDir = "/tmp/dqtemp/{0}".format(self.tableName)
        tempFile = "{0}/validationresults.json".format(tempDir)
        dbutils.fs.mkdirs(tempDir)
        dbutils.fs.put(tempFile, str(validation_results), True)
        df = spark.read.option("MultiLine", True).json(tempFile)
        self.ValidationResultsDF = df
        return df

    def createDataQualityRulesSchema(self):
        goldDataPath = "{0}/{1}".format(self.GoldProtectedBasePath, "dataqualityrulesengine")

        dataqualityrulesql = """
        CREATE TABLE IF NOT EXISTS goldprotected.DataQualityRule
        (
         pk STRING NOT NULL
        ,tableId INT NOT NULL
        ,fullyQualifiedTableName STRING NOT NULL
        ,rules ARRAY<STRING>
        ,isActive BOOLEAN NOT NULL
        ,effectiveStartDate TIMESTAMP
        ,effectiveEndDate TIMESTAMP
        )
        USING delta
        LOCATION '{0}/dataqualityrule'
        """.format(goldDataPath)
        spark.sql(dataqualityrulesql)

        vactivedataqualityrulesql = """
        CREATE VIEW IF NOT EXISTS goldprotected.vActiveDataQualityRule
        AS
        SELECT pk, tableId, fullyQualifiedTableName, isActive, effectiveStartDate, effectiveEndDate, rules
        FROM goldprotected.dataqualityrule
        WHERE isActive = 1
        """
        spark.sql(vactivedataqualityrulesql)

        dataqualityvalidationresultsql = """
        CREATE TABLE IF NOT EXISTS goldprotected.DataQualityValidationResult
        (
        stepLogGuid STRING NOT NULL
        ,batchId STRING NOT NULL
        ,expectationSuiteName STRING NOT NULL
        ,expectationsVersion STRING NOT NULL
        ,validationTime STRING NOT NULL
        ,runName STRING
        ,runTime STRING NOT NULL
        ,evaluatedExpectations LONG NOT NULL
        ,successPercent DOUBLE NOT NULL
        ,successfulExpectations LONG NOT NULL
        ,unsuccessfulExpectations LONG NOT NULL
        )
        USING delta
        LOCATION '{0}/dataqualityvalidationresult'
        """.format(goldDataPath)
        spark.sql(dataqualityvalidationresultsql)

        dataqualityvalidationresultdetailsql = """
        CREATE TABLE IF NOT EXISTS goldprotected.DataQualityValidationResultDetail
        (
         batchId STRING NOT NULL
        ,success BOOLEAN NOT NULL
        ,expectationType STRING NOT NULL
        ,exceptionMessage STRING
        ,exceptionTraceback STRING
        ,raisedException BOOLEAN
        ,kwargsColumn STRING
        ,kwargsColumnList ARRAY<STRING>
        ,kwargsMaxValue LONG
        ,kwargsMinValue LONG
        ,kwargsMostly DOUBLE
        ,kwargsRegex STRING
        ,kwargsResultFormat STRING
        ,kwargsTypeList ARRAY<STRING>
        ,kwargsValueSet ARRAY<STRING>
        ,resultMissingPercent DOUBLE
        ,resultObservedValue STRING
        ,resultPartialUnexpectedList ARRAY<STRING>
        ,resultUnexpectedCount LONG
        ,resultUnexpectedPercent DOUBLE
        ,resultUnexpectedPercentNonMissing DOUBLE
        ,resultUnexpectedPercentDouble DOUBLE
        )
        USING delta
        LOCATION '{0}/dataqualityvalidationresultdetail'
        """.format(goldDataPath)
        spark.sql(dataqualityvalidationresultdetailsql)

        vdataqualityvalidationresultdetailsql = """
        CREATE VIEW IF NOT EXISTS goldprotected.vDataQualityValidationResultDetail
        AS
        SELECT
         batchId
        ,success
        ,expectationType
        ,exceptionMessage
        ,exceptionTraceback
        ,raisedException
        ,kwargsColumn
        ,CAST(kwargsColumnList AS STRING) AS `kwargsColumnList`
        ,kwargsMaxValue
        ,kwargsMinValue
        ,kwargsMostly
        ,kwargsRegex
        ,kwargsResultFormat
        ,CAST(kwargsTypeList AS STRING) AS `kwargsTypeList`
        ,CAST(kwargsValueSet AS STRING) AS `kwargsValueSet`
        ,resultMissingPercent
        ,resultObservedValue
        ,CAST(resultPartialUnexpectedList AS STRING) AS `resultPartialUnexpectedList`
        ,resultUnexpectedCount
        ,resultUnexpectedPercent
        ,resultUnexpectedPercentNonMissing
        ,resultUnexpectedPercentDouble
        FROM goldprotected.DataQualityValidationResultDetail
        """
        spark.sql(vdataqualityvalidationresultdetailsql)

    def flattenGreatExpectationsValidationResults(self):
        from pyspark.sql.functions import lit, col, explode, split
        from pyspark.sql.types import LongType, StringType, ArrayType

        FlattenedValidationResultsDF = self.ValidationResultsDF.select(
            lit(self.stepLogGuid).alias("stepLogGuid"),
            col("meta.batch_kwargs.ge_batch_id").alias("batchId"),
            col("meta.expectation_suite_name").alias("expectationSuiteName"),
            col("meta.great_expectations_version").alias("expectationsVersion"),
            col("meta.validation_time").alias("validationTime"),
            col("meta.run_id.run_name").alias("runName"),
            col("meta.run_id.run_time").alias("runTime"),
            col("statistics.evaluated_expectations").alias("evaluatedExpectations"),
            col("statistics.success_percent").alias("successPercent"),
            col("statistics.successful_expectations").alias("successfulExpectations"),
            col("statistics.unsuccessful_expectations").alias("unsuccessfulExpectations"))

        FlattenedValidationResultsDetailDF = []
        FlattenedValidationResultsDetailDF.append(self.ValidationResultsDF.select(col("meta.batch_kwargs.ge_batch_id").alias("batchId"),explode(col("results")).alias("result")))
        FlattenedValidationResultsDetailDF.append(FlattenedValidationResultsDetailDF[-1].select(
            col("batchId")
            ,col("result.success").alias("success")
            ,col("result.expectation_config.expectation_type").alias("expectationType")
            ,col("result.exception_info.exception_message").alias("exceptionMessage")
            ,col("result.exception_info.exception_traceback").alias("exceptionTraceback")
            ,col("result.exception_info.raised_exception").alias("raisedException")
            ,col("result.expectation_config.kwargs.column").alias("kwargsColumn")
            ,col("result.expectation_config.kwargs.max_value").cast(LongType()).alias("kwargsMaxValue")
            ,col("result.expectation_config.kwargs.min_value").alias("kwargsMinValue")
            ,col("result.expectation_config.kwargs.mostly").alias("kwargsMostly")
            ,col("result.expectation_config.kwargs.regex").alias("kwargsRegex")
            ,col("result.expectation_config.kwargs.result_format").alias("kwargsResultFormat")
            ,col("result.expectation_config.kwargs.type_list").alias("kwargsTypeList")
            ,col("result.result.missing_percent").alias("resultMissingPercent")
            ,col("result.result.observed_value").alias("resultObservedValue")
            ,col("result.result.partial_unexpected_list").cast(ArrayType(StringType())).alias("resultPartialUnexpectedList")
            ,col("result.result.unexpected_count").alias("resultUnexpectedCount")
            ,col("result.result.unexpected_percent").alias("resultUnexpectedPercent")
            ,col("result.result.unexpected_percent_nonmissing").alias("resultUnexpectedPercentNonMissing")
            ,col("result.result.unexpected_percent_total").alias("resultUnexpectedPercentTotal")))
        try:
            FlattenedValidationResultsDetailDF.append(FlattenedValidationResultsDetailDF[-1].withColumn("kwargsValueSet",split(col("result.expectation_config.kwargs.value_set"),",")))
            FlattenedValidationResultsDetailDF.append(FlattenedValidationResultsDetailDF[-1].withColumn("kwargsColumnList",split(col("result.expectation_config.kwargs.column_list"),",")))
        except Exception as e:
            pass

        self.FlattenedValidationResultsDF = FlattenedValidationResultsDetailDF
        self.FlattenedValidationResultsDetailDF = FlattenedValidationResultsDetailDF[-1]
        return FlattenedValidationResultsDF, FlattenedValidationResultsDetailDF[-1]

    def formatGreatExpectationsList(self):
        expectation_list = []
        for expectation in self.expectation_suite.expectations:
            expectation_type, kwargs = expectation.to_json_dict()['expectation_type'], expectation.to_json_dict()['kwargs']
            args = []
            for k, v in kwargs.items():
                if isinstance(v, str):
                    args.append("{0}='{1}'".format(k, v))
                else:
                    args.append("{0}={1}".format(k, v))
            parameters = ",".join(args)
            expectation_list.append("{0}({1})".format(expectation_type, parameters))
        self.expectation_list = expectation_list
        return expectation_list

    def getDataQualityRulesForTable(self):
        try:
            sql = """
            SELECT rules
            FROM goldprotected.DataQualityRule
            WHERE isActive = 1 AND fullyQualifiedTableName = '{0}'
            """.format(self.tableName)
            expectation_list = spark.sql(sql).collect()[0][0]
        except Exception as e:
            expectation_list = []
        self.expectation_list = expectation_list
        return expectation_list

    def mergeDataQualityRuleType2(self, upsertTableName):
        spark.sql("""
        MERGE INTO goldprotected.DataQualityRule AS tgt
            USING
            (
            SELECT pk AS mergeKey, *
            FROM {0}

            UNION ALL

            SELECT null AS mergeKey, src.*
            FROM {0} src
            JOIN goldprotected.DataQualityRule tgt ON tgt.pk = src.pk
            WHERE tgt.`isActive` = true
            ) AS src ON src.`mergeKey` = tgt.`pk`
            WHEN MATCHED AND tgt.`isActive` = true THEN
            UPDATE SET tgt.`isActive` = false, tgt.`effectiveEndDate`=src.`effectiveDate`
            WHEN NOT MATCHED THEN
            INSERT (`fullyQualifiedTableName`,`rules`,`tableId`,`pk`,`isActive`,`effectiveStartDate`,`effectiveEndDate`)
            VALUES(src.`fullyQualifiedTableName`,src.`rules`,src.`tableId`,src.`pk`, true, src.`effectiveDate`, null)
        """.format(upsertTableName))

    def profileGreatExpectations (self):
        from great_expectations.profile.basic_dataset_profiler import BasicDatasetProfiler
        from great_expectations.dataset.sparkdf_dataset import SparkDFDataset
        from great_expectations.render.renderer import ProfilingResultsPageRenderer
        from great_expectations.render.view import DefaultJinjaPageView
        expectation_suite, validation_results = BasicDatasetProfiler.profile(SparkDFDataset(self.dfGE.spark_df))
        expectation_suite.expectation_suite_name = self.tableName
        validation_results.meta["expectation_suite_name"] = self.tableName
        model = ProfilingResultsPageRenderer().render(validation_results)
        html = DefaultJinjaPageView().render(model)
        
        self.expectation_suite = expectation_suite 
        self.validation_results = validation_results
        self.profiling_results_html = html        
        return expectation_suite, validation_results, html

    def saveDataQualityValidationResultsForTable(self):
        resultsDF, resultsDetailDF = self.flattenGreatExpectationsValidationResults()
        resultsDFDataPath = "{0}/dataqualityvalidationresult".format(self.goldDataPath)
        resultsDetailDataPath = "{0}/dataqualityvalidationresultdetail".format(self.goldDataPath)
        resultsDF.write.mode("APPEND").format("DELTA").option("mergeSchema", True).save(resultsDFDataPath)
        resultsDetailDF.write.mode("APPEND").format("DELTA").option("mergeSchema", True).save(resultsDetailDataPath)

    def saveExpectationList(self):
        from pyspark.sql.functions import lit, collect_list, current_timestamp, hash
        from pyspark.sql.types import StringType
        from ktk import utilities as u
        expectationsDF = []
        upsertTableName = "bronze.goldload_{0}_staging".format(self.tableName.replace(".","_"))
        pkcols="fullyQualifiedTableName"
        expectationsDF.append(spark.createDataFrame(self.expectation_list, StringType()).withColumnRenamed("value", "rules"))
        expectationsDF.append(expectationsDF[-1].withColumn("fullyQualifiedTableName", lit(self.tableName)))
        expectationsDF.append(expectationsDF[-1].groupBy("fullyQualifiedTableName").agg(collect_list("rules").alias("rules")))
        expectationsDF.append(expectationsDF[-1].withColumn("effectiveDate", current_timestamp()))
        expectationsDF.append(expectationsDF[-1].withColumn("tableId", hash("fullyQualifiedTableName")))
        expectationsDF.append(u.pkColSha(expectationsDF[-1], pkcols))
        spark.sql("DROP TABLE IF EXISTS " + upsertTableName)
        expectationsDF[-1].write.saveAsTable(upsertTableName)
        self.mergeDataQualityRuleType2(upsertTableName)

    def setGreatExpectationsDataset(self, df):
        import great_expectations as ge
        dfGE = ge.dataset.SparkDFDataset(df)
        self.dfGE = dfGE
         
    def validateGreatExpectations (self, dfGE):
        from great_expectations.render.renderer import ValidationResultsPageRenderer
        from great_expectations.render.view import DefaultJinjaPageView
        for exp in self.expectation_list:
            try:
                exec("dfGE.{0}".format(exp))
            except Exception as e:
                print("unable to exec expectation {0}".format(exp))
        validation_results = dfGE.validate()
        validation_results.meta["expectation_suite_name"] = self.tableName
        model = ValidationResultsPageRenderer().render(validation_results)
        
        self.validation_results = validation_results
        html = DefaultJinjaPageView().render(model)
        self.validation_results_html = html
        return validation_results, html