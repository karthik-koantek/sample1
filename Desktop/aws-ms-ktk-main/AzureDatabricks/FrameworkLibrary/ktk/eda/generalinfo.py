from .eda import EDA

class GeneralInfo(EDA):
    def __init__(self, df, tableName):
        super().__init__(df, tableName)
    
    def dataCatalogColumn(self, columnName, dataType):
        from pyspark.sql.functions import lit
        vc = []
        summary = []

        vc.append(self.getValueCountsForDataframeColumn(columnName))
        vc.append(vc[-1] \
            .withColumn("Catalog", lit(self.catalogName)) \
            .withColumn("Table", lit(self.tableName)) \
            .withColumn("Column", lit(columnName)) \
            .withColumn("DataType", lit(dataType)))

        summary.append(self.getSummaryStatisticsForDataframeColumn(columnName, dataType))
        summary.append(summary[-1] \
                    .withColumn("Catalog", lit(self.catalogName)) \
                    .withColumn("Table", lit(self.tableName)) \
                    .withColumn("Column", lit(columnName)) \
                    .withColumn("DataType", lit(dataType)))

        return vc[-1], summary[-1]

    def displayGeneralInfo(self):   
        rowCount, columnCount, columns, summary, nulls, descriptiveStats = self.getGeneralInfo()
        print(f'\nTotal Rows : {rowCount} \n')
        print(f'\nTotal Columns : {columnCount} \n')
        print(f'\nColumn Names\n' + f'\n{columns} \n')
        print(f'\nData Summary\n' + f'\n{summary} \n')
        print(f'\nNull values\n'+ f'\n{nulls} \n')
        print(f'\nDescriptive Statistics\n' +  f'\n{descriptiveStats}\n')

    def getColumnCount(self):
        return self.pdf.shape[1]

    def getColumnList(self):
        return self.pdf.columns

    def getCorrelations(self):
        from pyspark.ml.feature import VectorAssembler
        from pyspark.ml.stat import Correlation
        import pandas as pd 

        assembler = VectorAssembler(inputCols=self.ContinuousColumns, outputCol="features")
        featurized = assembler.transform(self.df.select(*self.ContinuousColumns).na.drop())
        pearsonCorr = Correlation.corr(featurized, 'features').collect()[0][0]
        pdCorr = pd.DataFrame(pearsonCorr.toArray())
        pdCorr.index, pdCorr.columns = self.df.select(*self.ContinuousColumns).columns, self.df.select(*self.ContinuousColumns).columns
        return pdCorr

    def getDataSummary(self):
        return self.pdf.info()

    def getDescriptiveStatistics(self):
        return self.pdf.describe()

    def getGeneralInfo(self):
        rowCount = self.getRowCount()
        columnCount = self.getColumnCount()
        columns = self.getColumnList() 
        summary =  self.getDataSummary()
        nulls = self.getNullValues()
        descriptiveStats = self.getDescriptiveStatistics()
        return rowCount, columnCount, columns, summary, nulls, descriptiveStats

    def getNullValues(self):
        return self.pdf.isnull().sum() 

    def getRowCount(self):
        return self.pdf.shape[0]

    def getSummaryStatisticsForDataframeColumn(self, columnName, dataType):
        from pyspark.sql.functions import avg, stddev, countDistinct, count, when, col, isnan, min, max
        from pyspark.sql.types import StringType
        dfList = []
        dfList.append(self.df)

        if dataType in ["date", "boolean", "timestamp", "vector", "struct", "array", "map<string,string>"
            ,"array<struct<id:string,timestamp:string,value:double>>", "array<string>"
            ,"array<struct<id:bigint,order:bigint,selectedInputs:array<struct<department:string,id:string,isCanned:boolean,isPartnerNeeded:string,order:bigint,text:string,timeframe:string>>,title:string>>"
            ,"array<struct<id:bigint,order:bigint,selectedInputs:array<struct<department:string,id:string,isCanned:boolean,order:bigint,text:string,timeframe:string>>,title:string>>"
            ,"array<struct<id:bigint,order:bigint,selectedInputs:array<struct<department:string,id:string,isCanned:boolean,order:bigint,text:string>>,title:string>>"
            ,"array<struct<id:bigint,peopleAdded:array<struct<department:string,email:string,firstName:string,id:string,lastName:string,order:bigint,role:string>>,title:string>>"
            ]:
            dfList.append(self.df.select(col(columnName).alias("ColumnName").cast(StringType())))
        else:
            dfList.append(self.df.select(col(columnName).alias("ColumnName")))

        dfList.append(dfList[-1] \
            .agg(
                min("ColumnName").alias("MinimumValue") \
                ,max("ColumnName").alias("MaximumValue") \
                ,avg("ColumnName").alias("AvgValue") \
                ,stddev("ColumnName").alias("StdDevValue") \
                ,countDistinct("ColumnName").alias("DistinctCountValue") \
                ,count(when(isnan("ColumnName") | col("ColumnName").isNull(), "ColumnName")).alias("NumberOfNulls") \
                ,count(when(col("ColumnName")=="", "ColumnName")).alias("NumberOfBlanks") \
                ,count(when(col("ColumnName")==0, "ColumnName")).alias("NumberOfZeros") \
                ,count("*").alias("RecordCount")
            ))
        dfList.append(dfList[-1] \
            .selectExpr( \
                "cast(MinimumValue as string) MinimumValue",
                "cast(MaximumValue as string) MaximumValue",
                "cast(AvgValue as string) AvgValue",
                "cast(StdDevValue as string) StdDevValue",
                "cast(DistinctCountValue as int) DistinctCountValue",
                "cast(NumberOfNulls + NumberOfBlanks as int) NumberOfNulls",
                "cast(NumberOfZeros as int) NumberOfZeros",
                "cast(RecordCount as int) RecordCount",
                "cast(NumberOfNulls + NumberOfBlanks / cast(RecordCount as float) as float) PercentNulls ",
                "cast(NumberOfZeros / cast(RecordCount as float) as float) PercentZeros ",
                "cast(DistinctCountValue / cast(RecordCount as float) as float) Selectivity "
            ))
        return dfList[-1]

    def getValueCountsForDataframeColumn(self, columnName):
        from pyspark.sql.functions import col, count, desc
        from pyspark.sql.types import StringType
        vc = []
        vc.append((self.df.select(col(columnName).alias("value").cast(StringType())) \
            .groupBy("value") \
            .agg(count("*").alias("total")) \
            .orderBy(desc("total")) \
            .limit(20)))
        vc.append(vc[-1] \
        .selectExpr( \
                    "cast(value as string) value",
                    "cast(Total as int) Total"
                ))
        return vc[-1]

    def pandasProfilingReport(self):
        import pandas_profiling 
        return pandas_profiling.ProfileReport(self.pdf)

    def plotCategoricalColumns(self):
        import matplotlib.pyplot as plt
        plt.close('all')
        numPlots = len(self.CategoricalColumns)
        fig = plt.figure(1, figsize=(72,144))
        ax = []
        n = 0
        for column in self.CategoricalColumns:
            vc = self.valueCountspdf[self.valueCountspdf['Column']==column].dropna()
            ax.append(plt.subplot2grid((numPlots,2),(n,0)))
            ax[-1].barh(list(vc['value']), list(vc['Total']))
            ax[-1].set_title(column)
            ax.append(plt.subplot2grid((numPlots,2),(n,1)))
            ax[-1].pie(list(vc['Total']), labels=list(vc['value']), autopct='%1.1f%%')
            ax[-1].set_title(column)
            n += 1

    def plotContinuousColumns(self, removeOutliers=True):
        import matplotlib.pyplot as plt 
        plt.close('all')
        numPlots = len(self.ContinuousColumns)
        fig = plt.figure(1, figsize = (12,12))
        ax = []
        n = 0
        pdSummary = self.summarypdf
        for column in self.ContinuousColumns:
            if removeOutliers == True:
                pdSummaryCol = self.removeColumnOutliers(column)
            else:
                pdSummaryCol = self.pdf[column]
            ax.append(plt.subplot2grid((numPlots,3),(n,0)))
            ax[-1].hist(pdSummaryCol)
            ax[-1].set_title(column)
            ax.append(plt.subplot2grid((numPlots,3),(n,1)))
            ax[-1].violinplot(pdSummaryCol)
            ax[-1].set_title(column)
            ax.append(plt.subplot2grid((numPlots,3),(n,2)))
            ax[-1].boxplot(pdSummaryCol, vert=False)
            ax[-1].set_title(column)
            n += 1

    def plotCorrelations(self):
        import matplotlib.pyplot as plt
        import seaborn as sns

        pdCorr = self.getCorrelations()
        fig = plt.figure(1, figsize=(10,10))
        sns.heatmap(pdCorr)
        return fig
        
    def plotDataPrepReport(self, title):
        import dataprep.eda as dataprep_eda 
        return dataprep_eda.create_report(self.pdf, title=title)

    def plotDiff(self, pdf):
        import dataprep.eda as dataprep_eda 
        return dataprep_eda.plot_diff(self.pdf, pdf)

    def plotGeneralInfo(self):
        import dataprep.eda as dataprep_eda
        return dataprep_eda.plot(self.pdf)

    def plotMissing(self):
        import dataprep.eda as dataprep_eda
        return dataprep_eda.plot_missing(self.pdf)

    def plotScatter(self):
        import pandas as pd 
        pd.plotting.scatter_matrix(self.pdf, figsize=(24,24))

    def removeColumnOutliers(self, column):
        pdSummaryCol = self.summarypdf[self.summarypdf['Column'] == column]
        lowerLimit = pdSummaryCol['LowerLimit'].item()
        upperLimit = pdSummaryCol['UpperLimit'].item()
        return self.pdf[(self.pdf[column] > lowerLimit) & (self.pdf[column] < upperLimit)][column]

    def saveDataPrepReport(self, rpt, outputPath):
        rpt.save(filename=rpt.title, to=outputPath)
    
    def setSummaryInfo(self):
        from pyspark.sql import DataFrame
        from pyspark.sql.functions import col
        from functools import reduce
        vcAll = []
        sumAll = []
        summary = []
        valueCounts = []

        for c in self.df.dtypes:
            vc, sum = self.dataCatalogColumn(c[0], c[1])
            vcAll.append(vc)
            sumAll.append(sum)

        valueCounts.append(reduce(DataFrame.unionAll, vcAll))

        summary.append(reduce(DataFrame.unionAll, sumAll))
        summary.append(summary[-1].withColumnRenamed("DistinctCountValue","DistinctRecordCount"))
        summary.append(summary[-1].withColumn("PercentNulls", col("NumberOfNulls") / col("RecordCount")))
        summary.append(summary[-1].withColumn("LowerLimit", col("AvgValue") - (col("StdDevValue") * 2)))
        summary.append(summary[-1].withColumn("UpperLimit", col("AvgValue") + (col("StdDevValue") * 2)))
        summary.append(summary[-1].select("Catalog", "Table", "Column", "DataType", "RecordCount", "DistinctRecordCount", "Selectivity", "NumberOfNulls", "PercentNulls", "NumberOfZeros", "PercentZeros", "MinimumValue", "LowerLimit", "UpperLimit", "MaximumValue"))
        
        self.summary = summary[-1]
        self.summarypdf = self.summary.toPandas()

        self.valueCounts = valueCounts[-1]
        self.valueCountspdf = self.valueCounts.toPandas()