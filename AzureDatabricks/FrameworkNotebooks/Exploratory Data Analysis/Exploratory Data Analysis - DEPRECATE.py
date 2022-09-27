# Databricks notebook source
# MAGIC %md
# MAGIC # Exploratory Data Analysis

# COMMAND ----------

# MAGIC %md
# MAGIC #### Initialize

# COMMAND ----------

# MAGIC %run "../Orchestration/Notebook Functions"

# COMMAND ----------

# MAGIC %run "../Development/Utilities"

# COMMAND ----------

# MAGIC %run "../ML/MLFunctions"

# COMMAND ----------

import datetime, json, uuid
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.ml.feature import Bucketizer
from functools import reduce
from pyspark.sql import DataFrame
import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.stat import Correlation

spark.conf.set("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation","true")

dbutils.widgets.text(name="stepLogGuid", defaultValue="00000000-0000-0000-0000-000000000000", label="stepLogGuid")
dbutils.widgets.text(name="stepKey", defaultValue="-1", label="stepKey")
dbutils.widgets.text(name="sourceTableName", defaultValue="", label="Source Table Name")
dbutils.widgets.text(name="samplePercentage", defaultValue="1.00", label="Sample Percentage")
dbutils.widgets.text(name="excludedColumns", defaultValue="", label="Columns to Exclude")
dbutils.widgets.text(name="label", defaultValue="", label="Label")
dbutils.widgets.text(name="continuousColumns", defaultValue="", label="Continuous Columns")

stepLogGuid = dbutils.widgets.get("stepLogGuid")
stepKey = int(dbutils.widgets.get("stepKey"))
sourceTableName = dbutils.widgets.get("sourceTableName")
catalogName, tableName = sourceTableName.split(".")
samplePercentage = float(dbutils.widgets.get("samplePercentage"))
excludedColumns = dbutils.widgets.get("excludedColumns")
label = dbutils.widgets.get("label")
continuousColumns = dbutils.widgets.get("continuousColumns")

context = dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson()
p = {
  "stepLogGuid": stepLogGuid,
  "stepKey": stepKey,
  "sourceTableName": sourceTableName,
  "samplePercentage": samplePercentage,
  "excludedColumns": excludedColumns,
  "label": label,
  "continuousColumns": continuousColumns
}
parameters = json.dumps(p)

notebookLogGuid = str(uuid.uuid4())
log_notebook_start(notebookLogGuid, stepLogGuid, stepKey, parameters, context, server, database, login, pwd)

print("Notebook Log Guid: {0}".format(notebookLogGuid))
print("Step Log Guid: {0}".format(stepLogGuid))
print("Context: {0}".format(context))
print("Parameters: {0}".format(parameters))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Analyze

# COMMAND ----------

# MAGIC %md
# MAGIC #### Refresh, Sample and Cache Table

# COMMAND ----------

refreshed = refreshTable(sourceTableName)
if refreshed == False:
  log_notebook_end(notebookLogGuid, 0, server, database, login, pwd)
  dbutils.notebook.exit("Table does not exist")
dfList = []
dfList.append(spark.table(sourceTableName))
dfList.append(dfList[-1].sample(False,samplePercentage))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Classify Columns

# COMMAND ----------

dfList.append(excludeColumns(dfList[-1], excludedColumns))
if continuousColumns == "":
  continuousColumns = getContinuousColumns(dfList[-1])
categoricalColumns = set(dfList[-1].columns) - set(continuousColumns)
print("Label: {0}".format(label))
print("Continuous Columns: {0}".format(continuousColumns))
print("Categorical Columns: {0}".format(categoricalColumns))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Data Profiling

# COMMAND ----------

vcAll = []
sumAll = []
binsAll = []

for c in dfList[-1].dtypes:
  vc, sum = dataCatalogColumn(catalogName, tableName, dfList[-1], c[0], c[1])
  vcAll.append(vc)
  sumAll.append(sum)

topValueCounts = reduce(DataFrame.unionAll, vcAll)
summaryStatistics = reduce(DataFrame.unionAll, sumAll)
topValueCounts.createOrReplaceTempView("valueCounts")
summaryStatistics.createOrReplaceTempView("summary")

for c in continuousColumns:
  bins = getBinsForDataFrameColumn(catalogName, tableName, dfList[-1], c, numBins=20)
  binsAll.append(bins)
bins = reduce(DataFrame.unionAll, binsAll)
bins.createOrReplaceTempView("bins")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Review

# COMMAND ----------

# MAGIC %md
# MAGIC #### Summary Stats

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW summaryEnriched
# MAGIC AS
# MAGIC SELECT
# MAGIC Catalog, Table, Column, DataType
# MAGIC , RecordCount, DistinctCountValue AS DistinctRecordCount, Selectivity
# MAGIC , NumberOfNulls, NumberOfNulls / RecordCount AS PercentNulls, NumberOfZeros, PercentZeros
# MAGIC ,MinimumValue, AvgValue - (StdDevValue * 2) AS LowerLimit, AvgValue, StdDevValue, AvgValue + (StdDevValue * 2) AS UpperLimit, MaximumValue, ApproxQuantile
# MAGIC FROM summary;
# MAGIC 
# MAGIC SELECT * FROM summaryEnriched;

# COMMAND ----------

# MAGIC %md
# MAGIC #### Bins

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM bins ORDER BY Catalog, Table, Column, bins

# COMMAND ----------

sql = """
SELECT
Catalog
, Table
, Column
, collect_list(bins) AS Bin
, collect_list(count) AS BinCount
, arrays_zip(collect_list(bins), collect_list(count)) AS BinsZipped
FROM bins
GROUP BY Catalog, Table, Column
"""
binsReview = spark.sql(sql)
display(binsReview)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Visualize

# COMMAND ----------

pdBins = binsReview.drop("BinsZipped").toPandas()
pdf = dfList[-1].toPandas()
pdSummaryEnriched = spark.table("summaryEnriched").toPandas()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Continuous Columns

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Outliers Removed

# COMMAND ----------

plt.close('all')
numPlots = len(continuousColumns)
fig = plt.figure(1, figsize=(12,12))
ax = []
n = 0
for column in continuousColumns:
  pdSummaryCol = pdSummaryEnriched[pdSummaryEnriched['Column'] == column]
  lowerLimit = pdSummaryCol['LowerLimit'].item()
  upperLimit = pdSummaryCol['UpperLimit'].item()
  pdfOutliersRemoved = pdf[(pdf[column] > lowerLimit) & (pdf[column] < upperLimit)][column]
  ax.append(plt.subplot2grid((numPlots,3),(n,0)))
  ax[-1].hist(pdfOutliersRemoved)
  ax[-1].set_title(column)
  ax.append(plt.subplot2grid((numPlots,3),(n,1)))
  ax[-1].violinplot(pdfOutliersRemoved)
  ax[-1].set_title(column)
  ax.append(plt.subplot2grid((numPlots,3),(n,2)))
  ax[-1].boxplot(pdfOutliersRemoved, vert=False)
  ax[-1].set_title(column)
  n += 1

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Original

# COMMAND ----------

plt.close('all')
numPlots = len(continuousColumns)
fig = plt.figure(1, figsize=(12,12))
ax = []
n = 0
for column in continuousColumns:
  ax.append(plt.subplot2grid((numPlots,3),(n,0)))
  ax[-1].hist(pdf[column])
  ax[-1].set_title(column)
  ax.append(plt.subplot2grid((numPlots,3),(n,1)))
  ax[-1].violinplot(pdf[column])
  ax[-1].set_title(column)
  ax.append(plt.subplot2grid((numPlots,3),(n,2)))
  ax[-1].boxplot(pdf[column], vert=False)
  ax[-1].set_title(column)
  n += 1

# COMMAND ----------

# MAGIC %md
# MAGIC #### Categorical Columns

# COMMAND ----------

pdValueCounts = topValueCounts.toPandas()
plt.close('all')
numPlots = len(categoricalColumns)
fig = plt.figure(1, figsize=(72,144))
ax = []
n = 0
for column in categoricalColumns:
  vc = pdValueCounts[pdValueCounts['Column']==column].dropna()
  ax.append(plt.subplot2grid((numPlots,2),(n,0)))
  ax[-1].barh(list(vc['value']), list(vc['Total']))
  ax[-1].set_title(column)
  ax.append(plt.subplot2grid((numPlots,2),(n,1)))
  ax[-1].pie(list(vc['Total']), labels=list(vc['value']), autopct='%1.1f%%')
  ax[-1].set_title(column)
  n += 1

# COMMAND ----------

# MAGIC %md
# MAGIC #### Scatter Plot

# COMMAND ----------

pd.plotting.scatter_matrix(pdf, figsize=(24,24))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Correlations

# COMMAND ----------

correlationColumns = [c[0] for c in dfList[-1].dtypes if c[1] not in ['string','date']]

# COMMAND ----------

assembler = VectorAssembler(inputCols=correlationColumns, outputCol="features")
featurized = assembler.transform(dfList[-1].select(*correlationColumns).na.drop())
pearsonCorr = Correlation.corr(featurized, 'features').collect()[0][0]
pdCorr = pd.DataFrame(pearsonCorr.toArray())
pdCorr.index, pdCorr.columns = dfList[-1].select(*correlationColumns).columns, dfList[-1].select(*correlationColumns).columns
pdCorr

# COMMAND ----------

fig = plt.figure(1, figsize=(10,10))
sns.heatmap(pdCorr)
display(fig.figure)
