# Databricks notebook source
# MAGIC %md # Term Search
# MAGIC ###### Author: Neelima Vinod Karve
# MAGIC
# MAGIC * Provide a comma delimited list of terms to search
# MAGIC * For each term, the notebook will:
# MAGIC     * Evaluate whether the term partially matches a table (goldprotected.datacatalogtable), column (goldprotected.datacatalogcolumn) or column value (goldprotected.datacatalogcolumnvaluecounts)
# MAGIC     * Build a dataframe of the results which you can export as csv (and review in Excel)
# MAGIC     * Export the dataframe to managed table (goldprotected.termSearchResults)
# MAGIC
# MAGIC Prerequisites: Run the Data Catalog Job against the data lake in order to populate the data catalog tables.

# COMMAND ----------

# MAGIC %md #### Initialize

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Source

# COMMAND ----------

import json
from datetime import datetime, timedelta
import hashlib
import uuid
import re
from pyspark.sql import Row
from pyspark.sql.functions import levenshtein,col,greatest,length,coalesce #div,float

# COMMAND ----------

# MAGIC %run "../Orchestration/Notebook Functions"

# COMMAND ----------

# MAGIC %run ../Development/Utilities

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Widgets

# COMMAND ----------

dbutils.widgets.text(name="stepLogGuid", defaultValue="00000000-0000-0000-0000-000000000000", label="stepLogGuid")
dbutils.widgets.text(name="stepKey", defaultValue="-1", label="stepKey")
dbutils.widgets.text(name="searchTermsListCSV", defaultValue="", label="searchTermsListCSV")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Variables

# COMMAND ----------

stepLogGuid = dbutils.widgets.get("stepLogGuid")
stepKey = int(dbutils.widgets.get("stepKey"))
searchTermsList = re.split(',(?=(?:[^\"]*\"[^\"]*\")*(?![^\"]*\"))', dbutils.widgets.get("searchTermsListCSV"))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Log Start

# COMMAND ----------

context = dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson()

p = {
  "stepLogGuid": stepLogGuid,
  "stepKey": stepKey,
  "searchTermsList": searchTermsList
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
# MAGIC #### Create Search Terms Dataframe & Temp View

# COMMAND ----------

searchListDF = spark.createDataFrame(list(map(lambda x: Row(searchTerm=x.strip().strip('"').strip()), searchTermsList))).filter("searchTerm <> ''")
searchListDF.createOrReplaceTempView("searchTerms")
searchListDF.show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Validate

# COMMAND ----------

#validate inputs here

# e.g. use assert to ensure searchTermsList is not empty and the regex worked
# end the notebook if any failures

if searchListDF.count() == 0:
  raise ValueError("Search terms list is empty. Please provide terms to be matched.")


# COMMAND ----------

# MAGIC %md
# MAGIC #### Delete existing Term Search Results

# COMMAND ----------

sql = """
DELETE FROM goldprotected.termSearchResults
WHERE searchTerm IN (SELECT searchTerm FROM searchTerms)"""
spark.sql(sql)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Find Exact or Fuzzy Matches
# MAGIC
# MAGIC Exact match : complete search term appears in table name (goldprotected.datacatalogtable), column name (goldprotected.datacatalogcolumn) or column value (goldprotected.datacatalogcolumnvaluecounts) within the data catalog.
# MAGIC
# MAGIC Fuzzy match : we use levenshtein algorithm to determine similarity between search term and table name / column name / column value.
# MAGIC The confidence score will be calculated as below:
# MAGIC
# MAGIC     numerator: the longer of the two search terms minus the levenshtein distance
# MAGIC     denominator: longer of the two search terms
# MAGIC     Examples:
# MAGIC     -- CART|CAR --> (4-1)/4 = .75
# MAGIC     -- CAR|CAR --> (3-0)/3 = 1.0
# MAGIC     -- ABCDEFG|DEF --> (7-4)/7 = .42

# COMMAND ----------

# MAGIC %sql
# MAGIC -- this is not helping to get float output from div operator.
# MAGIC set spark.sql.legacy.integralDivide.returnBigint = false;
# MAGIC select 7.0 div 4.0

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC INSERT INTO goldprotected.termSearchResults
# MAGIC
# MAGIC SELECT *
# MAGIC FROM
# MAGIC (
# MAGIC SELECT DISTINCT DCC.databaseName, DCC.tableName , DCC.columnName, NULL AS columnSampleValue
# MAGIC , ST.searchTerm
# MAGIC , True AS tableNameMatch
# MAGIC , NULL AS columnNameMatch
# MAGIC , NULL AS columnValueMatch
# MAGIC , CASE WHEN locate(ST.searchTerm,DCC.tableName) > 0
# MAGIC        THEN true
# MAGIC        ELSE false
# MAGIC        END AS searchTermFullMatchFlag
# MAGIC , CASE WHEN greatest(length(ST.searchTerm),length(DCC.tableName)) = 0
# MAGIC        THEN 0.0
# MAGIC        ELSE
# MAGIC         float((greatest(length(ST.searchTerm),length(DCC.tableName)) - levenshtein(ST.searchTerm,DCC.tableName)))
# MAGIC         / float(greatest(length(ST.searchTerm),length(DCC.tableName)))
# MAGIC        END AS fuzzyMatchConfidenceScore
# MAGIC , DCC.tableId
# MAGIC , DCC.columnId
# MAGIC FROM goldprotected.datacatalogcolumn AS DCC
# MAGIC CROSS JOIN searchTerms AS ST
# MAGIC WHERE isActive = 1
# MAGIC ) AS tableNameMatch
# MAGIC WHERE searchTermFullMatchFlag = true
# MAGIC OR fuzzyMatchConfidenceScore >= 0.6
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT *
# MAGIC FROM
# MAGIC (
# MAGIC SELECT DISTINCT DCC.databaseName, DCC.tableName , DCC.columnName, NULL AS columnSampleValue
# MAGIC , ST.searchTerm
# MAGIC , NULL AS tableNameMatch
# MAGIC , True AS columnNameMatch
# MAGIC , NULL AS columnValueMatch
# MAGIC , CASE WHEN locate(ST.searchTerm,DCC.columnName) > 0
# MAGIC        THEN true
# MAGIC        ELSE false
# MAGIC        END AS searchTermFullMatchFlag
# MAGIC , CASE WHEN greatest(length(ST.searchTerm),length(DCC.columnName)) = 0
# MAGIC        THEN 0.0
# MAGIC        ELSE
# MAGIC         float((greatest(length(ST.searchTerm),length(DCC.columnName)) - levenshtein(ST.searchTerm,DCC.columnName)))
# MAGIC         / float(greatest(length(ST.searchTerm),length(DCC.columnName)))
# MAGIC        END AS fuzzyMatchConfidenceScore
# MAGIC , DCC.tableId
# MAGIC , DCC.columnId
# MAGIC FROM goldprotected.datacatalogcolumn AS DCC
# MAGIC CROSS JOIN searchTerms AS ST
# MAGIC WHERE isActive = 1
# MAGIC ) AS tableNameMatch
# MAGIC WHERE searchTermFullMatchFlag = true
# MAGIC OR fuzzyMatchConfidenceScore >= 0.6
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT *
# MAGIC FROM
# MAGIC (
# MAGIC SELECT DISTINCT DCC.databaseName, DCC.tableName , DCC.columnName, DCV.value AS columnSampleValue
# MAGIC , ST.searchTerm
# MAGIC , NULL AS tableNameMatch
# MAGIC , NULL AS columnNameMatch
# MAGIC , True AS columnValueMatch
# MAGIC , CASE WHEN locate(ST.searchTerm,DCV.value) > 0
# MAGIC        THEN true
# MAGIC        ELSE false
# MAGIC        END AS searchTermFullMatchFlag
# MAGIC , CASE WHEN greatest(length(ST.searchTerm),length(DCV.value)) = 0
# MAGIC        THEN 0.0
# MAGIC        ELSE
# MAGIC         float((greatest(length(ST.searchTerm),length(DCV.value)) - levenshtein(ST.searchTerm,DCV.value)))
# MAGIC         / float(greatest(length(ST.searchTerm),length(DCV.value)))
# MAGIC        END AS fuzzyMatchConfidenceScore
# MAGIC , DCC.tableId
# MAGIC , DCC.columnId
# MAGIC FROM goldprotected.datacatalogcolumn AS DCC
# MAGIC INNER JOIN goldprotected.datacatalogcolumnvaluecounts AS DCV
# MAGIC ON DCC.columnId = DCV.columnId
# MAGIC CROSS JOIN searchTerms AS ST
# MAGIC WHERE isActive = 1
# MAGIC ) AS tableNameMatch
# MAGIC WHERE searchTermFullMatchFlag = true
# MAGIC OR fuzzyMatchConfidenceScore >= 0.6

# COMMAND ----------

# MAGIC %md
# MAGIC #### Save Search Results

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT searchTerm, searchTermFullMatchFlag, fuzzyMatchConfidenceScore, databaseName, tableName, columnName, columnSampleValue
# MAGIC , tableNameMatch
# MAGIC , columnNameMatch
# MAGIC , columnValueMatch
# MAGIC FROM goldprotected.termSearchResults
# MAGIC ORDER BY databaseName, tableName, columnName, columnSampleValue

# COMMAND ----------

# MAGIC %md #### Log Completion

# COMMAND ----------

log_notebook_end(notebookLogGuid, 0, server, database, login, pwd)
dbutils.notebook.exit("Succeeded")
