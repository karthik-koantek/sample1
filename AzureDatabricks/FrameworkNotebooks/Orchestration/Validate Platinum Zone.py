# Databricks notebook source
# MAGIC %md
# MAGIC #### Initialize

# COMMAND ----------

# MAGIC %run "../Orchestration/Notebook Functions"

# COMMAND ----------

# MAGIC %run ../Development/Utilities

# COMMAND ----------

import datetime, json
from pyspark.sql.functions import col, concat, lit
from pyspark.sql.types import StringType
from datetime import datetime, timedelta

dbutils.widgets.text(name="stepLogGuid", defaultValue="00000000-0000-0000-0000-000000000000", label="stepLogGuid")
dbutils.widgets.text(name="stepKey", defaultValue="-1", label="stepKey")
dbutils.widgets.text(name="externalSystem", defaultValue="dwdb", label="External System")
dbutils.widgets.text(name="numPartitions", defaultValue="8", label="Number of Partitions")

stepLogGuid = dbutils.widgets.get("stepLogGuid")
stepKey = int(dbutils.widgets.get("stepKey"))

#we are comparing Prod and "Green Prod" as per our Blue/Green deployment upgrade plan.
externalSystem = dbutils.widgets.get("externalSystem")
externalServer = dbutils.secrets.get(scope=externalSystem, key="SQLServerName")
externalDatabase = dbutils.secrets.get(scope=externalSystem, key="DatabaseName")
externalLogin = dbutils.secrets.get(scope=externalSystem, key="Login")
externalPwd = dbutils.secrets.get(scope=externalSystem, key="Pwd")

prodExternalServer = "dma-prod-server.database.windows.net"
prodExternalDatabase = "mwtOnRampReporting"
prodExternalLogin = "dma-prod-admin"
prodExternalPwd = "PrebUyafezurEswadr8qeprebraxaVuy"
numPartitions = dbutils.widgets.get("numPartitions")
silverDataPath = "{0}/{1}".format(silverGeneralBasePath, "validation")
goldDataPath = "{0}/{1}".format(goldGeneralBasePath, "validation")

context = dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson()
p = {
  "stepLogGuid": stepLogGuid,
  "stepKey": stepKey,
  "externalSystem": externalSystem,
  "externalServer": externalServer,
  "externalDatabase": externalDatabase,
  "externalLogin": externalLogin,
  "externalPwd": externalPwd,
  "prodExternalServer": prodExternalServer,
  "prodExternalDatabase": prodExternalDatabase,
  "prodExternalPwd": prodExternalPwd,
  "numPartitions": numPartitions
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
# MAGIC #### Clear Silver Zone Data

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS silvergeneral.commonTableComparison;
# MAGIC DROP TABLE IF EXISTS silvergeneral.prodTablesNotInQA;
# MAGIC DROP TABLE IF EXISTS silvergeneral.qaTablesNotInProd;

# COMMAND ----------

try:
  dbutils.fs.rm(silverDataPath, True)
except Exception as e:
  err = {
    "sourceName" : "Validate Platinum Zone: Clear Silver Zone Data",
    "errorCode" : "50",
    "errorDescription": "Failed to delete data in validation silver zone path",
    "errorClass" : e.__class__.__name__
  }
  error = json.dumps(err)
  log_notebook_error(notebookLogGuid, error, server, database, login, pwd)
  raise(e)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create Schema
# MAGIC We will overwrite the silver zone table each time this runs, then propagate the data to a gold zone table using type 2 slowly changing dimension to track changes

# COMMAND ----------

# MAGIC %md
# MAGIC ##### prodTablesNotInQA

# COMMAND ----------

sql = """
CREATE TABLE IF NOT EXISTS goldgeneral.prodTablesNotInQA
(
   pk STRING
  ,fullyQualifiedTableName STRING
  ,isActive BOOLEAN NOT NULL
  ,effectiveStartDate TIMESTAMP
  ,effectiveEndDate TIMESTAMP
)
USING delta
LOCATION '{0}/goldgeneral_prodTablesNotInQA'
""".format(goldDataPath)
print(sql)
spark.sql(sql)

# COMMAND ----------

sql = """
CREATE TABLE IF NOT EXISTS silvergeneral.prodTablesNotInQA (
  fullyQualifiedTableName STRING
)
USING delta
LOCATION '{0}/silvergeneral_prodTablesNotInQA'
""".format(silverDataPath)
print(sql)
spark.sql(sql)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### qaTablesNotInProd

# COMMAND ----------

sql = """
CREATE TABLE IF NOT EXISTS goldgeneral.qaTablesNotInProd
(
   pk STRING
  ,fullyQualifiedTableName STRING
  ,isActive BOOLEAN NOT NULL
  ,effectiveStartDate TIMESTAMP
  ,effectiveEndDate TIMESTAMP
)
USING delta
LOCATION '{0}/goldgeneral_qaTablesNotInProd'
""".format(goldDataPath)
print(sql)
spark.sql(sql)

# COMMAND ----------

sql = """
CREATE TABLE IF NOT EXISTS silvergeneral.qaTablesNotInProd (
  fullyQualifiedTableName STRING
)
USING delta
LOCATION '{0}/silvergeneral_qaTablesNotInProd'
""".format(silverDataPath)
print(sql)
spark.sql(sql)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### commonTableComparison

# COMMAND ----------

sql = """
CREATE TABLE IF NOT EXISTS goldgeneral.commonTableComparison
(
   pk STRING
  ,tableId INT NOT NULL
  ,fullyQualifiedTableName STRING NOT NULL
  ,qaCount INT
  ,prodCount INT
  ,qaColumns STRING
  ,prodColumns STRING
  ,qaToProdColsComparison INT
  ,qaColsNotInProd STRING
  ,prodColsNotInQA STRING
  ,isActive BOOLEAN NOT NULL
  ,effectiveStartDate TIMESTAMP
  ,effectiveEndDate TIMESTAMP
)
USING delta
LOCATION '{0}/goldgeneral_commonTableComparison'
""".format(goldDataPath)
print(sql)
spark.sql(sql)

# COMMAND ----------

sql = """
CREATE TABLE IF NOT EXISTS silvergeneral.commonTableComparison(
   fullyQualifiedTableName STRING
  ,qaCount INT
  ,prodCount INT
  ,qaColumns ARRAY<STRING>
  ,prodColumns ARRAY<STRING>
  ,qaToProdColsComparison INT
  ,qaColsNotInProd ARRAY<STRING>
  ,prodColsNotInQA ARRAY<STRING>
)
USING delta
LOCATION '{0}/silvergeneral_commonTableComparison'
""".format(silverDataPath)
print(sql)
spark.sql(sql)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW goldgeneral.vCommonTableComparison
# MAGIC AS
# MAGIC SELECT
# MAGIC    hash(fullyQualifiedTableName) AS tableId
# MAGIC   ,fullyQualifiedTableName
# MAGIC   ,qaCount
# MAGIC   ,prodCount
# MAGIC   ,CAST(qaColumns AS STRING) as qaColumns
# MAGIC   ,CAST(prodColumns AS STRING) as prodColumns
# MAGIC   ,qaToProdColsComparison
# MAGIC   ,CAST(qaColsNotInProd AS STRING) as qaColsNotInProd
# MAGIC   ,CAST(prodColsNotInQA AS STRING) as prodColsNotInQA
# MAGIC FROM silvergeneral.commonTableComparison

# COMMAND ----------

# MAGIC %md
# MAGIC ##### vPlatinumZoneValidationEffectiveDates

# COMMAND ----------

currentDate = (datetime.utcnow() + timedelta(days=1)).strftime('%Y-%m-%d')
sequenceStartDate = (datetime.utcnow() + timedelta(days=-10)).strftime('%Y-%m-%d')
print(sequenceStartDate, currentDate)
sql = """
CREATE OR REPLACE VIEW goldgeneral.vPlatinumZoneValidationEffectiveDates
AS
SELECT
   explode(sequence(to_date('{0}'), to_date('{1}'), interval 1 day)) AS effectiveDate
""".format(sequenceStartDate,currentDate)
print(sql)
spark.sql(sql)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### commonTableComparisonHistory

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW goldgeneral.vCommonTableComparisonHistory
# MAGIC AS
# MAGIC SELECT
# MAGIC  d.effectiveDate
# MAGIC ,ct.tableId
# MAGIC ,ct.fullyQualifiedTableName
# MAGIC ,ct.qaCount
# MAGIC ,ct.prodCount
# MAGIC ,ct.qaColumns
# MAGIC ,ct.prodColumns
# MAGIC ,ct.qaToProdColsComparison
# MAGIC ,ct.qaColsNotInProd
# MAGIC ,ct.prodColsNotInQA
# MAGIC FROM goldgeneral.vPlatinumZoneValidationEffectiveDates d
# MAGIC JOIN goldgeneral.commonTableComparison ct ON to_date(d.effectiveDate) BETWEEN ct.effectiveStartDate AND COALESCE(ct.effectiveEndDate, to_date('2099-01-01'))

# COMMAND ----------

# MAGIC %md
# MAGIC ####Get Table Lists

# COMMAND ----------

try:
  qaJdbcUrl, connectionProperties = jdbcConnectionString(externalServer, externalDatabase, externalLogin, externalPwd)
  prodJdbcUrl, prodConnectionProperties = jdbcConnectionString(prodExternalServer, prodExternalDatabase, prodExternalLogin, prodExternalPwd)
  query = "SELECT TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE FROM INFORMATION_SCHEMA.TABLES"
  qaTablesDF = spark.read.format("jdbc").option("url", qaJdbcUrl).option("query", query).option("user", externalLogin).option("password", externalPwd).load()
  prodTablesDF = spark.read.format("jdbc").option("url", prodJdbcUrl).option("query", query).option("user", prodExternalLogin).option("password", prodExternalPwd).load()
except Exception as e:
  err = {
    "sourceName": "Platinum Zone Validation: Get Table Lists",
    "errorCode": "100",
    "errorDescription": "Failed to query source systems to get Table Lists",
    "errorClassName": e.__class__.__name__
  }
  error = json.dumps(err)
  log_notebook_error(notebookLogGuid, error, server, database, login, pwd)
  raise(e)

# COMMAND ----------

try:
  prodTables = [t[0] for t in
    prodTablesDF \
    .where("TABLE_TYPE=='BASE TABLE'") \
    .withColumn("fullyQualifiedTableName", concat(lit("["), col("TABLE_SCHEMA"), lit("].["), col("TABLE_NAME"), lit("]"))) \
    .drop("TABLE_SCHEMA", "TABLE_NAME", "TABLE_TYPE") \
    .collect()
  ]

  qaTables = [t[0] for t in
    qaTablesDF \
    .where("TABLE_TYPE=='BASE TABLE'") \
    .withColumn("fullyQualifiedTableName", concat(lit("["), col("TABLE_SCHEMA"), lit("].["), col("TABLE_NAME"), lit("]"))) \
    .drop("TABLE_SCHEMA", "TABLE_NAME", "TABLE_TYPE") \
    .collect()
  ]
except Exception as e:
  err = {
    "sourceName": "Platinum Zone Validation: Get Table Lists",
    "errorCode": "200",
    "errorDescription": "Failed to convert Table List Dataframes to Lists",
    "errorClassName": e.__class__.__name__
  }
  error = json.dumps(err)
  log_notebook_error(notebookLogGuid, error, server, database, login, pwd)
  raise(e)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Table Review

# COMMAND ----------

try:
  prodTablesMissingFromQA = list(set(prodTables) - set(qaTables))
  prodTablesMissingFromQADF = spark.createDataFrame(prodTablesMissingFromQA, StringType())
  prodTablesMissingFromQADF.createOrReplaceTempView("prodTablesMissingFromQA")

  qaTablesMissingFromProd = set(qaTables) - set(prodTables)
  qaTablesMissingFromProdDF = spark.createDataFrame(qaTablesMissingFromProd, StringType())
  qaTablesMissingFromProdDF.createOrReplaceTempView("qaTablesMissingFromProd")

  commonTables = list(set(qaTables) & set(prodTables))
except Exception as e:
  err = {
    "sourceName": "Platinum Zone Validation: Table Review",
    "errorCode": "300",
    "errorDescription": "Failed to generate missing Table Lists (missing from one database)",
    "errorClassName": e.__class__.__name__
  }
  error = json.dumps(err)
  log_notebook_error(notebookLogGuid, error, server, database, login, pwd)
  raise(e)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Common Table Comparison

# COMMAND ----------

def SQLQueryHelper (query):
  try:
    qaJdbcUrl, connectionProperties = jdbcConnectionString(externalServer, externalDatabase, externalLogin, externalPwd)
    prodJdbcUrl, prodConnectionProperties = jdbcConnectionString(prodExternalServer, prodExternalDatabase, prodExternalLogin, prodExternalPwd)
    qa_df = spark.read.format("jdbc").option("url", qaJdbcUrl).option("query", query).option("user", externalLogin).option("password", externalPwd).load()
    prod_df = spark.read.format("jdbc").option("url", prodJdbcUrl).option("query", query).option("user", prodExternalLogin).option("password", prodExternalPwd).load()
    return qa_df.count(), qa_df.columns, qa_df, prod_df.count(), prod_df.columns, prod_df
  except Exception as e:
    raise(e)

# COMMAND ----------

def compareTables(tables):
  testResults = []
  current_date = datetime.today().strftime('%Y-%m-%d')

  for table in tables:
    print(table)
    query = "SELECT * FROM {0}".format(table)
    qa_count, qa_cols, qa_df, prod_count, prod_cols, prod_df = SQLQueryHelper(query)
    if (set(prod_cols) - set(qa_cols)) != set():
      error = "Prod returns columns that QA does not"
    elif set(qa_cols) != set(prod_cols):
      error = "QA and Prod do not have matching column sets"
    else:
      error = ""

    p = {
      "tableName": table,
      "QA_count": qa_count,
      "PROD_count": prod_count,
      "QA_cols": qa_cols,
      "PROD_cols": prod_cols,
      #"error": error, # dropped for now, will come up with another idea to store info about non-matching columns between these
      "DATE": current_date
    }
    testResults.append(p)
  return spark.createDataFrame(testResults)

# COMMAND ----------

try:
  testResults = compareTables(commonTables)
  testResults.createOrReplaceTempView("commonTableComparison")
except Exception as e:
  err = {
    "sourceName": "Platinum Zone Validation: Common Table Comparison",
    "errorCode": "400",
    "errorDescription": "Failed while comparing common tables.",
    "errorClassName": e.__class__.__name__
  }
  error = json.dumps(err)
  log_notebook_error(notebookLogGuid, error, server, database, login, pwd)
  raise(e)

# COMMAND ----------

try:
  sql = """
  CREATE OR REPLACE TEMPORARY VIEW commonTableComparisonExtended
  AS
  SELECT
     tableName AS fullyQualifiedTableName
    ,QA_count AS qaCount
    ,PROD_count AS prodCount
    ,QA_COLS as qaColumns
    ,PROD_cols AS prodColumns
    ,CASE WHEN SIZE(QA_COLS) > SIZE(PROD_cols) THEN 1
      WHEN SIZE(QA_COLS) = SIZE(PROD_cols) THEN 0
      ELSE -1 END AS qaToProdColsComparison -- 1 if more cols in qa, 0 if equal, -1 otherwise
    ,ARRAY_EXCEPT(QA_COLS, PROD_cols) qaColsNotInProd
    ,ARRAY_EXCEPT(PROD_cols, QA_COLS) prodColsNotInQa
  FROM commonTableComparison
  """
  spark.sql(sql)
except Exception as e:
  err = {
    "sourceName": "Platinum Zone Validation: Common Table Comparison",
    "errorCode": "500",
    "errorDescription": "Failed while generating temp view commonTableComparisonExtended.",
    "errorClassName": e.__class__.__name__
  }
  error = json.dumps(err)
  log_notebook_error(notebookLogGuid, error, server, database, login, pwd)
  raise(e)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Write Dataframes to Silver Zone

# COMMAND ----------

sql = """
MERGE INTO silvergeneral.qaTablesNotInProd AS tgt
USING
(
  SELECT value AS fullyQualifiedTableName
  FROM qaTablesMissingFromProd
) AS src
ON src.fullyQualifiedTableName=tgt.fullyQualifiedTableName
WHEN NOT MATCHED THEN INSERT *
"""
print(sql)
spark.sql(sql)


# COMMAND ----------

sql = """
MERGE INTO silvergeneral.prodTablesNotInQA AS tgt
USING
(
  SELECT value AS fullyQualifiedTableName
  FROM prodTablesMissingFromQA
) AS SRC
ON src.fullyQualifiedTableName=tgt.fullyQualifiedTableName
WHEN NOT MATCHED THEN INSERT *
"""
print(sql)
spark.sql(sql)

# COMMAND ----------

sql = """
MERGE INTO silvergeneral.commonTableComparison AS tgt
USING commonTableComparisonExtended AS src
ON src.fullyQualifiedTableName=tgt.fullyQualifiedTableName
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
"""
print(sql)
spark.sql(sql)

# COMMAND ----------

# MAGIC %md #### Log Completion

# COMMAND ----------

log_notebook_end(notebookLogGuid, 0, server, database, login, pwd)
dbutils.notebook.exit("Succeeded")