# Databricks notebook source
# MAGIC %md # Gold Zone Load
# MAGIC #### Initialize

# COMMAND ----------

import ktk
from ktk import utilities as u
import datetime, json
from pyspark.sql.functions import current_timestamp, lit, unix_timestamp
from pyspark.sql.types import TimestampType, BooleanType

# COMMAND ----------

dbutils.widgets.text(name="stepLogGuid", defaultValue="00000000-0000-0000-0000-000000000000", label="stepLogGuid")
dbutils.widgets.text(name="stepKey", defaultValue="-1", label="stepKey")
dbutils.widgets.text(name="externalSystem", defaultValue="", label="External System")
dbutils.widgets.text(name="schemaName", defaultValue="", label="Table Schema Name")
dbutils.widgets.text(name="sourceTableName", defaultValue="", label=" Source Table Name")
dbutils.widgets.dropdown(name="sourceDestination", defaultValue="silvergeneral", choices=["silverprotected", "silvergeneral"], label="Source Destination")
dbutils.widgets.text(name="destinationTableName", defaultValue="", label="Destination Table Name")
dbutils.widgets.text(name="purpose", defaultValue="general", label="Purpose")
dbutils.widgets.text(name="primaryKeyColumns", defaultValue="", label="Primary Key Cols")
dbutils.widgets.text(name="partitionCol", defaultValue="", label="Partition By Column")
dbutils.widgets.text(name="clusterCol", defaultValue="pk", label="Cluster By Column")
dbutils.widgets.text(name="clusterBuckets", defaultValue="50", label="Cluster Buckets")
dbutils.widgets.dropdown(name="loadType", defaultValue="Merge", choices=["Append", "Overwrite", "Merge", "MergeType2"], label="Load Type")
dbutils.widgets.text(name="mergeSchema", defaultValue="false", label="Merge or Overwrite Schema")
dbutils.widgets.dropdown(name="targetDestination", defaultValue="goldgeneral", choices=["silverprotected", "silvergeneral", "goldprotected", "goldgeneral"], label="Destination")
dbutils.widgets.text(name="adswerveTableNames", defaultValue="", label="Adswerve Table Name")

widgets = ["stepLogGuid", "stepKey", "purpose", "primaryKeyColumns", "partitionCol", "clusterCol", "clusterBuckets","loadType","mergeSchema", "adswerveTableName", "externalSystem", "schemaName", "sourceTableName", "sourceDestination", "destinationTableName", "targetDestination"]
secrets = []

# COMMAND ----------

snb = ktk.SingleResponsibilityNotebook(widgets, secrets)

# COMMAND ----------

def createsilverZoneTableNames(sourceDestination, externalSystem, schemaName, sourceTableName, adswerveTableName):
    return "{0}.`{1}_{2}_{3}_{4}`".format(sourceDestination, externalSystem, schemaName, adswerveTableName, sourceTableName)

silverZoneTableNames = {name.strip(): createsilverZoneTableNames(snb.sourceDestination, snb.externalSystem, snb.schemaName, snb.sourceTableName, name.strip()) for name in snb.adswerveTableName.split(',')}

# COMMAND ----------

pkcols = snb.primaryKeyColumns.replace(" ", "")
upsertTableName = "bronze.`goldload_{0}_{1}_{2}_staging`".format(snb.externalSystem, snb.schemaName, snb.destinationTableName)
dataPath = "{0}/{1}/{2}".format(snb.basePath, snb.purpose, snb.destinationTableName.replace(".", "_"))

p = {
  "pkcols": pkcols,
  "dataPath": dataPath,
  "upsertTableName": upsertTableName
}
parameters = json.dumps(snb.mergeAttributes(p))
snb.log_notebook_start(parameters)
print("Parameters:")
snb.displayAttributes()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Validation

# COMMAND ----------

if snb.pkcols == "" and snb.loadType in ["Merge", "MergeType2"]:
  errorDescription = "Primary key column(s) were not supplied and are required for Delta Merge."
  err = {
    "sourceName": "Gold Load: Validation",
    "errorCode": "100",
    "errorDescription": errorDescription
  }
  error = json.dumps(err)
  snb.log_notebook_error(error)
  raise ValueError(errorDescription)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Aggregation

# COMMAND ----------

def custom_table():
    hitsPageTable = spark.read.table(silverZoneTableNames['hitsPage']).createOrReplaceTempView('hitsPageTable')
    sessionsTable = spark.read.table(silverZoneTableNames['sessions']).createOrReplaceTempView('sessionsTable')
    df = spark.sql('''
with x as (
    select date, hour,
    sessionid, fullvisitorid
    from hitsPageTable
    where hostname not like '%saymedia%'
and (pagepath_nonenglish_chars_removed not like '%smarts-lead-gen%' and  cd24 not like '%utm_term=leads_%')
    group by 1,2,3,4
), y as (
    select t.date, t.hour, t.fullvisitorid, t.sessionid
    from hitsPageTable t
    where hostname not like '%saymedia%'
)
select
x.date, z.campaign, z.source, z.devicecategory, z.browser, z.operatingsystem, z.country, z.metro
, count(distinct x.fullvisitorid) as mlp_users, count(distinct y.fullvisitorid) as conv
, count(distinct y.fullvisitorid)/cast(count(distinct x.fullvisitorid) as float)  as conv_rate
from x
left join y on x.date = y.date and x.fullvisitorid = y.fullvisitorid and x.hour <= y.hour
left join sessionsTable z on x.date = z.date
    and x.sessionid = z.sessionid
group by 1,2,3,4,5,6,7,8
order by 10 desc''')
    return df

# COMMAND ----------

bronzeDFList = []
bronzeDFList.append(custom_table())

# COMMAND ----------

# MAGIC %md
# MAGIC #### Load Delta Table

# COMMAND ----------

try:
  spark.sql("""DROP TABLE IF EXISTS {0}.{1}""".format(snb.targetDestination, snb.destinationTableName))
  bronzeDFList[-1].write.saveAsTable(f"{snb.targetDestination}.{snb.destinationTableName}")
except Exception as e:
  err = {
    "sourceName": "Delta Load: Load Delta Table",
    "errorCode": "300",
    "errorDescription": e.__class__.__name__
  }
  error = json.dumps(err)
  snb.log_notebook_error(error)
  raise(e)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Log Completion

# COMMAND ----------

rows = bronzeDFList[-1].count()
snb.log_notebook_end(rows)
dbutils.notebook.exit("Succeeded")
