# Databricks notebook source
# MAGIC %md
# MAGIC # UAT_ML_Teardown

# COMMAND ----------

# MAGIC %md
# MAGIC #### Initialize

# COMMAND ----------

import ktk
from ktk import utilities as u
import json

# COMMAND ----------

nb = ktk.Notebook()
print("Parameters:")
nb.displayAttributes()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Teardown

# COMMAND ----------

# MAGIC %sql
# MAGIC --DROP TABLE IF EXISTS sandbox.bikesharingml;
# MAGIC --DROP TABLE IF EXISTS sandbox.adult;
# MAGIC DROP TABLE IF EXISTS silvergeneral.uat_bikesharingpredictions;
# MAGIC --DROP TABLE IF EXISTS silvergeneral.uat_bikesharingpredictionsstreaming;
# MAGIC DROP TABLE IF EXISTS bronze.uat_bikesharingpredictions_upsert;

# COMMAND ----------

#sandboxPath = "{0}/uat_ml".format(nb.SandboxBasePath)
#dbutils.fs.rm(sandboxPath, True)

# COMMAND ----------

silvergeneralPath = "{0}/modelinference/uat_bikesharingpredictions".format(nb.SilverGeneralBasePath)
dbutils.fs.rm(silvergeneralPath, True)
#silvergeneralPath = "{0}/modelinference/uat_bikesharingpredictionsstreaming".format(nb.SilverGeneralBasePath)
#dbutils.fs.rm(silvergeneralPath, True)

# COMMAND ----------

#checkPointPath = "{0}/checkpoint/modelinference/uat_bikesharingpredictionsstreaming".format(nb.SilverGeneralBasePath)
#dbutils.fs.rm(checkPointPath, True)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Initialize Input Tables

# COMMAND ----------

# MAGIC %md
# MAGIC ##### bikesharingml

# COMMAND ----------

#from pyspark.sql.functions import *
#from pyspark.sql.types import * 
#
#path = "{0}/bikesharingml/bikesharingml".format(sandboxPath)
#
#df = spark \
#  .read \
#  .option("Header", True) \
#  .csv("dbfs:/databricks-datasets/bikeSharing/data-001/day.csv") \
#  .withColumn("cnt", col("cnt").cast("int")) \
#  .drop("casual", "registered") \
#  .withColumn("temp", col("temp").cast("float")) \
#  .withColumn("atemp", col("atemp").cast("float")) \
#  .withColumn("hum", col("hum").cast("float")) \
#  .withColumn("windSpeed", col("windSpeed").cast("float"))
#
#df \
#  .write \
#  .mode("OVERWRITE") \
#  .format("DELTA") \
#  .option("overwriteSchema", True) \
#  .save(path)

# COMMAND ----------

#sql = """
#CREATE TABLE IF NOT EXISTS sandbox.bikesharingml
#LOCATION '{0}'
#""".format(path)
#spark.sql(sql)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### adult

# COMMAND ----------

#path = "{0}/adult/adult".format(sandboxPath)
#dbutils.fs.rm(path, True)
#
#schema = """`age` DOUBLE,
#`workclass` STRING,
#`fnlwgt` DOUBLE,
#`education` STRING,
#`education_num` DOUBLE,
#`marital_status` STRING,
#`occupation` STRING,
#`relationship` STRING,
#`race` STRING,
#`sex` STRING,
#`capital_gain` DOUBLE,
#`capital_loss` DOUBLE,
#`hours_per_week` DOUBLE,
#`native_country` STRING,
#`income` STRING"""
#
#dfList = []
#dfList.append(
#  spark \
#    .read \
#    .csv("/databricks-datasets/adult/adult.data", schema=schema)
#)
#
#dfList[-1] \
#  .write \
#  .mode("OVERWRITE") \
#  .format("DELTA") \
#  .option("overwriteSchema", True) \
#  .save(path)

# COMMAND ----------

#sql = """
#CREATE TABLE IF NOT EXISTS sandbox.adult
#LOCATION '{0}'
#""".format(path)
#spark.sql(sql)
