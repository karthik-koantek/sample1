# Databricks notebook source
# MAGIC %md
# MAGIC # UAT_ExternalFiles

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
# MAGIC DROP TABLE IF EXISTS bronze.externalblobstore_noaa_noaaapi_staging;
# MAGIC DROP TABLE IF EXISTS bronze.externalblobstore_userdata_userdata1_staging;
# MAGIC DROP TABLE IF EXISTS bronze.externalblobstore_youtube_youtube_staging;
# MAGIC DROP TABLE IF EXISTS bronze.externalblobstore_menu_breakfast_staging;
# MAGIC DROP TABLE IF EXISTS silvergeneral.externalblobstore_noaa_noaaapi;
# MAGIC DROP TABLE IF EXISTS silvergeneral.externalblobstore_userdata_userdata1;
# MAGIC DROP TABLE IF EXISTS silvergeneral.externalblobstore_youtube_youtube;
# MAGIC DROP TABLE IF EXISTS silvergeneral.externalblobstore_menu_breakfast;
# MAGIC DROP TABLE IF EXISTS sandbox.userdata_userdata1;
# MAGIC DROP TABLE IF EXISTS sandbox.noaa_noaaapi;

# COMMAND ----------

bronzePath = "{0}/ExternalBlobStore".format(nb.BronzeBasePath)
schemasPath = "{0}/schemas/ExternalBlobStore".format(nb.BronzeBasePath)
silverGeneralPath = "{0}/ExternalBlobStore".format(nb.SilverGeneralBasePath)
goldGeneralPath = "{0}/export/silvergeneral.externalblobstore_youtube_youtube".format(nb.GoldGeneralBasePath)
sandboxPath = "{0}/uatgoldzone".format(nb.SandboxBasePath)

dbutils.fs.rm(bronzePath, True)
dbutils.fs.rm(schemasPath, True)
dbutils.fs.rm(silverGeneralPath, True)
dbutils.fs.rm(goldGeneralPath, True)
dbutils.fs.rm(sandboxPath, True)
