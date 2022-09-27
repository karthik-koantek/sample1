# Databricks notebook source
# MAGIC %md
# MAGIC # UAT_SQL

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
# MAGIC DROP TABLE IF EXISTS silverprotected.adventureworkslt_saleslt_address;
# MAGIC DROP TABLE IF EXISTS silvergeneral.adventureworkslt_saleslt_productmodel;
# MAGIC DROP TABLE IF EXISTS silvergeneral.adventureworkslt_saleslt_salesorderheader;
# MAGIC DROP TABLE IF EXISTS silvergeneral.adventureworkslt_saleslt_buildversion;
# MAGIC DROP TABLE IF EXISTS bronze.adventureworkslt_saleslt_address_staging;
# MAGIC DROP TABLE IF EXISTS bronze.adventureworkslt_saleslt_productmodel_staging;
# MAGIC DROP TABLE IF EXISTS bronze.adventureworkslt_saleslt_salesorderheader_staging;
# MAGIC DROP TABLE IF EXISTS bronze.adventureworkslt_saleslt_buildversion_staging;
# MAGIC DROP TABLE IF EXISTS goldgeneral.adventureworkslt_saleslt_salesorderheader;
# MAGIC DROP TABLE IF EXISTS goldgeneral.UAT_SQL_sales

# COMMAND ----------

bronzePath = "{0}/adventureworkslt".format(nb.BronzeBasePath)
schemasPath = "{0}/schemas/adventureworkslt".format(nb.BronzeBasePath)
silverGeneralPath = "{0}/adventureworkslt".format(nb.SilverGeneralBasePath)
silverProtectedPath = "{0}/adventureworkslt".format(nb.SilverProtectedBasePath)
goldGeneralPath = "{0}/uatsql".format(nb.GoldGeneralBasePath)

dbutils.fs.rm(bronzePath, True)
dbutils.fs.rm(schemasPath, True)
dbutils.fs.rm(silverGeneralPath, True)
dbutils.fs.rm(silverProtectedPath, True)
dbutils.fs.rm(goldGeneralPath, True)
