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
# MAGIC #### Gold Load Views

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW silvergeneral.vadventureworkslt_saleslt_salesorderheader
# MAGIC AS
# MAGIC SELECT AccountNumber, BillToAddressID, CustomerID, DueDate_ts, Freight, PurchaseOrderNumber, OrderDate_ts, OnlineOrderFlag, ShipDate_ts 
# MAGIC FROM silvergeneral.adventureworkslt_saleslt_salesorderheader

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW silvergeneral.UAT_SQL_sales
# MAGIC AS
# MAGIC SELECT soh.AccountNumber, soh.CustomerID, soh.DueDate, soh.Freight, a.AddressLine1, a.AddressLine2, a.City, a.CountryRegion, a.PostalCode, a.StateProvince
# MAGIC FROM silvergeneral.adventureworkslt_saleslt_salesorderheader soh
# MAGIC JOIN silverprotected.adventureworkslt_saleslt_address a ON soh.ShipToAddressID = a.AddressID

# COMMAND ----------

# MAGIC %md
# MAGIC #### Gold Zone Table

# COMMAND ----------

sql = """
CREATE TABLE IF NOT EXISTS goldgeneral.adventureworkslt_saleslt_salesorderheader
(
  AccountNumber STRING NOT NULL,
  BillToAddressID BIGINT,
  CustomerID BIGINT,
  DueDate_ts TIMESTAMP,
  Freight DOUBLE,
  PurchaseOrderNumber STRING,
  OrderDate_ts TIMESTAMP,
  OnlineOrderFlag BOOLEAN,
  ShipDate_ts TIMESTAMP,
  pk STRING NOT NULL,
  isActive BOOLEAN NOT NULL,
  effectiveStartDate TIMESTAMP,
  effectiveEndDate TIMESTAMP
) USING delta
LOCATION '{0}/uatsql/goldgeneral_adventureworkslt_saleslt_salesorderheader'
""".format(nb.GoldGeneralBasePath)
spark.sql(sql)
