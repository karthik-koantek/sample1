# Databricks notebook source
# MAGIC %run "../Orchestration/Notebook Functions"

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW vDataTypeLookup
# MAGIC AS
# MAGIC SELECT DISTINCT dataType
# MAGIC FROM silverprotected.datacatalogsummary
# MAGIC UNION
# MAGIC SELECT 'ALL'
# MAGIC ORDER BY dataType ASC;
# MAGIC
# MAGIC CREATE OR REPLACE TEMPORARY VIEW vCatalogLookup
# MAGIC AS
# MAGIC SELECT DISTINCT Catalog
# MAGIC FROM silverprotected.datacatalogsummary
# MAGIC UNION
# MAGIC SELECT 'ALL'
# MAGIC ORDER BY Catalog ASC;
# MAGIC
# MAGIC CREATE OR REPLACE TEMPORARY VIEW vTableLookup
# MAGIC AS
# MAGIC SELECT DISTINCT Table
# MAGIC FROM silverprotected.datacatalogsummary
# MAGIC WHERE getArgument('pCatalog') = Catalog OR getArgument('pCatalog') = 'ALL'
# MAGIC UNION
# MAGIC SELECT 'ALL'
# MAGIC ORDER BY Table ASC;
# MAGIC
# MAGIC CREATE OR REPLACE TEMPORARY VIEW valueCounts
# MAGIC AS
# MAGIC SELECT Catalog, Table, Column, value, total
# MAGIC FROM silverprotected.datacatalogvaluecounts
# MAGIC WHERE
# MAGIC  (Catalog LIKE getArgument('pCatalog') OR getArgument('pCatalog') = 'ALL')
# MAGIC AND (UPPER(DataType) = UPPER(getArgument('pDataType')) OR getArgument('pDataType') = 'ALL')
# MAGIC AND (Table LIKE getArgument('pTable') OR getArgument('pTable') = 'ALL')
# MAGIC AND (UPPER(Column) LIKE UPPER(getArgument('pColumn')) OR getArgument('pColumn') = 'ALL')
# MAGIC ORDER BY Catalog, Table, Column, Total DESC;
# MAGIC
# MAGIC CREATE OR REPLACE TEMPORARY VIEW valueCountsCollected
# MAGIC AS
# MAGIC SELECT Catalog, Table, Column, collect_set(concat(value,':',total)) AS valueCounts
# MAGIC FROM valueCounts
# MAGIC GROUP BY Catalog, Table, Column
# MAGIC ORDER BY Catalog, Table, Column;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE WIDGET DROPDOWN pCatalog DEFAULT 'ALL' CHOICES SELECT Catalog FROM vCatalogLookup;
# MAGIC CREATE WIDGET DROPDOWN pDataType DEFAULT 'ALL' CHOICES SELECT dataType FROM vDataTypeLookup;
# MAGIC CREATE WIDGET DROPDOWN pTable DEFAULT 'ALL' CHOICES SELECT Table FROM vTableLookup;
# MAGIC CREATE WIDGET TEXT pColumn DEFAULT 'ALL';

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT Catalog, COUNT(DISTINCT Table) AS `Total Table Count`
# MAGIC FROM silverprotected.datacatalogsummary
# MAGIC GROUP BY Catalog

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT Catalog, Table, MAX(RecordCount) AS `Record Count`
# MAGIC FROM silverprotected.datacatalogsummary
# MAGIC WHERE
# MAGIC (Catalog = getArgument('pCatalog') OR getArgument('pCatalog') = 'ALL')
# MAGIC GROUP BY Catalog, Table
# MAGIC ORDER BY MAX(RecordCount) DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT Column, Selectivity
# MAGIC FROM silverprotected.datacatalogsummary
# MAGIC WHERE
# MAGIC  (Catalog LIKE getArgument('pCatalog') OR getArgument('pCatalog') = 'ALL')
# MAGIC AND (Table LIKE getArgument('pTable') OR getArgument('pTable') = 'ALL')
# MAGIC AND (UPPER(Column) LIKE UPPER(getArgument('pColumn')) OR getArgument('pColumn') = 'ALL')
# MAGIC ORDER BY Selectivity ASC;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT s.Catalog, s.Table, s.Column, s.DataType, s.RecordCount, s.PercentNulls, s.PercentZeros, s.MinimumValue, s.AvgValue, s.StdDevValue, s.MaximumValue, vc.ValueCounts
# MAGIC FROM silverprotected.datacatalogsummary s
# MAGIC LEFT JOIN valueCountsCollected vc ON s.Catalog = vc.Catalog AND s.Table = vc.Table AND s.Column = vc.Column
# MAGIC WHERE
# MAGIC  (s.Catalog LIKE getArgument('pCatalog') OR getArgument('pCatalog') = 'ALL')
# MAGIC AND (UPPER(s.DataType) = UPPER(getArgument('pDataType')) OR getArgument('pDataType') = 'ALL')
# MAGIC AND (s.Table LIKE getArgument('pTable') OR getArgument('pTable') = 'ALL')
# MAGIC AND (UPPER(s.Column) LIKE UPPER(getArgument('pColumn')) OR getArgument('pColumn') = 'ALL');