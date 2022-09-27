# Databricks notebook source
# MAGIC %md
# MAGIC # Table Access Control Data Object Privileges

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create Databases

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS bronze COMMENT 'Stores restricted data engineering (raw) data.';
# MAGIC CREATE DATABASE IF NOT EXISTS silverprotected COMMENT 'Stores query-ready ACID compliant tables with restricted or protected data sensitivity classifications.';
# MAGIC CREATE DATABASE IF NOT EXISTS silvergeneral COMMENT 'Stores query-ready ACID compliant tables with non-sensitive data sensitivity classification.';
# MAGIC CREATE DATABASE IF NOT EXISTS goldprotected COMMENT 'Stores business-ready ACID compliant tables with restricted or protected data sensitivity classifications.';
# MAGIC CREATE DATABASE IF NOT EXISTS goldgeneral COMMENT 'Stores business-ready ACID compliant tables with non-sensitive data sensitivity classification.';
# MAGIC CREATE DATABASE IF NOT EXISTS sandbox COMMENT 'Stores data used for exploration, prototyping, feature engineering, ML development, model tuning etc.';
# MAGIC CREATE DATABASE IF NOT EXISTS archive COMMENT 'Stores periodic snapshots of tables for validation and historical purposes.';

# COMMAND ----------

# MAGIC %md
# MAGIC #### Database Permissions

# COMMAND ----------

# MAGIC %sql
# MAGIC GRANT READ_METADATA, SELECT ON DATABASE archive TO Readers;
# MAGIC GRANT READ_METADATA, SELECT ON DATABASE bronze TO Readers;
# MAGIC GRANT READ_METADATA, SELECT ON DATABASE default TO Readers;
# MAGIC GRANT READ_METADATA, SELECT ON DATABASE goldgeneral TO Readers;
# MAGIC GRANT READ_METADATA, SELECT ON DATABASE goldprotected TO Readers;
# MAGIC GRANT READ_METADATA, SELECT ON DATABASE sandbox TO Readers;
# MAGIC GRANT READ_METADATA, SELECT ON DATABASE silvergeneral TO Readers;
# MAGIC GRANT READ_METADATA, SELECT ON DATABASE silverprotected TO Readers;

# COMMAND ----------

# MAGIC %sql
# MAGIC GRANT ALL PRIVILEGES ON DATABASE archive TO Contributors;
# MAGIC GRANT ALL PRIVILEGES ON DATABASE bronze TO Contributors;
# MAGIC GRANT ALL PRIVILEGES ON DATABASE default TO Contributors;
# MAGIC GRANT ALL PRIVILEGES ON DATABASE goldgeneral TO Contributors;
# MAGIC GRANT ALL PRIVILEGES ON DATABASE goldprotected TO Contributors;
# MAGIC GRANT ALL PRIVILEGES ON DATABASE sandbox TO Contributors;
# MAGIC GRANT ALL PRIVILEGES ON DATABASE silvergeneral TO Contributors;
# MAGIC GRANT ALL PRIVILEGES ON DATABASE silverprotected TO Contributors;

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER DATABASE archive OWNER TO admins;
# MAGIC ALTER DATABASE bronze OWNER TO admins;
# MAGIC ALTER DATABASE default OWNER TO admins;
# MAGIC ALTER DATABASE goldgeneral OWNER TO admins;
# MAGIC ALTER DATABASE goldprotected OWNER TO admins;
# MAGIC ALTER DATABASE sandbox OWNER TO admins;
# MAGIC ALTER DATABASE silvergeneral OWNER TO admins;
# MAGIC ALTER DATABASE silverprotected OWNER TO admins;
