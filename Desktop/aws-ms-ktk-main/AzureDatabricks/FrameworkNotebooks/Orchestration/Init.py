# Databricks notebook source
# MAGIC %md
# MAGIC # Init

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create Pyodbc Init Script

# COMMAND ----------

dbutils.fs.put("/databricks/pyodbc.sh","""
curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -
curl https://packages.microsoft.com/config/ubuntu/16.04/prod.list > /etc/apt/sources.list.d/mssql-release.list
apt-get update
ACCEPT_EULA=Y apt-get install msodbcsql17
apt-get -y install unixodbc-dev
sudo apt-get install python3-pip -y
pip3 install --upgrade pyodbc
""", True)

# COMMAND ----------

dbutils.fs.ls("/databricks/pyodbc.sh")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS bronze COMMENT 'Stores restricted data engineering (raw) data.';
# MAGIC CREATE DATABASE IF NOT EXISTS silverprotected COMMENT 'Stores query-ready ACID compliant tables with restricted or protected data sensitivity classifications.';
# MAGIC CREATE DATABASE IF NOT EXISTS silvergeneral COMMENT 'Stores query-ready ACID compliant tables with non-sensitive data sensitivity classification.';
# MAGIC CREATE DATABASE IF NOT EXISTS goldprotected COMMENT 'Stores business-ready ACID compliant tables with restricted or protected data sensitivity classifications.';
# MAGIC CREATE DATABASE IF NOT EXISTS goldgeneral COMMENT 'Stores business-ready ACID compliant tables with non-sensitive data sensitivity classification.';
# MAGIC CREATE DATABASE IF NOT EXISTS sandbox COMMENT 'Stores data used for exploration, prototyping, feature engineering, ML development, model tuning etc.';
# MAGIC CREATE DATABASE IF NOT EXISTS archive COMMENT 'Stores periodic snapshots of tables for validation and historical purposes.';