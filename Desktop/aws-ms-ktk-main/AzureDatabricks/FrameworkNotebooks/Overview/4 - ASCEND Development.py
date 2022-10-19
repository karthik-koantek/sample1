# Databricks notebook source
# MAGIC %md
# MAGIC ## Development
# MAGIC 
# MAGIC Wherever possible Koantek develops reusable code for its solutions and organizes and maintains it into a distributable Python module.  
# MAGIC 
# MAGIC Benefits:
# MAGIC * Code is in a single place, and thus made more consise, readable, easy to upgrade and enhance
# MAGIC * Easy to distribute and import onto Databricks Clusters and Jobs, Databricks Notebooks or python scripts
# MAGIC * Fast installation
# MAGIC * Caching for testing and continuous integration
# MAGIC * Consistent installs across platforms and machines

# COMMAND ----------

# MAGIC %md
# MAGIC ### Koantek Wheel

# COMMAND ----------

# MAGIC %md
# MAGIC This wheel is forever evolving and is used ubiquitiously throughout our framework. The following is a summarized overview of some of the subpackages, classes and files. 
# MAGIC 
# MAGIC <img src="https://ktkdmdpe16public.blob.core.windows.net/public/Koantek Wheel.png" width=600>

# COMMAND ----------

# MAGIC %md
# MAGIC ### Engineering Subpackage
# MAGIC 
# MAGIC * Implements Koantek's Declarative Pipeline Framework
# MAGIC * Automatically collects logging for Operational Dashboards
# MAGIC * Connects to Data Lake Storage and internal objects 
# MAGIC * Secrets/Widgets
# MAGIC * Establishes consistency (with respect to data lake paths, patterns etc.)

# COMMAND ----------

# MAGIC %md
# MAGIC <img src="https://ktkdmdpe16public.blob.core.windows.net/public/engineering Class Diagram.png" width=1200>

# COMMAND ----------

# MAGIC %md
# MAGIC ### EDA Subpackage
# MAGIC 
# MAGIC * Automated Exploratory Data Analysis saves developer time and allows them to focus on what is critical
# MAGIC * Interactive EDA
# MAGIC * Feature Engineering 

# COMMAND ----------

# MAGIC %md
# MAGIC <img src="https://ktkdmdpe16public.blob.core.windows.net/public/eda.png" width=1400>

# COMMAND ----------

# MAGIC %md
# MAGIC ### AI/ML Subpackage
# MAGIC 
# MAGIC * Implements AI/ML experiements with MLFlow integration.  
# MAGIC * Greatly simplifies ML model development and ensemble methods
# MAGIC * Consistency with respect to data prep and feature engineering, logging etc.

# COMMAND ----------

# MAGIC %md
# MAGIC <img src="https://ktkdmdpe16public.blob.core.windows.net/public/aiml Class Diagram.png" width=1600>

# COMMAND ----------

# MAGIC %md
# MAGIC ### DevOps Subpackage
# MAGIC 
# MAGIC * CI/CD integration
# MAGIC * Interact with the Databricks REST API from within Databricks 
