# Databricks notebook source
# MAGIC %md
# MAGIC ## Declarative Data Pipelines
# MAGIC 
# MAGIC * Robust, tried and true repeatable assets (that have been successfully used on dozens of prior migrations)
# MAGIC * Implemented using a powerful declarative pipeline construct allowing for flexibility and data, schema, and requirements evolution
# MAGIC * Existing data source connectors  
# MAGIC * Robust cleansing and loading patterns
# MAGIC * Incorporated data quality validation (through Great Expectations and/or Delta Live Tables Expectations)
# MAGIC * Monitoring and oversight via operational dashboards written in Power BI and/or Databricks SQL
# MAGIC * Support for the most complex data requirements (including massively large source extracts, streaming, change data capture etc.)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Why?

# COMMAND ----------

# MAGIC %md
# MAGIC <img src="https://ktkdmdpe16public.blob.core.windows.net/public/declarativedatapipelines.png" width=1200>

# COMMAND ----------

# MAGIC %md
# MAGIC ### Use Cases

# COMMAND ----------

# MAGIC %md
# MAGIC #### Batch
# MAGIC 
# MAGIC <img src="https://ktkdmdpe16public.blob.core.windows.net/public/BatchETL.png" width=1200>

# COMMAND ----------

# MAGIC %md
# MAGIC #### Streaming
# MAGIC 
# MAGIC <img src="https://ktkdmdpe16public.blob.core.windows.net/public/StreamingETL.png" width=1200>

# COMMAND ----------

# MAGIC %md
# MAGIC #### ML/Predictions
# MAGIC 
# MAGIC <img src="https://ktkdmdpe16public.blob.core.windows.net/public/ML.png" width=1200>

# COMMAND ----------

# MAGIC %md
# MAGIC #### BI & Analytics
# MAGIC 
# MAGIC <img src="https://ktkdmdpe16public.blob.core.windows.net/public/BI.png" width=1200>

# COMMAND ----------

# MAGIC %md
# MAGIC ### Examples

# COMMAND ----------

# MAGIC %md
# MAGIC Our framework supports diverse and unique project types, including both batch and streaming, on-premises, cloud. These are just a few illustrative real world implementations of our declarative pipeline framework.  
# MAGIC <img src="https://ktkdmdpe16public.blob.core.windows.net/public/ETLOffload.png" width=800>
# MAGIC 
# MAGIC <img src="https://ktkdmdpe16public.blob.core.windows.net/public/IoT.png" width=800>
# MAGIC <img src="https://ktkdmdpe16public.blob.core.windows.net/public/TwitterML.png" width=800>
# MAGIC 
# MAGIC <img src="https://ktkdmdpe16public.blob.core.windows.net/public/APILed.png" width=800>
# MAGIC 
# MAGIC Other examples:
# MAGIC 
# MAGIC * Inventory, Sample and Explore thousands of new data sources for fitness and utility using ASCEND Spider crawling process
# MAGIC * ASCEND python package and Single Responsibility Notebooks bring easy to implement, reusable code for supporting the entire ML Lifecycle and abstract many of the complexities for maturing Data Science teams.
# MAGIC * Both automated (offline) and interactive (online) Exploratory Data Analysis (including general info, univariate, bivariate, and multivariate analysis)
# MAGIC * Highly parameterized, generic experiments support consistent model development and training in a variety of data science frameworks and libraries

# COMMAND ----------

# MAGIC %md
# MAGIC ### Configuration Review

# COMMAND ----------

# MAGIC %md
# MAGIC <img src="https://ktkdmdpe16public.blob.core.windows.net/public/ASCENDconfigurationreview.png" width=1200>

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Orchestration Metadata
# MAGIC 
# MAGIC Pipelines can usually be described in a dozen or so lines of SQL code.  
# MAGIC 
# MAGIC Code is stored in source control to enable versioning etc. 

# COMMAND ----------

# MAGIC %md
# MAGIC <img src="https://ktkdmdpe16public.blob.core.windows.net/public/sqlmetadatahydration.png" width=1200>

# COMMAND ----------

# MAGIC %md
# MAGIC ### Orchestration Schema

# COMMAND ----------

# MAGIC %md
# MAGIC <img src="https://ktkdmdpe16public.blob.core.windows.net/public/ASCENDDeclarativePipelineFrameworkLabels.png" width=1000>

# COMMAND ----------

# MAGIC %md
# MAGIC <img src="https://ktkdmdpe16public.blob.core.windows.net/public/ASCENDDeclarativePipelineFramework.png" width=600>
