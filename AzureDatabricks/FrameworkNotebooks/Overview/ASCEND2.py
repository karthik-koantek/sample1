# Databricks notebook source
# MAGIC %md
# MAGIC ## Overview

# COMMAND ----------

# MAGIC %md
# MAGIC These slides describe Koantek's Framework and set of reusable assets for creating modern data platforms featuring Databricks.
# MAGIC 
# MAGIC Think of it as a layer of software that can be applied on top of your cloud of choice and Databricks to accelerate and improve any solution architecture: 
# MAGIC 
# MAGIC <img src="https://ktkdmdpe16public.blob.core.windows.net/public/ASCENDFramework.png" width=650>
# MAGIC 
# MAGIC Benefits fall into 3 main categories:
# MAGIC * **Configuration**: Metadata driven **declarative data pipelines** using robust and reusable components
# MAGIC * **Development**: High quality Modules and Libraries of tested reusable code
# MAGIC * **Runtime**: End-to-end logging gives visibility into the platform.  Robust orchestration that can handle errors and bad data

# COMMAND ----------

# MAGIC %md
# MAGIC Our reusable assets comprise and span all layers of the software stack:
# MAGIC 
# MAGIC <img src="https://ktkdmdpe16public.blob.core.windows.net/public/ASCENDFrameworkStack2.png" width=1000>

# COMMAND ----------

# MAGIC %md
# MAGIC We also describe our Framework in terms of Foundations, which are separate and distinct components that can be delivered independently:
# MAGIC 
# MAGIC <img src="https://ktkdmdpe16public.blob.core.windows.net/public/ASCENDFoundations.png" width=1000>
# MAGIC 
# MAGIC A customer's choice of foundations is largely dependent on their maturity and needs.  We can help in a variety of ways:
# MAGIC 
# MAGIC <img src="https://ktkdmdpe16public.blob.core.windows.net/public/ASCENDFoundations2.png" width=1000>

# COMMAND ----------

# MAGIC %md
# MAGIC Koantek has vast experience in delivering production grade Databricks implementations. 
# MAGIC 
# MAGIC <img src="https://ktkdmdpe16public.blob.core.windows.net/public/ASCENDOperationalizingDatabricks2.png" width=1000>

# COMMAND ----------

# MAGIC %md
# MAGIC <img src="https://ktkdmdpe16public.blob.core.windows.net/public/ASCENDadvantage.png" width=1000>

# COMMAND ----------

# MAGIC %md
# MAGIC ## Operations Dashboards

# COMMAND ----------

# MAGIC %md
# MAGIC Data Engineers need visibility into summarized data pipeline run information in order to be able to assess their health and quality (and especially so they know when they need to intervene and fix issues).  
# MAGIC 
# MAGIC <img src="https://ktkdmdpe16public.blob.core.windows.net/public/ASCENDDataPipelineVisibility.png" width=1000>

# COMMAND ----------

# MAGIC %md
# MAGIC While summary information is important, Engineers also need to be able to drill into pipeline run details to review configurations, review updated row counts, data quality expectations, performance, and errors.   
# MAGIC 
# MAGIC <img src="https://ktkdmdpe16public.blob.core.windows.net/public/ASCENDDataPipelineDetail.png" width=1200>

# COMMAND ----------

# MAGIC %md
# MAGIC Engineers need to be able to review pipeline performance in order to find bottlenecks, be alerted to longer (or shorter) than expected run times etc. 
# MAGIC <img src="https://ktkdmdpe16public.blob.core.windows.net/public/ASCENDDataPipelinePerformance.png" width=1200>

# COMMAND ----------

# MAGIC %md
# MAGIC Engineers also need to be able to be directed to the code generating specific errors and gather all the configuration used to be able to reproduce and fix any issues.
# MAGIC <img src="https://ktkdmdpe16public.blob.core.windows.net/public/ASCENDDataPipelineTroubleshooting.png" width=1200>

# COMMAND ----------

# MAGIC %md
# MAGIC The same logging data can be reviewed in SQL or Databricks IDE. 
# MAGIC 
# MAGIC <img src="https://ktkdmdpe16public.blob.core.windows.net/public/ASCENDSQLMetadataDB.png" width=1200>

# COMMAND ----------

# MAGIC %md
# MAGIC This will be discussed further in the next section, but our operational dashboards also allow for Engineers to configure and preview pipeline runs declaratively using our declarative data pipeline framework.  
# MAGIC <img src="https://ktkdmdpe16public.blob.core.windows.net/public/ASCENDconfigurationreview.png" width=1200>

# COMMAND ----------

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
# MAGIC <img src="https://ktkdmdpe16public.blob.core.windows.net/public/ASCENDdeclarativedatapipelines.png" width=1200>

# COMMAND ----------

# MAGIC %md
# MAGIC Our framework supports diverse and unique project types, including both batch and streaming, on-premises, cloud. These are just a few illustrative real world implementations of our declarative pipeline framework.  
# MAGIC <img src="https://ktkdmdpe16public.blob.core.windows.net/public/IoT.png" width=800>
# MAGIC <img src="https://ktkdmdpe16public.blob.core.windows.net/public/TwitterML.png" width=800>
# MAGIC 
# MAGIC <img src="https://ktkdmdpe16public.blob.core.windows.net/public/ASCENDAPILed.png" width=800>
# MAGIC <img src="https://ktkdmdpe16public.blob.core.windows.net/public/ASCENDETLOffload.png" width=800>
# MAGIC 
# MAGIC Other examples:
# MAGIC 
# MAGIC * Inventory, Sample and Explore new data sources for fitness and utility using ASCEND Spider crawling process
# MAGIC * ASCEND python package and Single Responsibility Notebooks bring easy to implement, reusable code for supporting the entire ML Lifecycle and abstract many of the complexities for maturing Data Science teams.
# MAGIC * Both automated (offline) and interactive (online) Exploratory Data Analysis (including general info, univariate, bivariate, and multivariate analysis)
# MAGIC * Highly parameterized, generic experiments support consistent model development and training in a variety of data science frameworks and libraries

# COMMAND ----------

# MAGIC %md
# MAGIC <img src="https://ktkdmdpe16public.blob.core.windows.net/public/ASCENDsqlmetadatahydration.png" width=1200>

# COMMAND ----------

# MAGIC %md
# MAGIC <img src="https://ktkdmdpe16public.blob.core.windows.net/public/ASCENDDeclarativePipelineFrameworkLabels.png" width=1000>

# COMMAND ----------

# MAGIC %md
# MAGIC <img src="https://ktkdmdpe16public.blob.core.windows.net/public/ASCENDDeclarativePipelineFramework.png" width=600>

# COMMAND ----------

# MAGIC %md
# MAGIC <img src="https://ktkdmdpe16public.blob.core.windows.net/public/ASCENDconfigurationreview.png" width=1200>

# COMMAND ----------

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
# MAGIC This wheel is forever evolving and is used ubiquitiously throughout our framework. The following is a summarized overview of some of the subpackages, classes and files. 
# MAGIC 
# MAGIC <img src="https://ktkdmdpe16public.blob.core.windows.net/public/Koantek Wheel.png" width=600>

# COMMAND ----------

# MAGIC %md
# MAGIC <img src="https://ktkdmdpe16public.blob.core.windows.net/public/engineering Class Diagram.png" width=800>

# COMMAND ----------

# MAGIC %md
# MAGIC <img src="https://ktkdmdpe16public.blob.core.windows.net/public/aiml Class Diagram.png" width=800>

# COMMAND ----------

# MAGIC %md
# MAGIC ## Productionizing & Operationalizing

# COMMAND ----------

# MAGIC %md
# MAGIC <img src="https://ktkdmdpe16public.blob.core.windows.net/public/ASCENDCICD2.png" width=800>

# COMMAND ----------

# MAGIC %md
# MAGIC <img src="https://ktkdmdpe16public.blob.core.windows.net/public/ASCENDIaC.png" width=900>

# COMMAND ----------

# MAGIC %md
# MAGIC <img src="https://ktkdmdpe16public.blob.core.windows.net/public/ASCENDSecurity.png" width=1000>

# COMMAND ----------

# MAGIC %md
# MAGIC <img src="https://ktkdmdpe16public.blob.core.windows.net/public/ASCENDSQLDACPACDeployment.png" width=1000>

# COMMAND ----------

# MAGIC %md
# MAGIC <img src="https://ktkdmdpe16public.blob.core.windows.net/public/ASCENDconfiguration.png" width=1000>

# COMMAND ----------

# MAGIC %md
# MAGIC <img src="https://ktkdmdpe16public.blob.core.windows.net/public/ASCENDtesting.png" width=1000>

# COMMAND ----------

# MAGIC %md
# MAGIC ## Architecture

# COMMAND ----------

# MAGIC %md
# MAGIC <img src="https://ktkdmdpe16public.blob.core.windows.net/public/ASCENDModernDataEstate.png" width=1000>

# COMMAND ----------

# MAGIC %md
# MAGIC <img src="https://ktkdmdpe16public.blob.core.windows.net/public/ASCENDDataLakeZones.png" width=1300>

# COMMAND ----------

# MAGIC %md
# MAGIC <img src="https://ktkdmdpe16public.blob.core.windows.net/public/ASCENDModernDataPlatform.png" width=1000>

# COMMAND ----------

# MAGIC %md
# MAGIC <img src="https://ktkdmdpe16public.blob.core.windows.net/public/ASCENDBatchETL.png" width=1200>

# COMMAND ----------

# MAGIC %md
# MAGIC <img src="https://ktkdmdpe16public.blob.core.windows.net/public/ASCENDStreamingETL.png" width=1200>

# COMMAND ----------

# MAGIC %md
# MAGIC <img src="https://ktkdmdpe16public.blob.core.windows.net/public/ASCENDML.png" width=1200>

# COMMAND ----------

# MAGIC %md
# MAGIC <img src="https://ktkdmdpe16public.blob.core.windows.net/public/ASCENDBI.png" width=1200>

# COMMAND ----------

# MAGIC %md
# MAGIC <img src="https://ktkdmdpe16public.blob.core.windows.net/public/ASCENDDeltaLakehouse.png" width=1000>

# COMMAND ----------

# MAGIC %md
# MAGIC <img src="https://ktkdmdpe16public.blob.core.windows.net/public/ASCENDdeltalake.png" width=1000>

# COMMAND ----------

# MAGIC %md
# MAGIC <img src="https://ktkdmdpe16public.blob.core.windows.net/public/ASCENDTwitterML.png" width=1000>

# COMMAND ----------

# MAGIC %md
# MAGIC <img src="https://ktkdmdpe16public.blob.core.windows.net/public/ASCENDIoT.png" width=1000>

# COMMAND ----------

# MAGIC %md
# MAGIC <img src="https://ktkdmdpe16public.blob.core.windows.net/public/ASCENDAPILed.png" width=800>

# COMMAND ----------

# MAGIC %md
# MAGIC <img src="https://ktkdmdpe16public.blob.core.windows.net/public/ASCENDETLOffload.png" width=1000>

# COMMAND ----------

# MAGIC %md
# MAGIC ## Other Topics

# COMMAND ----------

# MAGIC %md
# MAGIC <img src="https://ktkdmdpe16public.blob.core.windows.net/public/ASCENDsilos.png" width=1000>

# COMMAND ----------

# MAGIC %md
# MAGIC <img src="https://ktkdmdpe16public.blob.core.windows.net/public/ASCENDacceleratinginsights.png" width=1000>

# COMMAND ----------

# MAGIC %md
# MAGIC <img src="https://ktkdmdpe16public.blob.core.windows.net/public/ASCENDdatascienceiterative.png" width=1000>

# COMMAND ----------

# MAGIC %md
# MAGIC <img src="https://ktkdmdpe16public.blob.core.windows.net/public/ASCENDmlflow.png" width=1000>

# COMMAND ----------

# MAGIC %md
# MAGIC <img src="https://ktkdmdpe16public.blob.core.windows.net/public/ASCENDmlflowtracking.png" width=1000>

# COMMAND ----------

# MAGIC %md
# MAGIC <img src="https://ktkdmdpe16public.blob.core.windows.net/public/ASCENDdataengineering.png" width=1000>
