# Databricks notebook source
# MAGIC %md
# MAGIC ## Overview

# COMMAND ----------

# MAGIC %md
# MAGIC These slides describe Koantek's Framework and set of reusable assets for creating modern data platforms on Azure.
# MAGIC 
# MAGIC Think of it as a layer of software that can be applied on top of Azure and Azure Databricks to accelerate and improve any solution architecture: 
# MAGIC 
# MAGIC 
# MAGIC <img src="https://ktkdmdpe16public.blob.core.windows.net/public/KoantekFramework.png" width=800>
# MAGIC <img src="https://ktkdmdpe16public.blob.core.windows.net/public/ASCENDFramework2.png" width=800>
# MAGIC 
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
# MAGIC Framework Foundations:
# MAGIC 
# MAGIC <img src="https://ktkdmdpe16public.blob.core.windows.net/public/Foundations.png" width=1000>
# MAGIC <img src="https://ktkdmdpe16public.blob.core.windows.net/public/ASCENDFoundations.png" width=1000>
# MAGIC <img src="https://ktkdmdpe16public.blob.core.windows.net/public/ASCENDFoundations2.png" width=1000>

# COMMAND ----------

# MAGIC %md
# MAGIC <img src="https://ktkdmdpe16public.blob.core.windows.net/public/OperationalizingDatabricks2.png" width=1000>
# MAGIC <img src="https://ktkdmdpe16public.blob.core.windows.net/public/ASCENDOperationalizingDatabricks2.png" width=1000>

# COMMAND ----------

# MAGIC %md
# MAGIC <img src="https://ktkdmdpe16public.blob.core.windows.net/public/advantage.png" width=1000>
# MAGIC <img src="https://ktkdmdpe16public.blob.core.windows.net/public/ASCENDadvantage.png" width=1000>

# COMMAND ----------

# MAGIC %md
# MAGIC ## Operations Dashboards

# COMMAND ----------

# MAGIC %md
# MAGIC <img src="https://ktkdmdpe16public.blob.core.windows.net/public/DataPipelineVisibility.png" width=1200>
# MAGIC <img src="https://ktkdmdpe16public.blob.core.windows.net/public/ASCENDDataPipelineVisibility.png" width=1000>

# COMMAND ----------

# MAGIC %md
# MAGIC <img src="https://ktkdmdpe16public.blob.core.windows.net/public/DataPipelineDetail.png" width=1200>
# MAGIC <img src="https://ktkdmdpe16public.blob.core.windows.net/public/ASCENDDataPipelineDetail.png" width=1200>

# COMMAND ----------

# MAGIC %md
# MAGIC <img src="https://ktkdmdpe16public.blob.core.windows.net/public/DataPipelinePerformance.png" width=1200>
# MAGIC <img src="https://ktkdmdpe16public.blob.core.windows.net/public/ASCENDDataPipelinePerformance.png" width=1200>

# COMMAND ----------

# MAGIC %md
# MAGIC <img src="https://ktkdmdpe16public.blob.core.windows.net/public/DataPipelineTroubleshooting.png" width=1200>
# MAGIC <img src="https://ktkdmdpe16public.blob.core.windows.net/public/ASCENDDataPipelineTroubleshooting.png" width=1200>

# COMMAND ----------

# MAGIC %md
# MAGIC <img src="https://ktkdmdpe16public.blob.core.windows.net/public/SQLMetadataDB.png" width=1200>
# MAGIC <img src="https://ktkdmdpe16public.blob.core.windows.net/public/ASCENDSQLMetadataDB.png" width=1200>

# COMMAND ----------

# MAGIC %md
# MAGIC <img src="https://ktkdmdpe16public.blob.core.windows.net/public/configurationreview.png" width=1200>
# MAGIC <img src="https://ktkdmdpe16public.blob.core.windows.net/public/ASCENDconfigurationreview.png" width=1200>

# COMMAND ----------

# MAGIC %md
# MAGIC ## Declarative Data Pipelines

# COMMAND ----------

# MAGIC %md
# MAGIC <img src="https://ktkdmdpe16public.blob.core.windows.net/public/declarativedatapipelines.png" width=1200>
# MAGIC <img src="https://ktkdmdpe16public.blob.core.windows.net/public/ASCENDdeclarativedatapipelines.png" width=1200>

# COMMAND ----------

# MAGIC %md
# MAGIC <img src="https://ktkdmdpe16public.blob.core.windows.net/public/sqlmetadatahydration.png" width=1200>
# MAGIC <img src="https://ktkdmdpe16public.blob.core.windows.net/public/ASCENDsqlmetadatahydration.png" width=1200>

# COMMAND ----------

# MAGIC %md
# MAGIC <img src="https://ktkdmdpe16public.blob.core.windows.net/public/configurationreview.png" width=1200>
# MAGIC <img src="https://ktkdmdpe16public.blob.core.windows.net/public/ASCENDconfigurationreview.png" width=1200>

# COMMAND ----------

# MAGIC %md
# MAGIC <img src="https://ktkdmdpe16public.blob.core.windows.net/public/DeclarativePipelineFrameworkLabels.png" width=1000>
# MAGIC <img src="https://ktkdmdpe16public.blob.core.windows.net/public/ASCENDDeclarativePipelineFrameworkLabels.png" width=1000>

# COMMAND ----------

# MAGIC %md
# MAGIC <img src="https://ktkdmdpe16public.blob.core.windows.net/public/DeclarativePipelineFramework.png" width=600>
# MAGIC 
# MAGIC <img src="https://ktkdmdpe16public.blob.core.windows.net/public/ASCENDDeclarativePipelineFramework.png" width=600>

# COMMAND ----------

# MAGIC %md
# MAGIC ## Productionizing & Operationalizing

# COMMAND ----------

# MAGIC %md
# MAGIC <img src="https://ktkdmdpe16public.blob.core.windows.net/public/CICD2.png" width=800>
# MAGIC <img src="https://ktkdmdpe16public.blob.core.windows.net/public/ASCENDCICD2.png" width=800>

# COMMAND ----------

# MAGIC %md
# MAGIC <img src="https://ktkdmdpe16public.blob.core.windows.net/public/IaC.png" width=900>
# MAGIC <img src="https://ktkdmdpe16public.blob.core.windows.net/public/ASCENDIaC.png" width=900>

# COMMAND ----------

# MAGIC %md
# MAGIC <img src="https://ktkdmdpe16public.blob.core.windows.net/public/Security.png" width=1000>
# MAGIC <img src="https://ktkdmdpe16public.blob.core.windows.net/public/ASCENDSecurity.png" width=1000>

# COMMAND ----------

# MAGIC %md
# MAGIC <img src="https://ktkdmdpe16public.blob.core.windows.net/public/SQLDACPACDeployment.png" width=1000>
# MAGIC <img src="https://ktkdmdpe16public.blob.core.windows.net/public/ASCENDSQLDACPACDeployment.png" width=1000>

# COMMAND ----------

# MAGIC %md
# MAGIC <img src="https://ktkdmdpe16public.blob.core.windows.net/public/configuration.png" width=1000>
# MAGIC <img src="https://ktkdmdpe16public.blob.core.windows.net/public/ASCENDconfiguration.png" width=1000>

# COMMAND ----------

# MAGIC %md
# MAGIC <img src="https://ktkdmdpe16public.blob.core.windows.net/public/testing.png" width=1000>
# MAGIC <img src="https://ktkdmdpe16public.blob.core.windows.net/public/ASCENDtesting.png" width=1000>

# COMMAND ----------

# MAGIC %md
# MAGIC ## Architecture

# COMMAND ----------

# MAGIC %md
# MAGIC <img src="https://ktkdmdpe16public.blob.core.windows.net/public/ModernDataEstate.png" width=1000>
# MAGIC <img src="https://ktkdmdpe16public.blob.core.windows.net/public/ASCENDModernDataEstate.png" width=1000>

# COMMAND ----------

# MAGIC %md
# MAGIC <img src="https://ktkdmdpe16public.blob.core.windows.net/public/DataLakeZones.png" width=1300>
# MAGIC <img src="https://ktkdmdpe16public.blob.core.windows.net/public/ASCENDDataLakeZones.png" width=1300>

# COMMAND ----------

# MAGIC %md
# MAGIC <img src="https://ktkdmdpe16public.blob.core.windows.net/public/FrameworkClassDiagram.png" width=1200>

# COMMAND ----------

# MAGIC %md
# MAGIC <img src="https://ktkdmdpe16public.blob.core.windows.net/public/ModernDataPlatform.png" width=1000>
# MAGIC <img src="https://ktkdmdpe16public.blob.core.windows.net/public/ASCENDModernDataPlatform.png" width=1000>

# COMMAND ----------

# MAGIC %md
# MAGIC <img src="https://ktkdmdpe16public.blob.core.windows.net/public/BatchETL.png" width=1200>
# MAGIC <img src="https://ktkdmdpe16public.blob.core.windows.net/public/ASCENDBatchETL.png" width=1200>

# COMMAND ----------

# MAGIC %md
# MAGIC <img src="https://ktkdmdpe16public.blob.core.windows.net/public/StreamingETL.png" width=1200>
# MAGIC <img src="https://ktkdmdpe16public.blob.core.windows.net/public/ASCENDStreamingETL.png" width=1200>

# COMMAND ----------

# MAGIC %md
# MAGIC <img src="https://ktkdmdpe16public.blob.core.windows.net/public/ML.png" width=1200>
# MAGIC <img src="https://ktkdmdpe16public.blob.core.windows.net/public/ASCENDML.png" width=1200>

# COMMAND ----------

# MAGIC %md
# MAGIC <img src="https://ktkdmdpe16public.blob.core.windows.net/public/BI.png" width=1200>
# MAGIC <img src="https://ktkdmdpe16public.blob.core.windows.net/public/ASCENDBI.png" width=1200>

# COMMAND ----------

# MAGIC %md
# MAGIC <img src="https://ktkdmdpe16public.blob.core.windows.net/public/DeltaLakehouse.png" width=1000>
# MAGIC <img src="https://ktkdmdpe16public.blob.core.windows.net/public/ASCENDDeltaLakehouse.png" width=1000>

# COMMAND ----------

# MAGIC %md
# MAGIC <img src="https://ktkdmdpe16public.blob.core.windows.net/public/deltalake.png" width=1000>
# MAGIC <img src="https://ktkdmdpe16public.blob.core.windows.net/public/ASCENDdeltalake.png" width=1000>

# COMMAND ----------

# MAGIC %md
# MAGIC <img src="https://ktkdmdpe16public.blob.core.windows.net/public/TwitterML.png" width=1000>
# MAGIC <img src="https://ktkdmdpe16public.blob.core.windows.net/public/ASCENDTwitterML.png" width=1000>

# COMMAND ----------

# MAGIC %md
# MAGIC <img src="https://ktkdmdpe16public.blob.core.windows.net/public/IoT.png" width=1000>
# MAGIC <img src="https://ktkdmdpe16public.blob.core.windows.net/public/ASCENDIoT.png" width=1000>

# COMMAND ----------

# MAGIC %md
# MAGIC <img src="https://ktkdmdpe16public.blob.core.windows.net/public/APILed.png" width=800>
# MAGIC <img src="https://ktkdmdpe16public.blob.core.windows.net/public/ASCENDAPILed.png" width=800>

# COMMAND ----------

# MAGIC %md
# MAGIC <img src="https://ktkdmdpe16public.blob.core.windows.net/public/ETLOffload.png" width=1000>
# MAGIC <img src="https://ktkdmdpe16public.blob.core.windows.net/public/ASCENDETLOffload.png" width=1000>

# COMMAND ----------

# MAGIC %md
# MAGIC ## Other Topics

# COMMAND ----------

# MAGIC %md
# MAGIC <img src="https://ktkdmdpe16public.blob.core.windows.net/public/silos.png" width=1000>
# MAGIC <img src="https://ktkdmdpe16public.blob.core.windows.net/public/ASCENDsilos.png" width=1000>

# COMMAND ----------

# MAGIC %md
# MAGIC <img src="https://ktkdmdpe16public.blob.core.windows.net/public/acceleratinginsights.png" width=1000>
# MAGIC <img src="https://ktkdmdpe16public.blob.core.windows.net/public/ASCENDacceleratinginsights.png" width=1000>

# COMMAND ----------

# MAGIC %md
# MAGIC <img src="https://ktkdmdpe16public.blob.core.windows.net/public/datascienceiterative.png" width=1000>
# MAGIC <img src="https://ktkdmdpe16public.blob.core.windows.net/public/ASCENDdatascienceiterative.png" width=1000>

# COMMAND ----------

# MAGIC %md
# MAGIC <img src="https://ktkdmdpe16public.blob.core.windows.net/public/mlflow.png" width=1000>
# MAGIC <img src="https://ktkdmdpe16public.blob.core.windows.net/public/ASCENDmlflow.png" width=1000>

# COMMAND ----------

# MAGIC %md
# MAGIC <img src="https://ktkdmdpe16public.blob.core.windows.net/public/mlflowtracking.png" width=1000>
# MAGIC <img src="https://ktkdmdpe16public.blob.core.windows.net/public/ASCENDmlflowtracking.png" width=1000>

# COMMAND ----------

# MAGIC %md
# MAGIC <img src="https://ktkdmdpe16public.blob.core.windows.net/public/dataengineering.png" width=1000>
# MAGIC <img src="https://ktkdmdpe16public.blob.core.windows.net/public/ASCENDdataengineering.png" width=1000>
