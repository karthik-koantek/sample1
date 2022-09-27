# Databricks notebook source
# MAGIC %md
# MAGIC ## Operations Dashboards

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pipeline Visiblity

# COMMAND ----------

# MAGIC %md
# MAGIC Data Engineers need visibility into summarized data pipeline run information in order to be able to assess their health and quality (and especially so they know when they need to intervene and fix issues).  
# MAGIC 
# MAGIC <img src="https://ktkdmdpe16public.blob.core.windows.net/public/ASCENDDataPipelineVisibility.png" width=1000>

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pipeline Detail

# COMMAND ----------

# MAGIC %md
# MAGIC While summary information is important, Engineers also need to be able to drill into pipeline run details to review configurations, review updated row counts, data quality expectations, performance, and errors.   
# MAGIC 
# MAGIC <img src="https://ktkdmdpe16public.blob.core.windows.net/public/ASCENDDataPipelineDetail.png" width=1200>

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pipeline Performance

# COMMAND ----------

# MAGIC %md
# MAGIC Engineers need to be able to review pipeline performance in order to find bottlenecks, be alerted to longer (or shorter) than expected run times etc. 
# MAGIC <img src="https://ktkdmdpe16public.blob.core.windows.net/public/ASCENDDataPipelinePerformance.png" width=1200>

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pipeline Troubleshooting

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
# MAGIC ### Pipeline Validation

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Decision makers want to be confident in the quality of the data used for modeling and analytics purposes.  
# MAGIC 
# MAGIC The ASCEND Framework supports building automated tests for data pipelines.  These data quality expectations can be reviewed immediately after pipelne runs to find and mitigate data quality issues before data makes its way to the business. 
# MAGIC 
# MAGIC <img src="https://ktkdmdpe16public.blob.core.windows.net/public/ASCENDDataQualityExpecations.png" width=1200>

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pipeline Configuration

# COMMAND ----------

# MAGIC %md
# MAGIC This will be discussed further in the next section, but our operational dashboards also allow for Engineers to configure and preview pipeline runs declaratively using our declarative data pipeline framework.  
# MAGIC <img src="https://ktkdmdpe16public.blob.core.windows.net/public/ASCENDconfigurationreview.png" width=1200>
