# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.dbutils import DBUtils
from datetime import date,timedelta

class BigQuery:
  
  def __ini__(self, parent_project_name = None, project_name = None, dataset_id = None, historical_ingestion = False, full_load_csv_path=None):
    self.credentials = _get_secret(scope="bigQuery", keyname="credentials") # arena-big-query/ big-query, credentials
    spark.conf.set("credentials", self.credentials)
    self.accountEmail = _get_secret(scope="bigQuery", keyname="accountEmail")
    spark.conf.set("spark.hadoop.fs.gs.auth.service.account.email", self.accountEmail)
    spark.conf.set("spark.hadoop.fs.gs.project.id", "thestreet-analytics")
    spark.conf.set("spark.hadoop.google.cloud.auth.service.account.enable","true")
    self.privateKey =  _get_secret(scope="bigQuery", keyname="privateKey")
    spark.conf.set("spark.hadoop.fs.gs.auth.service.account.private.key", self.privateKey)
    self.privateKeyId =  _get_secret(scope="bigQuery", keyname="privateKeyId")
    spark.conf.set("spark.hadoop.fs.gs.auth.service.account.private.key.id", self.privateKeyId)
    self.spark = self._init_spark_session()
    self.dbutils = self._init_dbutils()
    self.historical_ingestion = historical_ingestion
    self.parent_project_name = parent_project_name
    self.project_name = project_name
    self.dataset_id = dataset_id
    self.csv_path = full_load_csv_path
    self._initialize_metatables()
    
    
  def _init_spark_session(self):
    return SparkSession.builder.appName('DatabricksFramework').getOrCreate()
    
    
  def _init_dbutils(self):
    return DBUtils(self.spark)
    
    
  def _get_secret(self, scope, keyname):
    auth_token = self.dbutils.secrets.get(scope='MarketoAPI', key='AuthorizationCode')
    return auth_token
  
  def _initialize_metatables(self):
    self.spark.sql("""CREATE DATABASE IF NOT EXISTS bigquery_metadata""")
    self.spark.sql("""CREATE TABLE IF NOT EXISTS bigquery_metadata.meta_table (project_id STRING, dataset_id STRING, table_id STRING, creation_time TIMESTAMP, last_modified_time TIMESTAMP, row_count BIGINT, size_bytes BIGINT, type INT, ingestion_time TIMESTAMP, ingestion_status BOOLEAN)""")
  
  
  def load_data(self):
    assert(self.parent_project_name is None, "parent_project_name cannot be none")
    assert(self.project_name is None, "project_name cannot be none")
    assert(self.dataset_id is None, "database_id cannot be none")
    if !self.historical_ingestion:
      days_to_subtract = 1
      today = date.today() - timedelta(days=days_to_subtract)
      table_name = self.parent_project_name + "." + self.dataset_id + ".ga_sessions_" + str(today).replace("-","")
      self.spark.sql(f"""INSERT INTO bigquery_metadata.meta_table VALUES ('{self.parent_project_name}', '{self.dataset_id}', '{table_name}', NULL, NULL, NULL, NULL, NULL, current_timestamp(), False)""")
      data = self.spark.read.format("bigquery").option("table", table_name).option("project", project_name).option("parentProject", parentProject_name).load()
      return data
#     else:
#       assert(self.csv_path is None, "provide path of csv metadata file for historical load")
#       df = 
      
      
      
      
    
  
  
    
  
    
