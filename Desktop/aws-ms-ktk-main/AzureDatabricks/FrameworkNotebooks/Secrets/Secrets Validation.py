# Databricks notebook source
# MAGIC %md # Secrets Validation
# MAGIC
# MAGIC This notebook iterates through all the existing secret scopes and secrets, saves them to global temp tables which can be queried from another session (making the secret values visible for validation).
# MAGIC
# MAGIC #### Usage
# MAGIC * Run the notebook
# MAGIC * Copy the output from the final step and paste in a new notebook (session) to make the secrets visible.

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType

# COMMAND ----------

def concatenateFields(fieldList):
  fields = [f.fields for f in fieldList]
  for n in range(0,len(fields)):
    if n == 0:
      concatenatedFields = fields[0]
    else:
      concatenatedFields += fields[n]
  return StructType(concatenatedFields)

# COMMAND ----------

def createGlobalTempViewForSecretScope(scope):
  secretValues = {}
  secrets = [secret[0] for secret in dbutils.secrets.list(scope=scope)]
  if len(secrets) > 0:
    schemaFieldList = [StructType([StructField(str(secret), StringType())]) for secret in secrets]
    for secret in secrets:
      secretValues[secret] = str(dbutils.secrets.get(scope=scope, key=secret))
    schema = concatenateFields(schemaFieldList)
    json_rdd=sc.parallelize([secretValues])
    df = json_rdd.toDF(schema)
    df.createOrReplaceGlobalTempView(scope)

# COMMAND ----------

[createGlobalTempViewForSecretScope(scope[0]) for scope in dbutils.secrets.listScopes()]

# COMMAND ----------

["SELECT * FROM " + t[1] + "." + t[0] for t in spark.catalog.listTables(dbName="global_temp")]