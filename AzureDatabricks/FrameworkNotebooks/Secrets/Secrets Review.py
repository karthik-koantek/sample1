# Databricks notebook source
scopes = dbutils.secrets.listScopes()
[s.name for s in scopes]

# COMMAND ----------

for scope in scopes:
  print("Scope: {0}".format(scope.name))
  secrets = dbutils.secrets.list(scope=scope.name)
  for secret in secrets:
    print(secret)