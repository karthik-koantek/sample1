# Databricks notebook source
INTERNAL_ACCOUNT_ID = 'internal_account_id'
EXTERNAL_ACCOUNT_ID = 'external_account_id'
CLIENT_ID = 'client_id'
ACTOR_ID = 'actor_id'

ALL_FIELDS = [CLIENT_ID, EXTERNAL_ACCOUNT_ID, INTERNAL_ACCOUNT_ID, ACTOR_ID]

# COMMAND ----------

from enum import Enum

class TimeScale(Enum):
    SECONDS = 1
    MINUTES = 60
    HOURS = 3600
    DAYS = 86400
    WEEKS = 604800
    MONTHS = 2592000
    YEARS = 31536000
