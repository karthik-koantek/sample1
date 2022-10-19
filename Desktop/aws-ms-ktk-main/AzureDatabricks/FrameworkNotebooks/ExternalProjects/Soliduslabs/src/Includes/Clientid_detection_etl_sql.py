# Databricks notebook source

def compose_client_id_detection_query(solidus_client: str, exec_time_millis: float, days_back: int, database_name: str) -> str:
    return f"""
        SELECT client_id, ARRAY_AGG(DISTINCT accounts) accounts
        FROM (
          SELECT client_id, account accounts, 'private orders' data_type
          FROM "{database_name}"."MS"."PRIVATE_ORDERS"
          WHERE solidus_client = '{solidus_client}'
            AND TO_DATE(transact_time) >= DATEADD(DAY,-1 * {days_back}, TO_TIMESTAMP_NTZ({float(exec_time_millis) / 1000}))
            
        UNION ALL
            
          SELECT client_id, internal_account_id accounts, 'transactions' data_type
          FROM "{database_name}"."TM"."TRANSACTIONS"
          WHERE solidus_client = '{solidus_client}'
            AND TO_DATE(transact_time) >= DATEADD(DAY,-1 * {days_back}, TO_TIMESTAMP_NTZ({float(exec_time_millis) / 1000}))
        ) 
        GROUP BY client_id
    """

