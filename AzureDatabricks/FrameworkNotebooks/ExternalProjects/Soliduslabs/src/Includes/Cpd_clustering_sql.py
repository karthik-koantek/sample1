# Databricks notebook source
INTERNAL_ACCOUNT_ID = 'internal_account_id'
EXTERNAL_ACCOUNT_ID = 'external_account_id'
CLIENT_ID = 'client_id'
ACTOR_ID = 'actor_id'

ALL_FIELDS = {'CLIENT_ID':CLIENT_ID, 'EXTERNAL_ACCOUNT_ID':EXTERNAL_ACCOUNT_ID, 'INTERNAL_ACCOUNT_ID':INTERNAL_ACCOUNT_ID, 'ACTOR_ID':ACTOR_ID}


# COMMAND ----------

#from src.shared.consts import fields as ALL_FIELDS


def compose_cpd_clustering_query(solidus_client: str, exec_time_millis: float, days_back: int, solidus_db: str) -> str:
    return f"""
        -- Aggregate TXNs based on hourly activity, while squeeshig "type" (deposit/withdrawal) into same row
        WITH days_clustered AS (
            SELECT {days_back} days_back
        ), 
        txns AS (
            SELECT t.client_id, 
                   t.external_account_id,
                   t.internal_account_id,
                   t.actor_id,
                   t.solidus_client,
                   t.ex_venue,
                   DATE_PART('HOUR', t.transact_time) hour,
                   COUNT(CASE WHEN t.type = 'DEPOSIT' THEN t.transact_time ELSE null END) counter_deposit,
                   SUM(CASE WHEN t.type = 'DEPOSIT' THEN t.usd_notional else 0 END) summed_usd_notional_deposit,
                   COUNT(CASE WHEN t.type = 'WITHDRAWAL' THEN t.transact_time ELSE null END) counter_withdrawal,
                   SUM(CASE WHEN t.type = 'WITHDRAWAL' THEN t.usd_notional else 0 END) summed_usd_notional_withdrawal
        FROM TM.TRANSACTIONS t CROSS JOIN days_clustered dc
        WHERE t.solidus_client = '{solidus_client}'
          AND TO_DATE(t.transact_time) BETWEEN 
                    TIMESTAMPADD('DAYS', -1 * {days_back}, TO_TIMESTAMP({float(exec_time_millis) / 1000})) 
                        AND TO_TIMESTAMP({float(exec_time_millis) / 1000}) 
          AND t.version = 1
          AND t.status <> 'REJECTED'
        GROUP BY t.client_id, 
                 t.external_account_id,
                 t.internal_account_id,
                 t.actor_id,
                 DATE_PART('HOUR', transact_time),
                 solidus_client,
                 ex_venue
        ),
        -- Dividing previous query into 4 queries in order to get a single identifier column 
        -- (client_id, actor_id, internal_account_id, external_account_id)
        -- And aggregating again since a identifiers that appear with different combinations of other identifiers 
        -- (for example, a client_id appearing with 2 different actor_ids) 
        -- will appear in this query in multiple rows
        txns_by_id AS (
            SELECT  t2.client_id identifier, 
                    '{ALL_FIELDS['CLIENT_ID']}' id_type, 
                    t2.solidus_client, 
                    t2.ex_venue, 
                    t2.hour, 
                    SUM(t2.counter_deposit) counter_deposit, 
                    SUM(t2.summed_usd_notional_deposit) summed_usd_notional_deposit, 
                    SUM(t2.counter_withdrawal) counter_withdrawal, 
                    SUM(t2.summed_usd_notional_withdrawal) summed_usd_notional_withdrawal
            FROM txns t2 
            WHERE t2.client_id IS NOT NULL 
            GROUP BY 1, 2, 3, 4, 5
                
                UNION ALL
          
            SELECT  t2.external_account_id identifier, 
                    '{ALL_FIELDS['EXTERNAL_ACCOUNT_ID']}' id_type, 
                    t2.solidus_client,  
                    t2.ex_venue, 
                    t2.hour, 
                    SUM(t2.counter_deposit) counter_deposit, 
                    SUM(t2.summed_usd_notional_deposit) summed_usd_notional_deposit, 
                    SUM(t2.counter_withdrawal) counter_withdrawal, 
                    SUM(t2.summed_usd_notional_withdrawal) summed_usd_notional_withdrawal
            FROM txns t2 
            WHERE t2.external_account_id IS NOT NULL
            GROUP BY 1, 2, 3, 4, 5
                
                UNION ALL
            
            SELECT  t2.internal_account_id identifier, 
                    '{ALL_FIELDS['INTERNAL_ACCOUNT_ID']}' id_type, 
                    t2.solidus_client, 
                    t2.ex_venue, 
                    t2.hour, 
                    SUM(t2.counter_deposit) counter_deposit, 
                    SUM(t2.summed_usd_notional_deposit) summed_usd_notional_deposit, 
                    SUM(t2.counter_withdrawal) counter_withdrawal, 
                    SUM(t2.summed_usd_notional_withdrawal) summed_usd_notional_withdrawal
            FROM txns t2 
            WHERE t2.internal_account_id IS NOT NULL
            GROUP BY 1, 2, 3, 4, 5
                
                UNION ALL
            
            SELECT  t2.actor_id identifier, 
                    '{ALL_FIELDS['ACTOR_ID']}' id_type, 
                    t2.solidus_client, 
                    t2.ex_venue, 
                    t2.hour, 
                    SUM(t2.counter_deposit) counter_deposit, 
                    SUM(t2.summed_usd_notional_deposit) summed_usd_notional_deposit, 
                    SUM(t2.counter_withdrawal) counter_withdrawal, 
                    SUM(t2.summed_usd_notional_withdrawal) summed_usd_notional_withdrawal
            FROM txns t2 
            WHERE t2.actor_id IS NOT NULL
            GROUP BY 1, 2, 3, 4, 5
        ),
        -- Calculating weighted hourly sum (pre-calculating before daily_hourly_avg feature) 
        -- and moving to aggregation without hours
        txn_with_daily_hourly_avg AS (
          SELECT tbi3.identifier,
                 tbi3.id_type,
                 tbi3.solidus_client,
                 tbi3.ex_venue,
                 tbi3.counter_deposit,
                 tbi3.summed_usd_notional_deposit,
                 tbi3.counter_withdrawal,
                 tbi3.summed_usd_notional_withdrawal,
                 tbi3.weighted_notional_hour_value / (tbi3.counter_deposit + tbi3.counter_withdrawal) daily_hourly_avg,
                 -- deposit/withdrawal ratio: sigmoid function equivalant to Java's Apache3 commons
                 CASE WHEN summed_usd_notional_deposit = 0 AND summed_usd_notional_withdrawal = 0 THEN 0.5
                      WHEN summed_usd_notional_deposit = 0 THEN 0
                      WHEN summed_usd_notional_withdrawal = 0 THEN 1
                      ELSE {solidus_db.upper()}.TM.sigmoid(ln(summed_usd_notional_deposit / summed_usd_notional_withdrawal), 0, 1) 
                      END deposit_withdrawal_ratio
          FROM (
            SELECT tbi2.identifier,
                 tbi2.id_type, 
                 tbi2.solidus_client,
                 tbi2.ex_venue,
                 SUM(tbi2.counter_deposit) counter_deposit,
                 SUM(tbi2.summed_usd_notional_deposit) summed_usd_notional_deposit,
                 SUM(tbi2.counter_withdrawal) counter_withdrawal,
                 SUM(tbi2.summed_usd_notional_withdrawal) summed_usd_notional_withdrawal,
                 SUM(tbi2.weighted_hour_value) weighted_notional_hour_value
            FROM (
              SELECT tbi.*, ((tbi.hour) * (tbi.counter_deposit + tbi.counter_withdrawal)) weighted_hour_value
              FROM txns_by_id tbi
            ) tbi2
            GROUP BY 1, 2, 3, 4
          ) tbi3
        ),
        -- Calculating features necessary for the Customer Profile Deviation algo
        txn_daily_features AS (
          SELECT twdha.identifier,
                 twdha.id_type, 
                 twdha.solidus_client, 
                 twdha.ex_venue,
                 daily_hourly_avg,
                 (twdha.counter_deposit + twdha.counter_withdrawal) / (dc.days_back * 24) daily_freq_avg,
                 (twdha.summed_usd_notional_deposit + twdha.summed_usd_notional_withdrawal) / 
                    (twdha.counter_deposit + twdha.counter_withdrawal) daily_usd_size_avg,
                 twdha.deposit_withdrawal_ratio
          FROM txn_with_daily_hourly_avg twdha CROSS JOIN days_clustered dc
        )
        
        SELECT * FROM txn_daily_features;
    """

