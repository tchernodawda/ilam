"""
ILAM — PyFlink Streaming Job
9 topics Kafka → 9 tables Iceberg Bronze
Exactly-once via checkpointing toutes les 30 secondes
"""

import logging
from pyflink.datastream import StreamExecutionEnvironment, CheckpointingMode
from pyflink.table import StreamTableEnvironment, EnvironmentSettings

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP     = "ilam-kafka:9092"
ICEBERG_REST_URI    = "http://iceberg-rest:8181"
MINIO_ENDPOINT      = "http://minio:9000"
MINIO_ACCESS_KEY    = "ilam_admin"
MINIO_SECRET_KEY    = "ilam_password_2025"
CHECKPOINT_INTERVAL = 30000
WAREHOUSE           = "s3://warehouse/"

def create_kafka_table(t_env, table_name, topic, columns):
    cols_sql = ",\n            ".join(columns)
    t_env.execute_sql(f"""
        CREATE TABLE {table_name} (
            {cols_sql}
        ) WITH (
            'connector'                    = 'kafka',
            'topic'                        = '{topic}',
            'properties.bootstrap.servers' = '{KAFKA_BOOTSTRAP}',
            'properties.group.id'          = 'pyflink-{topic}',
            'scan.startup.mode'            = 'latest-offset',
            'format'                       = 'json',
            'json.fail-on-missing-field'   = 'false',
            'json.ignore-parse-errors'     = 'true'
        )
    """)
    logger.info(f"Kafka table: {table_name} → {topic}")

def main():
    # =========================================================================
    # 1. Environnement Flink avec checkpointing
    # =========================================================================
    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(CHECKPOINT_INTERVAL)
    env.get_checkpoint_config().set_checkpointing_mode(
        CheckpointingMode.EXACTLY_ONCE)
    env.get_checkpoint_config().set_min_pause_between_checkpoints(5000)
    env.get_checkpoint_config().set_checkpoint_timeout(60000)
    env.get_checkpoint_config().set_max_concurrent_checkpoints(1)
    env.set_parallelism(1)

    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, settings)
    logger.info("Environment ready — checkpointing 30s EXACTLY_ONCE")

    # =========================================================================
    # 2. Tables sources Kafka — catalog DEFAULT
    # =========================================================================

    create_kafka_table(t_env, "kafka_cdr_voice", "cdr_voice_stream", [
        "cdr_id STRING", "calling_msisdn STRING", "called_msisdn STRING",
        "call_direction STRING", "call_type STRING", "duration_seconds INT",
        "call_status STRING", "disconnect_cause STRING",
        "origin_cell_id STRING", "origin_region STRING",
        "destination_type STRING", "roaming_flag BOOLEAN",
        "roaming_country STRING", "charge_amount DOUBLE",
        "currency STRING", "offer_code STRING",
        "start_ts STRING", "end_ts STRING",
        "ingested_at STRING", "call_date STRING"
    ])

    create_kafka_table(t_env, "kafka_cdr_sms", "cdr_sms_stream", [
        "cdr_id STRING", "sender_msisdn STRING", "receiver_msisdn STRING",
        "sms_type STRING", "delivery_status STRING",
        "origin_cell_id STRING", "origin_region STRING",
        "roaming_flag BOOLEAN", "charge_amount DOUBLE",
        "currency STRING", "offer_code STRING",
        "sent_ts STRING", "delivered_ts STRING",
        "ingested_at STRING", "sms_date STRING"
    ])

    create_kafka_table(t_env, "kafka_cdr_data", "cdr_data_stream", [
        "session_id STRING", "msisdn STRING", "technology STRING",
        "cell_id STRING", "region STRING",
        "bytes_uploaded BIGINT", "bytes_downloaded BIGINT",
        "duration_seconds INT", "apn STRING",
        "roaming_flag BOOLEAN", "roaming_country STRING",
        "charge_amount DOUBLE", "currency STRING", "offer_code STRING",
        "start_ts STRING", "end_ts STRING",
        "ingested_at STRING", "session_date STRING"
    ])

    create_kafka_table(t_env, "kafka_network_events", "network_events_stream", [
        "event_id STRING", "event_type STRING", "severity STRING",
        "cell_id STRING", "site_id STRING", "region STRING",
        "technology STRING", "metric_name STRING",
        "metric_value DOUBLE", "threshold_value DOUBLE",
        "vendor STRING", "raw_payload STRING", "source_system STRING",
        "event_ts STRING", "ingested_at STRING", "event_date STRING"
    ])

    create_kafka_table(t_env, "kafka_om_transactions", "om_transactions_stream", [
        "transaction_id STRING", "msisdn STRING",
        "transaction_type STRING", "amount DOUBLE", "currency STRING",
        "fee_amount DOUBLE", "counterpart_msisdn STRING",
        "counterpart_type STRING", "agent_id STRING",
        "agent_region STRING", "channel STRING", "status STRING",
        "failure_reason STRING", "balance_before DOUBLE",
        "balance_after DOUBLE", "raw_metadata STRING",
        "transaction_ts STRING", "ingested_at STRING",
        "transaction_date STRING"
    ])

    create_kafka_table(t_env, "kafka_subscribers", "subscribers_stream", [
        "subscriber_id STRING", "msisdn STRING", "account_type STRING",
        "status STRING", "id_type STRING", "id_number STRING",
        "first_name STRING", "last_name STRING", "birth_date STRING",
        "gender STRING", "region STRING", "city STRING",
        "channel STRING", "nationality STRING", "raw_metadata STRING",
        "activated_at STRING", "ingested_at STRING",
        "ingestion_date STRING"
    ])

    create_kafka_table(t_env, "kafka_contracts", "contracts_stream", [
        "contract_id STRING", "subscriber_id STRING", "msisdn STRING",
        "offer_code STRING", "offer_name STRING", "product_type STRING",
        "plan_type STRING", "monthly_fee DOUBLE", "currency STRING",
        "status STRING", "activation_date STRING", "expiry_date STRING",
        "termination_date STRING", "termination_reason STRING",
        "ingested_at STRING", "ingestion_date STRING"
    ])

    create_kafka_table(t_env, "kafka_recharges", "recharges_stream", [
        "recharge_id STRING", "msisdn STRING", "amount DOUBLE",
        "currency STRING", "channel STRING", "voucher_code STRING",
        "bonus_amount DOUBLE", "region STRING", "status STRING",
        "recharged_at STRING", "ingested_at STRING",
        "recharge_date STRING"
    ])

    create_kafka_table(t_env, "kafka_complaints", "complaints_stream", [
        "complaint_id STRING", "subscriber_id STRING", "msisdn STRING",
        "channel STRING", "category STRING", "sub_category STRING",
        "priority STRING", "status STRING", "description STRING",
        "resolution STRING", "agent_id STRING", "region STRING",
        "opened_at STRING", "resolved_at STRING",
        "ingested_at STRING", "complaint_date STRING"
    ])

    logger.info("All 9 Kafka source tables created")

    # =========================================================================
    # 3. Catalogue Iceberg REST
    # =========================================================================
    t_env.execute_sql(f"""
        CREATE CATALOG iceberg_catalog WITH (
            'type'                  = 'iceberg',
            'catalog-type'          = 'rest',
            'uri'                   = '{ICEBERG_REST_URI}',
            'warehouse'             = '{WAREHOUSE}',
            'io-impl'               = 'org.apache.iceberg.aws.s3.S3FileIO',
            's3.endpoint'           = '{MINIO_ENDPOINT}',
            's3.access-key-id'      = '{MINIO_ACCESS_KEY}',
            's3.secret-access-key'  = '{MINIO_SECRET_KEY}',
            's3.path-style-access'  = 'true',
            'property-version'      = '1'
        )
    """)
    logger.info("Iceberg catalog created")

    # =========================================================================
    # 4. STATEMENT SET — 9 INSERT en parallèle
    # =========================================================================
    statement_set = t_env.create_statement_set()

    statement_set.add_insert_sql("""
        INSERT INTO iceberg_catalog.bronze.cdr_voice
        SELECT cdr_id, calling_msisdn, called_msisdn, call_direction,
               call_type, duration_seconds, call_status, disconnect_cause,
               origin_cell_id, origin_region, destination_type, roaming_flag,
               roaming_country, charge_amount, currency, offer_code,
               TO_TIMESTAMP(start_ts,    'yyyy-MM-dd HH:mm:ss.SSSSSS'),
               TO_TIMESTAMP(end_ts,      'yyyy-MM-dd HH:mm:ss.SSSSSS'),
               TO_TIMESTAMP(ingested_at, 'yyyy-MM-dd HH:mm:ss.SSSSSS'),
               TO_DATE(call_date, 'yyyy-MM-dd')
        FROM kafka_cdr_voice
        WHERE cdr_id IS NOT NULL AND calling_msisdn IS NOT NULL
    """)

    statement_set.add_insert_sql("""
        INSERT INTO iceberg_catalog.bronze.cdr_sms
        SELECT cdr_id, sender_msisdn, receiver_msisdn, sms_type,
               delivery_status, origin_cell_id, origin_region, roaming_flag,
               charge_amount, currency, offer_code,
               TO_TIMESTAMP(sent_ts,      'yyyy-MM-dd HH:mm:ss.SSSSSS'),
               TO_TIMESTAMP(delivered_ts, 'yyyy-MM-dd HH:mm:ss.SSSSSS'),
               TO_TIMESTAMP(ingested_at,  'yyyy-MM-dd HH:mm:ss.SSSSSS'),
               TO_DATE(sms_date, 'yyyy-MM-dd')
        FROM kafka_cdr_sms
        WHERE cdr_id IS NOT NULL
    """)

    statement_set.add_insert_sql("""
        INSERT INTO iceberg_catalog.bronze.cdr_data
        SELECT session_id, msisdn, technology, cell_id, region,
               bytes_uploaded, bytes_downloaded, duration_seconds, apn,
               roaming_flag, roaming_country, charge_amount, currency,
               offer_code,
               TO_TIMESTAMP(start_ts,    'yyyy-MM-dd HH:mm:ss.SSSSSS'),
               TO_TIMESTAMP(end_ts,      'yyyy-MM-dd HH:mm:ss.SSSSSS'),
               TO_TIMESTAMP(ingested_at, 'yyyy-MM-dd HH:mm:ss.SSSSSS'),
               TO_DATE(session_date, 'yyyy-MM-dd')
        FROM kafka_cdr_data
        WHERE session_id IS NOT NULL
    """)

    statement_set.add_insert_sql("""
        INSERT INTO iceberg_catalog.bronze.network_events
        SELECT event_id, event_type, severity, cell_id, site_id,
               region, technology, metric_name, metric_value,
               threshold_value, vendor, raw_payload, source_system,
               TO_TIMESTAMP(event_ts,    'yyyy-MM-dd HH:mm:ss.SSSSSS'),
               TO_TIMESTAMP(ingested_at, 'yyyy-MM-dd HH:mm:ss.SSSSSS'),
               TO_DATE(event_date, 'yyyy-MM-dd')
        FROM kafka_network_events
        WHERE event_id IS NOT NULL
    """)

    statement_set.add_insert_sql("""
        INSERT INTO iceberg_catalog.bronze.om_transactions
        SELECT transaction_id, msisdn, transaction_type, amount, currency,
               fee_amount, counterpart_msisdn, counterpart_type, agent_id,
               agent_region, channel, status, failure_reason,
               balance_before, balance_after, raw_metadata,
               TO_TIMESTAMP(transaction_ts, 'yyyy-MM-dd HH:mm:ss.SSSSSS'),
               TO_TIMESTAMP(ingested_at,    'yyyy-MM-dd HH:mm:ss.SSSSSS'),
               TO_DATE(transaction_date, 'yyyy-MM-dd')
        FROM kafka_om_transactions
        WHERE transaction_id IS NOT NULL
    """)

    statement_set.add_insert_sql("""
        INSERT INTO iceberg_catalog.bronze.subscribers
        SELECT subscriber_id, msisdn, account_type, status, id_type,
               id_number, first_name, last_name,
               TO_DATE(birth_date, 'yyyy-MM-dd'),
               gender, region, city, channel, nationality, raw_metadata,
               TO_TIMESTAMP(activated_at, 'yyyy-MM-dd HH:mm:ss.SSSSSS'),
               TO_TIMESTAMP(ingested_at,  'yyyy-MM-dd HH:mm:ss.SSSSSS'),
               TO_DATE(ingestion_date, 'yyyy-MM-dd')
        FROM kafka_subscribers
        WHERE subscriber_id IS NOT NULL
    """)

    statement_set.add_insert_sql("""
        INSERT INTO iceberg_catalog.bronze.contracts
        SELECT contract_id, subscriber_id, msisdn, offer_code, offer_name,
               product_type, plan_type, monthly_fee, currency, status,
               TO_DATE(activation_date, 'yyyy-MM-dd'),
               TO_DATE(expiry_date,     'yyyy-MM-dd'),
               CASE WHEN termination_date = '' THEN NULL
                    ELSE TO_DATE(termination_date, 'yyyy-MM-dd') END,
               CASE WHEN termination_reason = '' THEN NULL
                    ELSE termination_reason END,
               TO_TIMESTAMP(ingested_at,   'yyyy-MM-dd HH:mm:ss.SSSSSS'),
               TO_DATE(ingestion_date, 'yyyy-MM-dd')
        FROM kafka_contracts
        WHERE contract_id IS NOT NULL
    """)

    statement_set.add_insert_sql("""
        INSERT INTO iceberg_catalog.bronze.recharges
        SELECT recharge_id, msisdn, amount, currency, channel,
               voucher_code, bonus_amount, region, status,
               TO_TIMESTAMP(recharged_at, 'yyyy-MM-dd HH:mm:ss.SSSSSS'),
               TO_TIMESTAMP(ingested_at,  'yyyy-MM-dd HH:mm:ss.SSSSSS'),
               TO_DATE(recharge_date, 'yyyy-MM-dd')
        FROM kafka_recharges
        WHERE recharge_id IS NOT NULL
    """)

    statement_set.add_insert_sql("""
        INSERT INTO iceberg_catalog.bronze.complaints
        SELECT complaint_id, subscriber_id, msisdn, channel, category,
               sub_category, priority, status, description,
               CASE WHEN resolution = '' THEN NULL ELSE resolution END,
               agent_id, region,
               TO_TIMESTAMP(opened_at,   'yyyy-MM-dd HH:mm:ss.SSSSSS'),
               CASE WHEN resolved_at = '' THEN NULL
                    ELSE TO_TIMESTAMP(resolved_at, 'yyyy-MM-dd HH:mm:ss.SSSSSS') END,
               TO_TIMESTAMP(ingested_at, 'yyyy-MM-dd HH:mm:ss.SSSSSS'),
               TO_DATE(complaint_date, 'yyyy-MM-dd')
        FROM kafka_complaints
        WHERE complaint_id IS NOT NULL
    """)

    logger.info("Submitting 9 streaming jobs...")
    statement_set.execute()
    logger.info("All jobs submitted. Check http://localhost:8081")

if __name__ == "__main__":
    main()
