"""
ILAM — PyFlink Streaming Job
Kafka (cdr_voice_stream + network_events_stream) → Iceberg Bronze
Exactly-once via checkpointing toutes les 30 secondes
"""

import os
import logging
from pyflink.datastream import StreamExecutionEnvironment, CheckpointingMode
from pyflink.table import StreamTableEnvironment, EnvironmentSettings

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# =============================================================================
# Configuration
# =============================================================================
KAFKA_BOOTSTRAP     = "ilam-kafka:9092"
ICEBERG_REST_URI    = "http://iceberg-rest:8181"
MINIO_ENDPOINT      = "http://minio:9000"
MINIO_ACCESS_KEY    = "ilam_admin"
MINIO_SECRET_KEY    = "ilam_password_2025"
CHECKPOINT_INTERVAL = 30000  # 30 secondes en ms
WAREHOUSE           = "s3://warehouse/"

def main():
    # =========================================================================
    # 1. Environnement Flink avec checkpointing
    # =========================================================================
    env = StreamExecutionEnvironment.get_execution_environment()

    # Checkpointing — c'est ici que le problème SQL Client était bloqué
    env.enable_checkpointing(CHECKPOINT_INTERVAL)
    env.get_checkpoint_config().set_checkpointing_mode(
        CheckpointingMode.EXACTLY_ONCE
    )
    env.get_checkpoint_config().set_min_pause_between_checkpoints(5000)
    env.get_checkpoint_config().set_checkpoint_timeout(60000)
    env.get_checkpoint_config().set_max_concurrent_checkpoints(1)

    # Parallélisme = 1 (on a 2 slots disponibles)
    env.set_parallelism(1)

    logger.info("Checkpointing configured: 30s EXACTLY_ONCE")

    # =========================================================================
    # 2. Table Environment
    # =========================================================================
    settings = EnvironmentSettings.new_instance() \
        .in_streaming_mode() \
        .build()

    t_env = StreamTableEnvironment.create(env, settings)

    # Variables AWS pour le SDK Iceberg
    t_env.get_config().set("pipeline.global-job-parameters.aws.region",
                           "us-east-1")

    logger.info("Table Environment created")

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
    # 4. Tables sources Kafka
    # =========================================================================
    t_env.execute_sql(f"""
        CREATE TABLE kafka_cdr_voice (
            cdr_id            STRING,
            calling_msisdn    STRING,
            called_msisdn     STRING,
            call_direction    STRING,
            call_type         STRING,
            duration_seconds  INT,
            call_status       STRING,
            disconnect_cause  STRING,
            origin_cell_id    STRING,
            origin_region     STRING,
            destination_type  STRING,
            roaming_flag      BOOLEAN,
            roaming_country   STRING,
            charge_amount     DOUBLE,
            currency          STRING,
            offer_code        STRING,
            start_ts          STRING,
            end_ts            STRING,
            ingested_at       STRING,
            call_date         STRING
        ) WITH (
            'connector'                    = 'kafka',
            'topic'                        = 'cdr_voice_stream',
            'properties.bootstrap.servers' = '{KAFKA_BOOTSTRAP}',
            'properties.group.id'          = 'pyflink-cdr-consumer',
            'scan.startup.mode'            = 'latest-offset',
            'format'                       = 'json',
            'json.fail-on-missing-field'   = 'false',
            'json.ignore-parse-errors'     = 'true'
        )
    """)

    t_env.execute_sql(f"""
        CREATE TABLE kafka_network_events (
            event_id          STRING,
            event_type        STRING,
            severity          STRING,
            cell_id           STRING,
            site_id           STRING,
            region            STRING,
            technology        STRING,
            metric_name       STRING,
            metric_value      DOUBLE,
            threshold_value   DOUBLE,
            vendor            STRING,
            raw_payload       STRING,
            source_system     STRING,
            event_ts          STRING,
            ingested_at       STRING,
            event_date        STRING
        ) WITH (
            'connector'                    = 'kafka',
            'topic'                        = 'network_events_stream',
            'properties.bootstrap.servers' = '{KAFKA_BOOTSTRAP}',
            'properties.group.id'          = 'pyflink-network-consumer',
            'scan.startup.mode'            = 'latest-offset',
            'format'                       = 'json',
            'json.fail-on-missing-field'   = 'false',
            'json.ignore-parse-errors'     = 'true'
        )
    """)

    logger.info("Kafka source tables created")

    # =========================================================================
    # 5. Soumettre les deux INSERT en parallèle via STATEMENT SET
    # =========================================================================
    logger.info("Submitting streaming jobs...")

    statement_set = t_env.create_statement_set()

    statement_set.add_insert_sql("""
        INSERT INTO iceberg_catalog.bronze.cdr_voice
        SELECT
            cdr_id,
            calling_msisdn,
            called_msisdn,
            call_direction,
            call_type,
            duration_seconds,
            call_status,
            disconnect_cause,
            origin_cell_id,
            origin_region,
            destination_type,
            roaming_flag,
            roaming_country,
            charge_amount,
            currency,
            offer_code,
            TO_TIMESTAMP(start_ts,    'yyyy-MM-dd HH:mm:ss.SSSSSS'),
            TO_TIMESTAMP(end_ts,      'yyyy-MM-dd HH:mm:ss.SSSSSS'),
            TO_TIMESTAMP(ingested_at, 'yyyy-MM-dd HH:mm:ss.SSSSSS'),
            TO_DATE(call_date, 'yyyy-MM-dd')
        FROM kafka_cdr_voice
        WHERE cdr_id IS NOT NULL
          AND calling_msisdn IS NOT NULL
    """)

    statement_set.add_insert_sql("""
        INSERT INTO iceberg_catalog.bronze.network_events
        SELECT
            event_id,
            event_type,
            severity,
            cell_id,
            site_id,
            region,
            technology,
            metric_name,
            metric_value,
            threshold_value,
            vendor,
            raw_payload,
            source_system,
            TO_TIMESTAMP(event_ts,    'yyyy-MM-dd HH:mm:ss.SSSSSS'),
            TO_TIMESTAMP(ingested_at, 'yyyy-MM-dd HH:mm:ss.SSSSSS'),
            TO_DATE(event_date, 'yyyy-MM-dd')
        FROM kafka_network_events
        WHERE event_id IS NOT NULL
    """)

    # execute_async — soumet le job et retourne immédiatement
    # Le job continue de tourner dans le cluster Flink
    job_client = statement_set.execute()

    logger.info(f"Jobs submitted successfully")
    logger.info(f"Check Flink UI: http://localhost:8081")
    logger.info(f"Jobs are running in the Flink cluster.")
    logger.info(f"Checkpointing every 30s — data will appear in Iceberg.")

if __name__ == "__main__":
    main()
