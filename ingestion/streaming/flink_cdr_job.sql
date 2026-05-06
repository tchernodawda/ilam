SET 'execution.checkpointing.interval' = '30s';
SET 'execution.checkpointing.mode' = 'EXACTLY_ONCE';

CREATE CATALOG iceberg_catalog WITH (
    'type'                  = 'iceberg',
    'catalog-type'          = 'rest',
    'uri'                   = 'http://172.19.0.8:8181',
    'warehouse'             = 's3://warehouse/',
    'io-impl'               = 'org.apache.iceberg.aws.s3.S3FileIO',
    's3.endpoint'           = 'http://minio:9000',
    's3.access-key-id'      = 'ilam_admin',
    's3.secret-access-key'  = 'ilam_password_2025',
    's3.path-style-access'  = 'true',
    'property-version'      = '1'
);

USE CATALOG iceberg_catalog;
USE bronze;

CREATE TABLE IF NOT EXISTS kafka_cdr_voice (
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
    'properties.bootstrap.servers' = 'ilam-kafka:9092',
    'properties.group.id'          = 'flink-cdr-consumer',
    'scan.startup.mode'            = 'earliest-offset',
    'format'                       = 'json',
    'json.fail-on-missing-field'   = 'false',
    'json.ignore-parse-errors'     = 'true'
);

CREATE TABLE IF NOT EXISTS kafka_network_events (
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
    'properties.bootstrap.servers' = 'ilam-kafka:9092',
    'properties.group.id'          = 'flink-network-consumer',
    'scan.startup.mode'            = 'earliest-offset',
    'format'                       = 'json',
    'json.fail-on-missing-field'   = 'false',
    'json.ignore-parse-errors'     = 'true'
);

EXECUTE STATEMENT SET
BEGIN

INSERT INTO iceberg_catalog.bronze.cdr_voice
SELECT
    cdr_id, calling_msisdn, called_msisdn, call_direction, call_type,
    duration_seconds, call_status, disconnect_cause, origin_cell_id,
    origin_region, destination_type, roaming_flag, roaming_country,
    charge_amount, currency, offer_code,
    TO_TIMESTAMP(start_ts,    'yyyy-MM-dd''T''HH:mm:ss.SSSSSS'),
    TO_TIMESTAMP(end_ts,      'yyyy-MM-dd''T''HH:mm:ss.SSSSSS'),
    TO_TIMESTAMP(ingested_at, 'yyyy-MM-dd''T''HH:mm:ss.SSSSSS'),
    TO_DATE(call_date, 'yyyy-MM-dd')
FROM kafka_cdr_voice
WHERE cdr_id IS NOT NULL AND calling_msisdn IS NOT NULL;

INSERT INTO iceberg_catalog.bronze.network_events
SELECT
    event_id, event_type, severity, cell_id, site_id, region,
    technology, metric_name, metric_value, threshold_value,
    vendor, raw_payload, source_system,
    TO_TIMESTAMP(event_ts,    'yyyy-MM-dd''T''HH:mm:ss.SSSSSS'),
    TO_TIMESTAMP(ingested_at, 'yyyy-MM-dd''T''HH:mm:ss.SSSSSS'),
    TO_DATE(event_date, 'yyyy-MM-dd')
FROM kafka_network_events
WHERE event_id IS NOT NULL;

END;
