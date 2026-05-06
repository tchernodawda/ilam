CREATE CATALOG iceberg_catalog WITH (
    'type'                  = 'iceberg',
    'catalog-type'          = 'rest',
    'uri'                   = 'http://iceberg-rest:8181',
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
    'scan.startup.mode'            = 'latest-offset',
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
    'scan.startup.mode'            = 'latest-offset',
    'format'                       = 'json',
    'json.fail-on-missing-field'   = 'false',
    'json.ignore-parse-errors'     = 'true'
);
