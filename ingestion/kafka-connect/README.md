# Kafka Connect — Sonatel Source Connectors

This directory contains the Kafka Connect connector configurations for
ingesting real Sonatel data sources into the Ilam Data Lakehouse.

## Architecture

Sonatel Systems                Kafka Connect              Kafka Topics
───────────────                ─────────────              ────────────
Oracle CRM          ──────→  Debezium CDC         ──→  sonatel.CRM.SUBSCRIBERS
(subscribers,                                           sonatel.CRM.CONTRACTS
contracts)
MSC/GGSN            ──────→  FileStream Source    ──→  cdr_voice_stream
(CDR files SFTP)                                        cdr_data_stream
Orange Money API    ──────→  HTTP Source           ──→  om_transactions_stream
(REST API)
Huawei eSight OSS   ──────→  HTTP Source           ──→  network_events_stream
(SNMP/REST alarms)

## Connectors

| File | Connector | Source | Topic |
|------|-----------|--------|-------|
| debezium-oracle-crm.json | Debezium Oracle | Oracle CRM | sonatel.CRM.* |
| filestream-cdr.json | FileStream Source | SFTP CDR files | cdr_voice_stream |
| http-source-om.json | HTTP Source | Orange Money API | om_transactions_stream |
| http-source-network.json | HTTP Source | Huawei eSight OSS | network_events_stream |

## Deployment (Production)

```bash
# Deploy a connector via Kafka Connect REST API
curl -X POST http://kafka-connect:8083/connectors \
  -H "Content-Type: application/json" \
  -d @connectors/debezium-oracle-crm.json

# Check connector status
curl http://kafka-connect:8083/connectors/sonatel-crm-cdc-connector/status

# List all connectors
curl http://kafka-connect:8083/connectors
```

## Note

In the PFE development environment, these connectors are replaced by a
Python producer script (`ingestion/streaming/kafka_producer.py`) that
simulates the same event structure at a reduced rate (3 CDR/second vs
millions/second in production).

The connector configurations above represent the production architecture
for a Sonatel deployment and are provided for documentation purposes.
EOF