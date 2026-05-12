# Ilam — Open-Source Data Lakehouse Platform

> *Ilam* means **"the lake"**, **"the flow"**, **"the water current"** in Pulaar: data flowing from raw sources to insight, continuously and autonomously.

<p align="center">
  <a href="LICENSE">
    <img src="https://img.shields.io/badge/License-Apache%202.0-blue.svg" />
  </a>
  <img src="https://img.shields.io/badge/stack-Iceberg%20%7C%20Trino%20%7C%20Flink%20%7C%20NiFi%20%7C%20dbt%20%7C%20Airflow-orange" />
  <img src="https://img.shields.io/badge/status-WIP-yellow" />
  <img src="https://img.shields.io/badge/PRs-welcome-brightgreen.svg" />
</p>

Ilam is a fully open-source, end-to-end Data Lakehouse platform built on real-world telecom data from **Sonatel / Orange Sénégal**, covering CDR (voice, SMS, data), network events, CRM, and Orange Money transactions.

## Architecture

```
+------------------------------------------------------------------+
|  Agentic Layer         LangGraph / ReAct / autonomous agents     |
+------------------------------------------------------------------+
|  Orchestration         Apache Airflow / DAGs / Sensors           |
+------------------------------------------------------------------+
|  Transformation        dbt Core    |  Query Engine  Trino MPP    |
+------------------------------------------------------------------+
|  Medallion Architecture      Bronze -> Silver -> Gold            |
+------------------------------------------------------------------+
|  Storage               Apache Iceberg / MinIO / Parquet          |
+--------------------------------------+---------------------------+
|  Ingestion Streaming                 |  Ingestion Batch          |
|  NiFi -> Kafka -> PyFlink            |  NiFi CRON -> Kafka       |
|  exactly-once / 5 topics             |  4 topics / hourly        |
+--------------------------------------+---------------------------+
|  Security              Keycloak / Apache Ranger                  |
|  Observability         Prometheus / Grafana                      |
+------------------------------------------------------------------+
```

## Tech Stack

| Layer | Tool | Version |
|-------|------|---------|
| Ingestion | Apache NiFi | 1.23.2 |
| Streaming | Apache Kafka | KRaft |
| Stream processing | Apache Flink + PyFlink | 1.18 |
| Storage | MinIO | latest |
| Table format | Apache Iceberg | 1.5.2 |
| Catalog | iceberg-rest + PostgreSQL | — |
| Query engine | Trino | 435 |
| Transformation | dbt Core + dbt-trino | 1.11 |
| Orchestration | Apache Airflow | 2.8.1 |
| Data quality | Great Expectations | coming |
| Monitoring | Prometheus + Grafana | coming |
| Security | Keycloak + Ranger | coming |
| Agents | LangGraph | coming |

## Data Model — Sonatel Telecom

| Layer | Tables |
|-------|--------|
| Bronze (9) | `cdr_voice` `cdr_sms` `cdr_data` `network_events` `om_transactions` `subscribers` `contracts` `recharges` `complaints` |
| Silver (5) | `cdr_enriched` `network_events` `subscribers` `contracts` `om_transactions` |
| Gold (6) | `mart_revenue` `mart_network_quality` `mart_churn` `mart_arpu` `mart_om_activity` `mart_usage` |

## Streaming Pipeline

```
NiFi (9 Process Groups)
  |-- CDR Voice Pipeline       -> cdr_voice_stream       (500ms)
  |-- CDR SMS Pipeline         -> cdr_sms_stream          (500ms)
  |-- CDR Data Pipeline        -> cdr_data_stream         (500ms)
  |-- Network Events Pipeline  -> network_events_stream   (2s)
  |-- OM Transactions Pipeline -> om_transactions_stream  (1s)
  |-- Subscribers Batch        -> subscribers_stream      (@hourly)
  |-- Contracts Batch          -> contracts_stream        (@hourly)
  |-- Recharges Batch          -> recharges_stream        (@30min)
  +-- Complaints Batch         -> complaints_stream       (@8am)
          |
          v
  Apache Kafka (9 topics, 3 partitions each)
          |
          v
  PyFlink (exactly-once, 30s checkpointing)
          |
          v
  Iceberg Bronze (9 tables) -> MinIO
```

## Prerequisites

- Docker >= 24.0 and Docker Compose >= 2.20
- Python 3.10+ with venv
- 8 GB RAM minimum (16 GB recommended)
- WSL2 + Ubuntu (on Windows)

## Quick Start

```bash
# 1. Clone
git clone https://github.com/thiernodaoudaly/ilam.git
cd ilam

# 2. Python environment
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# 3. Start and initialize (first time only)
make setup

# 4. Run transformations
make dbt-run && make dbt-test

# 5. Deploy and trigger Airflow pipeline
make deploy-dags && make airflow-trigger
```

## Service URLs

| Service | URL | Credentials |
|---------|-----|-------------|
| MinIO Console | http://localhost:9001 | ilam_admin / ilam_password_2025 |
| Trino UI | http://localhost:8080 | — |
| Flink UI | http://localhost:8081 | — |
| NiFi UI | https://localhost:8443 | ilam_admin / ilam_password_2025 |
| Airflow UI | http://localhost:8082 | ilam_admin / ilam_password_2025 |
| Grafana | http://localhost:3000 | — |
| dbt docs | http://localhost:8086 | run `make docs` |

## Key Commands

All commands are documented in the `Makefile`. Run `make help` for the full reference.

```bash
make setup              # First-time full setup
make health             # Check all services health
make verify-data        # Bronze table row counts
make pyflink-job        # Submit PyFlink streaming job
make streaming-status   # Streaming pipeline end-to-end status
make dbt-run            # Run dbt transformations (Bronze → Silver → Gold)
make dbt-test           # Run dbt data quality tests
make docs               # Generate and serve dbt documentation
make deploy-dags        # Deploy DAGs to Airflow
make airflow-trigger    # Trigger pipeline manually
```

## Roadmap

- [x] Step 1 — Infrastructure (Docker, MinIO, Iceberg, Trino, Flink, Airflow)
- [x] Step 2 — Iceberg DDL — 20 tables (Bronze / Silver / Gold)
- [x] Step 3 — Sonatel data generator (tests only)
- [x] Step 3b — Streaming pipeline (NiFi + Kafka + PyFlink, exactly-once)
- [x] Step 4 — dbt transformations — 18 models (Bronze → Silver → Gold)
- [x] Step 5 — Airflow orchestration — batch + streaming DAG
- [ ] Step 6 — Data quality (Great Expectations)
- [ ] Step 7 — Observability (Prometheus, Grafana)
- [ ] Step 8 — Security (Keycloak, Ranger)
- [ ] Step 9 — Agentic intelligence layer (LangGraph, ReAct)

## Architecture Decision Records

| ADR | Decision |
|-----|----------|
| [ADR-001](docs/decisions/ADR-001-iceberg-vs-delta.md) | Apache Iceberg over Delta Lake |
| [ADR-002](docs/decisions/ADR-002-trino-vs-spark.md) | Trino over Spark SQL |
| [ADR-003](docs/decisions/ADR-003-minio-vs-s3.md) | MinIO for data sovereignty |
| [ADR-004](docs/decisions/ADR-004-iceberg-rest-vs-nessie.md) | iceberg-rest + PostgreSQL over Nessie |

## License

[Apache License 2.0](LICENSE) — *Built with purpose. Named after the flow.*
