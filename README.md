# Ilam — Open-Source Data Lakehouse Platform

> *Ilam* means **"the lake"**, **"the flow"**, **"the water current"**, or **"the stream"** in Pulaar — data flowing from raw sources to insight, continuously and autonomously.

<p align="center">
  <a href="LICENSE">
    <img src="https://img.shields.io/badge/License-Apache%202.0-blue.svg" />
  </a>
  <img src="https://img.shields.io/badge/stack-Iceberg%20%7C%20Trino%20%7C%20Flink%20%7C%20dbt%20%7C%20Airflow-orange" />
  <img src="https://img.shields.io/badge/status-WIP-yellow" />
  <img src="https://img.shields.io/badge/PRs-welcome-brightgreen.svg" />
</p>

Ilam is a fully open-source, end-to-end Data Lakehouse platform designed to unify high-performance storage, real-time stream processing, semantic transformation, and intelligent orchestration — with zero proprietary dependencies.

At its core, Ilam introduces an **agentic intelligence layer** that transcends static pipeline orchestration toward a dynamic, self-adaptive data flow management system, optimizing resilience and observability from end to end.

Built on real-world telecom data from the **Sonatel / Orange Sénégal** context, covering CDR (voice, SMS, data), network events, CRM, and Orange Money transactions.

## Table of Contents

- [Project Structure](#project-structure)
- [Architecture](#architecture)
- [Tech Stack](#tech-stack)
- [Data Model](#data-model--sonatel-telecom)
- [Prerequisites](#prerequisites)
- [Quick Start](#quick-start)
- [Service URLs](#service-urls)
- [Makefile Reference](#makefile-reference)
- [Streaming Pipeline](#streaming-pipeline-kafka--flink)
- [Orchestration](#orchestration--airflow)
- [Architecture Decision Records](#architecture-decision-records)
- [Roadmap](#roadmap)
- [License](#license)

## Project Structure

```
ilam/
├── docs/
│   └── decisions/               Architecture Decision Records (ADR-001 to ADR-004)
├── infra/
│   ├── trino/                   Trino catalog and server configuration
│   ├── flink/                   Flink configuration and JARs (see make download-flink-jars)
│   └── monitoring/              Prometheus and Grafana configuration
├── ingestion/
│   ├── generators/              Sonatel simulated data generator (Python + Faker)
│   ├── streaming/               Kafka producer + Flink SQL job
│   └── kafka-connect/           Kafka Connect connector configs (production)
├── warehouse/
│   ├── bronze/ddl/              Raw layer DDL — 9 tables
│   ├── silver/ddl/              Enriched layer DDL — 5 tables
│   └── gold/ddl/                Analytical layer DDL — 6 tables
├── transform/
│   └── ilam_transform/          dbt project (models, tests, macros)
├── orchestration/
│   └── dags/                    Airflow DAGs + data generator
├── scripts/
│   └── deploy_dags.sh           DAG deployment script (WSL2 workaround)
├── agents/                      Agentic intelligence layer (coming — Step 9)
├── quality/                     Great Expectations suites (coming — Step 6)
├── .env.example                 Environment variables template
├── docker-compose.yml           Full infrastructure definition
├── Makefile                     All lifecycle commands
└── requirements.txt             Python dependencies
```

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│  Agentic Layer         LangGraph · ReAct · autonomous agents    │
├─────────────────────────────────────────────────────────────────┤
│  Orchestration         Apache Airflow · DAGs · Sensors          │
├───────────────────────────┬─────────────────────────────────────┤
│  Transformation           │  Query Engine                       │
│  dbt Core                 │  Trino · MPP · federation           │
├───────────────────────────┴─────────────────────────────────────┤
│  Medallion Architecture   Bronze → Silver → Gold                │
├─────────────────────────────────────────────────────────────────┤
│  Storage                  Apache Iceberg · MinIO · Parquet      │
├─────────────────────────────────────────────────────────────────┤
│  Ingestion                Apache Kafka · Apache Flink           │
│                           stream processing · exactly-once      │
├─────────────────────────────────────────────────────────────────┤
│  Security                 Keycloak · Apache Ranger · mTLS       │
│  Observability            Prometheus · Grafana · OpenLineage    │
└─────────────────────────────────────────────────────────────────┘
```

## Tech Stack

| Layer | Tool | Version | Role |
|-------|------|---------|------|
| Ingestion (stream) | Apache Kafka | KRaft | Event streaming, CDR ingestion |
| Ingestion (stream) | Apache Flink | 1.18 | Stream processing, exactly-once |
| Storage | MinIO | latest | S3-compatible object storage |
| Table format | Apache Iceberg | 1.5.2 | ACID transactions, time travel |
| Catalog | iceberg-rest + PostgreSQL | — | Persistent REST catalog |
| Query engine | Trino | 435 | Distributed SQL, MPP |
| Transformation | dbt Core | — | ELT pipelines, semantic layer |
| Orchestration | Apache Airflow | 2.8.1 | DAG orchestration |
| Data quality | Great Expectations | — | Automated quality checks |
| Monitoring | Prometheus + Grafana | — | Metrics and dashboards |
| Lineage | OpenLineage + Marquez | — | End-to-end data lineage |
| Security | Keycloak + Apache Ranger | — | AuthN/AuthZ |
| Agents | LangGraph | — | Agentic intelligence layer |

## Data Model — Sonatel Telecom

The platform is built around a realistic Senegalese telecom data model following the Medallion architecture.

| Layer | Tables | Description |
|-------|--------|-------------|
| Bronze | `cdr_voice`, `cdr_sms`, `cdr_data`, `network_events` | Raw CDR and network data |
| Bronze | `subscribers`, `contracts`, `recharges`, `complaints`, `om_transactions` | Raw CRM and Orange Money |
| Silver | `cdr_enriched`, `network_events`, `subscribers`, `contracts`, `om_transactions` | Enriched business entities |
| Gold | `mart_revenue`, `mart_network_quality`, `mart_churn`, `mart_arpu`, `mart_om_activity`, `mart_usage` | Analytical Data Marts |

## Prerequisites

- Docker >= 24.0 and Docker Compose >= 2.20
- Make
- Python 3.10+ with venv
- 8 GB RAM minimum (16 GB recommended)
- WSL2 + Ubuntu (on Windows)

## Quick Start

```bash
# 1. Clone and configure
git clone https://github.com/thiernodaoudaly/ilam.git
cd ilam
cp .env.example .env
# Generate a Fernet key for Airflow:
# python3 -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"

# 2. Create Python virtual environment
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# 3. Start all services
make start && make health

# 4. Initialize the warehouse (first time only)
make init-warehouse

# 5. Generate simulated Sonatel data
make generate-data && make verify-data

# 6. Run dbt transformations
make dbt-run && make dbt-test

# 7. Deploy and trigger Airflow pipeline
make deploy-dags && make pipeline-run
```

## Service URLs

| Service | URL | Credentials |
|---------|-----|-------------|
| MinIO Console | http://localhost:9001 | See `.env` |
| Trino UI | http://localhost:8080 | None (dev) |
| Flink UI | http://localhost:8081 | None (dev) |
| Airflow UI | http://localhost:8082 | See `.env` |
| Prometheus | http://localhost:9090 | None |
| Grafana | http://localhost:3000 | See `.env` |
| Iceberg REST | http://localhost:8181 | None |

## Makefile Reference

```bash
# Infrastructure
make start              # Start all services
make stop               # Stop all services
make health             # Check all services health
make clean              # Remove containers and volumes

# Warehouse
make init-warehouse     # Create Iceberg schemas and tables (first time only)
make verify-data        # Check row counts in Bronze tables
make trino-cli          # Open interactive Trino SQL shell

# Data & Transformations
make generate-data      # Generate simulated Sonatel data
make dbt-run            # Run dbt transformations (Bronze → Silver → Gold)
make dbt-test           # Run dbt data quality tests

# Streaming (Kafka + Flink)
make produce            # Start Kafka CDR producer
make flink-job          # Submit Flink streaming job
make download-flink-jars # Download required Flink JARs

# Orchestration (Airflow)
make deploy-dags        # Deploy DAGs to Airflow containers
make pipeline-run       # Run full pipeline (stops non-essential services)
make pipeline-stop      # Stop pipeline
make airflow-status     # Show last DAG runs status
make airflow-logs       # Tail scheduler logs
```

## Streaming Pipeline — Kafka + Flink

The streaming pipeline ingests real-time CDR events from Kafka into Iceberg Bronze tables.

```
Python Producer (simulates MSC/GGSN)
        ↓  localhost:9094
    Apache Kafka (KRaft)
    ├── cdr_voice_stream    (3 partitions)
    └── network_events_stream (3 partitions)
        ↓  ilam-kafka:9092
    Apache Flink SQL Job
        ↓
    iceberg.bronze.cdr_voice
    iceberg.bronze.network_events
```

```bash
# Start producer (Terminal 1)
.venv/bin/python3 ingestion/streaming/kafka_producer.py

# Submit Flink job (Terminal 2)
make flink-job

# Download JARs (first time)
make download-flink-jars
```

> **Note:** In production, the Python producer is replaced by Kafka Connect connectors (Debezium CDC for Oracle CRM, FileStream for CDR files, HTTP Source for Orange Money). See `ingestion/kafka-connect/README.md`.

> **Known limitation:** Flink SQL Client checkpointing not triggering in WSL2+Docker environment. Jobs run in RUNNING state but Iceberg commits require a dedicated infrastructure. See `docs/decisions/` for details.

## Orchestration — Airflow

The `ilam_sonatel_pipeline` DAG orchestrates the full Bronze → Silver → Gold pipeline.

```
health_check → generate_bronze_data → verify_bronze_counts
    → dbt_run → dbt_test → verify_gold_marts → pipeline_summary
```

**Execution results (last successful run):**

| Mart | Rows |
|------|------|
| mart_revenue | 897 |
| mart_arpu | 112 |
| mart_churn | 40 |
| mart_usage | 840 |

```bash
# Deploy and run
make deploy-dags
make pipeline-run

# Monitor at http://localhost:8082
```

> **Deployment note:** Due to WSL2 bind mount limitations, DAGs are deployed via `docker cp` using `scripts/deploy_dags.sh` rather than a volume mount.

## Architecture Decision Records

| ADR | Decision | Rationale |
|-----|----------|-----------|
| [ADR-001](docs/decisions/ADR-001-iceberg-vs-delta.md) | Apache Iceberg over Delta Lake | Vendor-neutral, REST catalog support |
| [ADR-002](docs/decisions/ADR-002-trino-vs-spark.md) | Trino over Spark SQL | Lighter footprint, faster ad-hoc queries |
| [ADR-003](docs/decisions/ADR-003-minio-vs-s3.md) | MinIO for data sovereignty | On-premise, S3-compatible, no cloud dependency |
| [ADR-004](docs/decisions/ADR-004-iceberg-rest-vs-nessie.md) | iceberg-rest + PostgreSQL over Nessie | Simpler setup, persistent catalog |

## Roadmap

- [x] **Step 1** — Base infrastructure (Docker Compose, MinIO, Iceberg REST, Trino, Flink, Airflow)
- [x] **Step 2** — Iceberg DDL — 20 tables (Bronze / Silver / Gold)
- [x] **Step 3** — Sonatel data generator — 9 Bronze tables, realistic telecom data
- [x] **Step 3b** — Streaming pipeline — Kafka (KRaft) + Flink SQL *(partial — checkpointing blocked on WSL2)*
- [x] **Step 4** — dbt transformations — 18 models (Bronze → Silver → Gold)
- [x] **Step 5** — Airflow orchestration — 7-task DAG, end-to-end pipeline
- [ ] **Step 6** — Data quality (Great Expectations)
- [ ] **Step 7** — Observability (Prometheus dashboards, Grafana, OpenLineage)
- [ ] **Step 8** — Security (Keycloak, Apache Ranger)
- [ ] **Step 9** — Agentic intelligence layer (LangGraph, ReAct)

## License

[Apache License 2.0](LICENSE)

*Built with purpose. Named after the flow.*