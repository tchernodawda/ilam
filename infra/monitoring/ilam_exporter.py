"""
ILAM — Custom Prometheus Exporter
Collecte les métriques depuis les APIs REST de tous les services
et les expose au format Prometheus sur :8089/metrics.
"""

import time
import requests
from prometheus_client import start_http_server, Gauge, Counter, Info
from prometheus_client.core import CollectorRegistry
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# =============================================================================
# Métriques Prometheus
# =============================================================================

# Flink
flink_jobs_running    = Gauge("ilam_flink_jobs_running",
                               "Number of running Flink jobs")
flink_jobs_finished   = Gauge("ilam_flink_jobs_finished",
                               "Number of finished Flink jobs")
flink_taskmanagers    = Gauge("ilam_flink_taskmanagers",
                               "Number of Flink task managers")
flink_slots_total     = Gauge("ilam_flink_slots_total",
                               "Total Flink slots")
flink_slots_available = Gauge("ilam_flink_slots_available",
                               "Available Flink slots")
flink_checkpoints_completed = Gauge(
    "ilam_flink_checkpoints_completed",
    "Completed checkpoints for streaming job")
flink_checkpoints_failed = Gauge(
    "ilam_flink_checkpoints_failed",
    "Failed checkpoints for streaming job")

# Kafka
kafka_topics_count    = Gauge("ilam_kafka_topics_count",
                               "Number of Kafka topics")

# Trino
trino_up              = Gauge("ilam_trino_up",
                               "Trino coordinator is up")
trino_starting        = Gauge("ilam_trino_starting",
                               "Trino is starting")

# MinIO
minio_up              = Gauge("ilam_minio_up",
                               "MinIO is up")

# Iceberg
iceberg_up            = Gauge("ilam_iceberg_up",
                               "Iceberg REST catalog is up")

# Services global
service_up            = Gauge("ilam_service_up",
                               "Service health status",
                               ["service"])

# Iceberg table counts
iceberg_table_rows    = Gauge("ilam_iceberg_table_rows",
                               "Number of rows in Iceberg table",
                               ["schema", "table"])

# =============================================================================
# Collecte des métriques
# =============================================================================

def collect_flink_metrics():
    try:
        # Overview
        r = requests.get("http://flink-jobmanager:8081/overview",
                         timeout=5)
        data = r.json()
        flink_jobs_running.set(data.get("jobs-running", 0))
        flink_jobs_finished.set(data.get("jobs-finished", 0))
        flink_taskmanagers.set(data.get("taskmanagers", 0))
        flink_slots_total.set(data.get("slots-total", 0))
        flink_slots_available.set(data.get("slots-available", 0))
        service_up.labels(service="flink").set(1)

        # Checkpoints du job en cours
        jobs = requests.get("http://flink-jobmanager:8081/jobs",
                            timeout=5).json()
        running = [j for j in jobs["jobs"]
                   if j["status"] == "RUNNING"]
        if running:
            job_id = running[0]["id"]
            cp = requests.get(
                f"http://flink-jobmanager:8081/jobs/{job_id}/checkpoints",
                timeout=5).json()
            counts = cp.get("counts", {})
            flink_checkpoints_completed.set(
                counts.get("completed", 0))
            flink_checkpoints_failed.set(
                counts.get("failed", 0))

    except Exception as e:
        service_up.labels(service="flink").set(0)
        logger.warning(f"Flink metrics error: {e}")


def collect_trino_metrics():
    try:
        r = requests.get("http://trino:8080/v1/info", timeout=5)
        data = r.json()
        trino_up.set(1 if not data.get("starting", True) else 0)
        trino_starting.set(1 if data.get("starting", False) else 0)
        service_up.labels(service="trino").set(1)
    except Exception as e:
        service_up.labels(service="trino").set(0)
        trino_up.set(0)
        logger.warning(f"Trino metrics error: {e}")


def collect_minio_metrics():
    try:
        r = requests.get(
            "http://minio:9000/minio/health/live", timeout=5)
        minio_up.set(1 if r.status_code == 200 else 0)
        service_up.labels(service="minio").set(
            1 if r.status_code == 200 else 0)
    except Exception as e:
        service_up.labels(service="minio").set(0)
        minio_up.set(0)
        logger.warning(f"MinIO metrics error: {e}")


def collect_iceberg_metrics():
    try:
        r = requests.get(
            "http://iceberg-rest:8181/v1/config", timeout=5)
        iceberg_up.set(1 if r.status_code == 200 else 0)
        service_up.labels(service="iceberg").set(
            1 if r.status_code == 200 else 0)
    except Exception as e:
        service_up.labels(service="iceberg").set(0)
        iceberg_up.set(0)
        logger.warning(f"Iceberg metrics error: {e}")


def collect_table_counts():
    """Collecte les counts des tables Bronze/Silver/Gold via Trino."""
    try:
        import trino as trino_client
        conn = trino_client.dbapi.connect(
            host="trino", port=8080,
            user="exporter", catalog="iceberg",
            request_timeout=30,
        )
        cursor = conn.cursor()

        tables = {
            "bronze": ["cdr_voice", "cdr_sms", "cdr_data",
                       "network_events", "om_transactions",
                       "subscribers", "contracts",
                       "recharges", "complaints"],
            "silver": ["cdr_enriched", "subscribers",
                       "contracts", "om_transactions",
                       "network_events"],
            "gold"  : ["mart_revenue", "mart_network_quality",
                       "mart_churn", "mart_arpu",
                       "mart_om_activity", "mart_usage"],
        }

        for schema, tbls in tables.items():
            for tbl in tbls:
                try:
                    cursor.execute(
                        f"SELECT COUNT(*) FROM iceberg.{schema}.{tbl}")
                    count = cursor.fetchone()[0]
                    iceberg_table_rows.labels(
                        schema=schema, table=tbl).set(count)
                except Exception:
                    pass

    except Exception as e:
        logger.warning(f"Table counts error: {e}")


def collect_all_metrics():
    """Collecte toutes les métriques."""
    collect_flink_metrics()
    collect_trino_metrics()
    collect_minio_metrics()
    collect_iceberg_metrics()
    collect_table_counts()
    logger.info("Metrics collected")


if __name__ == "__main__":
    # Démarrer le serveur HTTP Prometheus sur le port 8089
    start_http_server(8089)
    logger.info("ILAM Exporter started on :8089/metrics")

    while True:
        collect_all_metrics()
        time.sleep(30)  # Collecter toutes les 30 secondes
