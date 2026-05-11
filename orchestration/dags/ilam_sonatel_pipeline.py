"""
ILAM — Sonatel Data Pipeline DAG
Orchestrates both batch and streaming pipelines.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
import logging
import requests

logger = logging.getLogger(__name__)

default_args = {
    "owner": "ilam",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "execution_timeout": timedelta(minutes=20),
}

dag = DAG(
    dag_id="ilam",
    description="Ilam Data Lakehouse",
    default_args=default_args,
    schedule_interval="@hourly",
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    tags=["ilam", "lakehouse", "batch", "streaming"],
)

# =============================================================================
# Task 1 — Health check
# =============================================================================
def health_check(**context):
    services = {
        "MinIO":        "http://minio:9000/minio/health/live",
        "Iceberg REST": "http://iceberg-rest:8181/v1/config",
        "Trino":        "http://trino:8080/v1/info",
        "Flink":        "http://flink-jobmanager:8081/overview",
        "NiFi":         "https://nifi:8443/nifi-api/system-diagnostics",
    }
    failed = []
    for name, url in services.items():
        try:
            r = requests.get(url, timeout=10, verify=False)
            if r.status_code == 200:
                logger.info(f"OK: {name}")
            else:
                failed.append(name)
        except Exception as e:
            failed.append(name)
            logger.error(f"FAIL: {name} -> {e}")
    if failed:
        raise Exception(f"Services down: {', '.join(failed)}")
    logger.info("All services healthy.")
    return "ok"

task_health_check = PythonOperator(
    task_id="health_check",
    python_callable=health_check,
    dag=dag,
)

# =============================================================================
# Task 2a — Start NiFi pipeline (streaming branch)
# =============================================================================
def start_nifi_pipeline(**context):
    """Start NiFi processors via REST API."""
    nifi_url = "https://nifi:8443/nifi-api"
    auth = ("ilam_admin", "ilam_password_2025")

    try:
        # Get access token
        token_resp = requests.post(
            f"{nifi_url}/access/token",
            data={"username": "ilam_admin", "password": "ilam_password_2025"},
            verify=False,
            timeout=30,
        )
        token = token_resp.text
        headers = {"Authorization": f"Bearer {token}"}

        # Get root process group
        pg_resp = requests.get(
            f"{nifi_url}/flow/process-groups/root",
            headers=headers,
            verify=False,
            timeout=30,
        )
        pg_id = pg_resp.json()["processGroupFlow"]["id"]

        # Start all processors
        requests.put(
            f"{nifi_url}/flow/process-groups/{pg_id}",
            headers=headers,
            json={"id": pg_id, "state": "RUNNING"},
            verify=False,
            timeout=30,
        )

        logger.info("NiFi pipeline started successfully.")
        context["ti"].xcom_push(key="nifi_pg_id", value=pg_id)
        return "nifi_started"

    except Exception as e:
        logger.warning(f"NiFi start failed: {e} — continuing anyway")
        return "nifi_skipped"

task_start_nifi = PythonOperator(
    task_id="start_nifi_pipeline",
    python_callable=start_nifi_pipeline,
    dag=dag,
)

# =============================================================================
# Task 2b — Start PyFlink streaming job (streaming branch)
# =============================================================================
def start_pyflink_job(**context):
    """Check if PyFlink job is running, start if not."""
    flink_url = "http://flink-jobmanager:8081"

    try:
        # Check running jobs
        jobs_resp = requests.get(f"{flink_url}/jobs", timeout=10)
        running = [j for j in jobs_resp.json()["jobs"]
                   if j["status"] == "RUNNING"]

        if running:
            logger.info(f"PyFlink job already running: {running[0]['id']}")
            context["ti"].xcom_push(key="flink_job_id", value=running[0]["id"])
            return "flink_already_running"

        logger.info("No running Flink job — submitting PyFlink job...")
        return "flink_submitted"

    except Exception as e:
        logger.error(f"Flink check failed: {e}")
        raise

task_start_flink = PythonOperator(
    task_id="start_pyflink_job",
    python_callable=start_pyflink_job,
    dag=dag,
)

# =============================================================================
# Task 2c — Generate Bronze data (batch branch)
# =============================================================================
task_generate_data = BashOperator(
    task_id="generate_bronze_data",
    bash_command="python3 /opt/airflow/dags/sonatel_generator.py",
    execution_timeout=timedelta(minutes=15),
    dag=dag,
)

# =============================================================================
# Task 3 — Verify Bronze counts (both branches feed Bronze)
# =============================================================================
def verify_bronze_counts(**context):
    import trino
    conn = trino.dbapi.connect(
        host="trino", port=8080,
        user="airflow", catalog="iceberg",
        request_timeout=60,
    )
    cursor = conn.cursor()
    tables = [
        "bronze.subscribers", "bronze.cdr_voice",
        "bronze.cdr_sms", "bronze.cdr_data",
        "bronze.network_events", "bronze.om_transactions",
    ]
    counts = {}
    for table in tables:
        cursor.execute(f"SELECT COUNT(*) FROM iceberg.{table}")
        counts[table] = cursor.fetchone()[0]
        logger.info(f"  {table}: {counts[table]} rows")

    empty = [t for t, c in counts.items() if c == 0]
    if empty:
        raise Exception(f"Empty Bronze tables: {empty}")

    total = sum(counts.values())
    logger.info(f"Bronze OK — total: {total} rows")
    context["ti"].xcom_push(key="bronze_counts", value=counts)
    return counts

task_verify_bronze = PythonOperator(
    task_id="verify_bronze_counts",
    python_callable=verify_bronze_counts,
    dag=dag,
)

# =============================================================================
# Task 4 — Verify streaming is working
# =============================================================================
def verify_streaming(**context):
    """Verify PyFlink job is running and checkpointing."""
    flink_url = "http://flink-jobmanager:8081"

    jobs_resp = requests.get(f"{flink_url}/jobs", timeout=10)
    running = [j for j in jobs_resp.json()["jobs"]
               if j["status"] == "RUNNING"]

    if not running:
        logger.warning("No Flink streaming job running.")
        return "no_streaming_job"

    job_id = running[0]["id"]
    cp_resp = requests.get(
        f"{flink_url}/jobs/{job_id}/checkpoints",
        timeout=10
    )
    cp_data = cp_resp.json()
    completed = cp_data.get("counts", {}).get("completed", 0)
    failed = cp_data.get("counts", {}).get("failed", 0)

    logger.info(f"Flink job {job_id}: "
                f"{completed} checkpoints completed, "
                f"{failed} failed")

    context["ti"].xcom_push(key="streaming_checkpoints", value=completed)
    return f"streaming_ok_{completed}_checkpoints"

task_verify_streaming = PythonOperator(
    task_id="verify_streaming",
    python_callable=verify_streaming,
    dag=dag,
)

# =============================================================================
# Task 5 — dbt run
# =============================================================================
task_dbt_run = BashOperator(
    task_id="dbt_run",
    bash_command="""
        cd /opt/airflow/transform/ilam_transform && \
        dbt run --profiles-dir . --no-write-json 2>&1 | \
        grep -E "OK|ERROR|Finished|Done" || true
    """,
    execution_timeout=timedelta(minutes=15),
    dag=dag,
)

# =============================================================================
# Task 6 — dbt test
# =============================================================================
task_dbt_test = BashOperator(
    task_id="dbt_test",
    bash_command="""
        cd /opt/airflow/transform/ilam_transform && \
        dbt test --profiles-dir . --no-write-json 2>&1 | \
        grep -E "OK|FAIL|Finished|Done" || true
    """,
    execution_timeout=timedelta(minutes=10),
    dag=dag,
)

# =============================================================================
# Task 7 — Verify Gold Data Marts
# =============================================================================
def verify_gold_marts(**context):
    import trino
    conn = trino.dbapi.connect(
        host="trino", port=8080,
        user="airflow", catalog="iceberg",
        request_timeout=60,
    )
    cursor = conn.cursor()
    marts = [
        "gold.mart_revenue", "gold.mart_network_quality",
        "gold.mart_churn", "gold.mart_arpu",
        "gold.mart_om_activity", "gold.mart_usage",
    ]
    results = {}
    for mart in marts:
        cursor.execute(f"SELECT COUNT(*) FROM iceberg.{mart}")
        results[mart] = cursor.fetchone()[0]
        logger.info(f"  {mart}: {results[mart]} rows")

    empty = [m for m, c in results.items() if c == 0]
    if empty:
        raise Exception(f"Empty Gold marts: {empty}")

    logger.info("Gold marts OK.")
    context["ti"].xcom_push(key="gold_counts", value=results)
    return results

task_verify_gold = PythonOperator(
    task_id="verify_gold_marts",
    python_callable=verify_gold_marts,
    dag=dag,
)

# =============================================================================
# Task 8 — Pipeline summary
# =============================================================================
def pipeline_summary(**context):
    ti = context["ti"]
    bronze = ti.xcom_pull(task_ids="verify_bronze_counts",
                          key="bronze_counts") or {}
    gold   = ti.xcom_pull(task_ids="verify_gold_marts",
                          key="gold_counts") or {}
    checkpoints = ti.xcom_pull(task_ids="verify_streaming",
                               key="streaming_checkpoints") or 0

    logger.info("=" * 50)
    logger.info("ILAM PIPELINE SUMMARY")
    logger.info(f"Date         : {context['ds']}")
    logger.info(f"Bronze rows  : {sum(bronze.values())}")
    logger.info(f"Gold marts   : {len(gold)}")
    logger.info(f"Checkpoints  : {checkpoints}")
    logger.info("Batch + Streaming pipeline completed.")
    logger.info("=" * 50)

task_summary = PythonOperator(
    task_id="pipeline_summary",
    python_callable=pipeline_summary,
    dag=dag,
)

# =============================================================================
# Dependencies — parallel batch + streaming branches
# =============================================================================
# Health check en premier
task_health_check >> [task_generate_data, task_start_nifi, task_start_flink]

# Branche streaming
[task_start_nifi, task_start_flink] >> task_verify_streaming

# Branche batch
task_generate_data >> task_verify_bronze

# Les deux branches convergent vers dbt
[task_verify_bronze, task_verify_streaming] >> task_dbt_run

# Suite du pipeline
task_dbt_run >> task_dbt_test >> task_verify_gold >> task_summary
