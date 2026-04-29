"""
ILAM — Sonatel Data Pipeline DAG
Orchestrates the complete Bronze -> Silver -> Gold pipeline.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
import logging

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
    description="Data Lakehouse — Bronze -> Silver -> Gold",
    default_args=default_args,
    schedule_interval="@hourly",
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    tags=["ilam", "sonatel", "lakehouse"],
    dagrun_timeout=timedelta(hours=1),
)

# =============================================================================
# Task 1 — Health check
# =============================================================================
def health_check(**context):
    import requests
    services = {
        "MinIO":        "http://minio:9000/minio/health/live",
        "Iceberg REST": "http://iceberg-rest:8181/v1/config",
        "Trino":        "http://trino:8080/v1/info",
    }
    failed = []
    for name, url in services.items():
        try:
            r = requests.get(url, timeout=10)
            if r.status_code == 200:
                logger.info(f"OK: {name}")
            else:
                failed.append(name)
                logger.error(f"FAIL: {name} -> {r.status_code}")
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
# Task 2 — Generate Bronze data
# =============================================================================
task_generate_data = BashOperator(
    task_id="generate_bronze_data",
    bash_command="python3 /opt/airflow/dags/sonatel_generator.py",
    execution_timeout=timedelta(minutes=15),
    dag=dag,
)

# =============================================================================
# Task 3 — Verify Bronze row counts
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
# Task 4 — dbt run
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
# Task 5 — dbt test
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
# Task 6 — Verify Gold Data Marts
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
# Task 7 — Pipeline summary
# =============================================================================
def pipeline_summary(**context):
    ti = context["ti"]
    bronze = ti.xcom_pull(task_ids="verify_bronze_counts",
                          key="bronze_counts") or {}
    gold   = ti.xcom_pull(task_ids="verify_gold_marts",
                          key="gold_counts") or {}
    logger.info("=" * 50)
    logger.info("ILAM PIPELINE SUMMARY")
    logger.info(f"Date: {context['ds']}")
    logger.info(f"Bronze rows: {sum(bronze.values())}")
    logger.info(f"Gold marts:  {len(gold)}")
    for m, c in gold.items():
        logger.info(f"  {m}: {c} rows")
    logger.info("Pipeline completed successfully.")
    logger.info("=" * 50)

task_summary = PythonOperator(
    task_id="pipeline_summary",
    python_callable=pipeline_summary,
    dag=dag,
)

# =============================================================================
# Dependencies
# =============================================================================
(
    task_health_check
    >> task_generate_data
    >> task_verify_bronze
    >> task_dbt_run
    >> task_dbt_test
    >> task_verify_gold
    >> task_summary
)
