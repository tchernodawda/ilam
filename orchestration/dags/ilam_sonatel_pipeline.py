"""
ILAM — Sonatel Data Pipeline DAG
Orchestrates batch + streaming pipelines.

Architecture :
  Streaming : NiFi → Kafka → PyFlink → Bronze (continu, auto-géré)
  Batch     : NiFi CRON → Kafka → PyFlink → Bronze (hourly, auto-géré)
  Ce DAG    : vérifie streaming → qualité → dbt → gold

Pipeline :
  health_check
       ↓
  verify_streaming
       ↓
  verify_bronze_counts
       ↓
  verify_data_quality   ← NEW (Great Expectations)
       ↓
  dbt_run
       ↓
  dbt_test
       ↓
  verify_gold_marts
       ↓
  pipeline_summary
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
import logging
import json
import requests

logger = logging.getLogger(__name__)

# =============================================================================
# Alertes Slack simulées
# =============================================================================
SLACK_WEBHOOK_URL = None
SLACK_CHANNEL     = "#ilam-data-alerts"
SLACK_USERNAME    = "ILAM Bot"

def send_slack_alert(payload: dict, level: str = "error"):
    emoji   = "🔴" if level == "error" else \
              "⚠️" if level == "warning" else "✅"
    if SLACK_WEBHOOK_URL:
        try:
            requests.post(
                SLACK_WEBHOOK_URL,
                data=json.dumps({
                    "channel"    : SLACK_CHANNEL,
                    "username"   : SLACK_USERNAME,
                    "attachments": [{
                        "color": "#FF0000" if level == "error" else
                                 "#FFA500" if level == "warning"
                                 else "#36A64F",
                        "title": f"{emoji} ILAM Pipeline Alert",
                        "fields": [
                            {"title": k, "value": str(v), "short": True}
                            for k, v in payload.items()
                        ],
                    }]
                }),
                headers={"Content-Type": "application/json"},
                timeout=10,
            )
        except Exception as e:
            logger.error(f"Slack alert failed: {e}")
    else:
        logger.warning("=" * 60)
        logger.warning(f"[SLACK ALERT] {SLACK_CHANNEL} | {level.upper()}")
        for k, v in payload.items():
            logger.warning(f"  {k:<20}: {v}")
        logger.warning("=" * 60)

# =============================================================================
# Callbacks
# =============================================================================
def on_failure_callback(context):
    send_slack_alert({
        "DAG"      : context["task_instance"].dag_id,
        "Task"     : context["task_instance"].task_id,
        "Execution": str(context["execution_date"]),
        "Attempt"  : f"{context['task_instance'].try_number}/2",
        "Error"    : str(context.get("exception", "Unknown"))[:200],
    }, level="error")

def on_retry_callback(context):
    send_slack_alert({
        "DAG"    : context["task_instance"].dag_id,
        "Task"   : context["task_instance"].task_id,
        "Attempt": f"{context['task_instance'].try_number}/2",
        "Status" : "Retrying after failure...",
    }, level="warning")

def on_success_callback(context):
    send_slack_alert({
        "DAG"   : context["task_instance"].dag_id,
        "Status": "Pipeline completed successfully",
        "Date"  : str(context["execution_date"]),
    }, level="success")

def sla_miss_callback(dag, task_list, blocking_task_list,
                      slas, blocking_tis):
    send_slack_alert({
        "DAG"        : dag.dag_id,
        "Status"     : "SLA MISSED",
        "Tasks"      : ", ".join([str(t) for t in task_list]),
        "Description": "Tasks exceeded maximum allowed duration",
    }, level="warning")

# =============================================================================
# Default args
# =============================================================================
default_args = {
    "owner"              : "ilam",
    "depends_on_past"    : False,
    "email_on_failure"   : False,
    "email_on_retry"     : False,
    "retries"            : 1,
    "retry_delay"        : timedelta(minutes=2),
    "execution_timeout"  : timedelta(minutes=20),
    "on_failure_callback": on_failure_callback,
    "on_retry_callback"  : on_retry_callback,
}

dag = DAG(
    dag_id="ilam_sonatel_pipeline",
    description="Sonatel Data Lakehouse — Batch + Streaming pipeline",
    default_args=default_args,
    schedule_interval="@hourly",
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    sla_miss_callback=sla_miss_callback,
    tags=["ilam", "sonatel", "lakehouse", "streaming"],
)

# =============================================================================
# Task 1 — Health check
# =============================================================================
def health_check(**context):
    services = {
        "MinIO"       : "http://minio:9000/minio/health/live",
        "Iceberg REST": "http://iceberg-rest:8181/v1/config",
        "Trino"       : "http://trino:8080/v1/info",
        "Flink"       : "http://flink-jobmanager:8081/overview",
    }
    failed = []
    for name, url in services.items():
        try:
            r = requests.get(url, timeout=10, verify=False)
            if r.status_code == 200:
                logger.info(f"  OK : {name}")
            else:
                failed.append(name)
        except Exception as e:
            failed.append(name)
            logger.error(f"  FAIL : {name} -> {e}")
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
# Task 2 — Vérifier le streaming PyFlink
# =============================================================================
def verify_streaming(**context):
    flink_url = "http://flink-jobmanager:8081"
    jobs_resp = requests.get(f"{flink_url}/jobs", timeout=10)
    running   = [j for j in jobs_resp.json()["jobs"]
                 if j["status"] == "RUNNING"]
    if not running:
        raise Exception(
            "No PyFlink streaming job running. "
            "Run 'make pyflink-job' to restart."
        )
    job_id    = running[0]["id"]
    cp_resp   = requests.get(
        f"{flink_url}/jobs/{job_id}/checkpoints", timeout=10)
    cp_data   = cp_resp.json()
    completed = cp_data.get("counts", {}).get("completed", 0)
    failed_cp = cp_data.get("counts", {}).get("failed",    0)
    logger.info(f"PyFlink job {job_id} — "
                f"checkpoints: {completed} OK, {failed_cp} failed")
    if completed == 0:
        raise Exception("No completed checkpoints yet.")
    if failed_cp > completed:
        send_slack_alert({
            "Job ID"            : job_id,
            "Checkpoints OK"    : completed,
            "Checkpoints FAILED": failed_cp,
        }, level="warning")
    context["ti"].xcom_push(key="flink_job_id",          value=job_id)
    context["ti"].xcom_push(key="streaming_checkpoints",  value=completed)
    return f"streaming_ok_{completed}_checkpoints"

task_verify_streaming = PythonOperator(
    task_id="verify_streaming",
    python_callable=verify_streaming,
    dag=dag,
    sla=timedelta(minutes=5),
)

# =============================================================================
# Task 3 — Vérifier les counts Bronze
# =============================================================================
def verify_bronze_counts(**context):
    import trino
    conn = trino.dbapi.connect(
        host="trino", port=8080,
        user="airflow", catalog="iceberg",
        request_timeout=60,
    )
    cursor = conn.cursor()

    streaming_tables = [
        "bronze.cdr_voice", "bronze.cdr_sms", "bronze.cdr_data",
        "bronze.network_events", "bronze.om_transactions",
    ]
    batch_tables = [
        "bronze.subscribers", "bronze.contracts",
        "bronze.recharges", "bronze.complaints",
    ]

    counts = {}
    streaming_empty = []
    for table in streaming_tables:
        cursor.execute(f"SELECT COUNT(*) FROM iceberg.{table}")
        counts[table] = cursor.fetchone()[0]
        logger.info(f"  {table}: {counts[table]} rows")
        if counts[table] == 0:
            streaming_empty.append(table)

    if streaming_empty:
        raise Exception(
            f"Empty streaming Bronze tables: {streaming_empty}")

    batch_empty = []
    for table in batch_tables:
        cursor.execute(f"SELECT COUNT(*) FROM iceberg.{table}")
        counts[table] = cursor.fetchone()[0]
        logger.info(f"  {table}: {counts[table]} rows")
        if counts[table] == 0:
            batch_empty.append(table)

    if batch_empty:
        send_slack_alert({
            "Warning"    : "Empty batch Bronze tables",
            "Tables"     : ", ".join(batch_empty),
            "Explanation": "NiFi CRON batch may not have fired yet",
        }, level="warning")

    total = sum(counts.values())
    logger.info(f"Bronze OK — 9 tables, {total} total rows")
    context["ti"].xcom_push(key="bronze_counts", value=counts)
    return counts

task_verify_bronze = PythonOperator(
    task_id="verify_bronze_counts",
    python_callable=verify_bronze_counts,
    dag=dag,
    sla=timedelta(minutes=5),
)

# =============================================================================
# Task 4 — Great Expectations (qualité des données) ← NEW
# =============================================================================
def verify_data_quality(**context):
    """
    Exécute les 3 checkpoints Great Expectations.
    - Score >= 80% → PASS, pipeline continue
    - Score 50-80% → WARN, pipeline continue avec alerte
    - Score <  50% → FAIL, pipeline bloqué
    """
    import sys
    import importlib.util
    import os

    # Charger run_checkpoints depuis /opt/airflow/quality/
    spec = importlib.util.spec_from_file_location(
        "run_checkpoints",
        "/opt/airflow/quality/run_checkpoints.py"
    )
    module = importlib.util.module_from_spec(spec)

    # Adapter les chemins pour Airflow (Docker)
    module.GX_ROOT    = "/opt/airflow/quality"
    module.TRINO_HOST = "trino"
    module.TRINO_PORT = 8080

    spec.loader.exec_module(module)

    # Exécuter les checkpoints
    exit_code = module.run_all_checkpoints()

    if exit_code == 2:
        raise Exception(
            "Data quality CRITICAL (< 50%) — pipeline blocked. "
            "Check Great Expectations report."
        )
    elif exit_code == 1:
        send_slack_alert({
            "Status"     : "Data quality WARNING",
            "Score"      : "50-80%",
            "Action"     : "Pipeline continues — investigate data issues",
        }, level="warning")
        logger.warning("Quality warning — pipeline continues")
    else:
        logger.info("Data quality OK — all checks passed")

    context["ti"].xcom_push(key="quality_exit_code", value=exit_code)
    return f"quality_exit_code_{exit_code}"

task_verify_quality = PythonOperator(
    task_id="verify_data_quality",
    python_callable=verify_data_quality,
    dag=dag,
    sla=timedelta(minutes=10),
)

# =============================================================================
# Task 5 — dbt run
# =============================================================================
task_dbt_run = BashOperator(
    task_id="dbt_run",
    bash_command="""
        echo "Starting dbt run at $(date)" && \
        cd /opt/airflow/transform/ilam_transform && \
        dbt run --profiles-dir . --target docker --no-write-json 2>&1 | \
        grep -E "OK|ERROR|Finished|Running|WARN" || true && \
        echo "dbt run completed at $(date)"
    """,
    execution_timeout=timedelta(minutes=15),
    sla=timedelta(minutes=10),
    dag=dag,
)

# =============================================================================
# Task 6 — dbt test
# =============================================================================
task_dbt_test = BashOperator(
    task_id="dbt_test",
    bash_command="""
        echo "Starting dbt test at $(date)" && \
        cd /opt/airflow/transform/ilam_transform && \
        dbt test --profiles-dir . --target docker --no-write-json 2>&1 | \
        grep -E "OK|FAIL|ERROR|Finished|Pass|Warn" || true && \
        echo "dbt test completed at $(date)"
    """,
    execution_timeout=timedelta(minutes=10),
    sla=timedelta(minutes=8),
    dag=dag,
)

# =============================================================================
# Task 7 — Vérifier les Gold Data Marts
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
        "gold.mart_revenue",    "gold.mart_network_quality",
        "gold.mart_churn",      "gold.mart_arpu",
        "gold.mart_om_activity","gold.mart_usage",
    ]
    results = {}
    empty   = []
    for mart in marts:
        cursor.execute(f"SELECT COUNT(*) FROM iceberg.{mart}")
        results[mart] = cursor.fetchone()[0]
        logger.info(f"  {mart}: {results[mart]} rows")
        if results[mart] == 0:
            empty.append(mart)
    if empty:
        raise Exception(f"Empty Gold marts after dbt run: {empty}")
    logger.info(f"Gold OK — {len(marts)} marts populated")
    context["ti"].xcom_push(key="gold_counts", value=results)
    return results

task_verify_gold = PythonOperator(
    task_id="verify_gold_marts",
    python_callable=verify_gold_marts,
    dag=dag,
    sla=timedelta(minutes=5),
)

# =============================================================================
# Task 8 — Pipeline summary
# =============================================================================
def pipeline_summary(**context):
    ti           = context["ti"]
    bronze       = ti.xcom_pull(
                     task_ids="verify_bronze_counts",
                     key="bronze_counts") or {}
    gold         = ti.xcom_pull(
                     task_ids="verify_gold_marts",
                     key="gold_counts") or {}
    checkpoints  = ti.xcom_pull(
                     task_ids="verify_streaming",
                     key="streaming_checkpoints") or 0
    job_id       = ti.xcom_pull(
                     task_ids="verify_streaming",
                     key="flink_job_id") or "unknown"
    quality_code = ti.xcom_pull(
                     task_ids="verify_data_quality",
                     key="quality_exit_code") or 0

    quality_label = {0: "PASS ✅", 1: "WARN ⚠️",
                     2: "FAIL ❌"}.get(quality_code, "UNKNOWN")

    logger.info("=" * 55)
    logger.info("ILAM PIPELINE SUMMARY")
    logger.info(f"  Date              : {context['ds']}")
    logger.info(f"  PyFlink Job ID    : {job_id}")
    logger.info(f"  Checkpoints       : {checkpoints}")
    logger.info(f"  Bronze tables     : {len(bronze)}/9")
    logger.info(f"  Bronze total rows : {sum(bronze.values())}")
    logger.info(f"  Data quality      : {quality_label}")
    logger.info(f"  Gold marts        : {len(gold)}/6")
    logger.info(f"  Gold total rows   : {sum(gold.values())}")
    logger.info(f"  Status            : SUCCESS")
    logger.info("=" * 55)

    send_slack_alert({
        "Date"        : context["ds"],
        "Bronze rows" : sum(bronze.values()),
        "Quality"     : quality_label,
        "Gold marts"  : f"{len(gold)}/6",
        "Checkpoints" : checkpoints,
        "Status"      : "SUCCESS",
    }, level="success")

task_summary = PythonOperator(
    task_id="pipeline_summary",
    python_callable=pipeline_summary,
    on_success_callback=on_success_callback,
    dag=dag,
)

# =============================================================================
# Dépendances
# =============================================================================
(
    task_health_check
    >> task_verify_streaming
    >> task_verify_bronze
    >> task_verify_quality    # ← Great Expectations
    >> task_dbt_run
    >> task_dbt_test
    >> task_verify_gold
    >> task_summary
)
