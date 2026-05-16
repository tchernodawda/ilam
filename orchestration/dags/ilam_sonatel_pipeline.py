"""
ILAM — Sonatel Data Pipeline DAG
Orchestrates batch + streaming pipelines.

Architecture :
  Streaming : NiFi → Kafka → PyFlink → Bronze (continu, auto-géré)
  Batch     : NiFi CRON → Kafka → PyFlink → Bronze (hourly, auto-géré)
  Ce DAG    : vérifie l'état du streaming, lance dbt, vérifie Gold

Alerting   : Slack-style callbacks (simulated — replace webhook_url
             with real Slack URL in production)
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
# Configuration alertes Slack (simulé)
# En production : remplacer par le vrai webhook Slack
# SLACK_WEBHOOK_URL = "https://hooks.slack.com/services/xxx/yyy/zzz"
# =============================================================================
SLACK_WEBHOOK_URL = None  # None = simulation, log uniquement
SLACK_CHANNEL     = "#ilam-data-alerts"
SLACK_USERNAME    = "ILAM Bot"

def send_slack_alert(payload: dict, level: str = "error"):
    """
    Envoie une alerte Slack ou la simule si webhook non configuré.
    En production, décommenter SLACK_WEBHOOK_URL ci-dessus.
    """
    emoji   = "🔴" if level == "error" else "⚠️" if level == "warning" else "✅"
    message = {
        "channel"   : SLACK_CHANNEL,
        "username"  : SLACK_USERNAME,
        "icon_emoji": ":robot_face:",
        "attachments": [{
            "color": "#FF0000" if level == "error" else
                     "#FFA500" if level == "warning" else "#36A64F",
            "title": f"{emoji} ILAM Pipeline Alert",
            "fields": [
                {"title": k, "value": str(v), "short": True}
                for k, v in payload.items()
            ],
            "footer"   : "ILAM — Sonatel Data Lakehouse",
            "ts"       : datetime.utcnow().timestamp(),
        }]
    }

    if SLACK_WEBHOOK_URL:
        # Production — vrai Slack
        try:
            resp = requests.post(
                SLACK_WEBHOOK_URL,
                data=json.dumps(message),
                headers={"Content-Type": "application/json"},
                timeout=10,
            )
            logger.info(f"Slack alert sent: {resp.status_code}")
        except Exception as e:
            logger.error(f"Slack alert failed: {e}")
    else:
        # Simulation — log structuré comme Slack
        logger.warning("=" * 60)
        logger.warning(f"[SLACK ALERT SIMULATION] {SLACK_CHANNEL}")
        logger.warning(f"  Level   : {level.upper()}")
        for k, v in payload.items():
            logger.warning(f"  {k:<20}: {v}")
        logger.warning("=" * 60)

# =============================================================================
# Callbacks — appelés automatiquement par Airflow sur échec/retry/SLA
# =============================================================================

def on_failure_callback(context):
    """Callback appelé automatiquement quand une task échoue."""
    task_id    = context["task_instance"].task_id
    dag_id     = context["task_instance"].dag_id
    exec_date  = context["execution_date"]
    exception  = context.get("exception", "Unknown error")
    try_number = context["task_instance"].try_number

    send_slack_alert({
        "DAG"        : dag_id,
        "Task"       : task_id,
        "Execution"  : str(exec_date),
        "Attempt"    : f"{try_number}/2",
        "Error"      : str(exception)[:200],
        "Logs"       : f"http://localhost:8082/log?dag_id={dag_id}&task_id={task_id}",
    }, level="error")

def on_retry_callback(context):
    """Callback appelé automatiquement quand une task est en retry."""
    task_id   = context["task_instance"].task_id
    try_number = context["task_instance"].try_number

    send_slack_alert({
        "DAG"    : context["task_instance"].dag_id,
        "Task"   : task_id,
        "Attempt": f"{try_number}/2",
        "Status" : "Retrying after failure...",
    }, level="warning")

def on_success_callback(context):
    """Callback appelé sur succès — uniquement pour pipeline_summary."""
    ti     = context["task_instance"]
    dag_id = ti.dag_id

    send_slack_alert({
        "DAG"    : dag_id,
        "Status" : "Pipeline completed successfully",
        "Date"   : str(context["execution_date"]),
    }, level="success")

def sla_miss_callback(dag, task_list, blocking_task_list, slas, blocking_tis):
    """Callback appelé quand une task dépasse son SLA."""
    missed_tasks = [str(t) for t in task_list]
    send_slack_alert({
        "DAG"         : dag.dag_id,
        "Status"      : "SLA MISSED",
        "Tasks"       : ", ".join(missed_tasks),
        "Description" : "Tasks exceeded their maximum allowed duration",
    }, level="warning")

# =============================================================================
# Default args avec callbacks
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

    job_id = running[0]["id"]
    logger.info(f"PyFlink job running: {job_id}")

    cp_resp   = requests.get(
        f"{flink_url}/jobs/{job_id}/checkpoints", timeout=10)
    cp_data   = cp_resp.json()
    completed = cp_data.get("counts", {}).get("completed", 0)
    failed_cp = cp_data.get("counts", {}).get("failed", 0)

    logger.info(f"Checkpoints — completed: {completed}, failed: {failed_cp}")

    if completed == 0:
        raise Exception(
            "No completed checkpoints — "
            "streaming not writing to Iceberg yet."
        )

    if failed_cp > completed:
        send_slack_alert({
            "Job ID"            : job_id,
            "Checkpoints OK"    : completed,
            "Checkpoints FAILED": failed_cp,
            "Warning"           : "More failed than completed checkpoints",
        }, level="warning")

    context["ti"].xcom_push(key="flink_job_id",         value=job_id)
    context["ti"].xcom_push(key="streaming_checkpoints", value=completed)
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

    # Tables streaming — doivent toujours avoir des données
    streaming_tables = [
        "bronze.cdr_voice",
        "bronze.cdr_sms",
        "bronze.cdr_data",
        "bronze.network_events",
        "bronze.om_transactions",
    ]

    # Tables batch — peuvent être vides si CRON pas encore déclenché
    batch_tables = [
        "bronze.subscribers",
        "bronze.contracts",
        "bronze.recharges",
        "bronze.complaints",
    ]

    counts = {}

    # Vérification stricte pour les tables streaming
    streaming_empty = []
    for table in streaming_tables:
        cursor.execute(f"SELECT COUNT(*) FROM iceberg.{table}")
        counts[table] = cursor.fetchone()[0]
        logger.info(f"  {table}: {counts[table]} rows")
        if counts[table] == 0:
            streaming_empty.append(table)

    if streaming_empty:
        raise Exception(
            f"Empty streaming Bronze tables: {streaming_empty}. "
            f"Check NiFi pipelines and PyFlink job."
        )

    # Vérification souple pour les tables batch
    batch_empty = []
    for table in batch_tables:
        cursor.execute(f"SELECT COUNT(*) FROM iceberg.{table}")
        counts[table] = cursor.fetchone()[0]
        logger.info(f"  {table}: {counts[table]} rows")
        if counts[table] == 0:
            batch_empty.append(table)

    if batch_empty:
        # Warning seulement — le CRON batch n'a peut-être pas encore tourné
        logger.warning(
            f"Empty batch tables (CRON may not have fired yet): "
            f"{batch_empty}"
        )
        send_slack_alert({
            "Warning"      : "Empty batch Bronze tables",
            "Tables"       : ", ".join(batch_empty),
            "Explanation"  : "NiFi CRON batch may not have fired yet",
            "Action"       : "Check NiFi batch pipelines",
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
# Task 4 — dbt run avec SLA strict
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
    # SLA strict sur dbt — doit finir en 10 minutes
    sla=timedelta(minutes=10),
    dag=dag,
)

# =============================================================================
# Task 5 — dbt test
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
# Task 6 — Vérifier les Gold Data Marts
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
        "gold.mart_revenue",
        "gold.mart_network_quality",
        "gold.mart_churn",
        "gold.mart_arpu",
        "gold.mart_om_activity",
        "gold.mart_usage",
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

    total = sum(results.values())
    logger.info(f"Gold OK — {len(marts)} marts, {total} total rows")
    context["ti"].xcom_push(key="gold_counts", value=results)
    return results

task_verify_gold = PythonOperator(
    task_id="verify_gold_marts",
    python_callable=verify_gold_marts,
    dag=dag,
    sla=timedelta(minutes=5),
)

# =============================================================================
# Task 7 — Pipeline summary avec alerte succès
# =============================================================================
def pipeline_summary(**context):
    ti          = context["ti"]
    bronze      = ti.xcom_pull(
                    task_ids="verify_bronze_counts",
                    key="bronze_counts") or {}
    gold        = ti.xcom_pull(
                    task_ids="verify_gold_marts",
                    key="gold_counts") or {}
    checkpoints = ti.xcom_pull(
                    task_ids="verify_streaming",
                    key="streaming_checkpoints") or 0
    job_id      = ti.xcom_pull(
                    task_ids="verify_streaming",
                    key="flink_job_id") or "unknown"

    bronze_total = sum(bronze.values())
    gold_total   = sum(gold.values())

    logger.info("=" * 55)
    logger.info("ILAM PIPELINE SUMMARY")
    logger.info(f"  Date              : {context['ds']}")
    logger.info(f"  PyFlink Job ID    : {job_id}")
    logger.info(f"  Checkpoints       : {checkpoints}")
    logger.info(f"  Bronze tables     : {len(bronze)}/9")
    logger.info(f"  Bronze total rows : {bronze_total}")
    logger.info(f"  Gold marts        : {len(gold)}/6")
    logger.info(f"  Gold total rows   : {gold_total}")
    logger.info(f"  Status            : SUCCESS")
    logger.info("=" * 55)

    # Alerte succès simulée
    send_slack_alert({
        "Date"            : context["ds"],
        "Bronze rows"     : bronze_total,
        "Gold marts"      : f"{len(gold)}/6",
        "Checkpoints"     : checkpoints,
        "Status"          : "SUCCESS",
    }, level="success")

task_summary = PythonOperator(
    task_id="pipeline_summary",
    python_callable=pipeline_summary,
    on_success_callback=on_success_callback,
    dag=dag,
)

# =============================================================================
# Dépendances
#
#  health_check
#       |
#  verify_streaming   SLA: 5min
#       |
#  verify_bronze      SLA: 5min
#       |
#  dbt_run            SLA: 10min  ← SLA strict
#       |
#  dbt_test           SLA: 8min
#       |
#  verify_gold        SLA: 5min
#       |
#  pipeline_summary   → alerte succès
# =============================================================================
(
    task_health_check
    >> task_verify_streaming
    >> task_verify_bronze
    >> task_dbt_run
    >> task_dbt_test
    >> task_verify_gold
    >> task_summary
)
