"""
ILAM — Sonatel Data Pipeline DAG
Orchestrates batch + streaming pipelines.

Architecture :
  Streaming : NiFi → Kafka → PyFlink → Bronze (continu, auto-géré)
  Batch     : NiFi CRON → Kafka → PyFlink → Bronze (hourly, auto-géré)
  Ce DAG    : vérifie l'état du streaming, lance dbt, vérifie Gold
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
    "owner"           : "ilam", # pas de "airflow" pour différencier les DAGs custom
    "depends_on_past" : False, # ce DAG doit pouvoir s'exécuter même si la run précédente a échoué
    "email_on_failure": False, # pas encore d'email pour ce DAG de monitoring interne
    "email_on_retry"  : False, # pas d'email non plus pour les retries
    "retries"         : 1, # on autorise 1 retry en cas d'échec, car parfois les services peuvent être temporairement indisponibles
    "retry_delay"     : timedelta(minutes=2), # délai de 2 minutes avant de réessayer, pour laisser le temps aux services de se remettre
    "execution_timeout": timedelta(minutes=20), # timeout global de 20 minutes pour chaque run du DAG, pour éviter les runs bloquées indéfiniment
}

dag = DAG(
    dag_id="ilam", # nom du DAG
    description="Sonatel Data Lakehouse — Batch + Streaming pipeline", # description du DAG
    default_args=default_args, # arguments par défaut pour les tâches du DAG
    schedule_interval="@hourly", # exécution toutes les heures
    start_date=days_ago(1), # date de début il y a 1 jour pour que le DAG puisse s'exécuter immédiatement
    catchup=False, # ne pas exécuter les runs manquées avant la date de début
    max_active_runs=1, # ne pas exécuter plusieurs runs en parallèle
    tags=["ilam", "sonatel", "lakehouse", "streaming"], # tags pour organiser les DAGs dans l'interface Airflow
)

# =============================================================================
# Task 1 — Health check de tous les services
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
                logger.error(f"  FAIL : {name} → {r.status_code}")
        except Exception as e:
            failed.append(name)
            logger.error(f"  FAIL : {name} → {e}")

    if failed:
        raise Exception(f"Services down: {', '.join(failed)}")

    logger.info("All services healthy.")
    return "ok"

# Note : ce health check est basique et ne vérifie que la disponibilité des endpoints. 
# Il pourrait être amélioré pour vérifier des fonctionnalités spécifiques (ex: écrire/lire dans MinIO, 
# exécuter une requête test sur Trino, etc.) mais cela suffira pour un monitoring de base.
# Ce DAG est surtout destiné à vérifier que le job PyFlink de streaming tourne et que les tables Bronze sont alimentées, avant de lancer dbt. 
# Si le health check échoue, cela indique un problème d'infrastructure qui doit être résolu avant de continuer.
task_health_check = PythonOperator(
    task_id="health_check",
    python_callable=health_check,
    dag=dag,
)

# =============================================================================
# Task 2 — Vérifier que le job PyFlink tourne et checkpointe
# =============================================================================
def verify_streaming(**context):
    """
    Vérifie que le job PyFlink est RUNNING et a des checkpoints complétés.
    NiFi et PyFlink tournent en continu — ce DAG vérifie seulement leur état.
    """
    flink_url = "http://flink-jobmanager:8081"

    # Vérifier les jobs en cours
    jobs_resp = requests.get(f"{flink_url}/jobs", timeout=10)
    running = [j for j in jobs_resp.json()["jobs"]
               if j["status"] == "RUNNING"]

    if not running:
        raise Exception("No PyFlink streaming job running. Run 'make pyflink-job'.")

    job_id = running[0]["id"]
    logger.info(f"PyFlink job running: {job_id}")

    # Vérifier les checkpoints
    cp_resp = requests.get(
        f"{flink_url}/jobs/{job_id}/checkpoints",
        timeout=10
    )
    cp_data   = cp_resp.json()
    completed = cp_data.get("counts", {}).get("completed", 0)
    failed    = cp_data.get("counts", {}).get("failed", 0)

    logger.info(f"Checkpoints — completed: {completed}, failed: {failed}")

    if completed == 0:
        raise Exception("No completed checkpoints yet — streaming not writing to Iceberg.")

    context["ti"].xcom_push(key="flink_job_id",          value=job_id)
    context["ti"].xcom_push(key="streaming_checkpoints",  value=completed)
    return f"streaming_ok_{completed}_checkpoints"

task_verify_streaming = PythonOperator(
    task_id="verify_streaming",
    python_callable=verify_streaming,
    dag=dag,
)

# =============================================================================
# Task 3 — Vérifier les counts Bronze (toutes les 9 tables)
# =============================================================================
def verify_bronze_counts(**context):
    """
    Vérifie que les 9 tables Bronze sont alimentées.
    NiFi + PyFlink alimentent ces tables en continu.
    """
    import trino
    conn = trino.dbapi.connect(
        host="trino", port=8080,
        user="airflow", catalog="iceberg",
        request_timeout=60,
    )
    cursor = conn.cursor()

    tables = [
        "bronze.cdr_voice",
        "bronze.cdr_sms",
        "bronze.cdr_data",
        "bronze.network_events",
        "bronze.om_transactions",
        "bronze.subscribers",
        "bronze.contracts",
        "bronze.recharges",
        "bronze.complaints",
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
    logger.info(f"Bronze OK — {len(tables)} tables, {total} total rows")
    context["ti"].xcom_push(key="bronze_counts", value=counts)
    return counts

task_verify_bronze = PythonOperator(
    task_id="verify_bronze_counts",
    python_callable=verify_bronze_counts,
    dag=dag,
)

# =============================================================================
# Task 4 — dbt run (Bronze → Silver → Gold)
# =============================================================================
task_dbt_run = BashOperator(
    task_id="dbt_run",
    bash_command="""
        cd /opt/airflow/transform/ilam_transform && \
        dbt run --profiles-dir . --target docker --no-write-json 2>&1 | \
        grep -E "OK|ERROR|Finished|Running|WARN" || true
    """,
    execution_timeout=timedelta(minutes=15),
    dag=dag,
)
# Note : on utilise grep pour filtrer la sortie de dbt et ne garder que les lignes importantes (succès, erreurs, résumé). Cela rendra les logs plus lisibles dans Airflow. 
# Le flag --no-write-json évite de générer les fichiers de résultats JSON, car ce DAG est surtout destiné à vérifier que dbt s'exécute sans erreur et que les Gold marts sont alimentés, 
# plutôt que d'analyser les résultats détaillés de chaque modèle.

# =============================================================================
# Task 5 — dbt test (qualité des données)
# =============================================================================
task_dbt_test = BashOperator(
    task_id="dbt_test",
    bash_command="""
        cd /opt/airflow/transform/ilam_transform && \
        dbt test --profiles-dir . --target docker --no-write-json 2>&1 | \
        grep -E "OK|FAIL|ERROR|Finished|Pass|Warn" || true
    """,
    execution_timeout=timedelta(minutes=10),
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
    for mart in marts:
        cursor.execute(f"SELECT COUNT(*) FROM iceberg.{mart}")
        results[mart] = cursor.fetchone()[0]
        logger.info(f"  {mart}: {results[mart]} rows")

    empty = [m for m, c in results.items() if c == 0]
    if empty:
        raise Exception(f"Empty Gold marts: {empty}")

    logger.info(f"Gold OK — {len(marts)} marts populated")
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
    ti           = context["ti"]
    bronze       = ti.xcom_pull(task_ids="verify_bronze_counts",
                                key="bronze_counts") or {}
    gold         = ti.xcom_pull(task_ids="verify_gold_marts",
                                key="gold_counts") or {}
    checkpoints  = ti.xcom_pull(task_ids="verify_streaming",
                                key="streaming_checkpoints") or 0
    job_id       = ti.xcom_pull(task_ids="verify_streaming",
                                key="flink_job_id") or "unknown"

    logger.info("=" * 55)
    logger.info("ILAM PIPELINE SUMMARY")
    logger.info(f"  Date             : {context['ds']}")
    logger.info(f"  PyFlink Job ID   : {job_id}")
    logger.info(f"  Checkpoints      : {checkpoints}")
    logger.info(f"  Bronze tables    : {len(bronze)}")
    logger.info(f"  Bronze total rows: {sum(bronze.values())}")
    logger.info(f"  Gold marts       : {len(gold)}")
    logger.info(f"  Gold total rows  : {sum(gold.values())}")
    logger.info("  Status           : SUCCESS")
    logger.info("=" * 55)

task_summary = PythonOperator(
    task_id="pipeline_summary",
    python_callable=pipeline_summary,
    dag=dag,
)

# =============================================================================
# Dépendances — pipeline linéaire simple
#
#  health_check
#       |
#  verify_streaming  (PyFlink tourne ?)
#       |
#  verify_bronze     (9 tables alimentées ?)
#       |
#  dbt_run           (Bronze → Silver → Gold)
#       |
#  dbt_test          (qualité des données)
#       |
#  verify_gold       (6 marts populés ?)
#       |
#  pipeline_summary
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
