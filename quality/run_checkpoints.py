"""
ILAM — Great Expectations Checkpoints
Exécute les validations de qualité sur les tables Bronze.
Appelé par le DAG Airflow avant dbt_run.

Retourne :
  - 0 : toutes les validations passent (>= 80%)
  - 1 : qualité dégradée (50-80%) → warning, pipeline continue
  - 2 : qualité critique (< 50%) → erreur, pipeline bloqué
"""

import great_expectations as gx
from great_expectations.core.batch import RuntimeBatchRequest
from great_expectations.core.expectation_validation_result import (
    ExpectationSuiteValidationResult
)
import trino
import pandas as pd
import logging
import json
import sys
from datetime import datetime

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

GX_ROOT    = "/opt/airflow/quality"
TRINO_HOST = "trino"
TRINO_PORT = 8080
TRINO_USER = "ilam"

# Seuils de qualité
THRESHOLD_WARNING  = 0.80  # < 80% → warning
THRESHOLD_CRITICAL = 0.50  # < 50% → erreur critique


def get_trino_df(query: str) -> pd.DataFrame:
    conn = trino.dbapi.connect(
        host=TRINO_HOST, port=TRINO_PORT,
        user=TRINO_USER, catalog="iceberg",
        request_timeout=120,
    )
    cursor = conn.cursor()
    cursor.execute(query)
    rows    = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]
    return pd.DataFrame(rows, columns=columns)


def run_validation(context, suite_name: str,
                   df: pd.DataFrame) -> dict:
    """
    Exécute une validation GX sur un DataFrame.
    Retourne un résumé avec le score de qualité.
    """
    validator = context.get_validator(
        batch_request=RuntimeBatchRequest(
            datasource_name="pandas_datasource",
            data_connector_name="runtime_data_connector",
            data_asset_name=suite_name,
            runtime_parameters={"batch_data": df},
            batch_identifiers={
                "run_id": datetime.now().strftime("%Y%m%d_%H%M%S")
            },
        ),
        expectation_suite_name=suite_name,
    )

    # Exécuter la validation
    results = validator.validate(
        result_format={
            "result_format"        : "SUMMARY",
            "include_unexpected_count": True,
        }
    )

    # Calculer le score de qualité
    total    = results.statistics["evaluated_expectations"]
    success  = results.statistics["successful_expectations"]
    score    = success / total if total > 0 else 0

    # Détail des expectations échouées
    failed = []
    for r in results.results:
        if not r.success:
            failed.append({
                "expectation": r.expectation_config.expectation_type,
                "column"     : r.expectation_config.kwargs.get(
                                   "column", "table"),
                "details"    : str(r.result)[:200],
            })

    summary = {
        "suite"  : suite_name,
        "total"  : total,
        "success": success,
        "failed" : len(failed),
        "score"  : round(score * 100, 1),
        "status" : "PASS" if score >= THRESHOLD_WARNING
                   else "WARN" if score >= THRESHOLD_CRITICAL
                   else "FAIL",
        "failed_expectations": failed,
    }

    return summary


def run_all_checkpoints():
    """
    Exécute les 3 checkpoints et retourne le rapport global.
    """
    logger.info("=" * 55)
    logger.info("ILAM — Great Expectations Validation")
    logger.info(f"  Run date : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 55)

    context = gx.get_context(
        mode="file",
        project_root_dir=GX_ROOT
    )

    results = []

    # ── 1. bronze.cdr_voice ──────────────────────────────────────
    logger.info("Validating bronze.cdr_voice...")
    df_cdr = get_trino_df("""
        SELECT cdr_id, calling_msisdn, called_msisdn,
               call_direction, call_type, duration_seconds,
               call_status, charge_amount, currency,
               origin_region, roaming_flag
        FROM iceberg.bronze.cdr_voice
        LIMIT 5000
    """)
    result_cdr = run_validation(context, "bronze.cdr_voice", df_cdr)
    results.append(result_cdr)
    logger.info(f"  Score : {result_cdr['score']}% "
                f"({result_cdr['success']}/{result_cdr['total']}) "
                f"→ {result_cdr['status']}")

    # ── 2. bronze.subscribers ────────────────────────────────────
    logger.info("Validating bronze.subscribers...")
    df_subs = get_trino_df("""
        SELECT subscriber_id, msisdn, account_type,
               status, region, first_name, last_name
        FROM iceberg.bronze.subscribers
        LIMIT 5000
    """)
    result_subs = run_validation(
        context, "bronze.subscribers", df_subs)
    results.append(result_subs)
    logger.info(f"  Score : {result_subs['score']}% "
                f"({result_subs['success']}/{result_subs['total']}) "
                f"→ {result_subs['status']}")

    # ── 3. bronze.om_transactions ────────────────────────────────
    logger.info("Validating bronze.om_transactions...")
    df_om = get_trino_df("""
        SELECT transaction_id, msisdn, transaction_type,
               amount, currency, status,
               balance_before, balance_after
        FROM iceberg.bronze.om_transactions
        LIMIT 5000
    """)
    result_om = run_validation(
        context, "bronze.om_transactions", df_om)
    results.append(result_om)
    logger.info(f"  Score : {result_om['score']}% "
                f"({result_om['success']}/{result_om['total']}) "
                f"→ {result_om['status']}")

    # ── Rapport global ───────────────────────────────────────────
    total_expectations = sum(r["total"]   for r in results)
    total_success      = sum(r["success"] for r in results)
    global_score       = (total_success / total_expectations * 100
                          if total_expectations > 0 else 0)
    global_status      = (
        "PASS" if global_score >= THRESHOLD_WARNING * 100
        else "WARN" if global_score >= THRESHOLD_CRITICAL * 100
        else "FAIL"
    )

    report = {
        "run_date"    : datetime.now().isoformat(),
        "global_score": round(global_score, 1),
        "global_status": global_status,
        "tables"      : results,
        "summary"     : {
            "total_expectations" : total_expectations,
            "total_success"      : total_success,
            "total_failed"       : total_expectations - total_success,
        }
    }

    # ── Affichage résumé ─────────────────────────────────────────
    logger.info("=" * 55)
    logger.info("QUALITY REPORT")
    logger.info(f"  Global score  : {global_score:.1f}%")
    logger.info(f"  Global status : {global_status}")
    logger.info(f"  Expectations  : {total_success}/{total_expectations}")
    logger.info("-" * 55)
    for r in results:
        icon = "✅" if r["status"] == "PASS" \
               else "⚠️" if r["status"] == "WARN" else "❌"
        logger.info(f"  {icon} {r['suite']:<30} "
                    f"{r['score']:>5.1f}%  {r['status']}")
        for f in r["failed_expectations"]:
            logger.warning(f"     ↳ FAILED: {f['expectation']} "
                           f"on {f['column']}")
    logger.info("=" * 55)

    # Sauvegarde JSON désactivée (permissions Docker)
    report_path = f"{GX_ROOT}/reports/quality_report_" \
                  f"{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    import os
    os.makedirs(f"{GX_ROOT}/reports", exist_ok=True)
    with open(report_path, "w") as f:
        json.dump(report, f, indent=2)
    logger.info(f"Report saved: {report_path}")

    # Code de retour selon le statut global
    if global_status == "FAIL":
        logger.error("CRITICAL: Quality below 50% — pipeline blocked.")
        return 2
    elif global_status == "WARN":
        logger.warning("WARNING: Quality between 50-80% — "
                       "pipeline continues with caution.")
        return 1
    else:
        logger.info("SUCCESS: All quality checks passed.")
        return 0


if __name__ == "__main__":
    exit_code = run_all_checkpoints()
    sys.exit(exit_code)
