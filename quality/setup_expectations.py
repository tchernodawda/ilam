"""
ILAM — Great Expectations Setup
Définit les suites de qualité pour les tables Bronze Sonatel.

Tables couvertes :
  - bronze.cdr_voice       : CDR voix (8 expectations)
  - bronze.subscribers     : Abonnés CRM (6 expectations)
  - bronze.om_transactions : Transactions Orange Money (7 expectations)
"""

import great_expectations as gx
from great_expectations.core.batch import RuntimeBatchRequest
from great_expectations.data_context.types.base import (
    DataContextConfig,
    InMemoryStoreBackendDefaults,
)
import trino
import pandas as pd
import logging

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# =============================================================================
# Configuration
# =============================================================================
GX_ROOT    = "/home/tcherno/ilam/quality"
TRINO_HOST = "localhost"
TRINO_PORT = 8080
TRINO_USER = "ilam"

def get_trino_df(query: str) -> pd.DataFrame:
    """Exécute une requête Trino et retourne un DataFrame pandas."""
    conn = trino.dbapi.connect(
        host=TRINO_HOST,
        port=TRINO_PORT,
        user=TRINO_USER,
        catalog="iceberg",
        request_timeout=120,
    )
    cursor = conn.cursor()
    cursor.execute(query)
    rows    = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]
    return pd.DataFrame(rows, columns=columns)


def setup_cdr_voice_suite(context):
    """
    Suite bronze.cdr_voice — 8 expectations.
    Valide la qualité des CDR voix Sonatel.
    """
    logger.info("Setting up suite: bronze.cdr_voice")

    suite = context.add_or_update_expectation_suite(
        expectation_suite_name="bronze.cdr_voice"
    )

    # Charger un échantillon de données
    df = get_trino_df("""
        SELECT cdr_id, calling_msisdn, called_msisdn,
               call_direction, call_type, duration_seconds,
               call_status, charge_amount, currency,
               origin_region, roaming_flag
        FROM iceberg.bronze.cdr_voice
        LIMIT 5000
    """)

    logger.info(f"  Loaded {len(df)} CDR voice records")

    # Créer un validator
    validator = context.get_validator(
        batch_request=RuntimeBatchRequest(
            datasource_name="pandas_datasource",
            data_connector_name="runtime_data_connector",
            data_asset_name="bronze.cdr_voice",
            runtime_parameters={"batch_data": df},
            batch_identifiers={"run_id": "setup"},
        ),
        expectation_suite=suite,
    )

    # ── Expectations ──────────────────────────────────────────────

    # 1. cdr_id ne doit jamais être null
    validator.expect_column_values_to_not_be_null("cdr_id")

    # 2. calling_msisdn ne doit jamais être null
    validator.expect_column_values_to_not_be_null("calling_msisdn")

    # 3. called_msisdn ne doit jamais être null
    validator.expect_column_values_to_not_be_null("called_msisdn")

    # 4. duration_seconds doit être >= 0 (pas de durée négative)
    validator.expect_column_values_to_be_between(
        "duration_seconds", min_value=0, max_value=7200
    )

    # 5. call_status doit être dans l'ensemble des valeurs valides
    validator.expect_column_values_to_be_in_set(
        "call_status",
        ["SUCCESS", "FAILED", "BUSY", "NO_ANSWER", "CONGESTION"]
    )

    # 6. charge_amount doit être positif et raisonnable (en XOF)
    validator.expect_column_values_to_be_between(
        "charge_amount", min_value=0, max_value=100000
    )

    # 7. currency doit être XOF (franc CFA)
    validator.expect_column_values_to_be_in_set(
        "currency", ["XOF"]
    )

    # 8. Le nombre total de CDR doit être > 1000
    #    (détecte une ingestion incomplète)
    validator.expect_table_row_count_to_be_between(
        min_value=1000, max_value=100000000
    )

    validator.save_expectation_suite(discard_failed_expectations=False)
    logger.info("  Suite bronze.cdr_voice saved — 8 expectations")
    return suite


def setup_subscribers_suite(context):
    """
    Suite bronze.subscribers — 6 expectations.
    Valide la qualité des données abonnés Sonatel.
    """
    logger.info("Setting up suite: bronze.subscribers")

    suite = context.add_or_update_expectation_suite(
        expectation_suite_name="bronze.subscribers"
    )

    df = get_trino_df("""
        SELECT subscriber_id, msisdn, account_type,
               status, region, first_name, last_name
        FROM iceberg.bronze.subscribers
        LIMIT 5000
    """)

    logger.info(f"  Loaded {len(df)} subscriber records")

    validator = context.get_validator(
        batch_request=RuntimeBatchRequest(
            datasource_name="pandas_datasource",
            data_connector_name="runtime_data_connector",
            data_asset_name="bronze.subscribers",
            runtime_parameters={"batch_data": df},
            batch_identifiers={"run_id": "setup"},
        ),
        expectation_suite=suite,
    )

    # 1. subscriber_id ne doit jamais être null
    validator.expect_column_values_to_not_be_null("subscriber_id")

    # 2. msisdn ne doit jamais être null
    validator.expect_column_values_to_not_be_null("msisdn")

    # 3. account_type doit être PREPAID ou POSTPAID
    validator.expect_column_values_to_be_in_set(
        "account_type", ["PREPAID", "POSTPAID"]
    )

    # 4. status doit être dans les valeurs valides
    validator.expect_column_values_to_be_in_set(
        "status", ["ACTIVE", "INACTIVE", "SUSPENDED"]
    )

    # 5. msisdn doit commencer par +221 (format Sonatel)
    validator.expect_column_values_to_match_regex(
        "msisdn", r"^\+221[0-9]{9}$"
    )

    # 6. Nombre d'abonnés doit être > 100
    validator.expect_table_row_count_to_be_between(
        min_value=100, max_value=100000000
    )

    validator.save_expectation_suite(discard_failed_expectations=False)
    logger.info("  Suite bronze.subscribers saved — 6 expectations")
    return suite


def setup_om_transactions_suite(context):
    """
    Suite bronze.om_transactions — 7 expectations.
    Valide la qualité des transactions Orange Money.
    """
    logger.info("Setting up suite: bronze.om_transactions")

    suite = context.add_or_update_expectation_suite(
        expectation_suite_name="bronze.om_transactions"
    )

    df = get_trino_df("""
        SELECT transaction_id, msisdn, transaction_type,
               amount, currency, status,
               balance_before, balance_after
        FROM iceberg.bronze.om_transactions
        LIMIT 5000
    """)

    logger.info(f"  Loaded {len(df)} OM transaction records")

    validator = context.get_validator(
        batch_request=RuntimeBatchRequest(
            datasource_name="pandas_datasource",
            data_connector_name="runtime_data_connector",
            data_asset_name="bronze.om_transactions",
            runtime_parameters={"batch_data": df},
            batch_identifiers={"run_id": "setup"},
        ),
        expectation_suite=suite,
    )

    # 1. transaction_id ne doit jamais être null
    validator.expect_column_values_to_not_be_null("transaction_id")

    # 2. msisdn ne doit jamais être null
    validator.expect_column_values_to_not_be_null("msisdn")

    # 3. amount doit être positif
    validator.expect_column_values_to_be_between(
        "amount", min_value=1, max_value=10000000
    )

    # 4. currency doit être XOF
    validator.expect_column_values_to_be_in_set(
        "currency", ["XOF"]
    )

    # 5. status doit être valide
    validator.expect_column_values_to_be_in_set(
        "status", ["SUCCESS", "FAILED", "PENDING"]
    )

    # 6. transaction_type doit être valide
    validator.expect_column_values_to_be_in_set(
        "transaction_type",
        ["TRANSFER", "PAYMENT", "WITHDRAWAL",
         "DEPOSIT", "AIRTIME", "BILL_PAYMENT"]
    )

    # 7. balance_after doit être >= 0 (pas de solde négatif)
    validator.expect_column_values_to_be_between(
        "balance_after", min_value=0, max_value=100000000
    )

    validator.save_expectation_suite(discard_failed_expectations=False)
    logger.info("  Suite bronze.om_transactions saved — 7 expectations")
    return suite


def configure_datasource(context):
    """Configure la datasource pandas pour GX."""
    datasource_config = {
        "name": "pandas_datasource",
        "class_name": "Datasource",
        "execution_engine": {
            "class_name": "PandasExecutionEngine"
        },
        "data_connectors": {
            "runtime_data_connector": {
                "class_name": "RuntimeDataConnector",
                "batch_identifiers": ["run_id"],
            }
        },
    }
    context.add_or_update_datasource(**datasource_config)
    logger.info("Pandas datasource configured")


def main():
    logger.info("=" * 55)
    logger.info("ILAM — Great Expectations Setup")
    logger.info("=" * 55)

    # Initialiser le contexte GX
    context = gx.get_context(
        mode="file",
        project_root_dir=GX_ROOT
    )

    # Configurer la datasource
    configure_datasource(context)

    # Créer les suites
    setup_cdr_voice_suite(context)
    setup_subscribers_suite(context)
    setup_om_transactions_suite(context)

    logger.info("=" * 55)
    logger.info("All expectation suites created successfully.")
    logger.info(f"  bronze.cdr_voice       : 8 expectations")
    logger.info(f"  bronze.subscribers     : 6 expectations")
    logger.info(f"  bronze.om_transactions : 7 expectations")
    logger.info("=" * 55)


if __name__ == "__main__":
    main()
