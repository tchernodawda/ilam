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




def setup_cdr_sms_suite(context):
    """Suite bronze.cdr_sms — 8 expectations."""
    logger.info("Setting up suite: bronze.cdr_sms")
    suite = context.add_or_update_expectation_suite(
        expectation_suite_name="bronze.cdr_sms")
    df = get_trino_df("""
        SELECT cdr_id, sender_msisdn, receiver_msisdn,
               sms_type, delivery_status, charge_amount,
               currency, origin_region
        FROM iceberg.bronze.cdr_sms LIMIT 5000
    """)
    logger.info(f"  Loaded {len(df)} CDR SMS records")
    validator = context.get_validator(
        batch_request=RuntimeBatchRequest(
            datasource_name="pandas_datasource",
            data_connector_name="runtime_data_connector",
            data_asset_name="bronze.cdr_sms",
            runtime_parameters={"batch_data": df},
            batch_identifiers={"run_id": "setup"},
        ),
        expectation_suite=suite,
    )
    validator.expect_column_values_to_not_be_null("cdr_id")
    validator.expect_column_values_to_not_be_null("sender_msisdn")
    validator.expect_column_values_to_not_be_null("receiver_msisdn")
    validator.expect_column_values_to_be_in_set(
        "delivery_status",
        ["DELIVERED", "FAILED", "PENDING", "EXPIRED"])
    validator.expect_column_values_to_be_in_set(
        "sms_type", ["MO", "MT", "P2P", "A2P"])
    validator.expect_column_values_to_be_between(
        "charge_amount", min_value=0, max_value=10000)
    validator.expect_column_values_to_be_in_set(
        "currency", ["XOF"])
    validator.expect_table_row_count_to_be_between(
        min_value=100, max_value=100000000)
    validator.save_expectation_suite(discard_failed_expectations=False)
    logger.info("  Suite bronze.cdr_sms saved — 8 expectations")
    return suite


def setup_cdr_data_suite(context):
    """Suite bronze.cdr_data — 8 expectations."""
    logger.info("Setting up suite: bronze.cdr_data")
    suite = context.add_or_update_expectation_suite(
        expectation_suite_name="bronze.cdr_data")
    df = get_trino_df("""
        SELECT session_id, msisdn, technology,
               bytes_uploaded, bytes_downloaded,
               duration_seconds, charge_amount, currency
        FROM iceberg.bronze.cdr_data LIMIT 5000
    """)
    logger.info(f"  Loaded {len(df)} CDR Data records")
    validator = context.get_validator(
        batch_request=RuntimeBatchRequest(
            datasource_name="pandas_datasource",
            data_connector_name="runtime_data_connector",
            data_asset_name="bronze.cdr_data",
            runtime_parameters={"batch_data": df},
            batch_identifiers={"run_id": "setup"},
        ),
        expectation_suite=suite,
    )
    validator.expect_column_values_to_not_be_null("session_id")
    validator.expect_column_values_to_not_be_null("msisdn")
    validator.expect_column_values_to_be_in_set(
        "technology", ["2G", "3G", "4G", "5G", "WIFI"])
    validator.expect_column_values_to_be_between(
        "bytes_uploaded", min_value=0, max_value=10000000000)
    validator.expect_column_values_to_be_between(
        "bytes_downloaded", min_value=0, max_value=10000000000)
    validator.expect_column_values_to_be_between(
        "duration_seconds", min_value=0, max_value=86400)
    validator.expect_column_values_to_be_in_set(
        "currency", ["XOF"])
    validator.expect_table_row_count_to_be_between(
        min_value=100, max_value=100000000)
    validator.save_expectation_suite(discard_failed_expectations=False)
    logger.info("  Suite bronze.cdr_data saved — 8 expectations")
    return suite


def setup_network_events_suite(context):
    """Suite bronze.network_events — 6 expectations."""
    logger.info("Setting up suite: bronze.network_events")
    suite = context.add_or_update_expectation_suite(
        expectation_suite_name="bronze.network_events")
    df = get_trino_df("""
        SELECT event_id, event_type, severity,
               technology, region, metric_value
        FROM iceberg.bronze.network_events LIMIT 5000
    """)
    logger.info(f"  Loaded {len(df)} network event records")
    validator = context.get_validator(
        batch_request=RuntimeBatchRequest(
            datasource_name="pandas_datasource",
            data_connector_name="runtime_data_connector",
            data_asset_name="bronze.network_events",
            runtime_parameters={"batch_data": df},
            batch_identifiers={"run_id": "setup"},
        ),
        expectation_suite=suite,
    )
    validator.expect_column_values_to_not_be_null("event_id")
    validator.expect_column_values_to_not_be_null("event_type")
    validator.expect_column_values_to_be_in_set(
        "severity", ["LOW", "MEDIUM", "HIGH", "CRITICAL"])
    validator.expect_column_values_to_be_in_set(
        "technology", ["2G", "3G", "4G", "5G"])
    validator.expect_column_values_to_be_between(
        "metric_value", min_value=0, max_value=1000000)
    validator.expect_table_row_count_to_be_between(
        min_value=100, max_value=100000000)
    validator.save_expectation_suite(discard_failed_expectations=False)
    logger.info("  Suite bronze.network_events saved — 6 expectations")
    return suite


def setup_recharges_suite(context):
    """Suite bronze.recharges — 7 expectations."""
    logger.info("Setting up suite: bronze.recharges")
    suite = context.add_or_update_expectation_suite(
        expectation_suite_name="bronze.recharges")
    df = get_trino_df("""
        SELECT recharge_id, msisdn, amount,
               currency, channel, status, region
        FROM iceberg.bronze.recharges LIMIT 5000
    """)
    logger.info(f"  Loaded {len(df)} recharge records")
    # Détecter les valeurs réelles
    channels = df["channel"].dropna().unique().tolist()
    statuses = df["status"].dropna().unique().tolist()
    logger.info(f"  channels: {channels}")
    logger.info(f"  statuses: {statuses}")
    validator = context.get_validator(
        batch_request=RuntimeBatchRequest(
            datasource_name="pandas_datasource",
            data_connector_name="runtime_data_connector",
            data_asset_name="bronze.recharges",
            runtime_parameters={"batch_data": df},
            batch_identifiers={"run_id": "setup"},
        ),
        expectation_suite=suite,
    )
    validator.expect_column_values_to_not_be_null("recharge_id")
    validator.expect_column_values_to_not_be_null("msisdn")
    validator.expect_column_values_to_be_between(
        "amount", min_value=1, max_value=1000000)
    validator.expect_column_values_to_be_in_set(
        "currency", ["XOF"])
    validator.expect_column_values_to_be_in_set(
        "channel", value_set=channels)
    validator.expect_column_values_to_be_in_set(
        "status", value_set=statuses)
    validator.expect_table_row_count_to_be_between(
        min_value=10, max_value=100000000)
    validator.save_expectation_suite(discard_failed_expectations=False)
    logger.info("  Suite bronze.recharges saved — 7 expectations")
    return suite


def setup_complaints_suite(context):
    """Suite bronze.complaints — 5 expectations."""
    logger.info("Setting up suite: bronze.complaints")
    suite = context.add_or_update_expectation_suite(
        expectation_suite_name="bronze.complaints")
    df = get_trino_df("""
        SELECT complaint_id, subscriber_id, msisdn,
               channel, category, priority, status, region
        FROM iceberg.bronze.complaints LIMIT 5000
    """)
    logger.info(f"  Loaded {len(df)} complaint records")
    priorities = df["priority"].dropna().unique().tolist()
    statuses   = df["status"].dropna().unique().tolist()
    channels   = df["channel"].dropna().unique().tolist()
    validator = context.get_validator(
        batch_request=RuntimeBatchRequest(
            datasource_name="pandas_datasource",
            data_connector_name="runtime_data_connector",
            data_asset_name="bronze.complaints",
            runtime_parameters={"batch_data": df},
            batch_identifiers={"run_id": "setup"},
        ),
        expectation_suite=suite,
    )
    validator.expect_column_values_to_not_be_null("complaint_id")
    validator.expect_column_values_to_not_be_null("msisdn")
    validator.expect_column_values_to_be_in_set(
        "priority", value_set=priorities)
    validator.expect_column_values_to_be_in_set(
        "status",   value_set=statuses)
    validator.expect_column_values_to_be_in_set(
        "channel",  value_set=channels)
    validator.save_expectation_suite(discard_failed_expectations=False)
    logger.info("  Suite bronze.complaints saved — 5 expectations")
    return suite


def setup_contracts_suite(context):
    """Suite bronze.contracts — 7 expectations."""
    logger.info("Setting up suite: bronze.contracts")
    suite = context.add_or_update_expectation_suite(
        expectation_suite_name="bronze.contracts")
    df = get_trino_df("""
        SELECT contract_id, subscriber_id, msisdn,
               offer_code, product_type, plan_type,
               monthly_fee, currency, status
        FROM iceberg.bronze.contracts LIMIT 5000
    """)
    logger.info(f"  Loaded {len(df)} contract records")
    product_types = df["product_type"].dropna().unique().tolist()
    plan_types    = df["plan_type"].dropna().unique().tolist()
    statuses      = df["status"].dropna().unique().tolist()
    validator = context.get_validator(
        batch_request=RuntimeBatchRequest(
            datasource_name="pandas_datasource",
            data_connector_name="runtime_data_connector",
            data_asset_name="bronze.contracts",
            runtime_parameters={"batch_data": df},
            batch_identifiers={"run_id": "setup"},
        ),
        expectation_suite=suite,
    )
    validator.expect_column_values_to_not_be_null("contract_id")
    validator.expect_column_values_to_not_be_null("msisdn")
    validator.expect_column_values_to_be_in_set(
        "product_type", value_set=product_types)
    validator.expect_column_values_to_be_in_set(
        "plan_type",    value_set=plan_types)
    validator.expect_column_values_to_be_in_set(
        "status",       value_set=statuses)
    validator.expect_column_values_to_be_between(
        "monthly_fee", min_value=0, max_value=1000000)
    validator.expect_column_values_to_be_in_set(
        "currency", ["XOF"])
    validator.save_expectation_suite(discard_failed_expectations=False)
    logger.info("  Suite bronze.contracts saved — 7 expectations")
    return suite

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

    setup_cdr_sms_suite(context)
    setup_cdr_data_suite(context)
    setup_network_events_suite(context)
    setup_recharges_suite(context)
    setup_complaints_suite(context)
    setup_contracts_suite(context)

    logger.info("=" * 55)
    logger.info("All expectation suites created successfully.")
    logger.info(f"  bronze.cdr_voice       : 8 expectations")
    logger.info(f"  bronze.subscribers     : 6 expectations")
    logger.info(f"  bronze.om_transactions : 7 expectations")
    logger.info(f"  bronze.cdr_sms         : 8 expectations")
    logger.info(f"  bronze.cdr_data        : 8 expectations")
    logger.info(f"  bronze.network_events  : 6 expectations")
    logger.info(f"  bronze.recharges       : 7 expectations")
    logger.info(f"  bronze.complaints      : 5 expectations")
    logger.info(f"  bronze.contracts       : 7 expectations")
    logger.info("=" * 55)


if __name__ == "__main__":
    main()
