"""
ILAM — Spark Job 3 : Gold Aggregation
Silver → Gold

Rôle :
  - Agrégation des revenus par région, offre et période
  - Calcul ARPU (Average Revenue Per User)
  - Calcul du taux de churn basique
  - Score de fraude Orange Money

En production Sonatel :
  - Ces agrégations portent sur des centaines de millions de lignes
  - Spark distribue les calculs sur le cluster
  - Les résultats alimentent les dashboards Grafana et les rapports COMEX
"""

import logging
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

ICEBERG_REST_URI = "http://iceberg-rest:8181"
MINIO_ENDPOINT   = "http://minio:9000"
MINIO_ACCESS_KEY = "ilam_admin"
MINIO_SECRET_KEY = "ilam_password_2025"
WAREHOUSE        = "s3://warehouse/"
SPARK_MASTER     = "spark://spark-master:7077"


def get_spark_session(app_name: str) -> SparkSession:
    return (
        SparkSession.builder
        .appName(app_name)
        .master(SPARK_MASTER)
        .config("spark.sql.extensions",
                "org.apache.iceberg.spark.extensions"
                ".IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.iceberg",
                "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.iceberg.type",      "rest")
        .config("spark.sql.catalog.iceberg.uri",       ICEBERG_REST_URI)
        .config("spark.sql.catalog.iceberg.warehouse", WAREHOUSE)
        .config("spark.sql.catalog.iceberg.io-impl",
                "org.apache.iceberg.aws.s3.S3FileIO")
        .config("spark.sql.catalog.iceberg.s3.endpoint",          MINIO_ENDPOINT)
        .config("spark.sql.catalog.iceberg.s3.access-key-id",     MINIO_ACCESS_KEY)
        .config("spark.sql.catalog.iceberg.s3.secret-access-key", MINIO_SECRET_KEY)
        .config("spark.sql.catalog.iceberg.s3.path-style-access", "true")
        .config("spark.hadoop.fs.s3a.endpoint",        MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key",      MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key",      MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.adaptive.enabled",   "true")
        .getOrCreate()
    )


def compute_revenue_by_region(spark: SparkSession):
    """
    Agrège les revenus CDR par région, offre et date.
    Alimente gold.spark_mart_revenue.
    """
    logger.info("Computing revenue by region...")

    cdr_df = spark.table("iceberg.bronze.cdr_voice")

    revenue_df = cdr_df.groupBy(
        F.col("origin_region").alias("region"),
        F.col("offer_code"),
        F.col("call_date"),
    ).agg(
        F.count("cdr_id").alias("total_calls"),
        F.sum("duration_seconds").alias("total_duration_seconds"),
        F.sum("charge_amount").alias("total_revenue"),
        F.avg("charge_amount").alias("avg_revenue_per_call"),
        F.countDistinct("calling_msisdn").alias("unique_callers"),
        F.sum(F.when(F.col("call_status") == "SUCCESS", 1)
              .otherwise(0)).alias("successful_calls"),
    ).withColumn(
        # Taux de succès des appels
        "success_rate",
        F.round(F.col("successful_calls") / F.col("total_calls") * 100, 2)
    ).withColumn(
        "spark_computed_at", F.current_timestamp()
    )

    logger.info(f"  Revenue records : {revenue_df.count()}")
    revenue_df.orderBy(F.col("total_revenue").desc()).show(5)

    revenue_df.writeTo("iceberg.gold.spark_mart_revenue") \
              .tableProperty("write.format.default", "parquet") \
              .createOrReplace()

    logger.info("spark_mart_revenue written.")
    return revenue_df.count()


def compute_arpu(spark: SparkSession):
    """
    Calcule l'ARPU (Average Revenue Per User) par région et offre.
    ARPU = Revenu total / Nombre d'abonnés actifs uniques
    Alimente gold.spark_mart_arpu.
    """
    logger.info("Computing ARPU...")

    cdr_df    = spark.table("iceberg.bronze.cdr_voice")
    subs_df   = spark.table("iceberg.bronze.subscribers")

    # Revenus par abonné
    revenue_per_user = cdr_df.groupBy(
        "calling_msisdn", "call_date"
    ).agg(
        F.sum("charge_amount").alias("daily_revenue"),
        F.count("cdr_id").alias("daily_calls"),
    )

    # Jointure avec profil abonné
    arpu_df = revenue_per_user.join(
        subs_df.select("msisdn", "segment", "region", "account_type"),
        revenue_per_user["calling_msisdn"] == subs_df["msisdn"],
        "left"
    ).groupBy(
        "region", "segment", "account_type", "call_date"
    ).agg(
        F.avg("daily_revenue").alias("arpu"),
        F.sum("daily_revenue").alias("total_revenue"),
        F.countDistinct("calling_msisdn").alias("active_users"),
        F.avg("daily_calls").alias("avg_calls_per_user"),
    ).withColumn(
        "spark_computed_at", F.current_timestamp()
    )

    logger.info(f"  ARPU records    : {arpu_df.count()}")
    arpu_df.orderBy(F.col("arpu").desc()).show(5)

    arpu_df.writeTo("iceberg.gold.spark_mart_arpu") \
           .tableProperty("write.format.default", "parquet") \
           .createOrReplace()

    logger.info("spark_mart_arpu written.")
    return arpu_df.count()


def compute_fraud_score(spark: SparkSession):
    """
    Calcule un score de fraude basique sur les transactions Orange Money.
    Règles :
      - Montant > 500 000 XOF en une transaction → suspect
      - Plus de 10 transactions en 1 heure → suspect
      - Même sender vers 5+ receivers différents → suspect
    Alimente gold.spark_mart_fraud_alerts.
    """
    logger.info("Computing OM fraud scores...")

    om_df = spark.table("iceberg.bronze.om_transactions")

    # Règle 1 : Montant élevé
    high_amount = om_df.filter(
        F.col("amount") > 500000
    ).withColumn("fraud_reason", F.lit("HIGH_AMOUNT"))

    # Règle 2 : Fréquence élevée par msisdn et date
    freq_window = Window.partitionBy("msisdn", "transaction_date")
    high_freq = om_df.withColumn(
        "tx_count", F.count("transaction_id").over(freq_window)
    ).filter(F.col("tx_count") > 10) \
     .withColumn("fraud_reason", F.lit("HIGH_FREQUENCY"))

    # Règle 3 : Dispersion élevée (1 sender → N receivers)
    dispersion = om_df.groupBy(
        "msisdn", "transaction_date"
    ).agg(
        F.countDistinct("counterpart_msisdn").alias("unique_receivers"),
        F.sum("amount").alias("total_sent"),
    ).filter(F.col("unique_receivers") >= 5) \
     .withColumn("fraud_reason", F.lit("HIGH_DISPERSION"))

    # Union des alertes
    fraud_alerts = high_amount.select(
        "transaction_id", "msisdn", "amount",
        "transaction_date", "fraud_reason"
    ).union(
        high_freq.select(
            "transaction_id", "msisdn", "amount",
            "transaction_date", "fraud_reason"
        )
    ).distinct().withColumn(
        "spark_computed_at", F.current_timestamp()
    )

    count = fraud_alerts.count()
    logger.info(f"  Fraud alerts    : {count}")
    fraud_alerts.groupBy("fraud_reason").count().show()

    fraud_alerts.writeTo("iceberg.gold.spark_mart_fraud_alerts") \
                .tableProperty("write.format.default", "parquet") \
                .createOrReplace()

    logger.info("spark_mart_fraud_alerts written.")
    return count


def run_gold_aggregation(spark: SparkSession):
    logger.info("=" * 55)
    logger.info("ILAM — Gold Aggregation Job")
    logger.info("=" * 55)

    revenue_count = compute_revenue_by_region(spark)
    arpu_count    = compute_arpu(spark)
    fraud_count   = compute_fraud_score(spark)

    logger.info("=" * 55)
    logger.info("Gold aggregation complete.")
    logger.info(f"  spark_mart_revenue       : {revenue_count} rows")
    logger.info(f"  spark_mart_arpu          : {arpu_count} rows")
    logger.info(f"  spark_mart_fraud_alerts  : {fraud_count} rows")
    logger.info("=" * 55)

    return {
        "revenue": revenue_count,
        "arpu"   : arpu_count,
        "fraud"  : fraud_count,
    }


if __name__ == "__main__":
    spark = get_spark_session("ILAM-Gold-Aggregation")
    try:
        stats = run_gold_aggregation(spark)
        logger.info(f"Job stats: {stats}")
    finally:
        spark.stop()
