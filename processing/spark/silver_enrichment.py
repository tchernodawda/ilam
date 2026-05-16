"""
ILAM — Spark Job 2 : Silver Enrichissement
Bronze → Silver

Rôle :
  - Jointure CDR voix × Subscribers (enrichissement profil abonné)
  - Jointure CDR voix × Contracts (enrichissement offre)
  - Calcul de métriques dérivées (durée en minutes, coût par minute)
  - Segmentation automatique des appels

En production Sonatel :
  - Cette jointure porte sur des milliards de CDR × 17M abonnés
  - Impossible à faire efficacement avec Trino/dbt sur ces volumes
  - Spark distribue la jointure sur plusieurs nœuds en mémoire
"""

import logging
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

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


def run_silver_enrichment(spark: SparkSession):
    """
    Pipeline d'enrichissement Silver :
    1. Lecture CDR voix Bronze (nettoyés)
    2. Lecture Subscribers Bronze
    3. Jointure CDR × Subscribers
    4. Calcul métriques dérivées
    5. Écriture dans Silver
    """

    logger.info("=" * 55)
    logger.info("ILAM — Silver Enrichment Job")
    logger.info("=" * 55)

    # ── 1. Lecture CDR Bronze ────────────────────────────────────
    # Utilise cdr_voice_clean si disponible, sinon cdr_voice
    try:
        cdr_df = spark.table("iceberg.bronze.cdr_voice_clean")
        logger.info("Using cdr_voice_clean (preprocessed)")
    except Exception:
        cdr_df = spark.table("iceberg.bronze.cdr_voice")
        logger.info("Using cdr_voice (raw)")

    count_cdr = cdr_df.count()
    logger.info(f"  CDR records     : {count_cdr}")

    # ── 2. Lecture Subscribers Bronze ───────────────────────────
    subs_df = spark.table("iceberg.bronze.subscribers").select(
        "msisdn", "account_type", "status",
        "segment", "region", "city",
        "first_name", "last_name"
    )
    count_subs = subs_df.count()
    logger.info(f"  Subscribers     : {count_subs}")

    # ── 3. Jointure CDR × Subscribers ───────────────────────────
    # LEFT JOIN — on garde tous les CDR même sans abonné connu
    # En production : certains MSISDN peuvent être des numéros étrangers
    enriched_df = cdr_df.join(
        subs_df.alias("caller"),
        cdr_df["calling_msisdn"] == F.col("caller.msisdn"),
        "left"
    ).select(
        cdr_df["cdr_id"],
        cdr_df["calling_msisdn"],
        cdr_df["called_msisdn"],
        cdr_df["call_direction"],
        cdr_df["call_type"],
        cdr_df["duration_seconds"],
        cdr_df["call_status"],
        cdr_df["origin_region"],
        cdr_df["charge_amount"],
        cdr_df["currency"],
        cdr_df["offer_code"],
        cdr_df["start_ts"],
        cdr_df["call_date"],
        F.col("caller.account_type").alias("caller_account_type"),
        F.col("caller.status").alias("caller_status"),
        F.col("caller.segment").alias("caller_segment"),
        F.col("caller.region").alias("caller_region"),
    )

    # ── 4. Métriques dérivées ────────────────────────────────────
    enriched_df = enriched_df.withColumn(
        # Durée en minutes (arrondie à 2 décimales)
        "duration_minutes",
        F.round(F.col("duration_seconds") / 60, 2)
    ).withColumn(
        # Coût par minute
        "cost_per_minute",
        F.when(
            F.col("duration_seconds") > 0,
            F.round(F.col("charge_amount") /
                    (F.col("duration_seconds") / 60), 4)
        ).otherwise(F.lit(0.0))
    ).withColumn(
        # Catégorie d'appel selon la durée
        "call_category",
        F.when(F.col("duration_seconds") < 30,   F.lit("VERY_SHORT"))
         .when(F.col("duration_seconds") < 120,  F.lit("SHORT"))
         .when(F.col("duration_seconds") < 300,  F.lit("MEDIUM"))
         .when(F.col("duration_seconds") < 600,  F.lit("LONG"))
         .otherwise(F.lit("VERY_LONG"))
    ).withColumn(
        "spark_enriched_at", F.current_timestamp()
    )

    # ── 5. Statistiques ──────────────────────────────────────────
    logger.info("Call categories distribution:")
    enriched_df.groupBy("call_category").count().orderBy("count",
                                                          ascending=False).show()

    logger.info("Top regions by call volume:")
    enriched_df.groupBy("origin_region").count().orderBy("count",
                                                          ascending=False).show(5)

    # ── 6. Écriture dans Silver ──────────────────────────────────
    logger.info("Writing to iceberg.silver.cdr_voice_enriched...")
    (
        enriched_df.writeTo("iceberg.silver.cdr_voice_enriched")
        .tableProperty("write.format.default", "parquet")
        .createOrReplace()
    )

    count_written = spark.table(
        "iceberg.silver.cdr_voice_enriched").count()
    logger.info(f"  Written records : {count_written}")
    logger.info("Silver enrichment complete.")

    return {"cdr": count_cdr, "subscribers": count_subs,
            "enriched": count_written}


if __name__ == "__main__":
    spark = get_spark_session("ILAM-Silver-Enrichment")
    try:
        stats = run_silver_enrichment(spark)
        logger.info(f"Job stats: {stats}")
    finally:
        spark.stop()
