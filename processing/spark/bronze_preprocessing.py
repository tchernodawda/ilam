"""
ILAM — Spark Job 1 : Bronze Preprocessing
Couche Bronze → Bronze Clean

Rôle :
  - Déduplication des CDR voix (doublons réseau fréquents en télécom)
  - Normalisation des MSISDN au format E.164 (+221XXXXXXXXX)
  - Filtrage des enregistrements corrompus (durée négative, MSISDN null)
  - Enrichissement avec métadonnées de traitement

En production Sonatel :
  - Les équipements MSC Huawei génèrent parfois des CDR dupliqués
  - Les MSISDN peuvent être au format 221, 00221 ou +221
  - Ce job tourne toutes les heures avant les transformations Silver
"""

import logging
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# =============================================================================
# Configuration
# =============================================================================
ICEBERG_REST_URI = "http://iceberg-rest:8181"
MINIO_ENDPOINT   = "http://minio:9000"
MINIO_ACCESS_KEY = "ilam_admin"
MINIO_SECRET_KEY = "ilam_password_2025"
WAREHOUSE        = "s3://warehouse/"
SPARK_MASTER     = "spark://spark-master:7077"


def get_spark_session(app_name: str) -> SparkSession:
    """Crée une SparkSession connectée au cluster Spark + Iceberg + MinIO."""
    return (
        SparkSession.builder
        .appName(app_name)
        .master(SPARK_MASTER)

        # Iceberg extensions
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

        # MinIO S3
        .config("spark.sql.catalog.iceberg.s3.endpoint",
                MINIO_ENDPOINT)
        .config("spark.sql.catalog.iceberg.s3.access-key-id",
                MINIO_ACCESS_KEY)
        .config("spark.sql.catalog.iceberg.s3.secret-access-key",
                MINIO_SECRET_KEY)
        .config("spark.sql.catalog.iceberg.s3.path-style-access", "true")
        .config("spark.hadoop.fs.s3a.endpoint",        MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key",      MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key",      MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")

        # Performance
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.adaptive.enabled",   "true")
        .getOrCreate()
    )


def normalize_msisdn(msisdn_col):
    """
    Normalise un MSISDN au format E.164 (+221XXXXXXXXX).

    Cas gérés en production Sonatel :
      221XXXXXXXXX  → +221XXXXXXXXX
      00221XXXXXXXXX → +221XXXXXXXXX
      +221XXXXXXXXX  → +221XXXXXXXXX (déjà correct)
    """
    return F.when(
        msisdn_col.startswith("00221"),
        F.concat(F.lit("+"), msisdn_col.substr(3, 100))
    ).when(
        msisdn_col.startswith("221"),
        F.concat(F.lit("+"), msisdn_col)
    ).otherwise(msisdn_col)


def run_bronze_preprocessing(spark: SparkSession):
    """
    Pipeline de preprocessing Bronze :
    1. Lecture depuis iceberg.bronze.cdr_voice
    2. Filtrage des enregistrements invalides
    3. Déduplication sur cdr_id
    4. Normalisation des MSISDN
    5. Ajout de métadonnées de traitement
    6. Écriture dans iceberg.bronze.cdr_voice_clean
    """

    logger.info("=" * 55)
    logger.info("ILAM — Bronze Preprocessing Job")
    logger.info("=" * 55)

    # ── 1. Lecture Bronze ────────────────────────────────────────
    logger.info("Reading bronze.cdr_voice...")
    df = spark.table("iceberg.bronze.cdr_voice")
    count_raw = df.count()
    logger.info(f"  Raw records     : {count_raw}")

    # ── 2. Filtrage des enregistrements invalides ────────────────
    # En production : les CDR avec durée négative sont des artefacts
    # réseau (signalisation sans appel réel)
    df_filtered = df.filter(
        F.col("calling_msisdn").isNotNull() &
        F.col("called_msisdn").isNotNull() &
        F.col("cdr_id").isNotNull() &
        (F.col("duration_seconds") >= 0)
    )
    count_filtered = df_filtered.count()
    logger.info(f"  After filtering : {count_filtered} "
                f"(-{count_raw - count_filtered} invalid)")

    # ── 3. Déduplication sur cdr_id ─────────────────────────────
    # Les équipements MSC peuvent envoyer des CDR en double
    # On garde le premier enregistrement par cdr_id
    df_deduped = df_filtered.dropDuplicates(["cdr_id"])
    count_deduped = df_deduped.count()
    logger.info(f"  After dedup     : {count_deduped} "
                f"(-{count_filtered - count_deduped} duplicates)")

    # ── 4. Normalisation des MSISDN ──────────────────────────────
    df_normalized = df_deduped.withColumn(
        "calling_msisdn", normalize_msisdn(F.col("calling_msisdn"))
    ).withColumn(
        "called_msisdn", normalize_msisdn(F.col("called_msisdn"))
    )

    # ── 5. Ajout métadonnées de traitement ───────────────────────
    df_enriched = df_normalized.withColumn(
        "spark_processed_at", F.current_timestamp()
    ).withColumn(
        "spark_job_version", F.lit("1.0.0")
    ).withColumn(
        "is_duplicate",      F.lit(False)
    )

    # ── 6. Statistiques avant écriture ──────────────────────────
    logger.info("Statistics :")
    df_enriched.groupBy("call_status").count().show()
    df_enriched.groupBy("origin_region").count().show()

    # ── 7. Écriture dans Iceberg ─────────────────────────────────
    logger.info("Writing to iceberg.bronze.cdr_voice_clean...")
    (
        df_enriched.writeTo("iceberg.bronze.cdr_voice_clean")
        .tableProperty("write.format.default", "parquet")
        .createOrReplace()
    )

    count_written = spark.table("iceberg.bronze.cdr_voice_clean").count()
    logger.info(f"  Written records : {count_written}")
    logger.info("Bronze preprocessing complete.")

    return {
        "raw"     : count_raw,
        "filtered": count_filtered,
        "deduped" : count_deduped,
        "written" : count_written,
    }


if __name__ == "__main__":
    spark = get_spark_session("ILAM-Bronze-Preprocessing")
    try:
        stats = run_bronze_preprocessing(spark)
        logger.info(f"Job stats: {stats}")
    finally:
        spark.stop()
