"""
ILAM — Spark Session Factory
Configuration commune pour tous les jobs Spark.
Spark lit/écrit dans Iceberg via le catalogue REST (MinIO comme stockage).
"""

import os
from pyspark.sql import SparkSession

# =============================================================================
# Configuration
# =============================================================================
ICEBERG_REST_URI  = "http://localhost:8181"
MINIO_ENDPOINT    = "http://localhost:9000"
MINIO_ACCESS_KEY  = "ilam_admin"
MINIO_SECRET_KEY  = "ilam_password_2025"
WAREHOUSE         = "s3://warehouse/"
JARS_DIR          = os.path.expanduser("~/ilam/infra/spark/jars")

# JARs Spark
JARS = ",".join([
    f"{JARS_DIR}/iceberg-spark-runtime-3.5_2.12-1.5.2.jar",
    f"{JARS_DIR}/bundle-2.20.18.jar",
    f"{JARS_DIR}/hadoop-aws-3.3.4.jar",
])


def get_spark_session(app_name: str, shuffle_partitions: int = 4) -> SparkSession:
    """
    Crée et retourne une SparkSession configurée pour Iceberg + MinIO.

    Args:
        app_name           : Nom du job Spark (visible dans les logs)
        shuffle_partitions : Nombre de partitions pour les shuffles
                             (4 est adapté à notre environnement dev)
    Returns:
        SparkSession configurée
    """
    spark = (
        SparkSession.builder
        .appName(app_name)

        # ── JARs ──────────────────────────────────────────────────────────
        .config("spark.jars", JARS)

        # ── Catalogue Iceberg REST ─────────────────────────────────────────
        # Déclare un catalogue nommé "iceberg" qui pointe vers notre REST API
        .config("spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.iceberg",
                "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.iceberg.type",       "rest")
        .config("spark.sql.catalog.iceberg.uri",        ICEBERG_REST_URI)
        .config("spark.sql.catalog.iceberg.warehouse",  WAREHOUSE)
        .config("spark.sql.catalog.iceberg.io-impl",
                "org.apache.iceberg.aws.s3.S3FileIO")

        # ── MinIO (compatible S3) ──────────────────────────────────────────
        # Spark utilise le protocole S3A pour accéder à MinIO
        .config("spark.sql.catalog.iceberg.s3.endpoint",        MINIO_ENDPOINT)
        .config("spark.sql.catalog.iceberg.s3.access-key-id",   MINIO_ACCESS_KEY)
        .config("spark.sql.catalog.iceberg.s3.secret-access-key", MINIO_SECRET_KEY)
        .config("spark.sql.catalog.iceberg.s3.path-style-access", "true")
        .config("spark.hadoop.fs.s3a.endpoint",                  MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key",                MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key",                MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access",         "true")
        .config("spark.hadoop.fs.s3a.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")

        # ── Performance ────────────────────────────────────────────────────
        # shuffle_partitions=4 adapté à notre env dev (évite 200 partitions)
        .config("spark.sql.shuffle.partitions",         shuffle_partitions)
        .config("spark.sql.adaptive.enabled",           "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")

        # ── Logs ───────────────────────────────────────────────────────────
        .config("spark.ui.showConsoleProgress", "false")

        .getOrCreate()
    )

    # Réduire les logs Spark (trop verbeux par défaut)
    spark.sparkContext.setLogLevel("WARN")

    return spark
