"""
ILAM Portal — Configuration centralisée
"""
from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    # Services ILAM
    FLINK_URL       : str = "http://flink-jobmanager:8081"
    TRINO_HOST      : str = "trino"
    TRINO_PORT      : int = 8080
    MINIO_URL       : str = "http://minio:9000"
    ICEBERG_URL     : str = "http://iceberg-rest:8181"
    KAFKA_BOOTSTRAP : str = "ilam-kafka:9092"
    AIRFLOW_URL     : str = "http://airflow-webserver:8080"
    AIRFLOW_USER    : str = "ilam_admin"
    AIRFLOW_PASS    : str = "ilam_password_2025"
    SPARK_URL       : str = "http://spark-master:8080"
    PROMETHEUS_URL  : str = "http://prometheus:9090"
    GRAFANA_URL     : str = "http://grafana:3000"
    NIFI_URL        : str = "https://nifi:8443"

    # Auth JWT
    SECRET_KEY      : str = "ilam-secret-key-sonatel-2025"
    ALGORITHM       : str = "HS256"
    TOKEN_EXPIRE    : int = 480  # 8 heures

    # App
    APP_NAME        : str = "ILAM Portal"
    APP_VERSION     : str = "1.0.0"
    DEBUG           : bool = False

    class Config:
        env_file = ".env"

settings = Settings()
