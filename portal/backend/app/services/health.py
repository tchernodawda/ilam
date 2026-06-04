"""
ILAM Portal — Service Health Check
Vérifie l'état de tous les services ILAM.
"""
import httpx
import logging
from app.config import settings
from app.models.schemas import ServiceStatus, PlatformOverview

logger = logging.getLogger(__name__)

SERVICES = [
    {
        "name"   : "MinIO",
        "url"    : f"{settings.MINIO_URL}/minio/health/live",
        "display": settings.MINIO_URL.replace("http://", "").split(":")[0],
    },
    {
        "name"   : "Iceberg REST",
        "url"    : f"{settings.ICEBERG_URL}/v1/config",
        "display": "iceberg-rest:8181",
    },
    {
        "name"   : "Trino",
        "url"    : f"http://{settings.TRINO_HOST}:{settings.TRINO_PORT}/v1/info",
        "display": f"{settings.TRINO_HOST}:{settings.TRINO_PORT}",
    },
    {
        "name"   : "Flink",
        "url"    : f"{settings.FLINK_URL}/overview",
        "display": "flink-jobmanager:8081",
    },
    {
        "name"   : "Airflow",
        "url"    : f"{settings.AIRFLOW_URL}/health",
        "display": "airflow-webserver:8080",
    },
    {
        "name"   : "Spark",
        "url"    : f"{settings.SPARK_URL}/json/",
        "display": "spark-master:8080",
    },
    {
        "name"   : "Prometheus",
        "url"    : f"{settings.PROMETHEUS_URL}/-/healthy",
        "display": "prometheus:9090",
    },
    {
        "name"   : "Grafana",
        "url"    : f"{settings.GRAFANA_URL}/api/health",
        "display": "grafana:3000",
    },
]


async def check_service(
    client: httpx.AsyncClient,
    service: dict
) -> ServiceStatus:
    """Vérifie l'état d'un service via son endpoint de santé."""
    try:
        r = await client.get(
            service["url"],
            timeout=5.0,
        )
        status  = "up" if r.status_code in [200, 204] else "degraded"
        details = None
        try:
            if r.content:
                details = r.json()
        except Exception:
            pass
        return ServiceStatus(
            name=service["name"],
            status=status,
            url=service["display"],
            details=details,
        )
    except Exception as e:
        logger.warning(f"Service {service['name']} down: {e}")
        return ServiceStatus(
            name=service["name"],
            status="down",
            url=service["display"],
        )


async def get_all_services() -> list[ServiceStatus]:
    """Vérifie tous les services en parallèle."""
    import asyncio
    async with httpx.AsyncClient(verify=False) as client:
        tasks = [check_service(client, s) for s in SERVICES]
        return await asyncio.gather(*tasks)


async def get_platform_overview() -> PlatformOverview:
    """Retourne une vue d'ensemble de la plateforme."""
    import trino as trino_client

    services  = await get_all_services()
    up_count  = sum(1 for s in services if s.status == "up")

    # Flink jobs
    flink_jobs   = 0
    checkpoints  = 0
    try:
        async with httpx.AsyncClient(verify=False) as client:
            r = await client.get(
                f"{settings.FLINK_URL}/jobs", timeout=5)
            jobs = r.json().get("jobs", [])
            flink_jobs = sum(
                1 for j in jobs if j["status"] == "RUNNING")
            if flink_jobs > 0:
                job_id = next(
                    j["id"] for j in jobs
                    if j["status"] == "RUNNING")
                cp = await client.get(
                    f"{settings.FLINK_URL}/jobs/{job_id}/checkpoints",
                    timeout=5)
                checkpoints = cp.json().get(
                    "counts", {}).get("completed", 0)
    except Exception:
        pass

    # Bronze total rows
    bronze_rows = 0
    try:
        conn = trino_client.dbapi.connect(
            host=settings.TRINO_HOST,
            port=settings.TRINO_PORT,
            user="portal",
            catalog="iceberg",
            request_timeout=10,
        )
        cursor = conn.cursor()
        cursor.execute("""
            SELECT SUM(cnt) FROM (
                SELECT COUNT(*) as cnt FROM iceberg.bronze.cdr_voice
                UNION ALL
                SELECT COUNT(*) FROM iceberg.bronze.cdr_sms
                UNION ALL
                SELECT COUNT(*) FROM iceberg.bronze.network_events
            )
        """)
        bronze_rows = cursor.fetchone()[0] or 0
    except Exception:
        pass

    return PlatformOverview(
        services=services,
        services_up=up_count,
        services_total=len(services),
        flink_jobs=flink_jobs,
        checkpoints=checkpoints,
        bronze_rows=bronze_rows,
        quality_score=100.0,
    )
