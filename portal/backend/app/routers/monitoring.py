"""ILAM Portal — Router Monitoring (Prometheus + Grafana)"""
from fastapi import APIRouter
import httpx
from app.config import settings

router = APIRouter(prefix="/api/monitoring", tags=["monitoring"])

async def query_prometheus(expr: str) -> float:
    """Exécute une requête PromQL et retourne la valeur."""
    async with httpx.AsyncClient() as client:
        try:
            r = await client.get(
                f"{settings.PROMETHEUS_URL}/api/v1/query",
                params={"query": expr},
                timeout=10,
            )
            result = r.json()["data"]["result"]
            return float(result[0]["value"][1]) if result else 0.0
        except Exception:
            return 0.0

@router.get("/metrics")
async def get_metrics():
    """Retourne les métriques clés depuis Prometheus."""
    import asyncio
    queries = {
        "flink_jobs_running"    : "ilam_flink_jobs_running",
        "flink_checkpoints"     : "ilam_flink_checkpoints_completed",
        "flink_slots_available" : "ilam_flink_slots_available",
        "flink_taskmanagers"    : "ilam_flink_taskmanagers",
        "trino_up"              : "ilam_trino_up",
        "minio_up"              : "ilam_minio_up",
        "iceberg_up"            : "ilam_iceberg_up",
    }
    results = await asyncio.gather(*[
        query_prometheus(expr)
        for expr in queries.values()
    ])
    return dict(zip(queries.keys(), results))

@router.get("/table-counts")
async def get_table_counts():
    """Retourne les counts de toutes les tables depuis Prometheus."""
    async with httpx.AsyncClient() as client:
        try:
            r = await client.get(
                f"{settings.PROMETHEUS_URL}/api/v1/query",
                params={"query": "ilam_iceberg_table_rows"},
                timeout=10,
            )
            results = r.json()["data"]["result"]
            counts  = {}
            for item in results:
                schema = item["metric"].get("schema", "")
                table  = item["metric"].get("table", "")
                value  = int(float(item["value"][1]))
                counts[f"{schema}.{table}"] = value
            return counts
        except Exception as e:
            return {"error": str(e)}

@router.get("/grafana/dashboards")
async def grafana_dashboards():
    """Liste les dashboards Grafana."""
    async with httpx.AsyncClient() as client:
        try:
            r = await client.get(
                f"{settings.GRAFANA_URL}/api/search?type=dash-db",
                auth=("admin", "ilam_password_2025"),
                timeout=10,
            )
            return r.json()
        except Exception as e:
            return {"error": str(e)}
