"""ILAM Portal — Router Health"""
from fastapi import APIRouter
from app.services.health import get_all_services, get_platform_overview

router = APIRouter(prefix="/api/health", tags=["health"])

@router.get("/services")
async def services_health():
    return await get_all_services()

@router.get("/overview")
async def platform_overview():
    return await get_platform_overview()

@router.get("/quality/report")
def get_quality_report():
    """Lit le dernier rapport Great Expectations."""
    import os, json, glob
    reports_dir = "/opt/airflow/quality/reports"
    try:
        # Trouver le rapport le plus récent
        files = glob.glob(f"{reports_dir}/quality_report_*.json")
        if not files:
            return {"error": "Aucun rapport disponible",
                    "global_score": 0, "global_status": "UNKNOWN",
                    "tables": []}
        latest = max(files, key=os.path.getctime)
        with open(latest, "r") as f:
            return json.load(f)
    except Exception as e:
        return {"error": str(e), "global_score": 0,
                "global_status": "UNKNOWN", "tables": []}
