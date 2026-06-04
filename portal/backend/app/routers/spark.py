"""ILAM Portal — Router Spark"""
from fastapi import APIRouter, BackgroundTasks
import httpx
import subprocess
from app.config import settings
from app.models.schemas import SparkJobRequest

router = APIRouter(prefix="/api/spark", tags=["spark"])

@router.get("/cluster")
async def spark_cluster():
    async with httpx.AsyncClient() as client:
        try:
            r = await client.get(
                f"{settings.SPARK_URL}/json/", timeout=10)
            d = r.json()
            return {
                "workers"        : len(d.get("workers", [])),
                "cores_total"    : d.get("cores", 0),
                "cores_used"     : d.get("coresused", 0),
                "memory_total"   : d.get("memory", 0),
                "memory_used"    : d.get("memoryused", 0),
                "apps_running"   : len(d.get("activeapps", [])),
                "apps_completed" : len(d.get("completedapps", [])),
                "workers_detail" : d.get("workers", []),
            }
        except Exception as e:
            return {"error": str(e)}

@router.get("/apps")
async def spark_apps():
    async with httpx.AsyncClient() as client:
        try:
            r = await client.get(
                f"{settings.SPARK_URL}/json/", timeout=10)
            d = r.json()
            return {
                "active"   : d.get("activeapps", []),
                "completed": d.get("completedapps", [])[-5:],
            }
        except Exception as e:
            return {"error": str(e)}

@router.post("/submit")
async def submit_spark_job(
    request: SparkJobRequest,
    background: BackgroundTasks
):
    """Soumet un job Spark en arrière-plan."""
    def run_job(job_type: str):
        subprocess.run([
            "docker", "exec", "ilam-spark-master",
            "bash", "-c",
            f"cd /opt/spark/jobs && "
            f"/opt/spark/bin/spark-submit "
            f"--master spark://spark-master:7077 "
            f"--deploy-mode client "
            f"--conf spark.executor.memory=512m "
            f"--conf spark.driver.memory=512m "
            f"{job_type}_preprocessing.py "
            f"2>&1 | tail -20"
        ], capture_output=True)

    job_map = {
        "bronze": "bronze",
        "silver": "silver",
        "gold"  : "gold_aggregation",
    }

    if request.job_type not in ["bronze", "silver", "gold", "all"]:
        return {"error": "job_type must be bronze|silver|gold|all"}

    if request.job_type == "all":
        for jt in ["bronze", "silver", "gold"]:
            background.add_task(run_job, job_map[jt])
    else:
        background.add_task(run_job, job_map[request.job_type])

    return {
        "status" : "submitted",
        "job"    : request.job_type,
        "message": f"Job {request.job_type} soumis en arrière-plan",
    }
