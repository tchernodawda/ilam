"""ILAM Portal — Router Pipeline (Flink + Airflow)"""
from fastapi import APIRouter
import httpx
from app.config import settings

router = APIRouter(prefix="/api/pipeline", tags=["pipeline"])

@router.get("/flink/jobs")
async def flink_jobs():
    async with httpx.AsyncClient() as client:
        try:
            r    = await client.get(
                f"{settings.FLINK_URL}/jobs", timeout=10)
            jobs = r.json().get("jobs", [])
            result = []
            for job in jobs:
                job_detail = {"id": job["id"], "status": job["status"]}
                try:
                    detail = await client.get(
                        f"{settings.FLINK_URL}/jobs/{job['id']}",
                        timeout=5)
                    d = detail.json()
                    job_detail["name"]     = d.get("name", "")
                    job_detail["duration"] = d.get("duration", 0)
                except Exception:
                    pass
                try:
                    cp = await client.get(
                        f"{settings.FLINK_URL}/jobs/{job['id']}/checkpoints",
                        timeout=5)
                    job_detail["checkpoints"] = cp.json().get("counts", {})
                except Exception:
                    pass
                result.append(job_detail)
            return result
        except Exception as e:
            return {"error": str(e)}

@router.get("/flink/overview")
async def flink_overview():
    async with httpx.AsyncClient() as client:
        try:
            r = await client.get(
                f"{settings.FLINK_URL}/overview", timeout=10)
            return r.json()
        except Exception as e:
            return {"error": str(e)}

@router.get("/airflow/dags")
async def airflow_dags():
    async with httpx.AsyncClient() as client:
        try:
            r = await client.get(
                f"{settings.AIRFLOW_URL}/api/v1/dags",
                auth=(settings.AIRFLOW_USER, settings.AIRFLOW_PASS),
                timeout=10,
            )
            return r.json()
        except Exception as e:
            return {"error": str(e)}

@router.post("/airflow/trigger/{dag_id}")
async def trigger_dag(dag_id: str):
    async with httpx.AsyncClient() as client:
        try:
            r = await client.post(
                f"{settings.AIRFLOW_URL}/api/v1/dags/{dag_id}/dagRuns",
                auth=(settings.AIRFLOW_USER, settings.AIRFLOW_PASS),
                json={"conf": {}},
                timeout=10,
            )
            return r.json()
        except Exception as e:
            return {"error": str(e)}

@router.get("/airflow/runs/{dag_id}")
async def dag_runs(dag_id: str):
    async with httpx.AsyncClient() as client:
        try:
            r = await client.get(
                f"{settings.AIRFLOW_URL}/api/v1/dags/{dag_id}/dagRuns"
                f"?limit=5&order_by=-start_date",
                auth=(settings.AIRFLOW_USER, settings.AIRFLOW_PASS),
                timeout=10,
            )
            return r.json()
        except Exception as e:
            return {"error": str(e)}

@router.post("/flink/start")
async def start_pyflink_job():
    """Soumet le job PyFlink via le jobmanager Flink."""
    import json as _json
    try:
        async with httpx.AsyncClient(
            transport=httpx.AsyncHTTPTransport(
                uds="/var/run/docker.sock"
            )
        ) as client:
            # Copier le script dans jobmanager si besoin
            # puis soumettre via exec sur flink-jobmanager
            r = await client.post(
                "http://localhost/containers/ilam-flink-jobmanager/exec",
                headers={"Content-Type": "application/json"},
                content=_json.dumps({
                    "AttachStdout": True,
                    "AttachStderr": True,
                    "Detach"      : False,
                    "Cmd": [
                        "bash", "-c",
                        "cd /opt/flink && "
                        "python3 /tmp/usrjobs/flink_cdr_job.py "
                        "> /tmp/pyflink_submit.log 2>&1 &"
                    ]
                }),
                timeout=10,
            )
            exec_id = r.json().get("Id")
            if not exec_id:
                return {"status": "error",
                        "message": "Exec creation failed"}

            await client.post(
                f"http://localhost/exec/{exec_id}/start",
                headers={"Content-Type": "application/json"},
                content=_json.dumps({"Detach": True}),
                timeout=10,
            )
            return {
                "status" : "submitted",
                "message": "Job PyFlink soumis — vérifiez dans 30s",
            }
    except Exception as e:
        return {"status": "error", "message": str(e)}

@router.get("/airflow/task-logs/{dag_id}/{run_id}/{task_id}")
async def get_task_logs(dag_id: str, run_id: str, task_id: str):
    """Retourne les logs d'une tâche Airflow."""
    async with httpx.AsyncClient(verify=False) as client:
        try:
            r = await client.get(
                f"{settings.AIRFLOW_URL}/api/v1/dags/{dag_id}"
                f"/dagRuns/{run_id}/taskInstances/{task_id}/logs/1",
                auth=(settings.AIRFLOW_USER, settings.AIRFLOW_PASS),
                timeout=10,
            )
            return {"logs": r.text[-2000:]}
        except Exception as e:
            return {"error": str(e)}

@router.get("/airflow/task-states/{dag_id}/{run_id}")
async def get_task_states(dag_id: str, run_id: str):
    """Retourne l'état de chaque tâche d'un run Airflow."""
    async with httpx.AsyncClient(verify=False) as client:
        try:
            r = await client.get(
                f"{settings.AIRFLOW_URL}/api/v1/dags/{dag_id}"
                f"/dagRuns/{run_id}/taskInstances",
                auth=(settings.AIRFLOW_USER, settings.AIRFLOW_PASS),
                timeout=10,
            )
            return r.json()
        except Exception as e:
            return {"error": str(e)}

@router.get("/kafka/topics")
async def get_kafka_topics():
    """Retourne les topics Kafka via l'API REST Docker socket."""
    import json as _json
    try:
        async with httpx.AsyncClient(
            transport=httpx.AsyncHTTPTransport(
                uds="/var/run/docker.sock"
            )
        ) as client:
            # Créer l'exec
            r = await client.post(
                "http://localhost/containers/ilam-kafka/exec",
                headers={"Content-Type": "application/json"},
                content=_json.dumps({
                    "AttachStdout": True,
                    "AttachStderr": True,
                    "Cmd": [
                        "/opt/kafka/bin/kafka-topics.sh",
                        "--bootstrap-server", "localhost:9092",
                        "--list"
                    ]
                }),
                timeout=10,
            )
            exec_id = r.json().get("Id")
            if not exec_id:
                return {"topics": [], "count": 0,
                        "error": "Exec creation failed"}
            # Démarrer et récupérer l'output
            out = await client.post(
                f"http://localhost/exec/{exec_id}/start",
                headers={"Content-Type": "application/json"},
                content=_json.dumps({"Detach": False, "Tty": False}),
                timeout=15,
            )
            # Parser l'output (format Docker multiplexed stream)
            raw = out.content
            topics = []
            i = 0
            while i < len(raw):
                if i + 8 <= len(raw):
                    size = int.from_bytes(raw[i+4:i+8], 'big')
                    chunk = raw[i+8:i+8+size].decode('utf-8', errors='ignore')
                    for line in chunk.strip().split('\n'):
                        line = line.strip()
                        if line and not line.startswith('__'):
                            topics.append(line)
                    i += 8 + size
                else:
                    break
            return {"topics": topics, "count": len(topics)}
    except Exception as e:
        return {"error": str(e), "topics": [], "count": 0}
