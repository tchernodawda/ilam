"""
ILAM Portal — Schémas Pydantic
"""
from pydantic import BaseModel
from typing import Optional, List, Dict, Any
from enum import Enum

# =============================================================================
# Auth
# =============================================================================
class UserRole(str, Enum):
    ADMIN    = "admin"
    ENGINEER = "engineer"
    ANALYST  = "analyst"
    VIEWER   = "viewer"

class LoginRequest(BaseModel):
    username : str
    password : str

class TokenResponse(BaseModel):
    access_token : str
    token_type   : str = "bearer"
    role         : str
    username     : str

class UserInfo(BaseModel):
    username : str
    role     : UserRole
    email    : Optional[str] = None

# =============================================================================
# Services
# =============================================================================
class ServiceStatus(BaseModel):
    name    : str
    status  : str  # "up" | "down" | "degraded"
    url     : str
    details : Optional[Dict[str, Any]] = None

class PlatformOverview(BaseModel):
    services        : List[ServiceStatus]
    services_up     : int
    services_total  : int
    flink_jobs      : int
    checkpoints     : int
    bronze_rows     : int
    quality_score   : float

# =============================================================================
# Tables Iceberg
# =============================================================================
class TableInfo(BaseModel):
    schema_name : str
    table_name  : str
    row_count   : int
    columns     : Optional[List[Dict[str, str]]] = None

class TablePreview(BaseModel):
    table_name : str
    columns    : List[str]
    rows       : List[List[Any]]
    total_rows : int

class QueryRequest(BaseModel):
    sql     : str
    limit   : int = 100

class QueryResult(BaseModel):
    columns      : List[str]
    rows         : List[List[Any]]
    row_count    : int
    duration_ms  : float
    error        : Optional[str] = None

# =============================================================================
# Pipeline
# =============================================================================
class FlinkJob(BaseModel):
    job_id      : str
    name        : str
    status      : str
    start_time  : Optional[str] = None
    duration    : Optional[int] = None
    checkpoints : Optional[Dict[str, int]] = None

class AirflowDagRun(BaseModel):
    dag_id       : str
    run_id       : str
    state        : str
    start_date   : Optional[str] = None
    end_date     : Optional[str] = None
    duration     : Optional[float] = None

class NifiProcessGroup(BaseModel):
    id          : str
    name        : str
    running     : int
    stopped     : int
    invalid     : int
    flow_files  : int

# =============================================================================
# Spark
# =============================================================================
class SparkJobRequest(BaseModel):
    job_type    : str  # "bronze" | "silver" | "gold" | "all"

class SparkJobStatus(BaseModel):
    app_id      : str
    name        : str
    status      : str
    cores       : int
    memory      : str
    duration    : Optional[int] = None

class SparkClusterInfo(BaseModel):
    workers         : int
    cores_total     : int
    cores_used      : int
    memory_total    : int
    memory_used     : int
    apps_running    : int
    apps_completed  : int

# =============================================================================
# Qualité
# =============================================================================
class QualityResult(BaseModel):
    suite       : str
    score       : float
    status      : str
    total       : int
    success     : int
    failed      : int
    failed_expectations : List[Dict[str, str]] = []

class QualityReport(BaseModel):
    run_date        : str
    global_score    : float
    global_status   : str
    tables          : List[QualityResult]

# =============================================================================
# Monitoring
# =============================================================================
class MetricValue(BaseModel):
    name    : str
    value   : float
    labels  : Optional[Dict[str, str]] = None

class MonitoringData(BaseModel):
    flink_jobs_running      : int
    flink_checkpoints       : int
    flink_slots_available   : int
    table_counts            : Dict[str, int]
    services_health         : Dict[str, bool]
