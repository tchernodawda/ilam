"""
ILAM Portal — Backend FastAPI
Point d'entrée principal de l'API.
"""
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.config import settings
from app.routers import health, tables, pipeline, spark, monitoring, auth

app = FastAPI(
    title=settings.APP_NAME,
    version=settings.APP_VERSION,
    description="ILAM Data Lakehouse Platform API — Sonatel/Orange Sénégal",
)

# CORS — permet au frontend React d'appeler l'API
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3001", "http://localhost:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Routers
app.include_router(auth.router)
app.include_router(health.router)
app.include_router(tables.router)
app.include_router(pipeline.router)
app.include_router(spark.router)
app.include_router(monitoring.router)

@app.get("/")
def root():
    return {
        "name"   : settings.APP_NAME,
        "version": settings.APP_VERSION,
        "docs"   : "/docs",
        "status" : "running",
    }

@app.get("/api/info")
def info():
    return {
        "name"        : settings.APP_NAME,
        "version"     : settings.APP_VERSION,
        "description" : "Sonatel / Orange Sénégal Data Lakehouse",
        "endpoints"   : {
            "auth"      : "/api/auth",
            "health"    : "/api/health",
            "tables"    : "/api/tables",
            "pipeline"  : "/api/pipeline",
            "spark"     : "/api/spark",
            "monitoring": "/api/monitoring",
        }
    }
