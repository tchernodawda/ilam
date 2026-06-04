"""ILAM Portal — Router Tables Iceberg"""
from fastapi import APIRouter, HTTPException
from app.services.trino_service import (
    get_all_tables, get_table_preview, execute_query)
from app.models.schemas import QueryRequest

router = APIRouter(prefix="/api/tables", tags=["tables"])

@router.get("/")
def list_tables():
    return get_all_tables()

@router.get("/{schema}/{table}/preview")
def preview_table(schema: str, table: str, limit: int = 50):
    try:
        return get_table_preview(schema, table, limit)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/query")
def run_query(request: QueryRequest):
    return execute_query(request.sql, request.limit)
