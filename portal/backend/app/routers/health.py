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
