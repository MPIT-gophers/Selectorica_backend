"""Фасад API v1: сборка приложения и re-export контрактов."""

from __future__ import annotations

from fastapi import FastAPI

from .dependencies import get_ask_service, get_pilot_kpi_service
from .router import router
from .schemas import (
    AskRequest,
    AskResponse,
    ClarificationOptionResponse,
    ClarificationPayloadResponse,
    ClarificationResponse,
    PilotKpiResponse,
)


def create_app() -> FastAPI:
    """Создает и конфигурирует FastAPI-приложение фазы 4."""

    app = FastAPI(
        title="Drivee NL2SQL API",
        version="0.1.0",
        description="Легковесный API для NL2SQL-пайплайна с guardrails.",
        openapi_url="/openapi.json",
        docs_url="/docs",
        redoc_url="/redoc",
        openapi_tags=[
            {
                "name": "ask",
                "description": "NL2SQL запросы с explain и guardrails.",
            },
            {
                "name": "pilot",
                "description": "Read-only KPI snapshot, derived from persisted report history.",
            },
        ],
    )
    app.include_router(router)
    return app


__all__ = [
    "AskRequest",
    "AskResponse",
    "ClarificationOptionResponse",
    "ClarificationPayloadResponse",
    "ClarificationResponse",
    "PilotKpiResponse",
    "create_app",
    "get_ask_service",
    "get_pilot_kpi_service",
]

