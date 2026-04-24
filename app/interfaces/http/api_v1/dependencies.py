"""DI-зависимости HTTP слоя."""

from __future__ import annotations

from functools import lru_cache

from backend.app.infrastructure.config.container import (
    build_default_ask_service,
    build_default_pilot_kpi_service,
)
from backend.app.application.services.ask_service import AskService
from backend.app.application.services.pilot_kpi_service import PilotKpiService


@lru_cache(maxsize=1)
def get_ask_service() -> AskService:
    """Возвращает singleton сервиса фазы 4 для всех запросов API."""

    return build_default_ask_service()


@lru_cache(maxsize=1)
def get_pilot_kpi_service() -> PilotKpiService:
    """Возвращает singleton сервиса KPI-снимка пилота для read-only endpoint."""

    return build_default_pilot_kpi_service()
