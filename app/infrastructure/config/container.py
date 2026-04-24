"""Composition root: сборка зависимостей приложения."""

from __future__ import annotations

import os
from pathlib import Path

from backend.app.infrastructure.history.sqlite_report_history_repo import (
    ReportHistoryRepository,
)
from backend.app.application.services.pilot_kpi_service import PilotKpiService
from backend.app.infrastructure.nl2sql_adapters import (
    McpQueryExecutor,
    VannaRuntime,
    VannaSqlExplainer,
    VannaSqlGenerator,
    build_intent_classifier,
)
from backend.app.application.services.ask_service import AskService


def _build_history_repo() -> ReportHistoryRepository:
    """Создает репозиторий истории с единым источником пути к SQLite-файлу."""

    default_history_path = Path(__file__).resolve().parents[3] / "history/reports.sqlite3"
    history_path = Path(os.getenv("REPORT_HISTORY_PATH", str(default_history_path)))
    return ReportHistoryRepository(history_path)


def build_default_ask_service() -> AskService:
    """Собирает production-конфигурацию сервиса фазы 4."""

    runtime = VannaRuntime()
    return AskService(
        sql_generator=VannaSqlGenerator(runtime),
        sql_explainer=VannaSqlExplainer(runtime),
        executor=McpQueryExecutor(),
        history_repo=_build_history_repo(),
        intent_classifier=build_intent_classifier(runtime),
    )


def build_default_pilot_kpi_service() -> PilotKpiService:
    """Собирает сервис KPI-снимка поверх той же persisted report history."""

    return PilotKpiService(_build_history_repo())
