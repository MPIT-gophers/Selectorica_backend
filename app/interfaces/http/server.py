"""HTTP entrypoint приложения: создание app и запуск Uvicorn."""

from __future__ import annotations

import uvicorn

from backend.app.interfaces.http.api_v1.ask_api import (
    AskRequest,
    AskResponse,
    ClarificationPayloadResponse,
    ClarificationResponse,
    PilotKpiResponse,
    create_app,
    get_ask_service,
    get_pilot_kpi_service,
)

app = create_app()

__all__ = [
    "AskRequest",
    "AskResponse",
    "ClarificationPayloadResponse",
    "ClarificationResponse",
    "PilotKpiResponse",
    "create_app",
    "get_ask_service",
    "get_pilot_kpi_service",
    "app",
    "run",
]


def run() -> None:
    """Запускает Uvicorn для API-приложения фазы 4."""

    uvicorn.run("backend.app.interfaces.http.server:app", host="0.0.0.0", port=8000, reload=False)


if __name__ == "__main__":
    run()
