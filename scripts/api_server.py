"""Совместимый entrypoint API: thin-wrapper над app.interfaces.http.server."""

from backend.app.interfaces.http.server import (
    AskRequest,
    AskResponse,
    ClarificationPayloadResponse,
    ClarificationResponse,
    app,
    create_app,
    get_ask_service,
    get_pilot_kpi_service,
    run,
    PilotKpiResponse,
)

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


if __name__ == "__main__":
    run()
