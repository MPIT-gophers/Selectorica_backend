"""Основная точка входа FastAPI-приложения в новой структуре."""

from __future__ import annotations

import uvicorn

from backend.app.interfaces.http.api_v1.ask_api import create_app

app = create_app()


if __name__ == "__main__":
    uvicorn.run("backend.app.main:app", host="0.0.0.0", port=8000, reload=False)

