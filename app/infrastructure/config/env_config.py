"""Единая загрузка `.env` и DB-конфигурации для инфраструктурного слоя."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


@dataclass
class DbConfig:
    """Параметры подключения к PostgreSQL для инфраструктурных адаптеров."""

    host: str
    port: int
    database: str
    user: str
    password: str


def load_repo_env() -> None:
    """Загружает переменные окружения из `backend/.env`, если файл существует."""

    env_path = Path(__file__).resolve().parents[3] / ".env"
    if not env_path.exists():
        return

    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


def get_db_config() -> DbConfig:
    """Собирает `DbConfig` из переменных окружения с безопасными fallback-значениями."""

    load_repo_env()
    host = os.getenv("PGHOST", "127.0.0.1")
    port = int(os.getenv("PGPORT", "15432"))
    database = os.getenv("PGDATABASE", os.getenv("POSTGRES_DB", "drivee_nl2sql"))
    user = os.getenv("PGUSER", os.getenv("POSTGRES_USER", "postgres"))
    password = os.getenv("PGPASSWORD", os.getenv("POSTGRES_PASSWORD", "postgres"))
    return DbConfig(
        host=host,
        port=port,
        database=database,
        user=user,
        password=password,
    )
