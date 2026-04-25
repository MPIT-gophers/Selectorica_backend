"""Единая загрузка `.env` и DB-конфигурации для инфраструктурного слоя."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine


_DEFAULT_STATEMENT_TIMEOUT_MS = 15_000


@dataclass
class DbConfig:
    """Параметры подключения к PostgreSQL для инфраструктурных адаптеров."""

    host: str
    port: int
    database: str
    user: str
    password: str


class RuntimeDbConfigError(RuntimeError):
    """Ошибка небезопасной runtime-конфигурации подключения к PostgreSQL."""


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


def get_runtime_db_config() -> DbConfig:
    """Собирает read-only `DbConfig` для runtime-выполнения NL2SQL-запросов."""

    admin_config = get_db_config()
    user = os.getenv("READONLY_DB_USER", "").strip()
    password = os.getenv("READONLY_DB_PASSWORD", "")

    if user and password:
        return DbConfig(
            host=admin_config.host,
            port=admin_config.port,
            database=admin_config.database,
            user=user,
            password=password,
        )

    if _env_flag_enabled("ALLOW_RUNTIME_DB_ADMIN_FALLBACK"):
        return admin_config

    raise RuntimeDbConfigError(
        "Runtime DB access requires READONLY_DB_USER and READONLY_DB_PASSWORD. "
        "Set ALLOW_RUNTIME_DB_ADMIN_FALLBACK=1 only for local development."
    )


def build_runtime_db_engine() -> Engine:
    """Создает runtime engine с read-only учеткой и connection-level timeout."""

    db = get_runtime_db_config()
    timeout_ms = _resolve_statement_timeout_ms()
    db_url = (
        f"postgresql+psycopg2://{db.user}:{db.password}@{db.host}:{db.port}/{db.database}"
    )
    return create_engine(
        db_url,
        future=True,
        connect_args={"options": f"-c statement_timeout={timeout_ms}"},
    )


def _resolve_statement_timeout_ms() -> int:
    """Возвращает timeout SQL-стейтмента в миллисекундах для каждого соединения."""

    raw_timeout = os.getenv("SQL_STATEMENT_TIMEOUT_MS")
    if raw_timeout is None:
        return _DEFAULT_STATEMENT_TIMEOUT_MS

    try:
        parsed = int(raw_timeout)
    except ValueError as error:
        raise RuntimeDbConfigError(
            "Переменная SQL_STATEMENT_TIMEOUT_MS должна быть целым числом."
        ) from error

    if parsed <= 0:
        raise RuntimeDbConfigError(
            "Переменная SQL_STATEMENT_TIMEOUT_MS должна быть больше нуля."
        )
    return parsed


def _env_flag_enabled(name: str) -> bool:
    """Проверяет явный opt-in флаг для локальных dev-обходов."""

    return os.getenv(name, "").strip().lower() in {"1", "true", "yes", "on"}
