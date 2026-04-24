"""Unit-тесты репозиторной гигиены для публичной проверки проекта."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

from backend.app.infrastructure.config.env_config import get_runtime_db_config


BACKEND_ROOT = Path(__file__).resolve().parents[2]


def test_init_sql_does_not_commit_readonly_password() -> None:
    """DDL-схема не должна хранить пароль readonly-пользователя в репозитории."""

    init_sql = (BACKEND_ROOT / "init.sql").read_text(encoding="utf-8")

    assert "readonly_password" not in init_sql
    assert "CREATE USER readonly_user WITH PASSWORD" not in init_sql


def test_runtime_db_config_prefers_readonly_credentials() -> None:
    """Runtime-запросы должны использовать read-only DB-пользователя поверх admin PGUSER."""

    with patch.dict(
        "os.environ",
        {
            "PGHOST": "127.0.0.1",
            "PGPORT": "15432",
            "PGDATABASE": "drivee_nl2sql",
            "PGUSER": "postgres",
            "PGPASSWORD": "postgres",
            "READONLY_DB_USER": "readonly_user",
            "READONLY_DB_PASSWORD": "readonly-secret",
        },
        clear=True,
    ):
        config = get_runtime_db_config()

    assert config.user == "readonly_user"
    assert config.password == "readonly-secret"
