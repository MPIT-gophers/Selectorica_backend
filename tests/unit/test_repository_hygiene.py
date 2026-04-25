"""Unit-тесты репозиторной гигиены для публичной проверки проекта."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import pytest

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


def test_runtime_db_config_rejects_admin_fallback_by_default() -> None:
    """Runtime не должен молча переходить на admin-пользователя без readonly env."""

    with patch.dict(
        "os.environ",
        {
            "PGHOST": "127.0.0.1",
            "PGPORT": "15432",
            "PGDATABASE": "drivee_nl2sql",
            "PGUSER": "postgres",
            "PGPASSWORD": "postgres",
        },
        clear=True,
    ), patch("backend.app.infrastructure.config.env_config.load_repo_env", lambda: None):
        with pytest.raises(RuntimeError, match="READONLY_DB_USER"):
            get_runtime_db_config()


def test_runtime_db_config_allows_admin_fallback_only_with_dev_flag() -> None:
    """Явный dev-флаг оставляет локальный аварийный обход видимым в конфиге."""

    with patch.dict(
        "os.environ",
        {
            "PGHOST": "127.0.0.1",
            "PGPORT": "15432",
            "PGDATABASE": "drivee_nl2sql",
            "PGUSER": "postgres",
            "PGPASSWORD": "postgres",
            "ALLOW_RUNTIME_DB_ADMIN_FALLBACK": "1",
        },
        clear=True,
    ), patch("backend.app.infrastructure.config.env_config.load_repo_env", lambda: None):
        config = get_runtime_db_config()

    assert config.user == "postgres"
    assert config.password == "postgres"
