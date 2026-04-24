"""FastMCP сервер безопасного выполнения read-only SQL-запросов."""

from __future__ import annotations

import os
from datetime import date, datetime, time
from decimal import Decimal
from typing import Any

from fastmcp import FastMCP
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError

from backend.app.infrastructure.config.env_config import get_runtime_db_config
from backend.app.infrastructure.security.sql_guardrails import (
    GuardrailError,
    check_query_cost,
    validate_ast,
)

_DEFAULT_MAX_ROWS = 200

mcp = FastMCP("Drivee NL2SQL Guarded Query Server")


@mcp.tool
def execute_safe_query(sql_string: str) -> dict[str, Any]:
    """Выполняет read-only SQL после AST-валидации и проверки стоимости."""

    try:
        normalized_sql = validate_ast(sql_string)
        total_cost = check_query_cost(normalized_sql)
        payload = _execute_query(normalized_sql)
        return {
            "status": "ok",
            "normalized_sql": normalized_sql,
            "estimated_total_cost": total_cost,
            **payload,
        }
    except GuardrailError as error:
        return error.to_dict()
    except SQLAlchemyError as error:
        return {
            "status": "error",
            "error_code": "SQL_EXECUTION_FAILED",
            "message": f"Ошибка выполнения SQL: {error}",
        }
    except Exception as error:
        return {
            "status": "error",
            "error_code": "UNEXPECTED_ERROR",
            "message": repr(error),
        }


def _execute_query(sql_string: str) -> dict[str, Any]:
    """Выполняет SQL в БД и возвращает сериализуемые строки результата."""

    row_limit = _resolve_row_limit()
    engine = _build_engine()
    try:
        with engine.connect() as connection:
            result = connection.execute(text(sql_string))
            rows = result.fetchmany(row_limit)
            columns = list(result.keys())
    finally:
        engine.dispose()

    serialized_rows = [_serialize_row(row._mapping) for row in rows]
    return {
        "columns": columns,
        "rows": serialized_rows,
        "row_count": len(serialized_rows),
        "row_limit": row_limit,
    }


def _build_engine() -> Engine:
    """Создает SQLAlchemy engine для PostgreSQL из локального env-конфига."""

    db = get_runtime_db_config()
    db_url = (
        f"postgresql+psycopg2://{db.user}:{db.password}@{db.host}:{db.port}/{db.database}"
    )
    return create_engine(db_url, future=True)


def _resolve_row_limit() -> int:
    """Возвращает лимит строк результата из ENV или дефолтное значение."""

    raw_limit = os.getenv("SQL_RESULT_MAX_ROWS")
    if raw_limit is None:
        return _DEFAULT_MAX_ROWS

    try:
        parsed = int(raw_limit)
    except ValueError as error:
        raise GuardrailError(
            "SQL_RESULT_LIMIT_INVALID",
            "Переменная SQL_RESULT_MAX_ROWS должна быть целым числом.",
        ) from error

    if parsed <= 0:
        raise GuardrailError(
            "SQL_RESULT_LIMIT_INVALID",
            "Переменная SQL_RESULT_MAX_ROWS должна быть больше нуля.",
        )
    return parsed


def _serialize_row(row_mapping: Any) -> dict[str, Any]:
    """Преобразует строку SQLAlchemy в JSON-friendly словарь."""

    return {key: _serialize_value(value) for key, value in dict(row_mapping).items()}


def _serialize_value(value: Any) -> Any:
    """Нормализует типы значений БД для JSON-сериализации."""

    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, (datetime, date, time)):
        return value.isoformat()
    return value


if __name__ == "__main__":
    mcp.run()
