"""Guardrails для AST-валидации SQL перед выполнением."""

from __future__ import annotations

import json
import os
from typing import Final, Type

import sqlglot
from sqlglot import exp
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError

from backend.app.infrastructure.config.env_config import get_runtime_db_config


_POSTGRES_DIALECT: Final[str] = "postgres"
_DEFAULT_MAX_QUERY_COST: Final[float] = 100_000.0
_MUTATION_EXPRESSIONS: Final[tuple[Type[exp.Expression], ...]] = (
    exp.Insert,
    exp.Update,
    exp.Delete,
    exp.Drop,
)


class GuardrailError(Exception):
    """Доменная ошибка валидации SQL с кодом для JSON-friendly ответов."""

    def __init__(self, error_code: str, message: str):
        super().__init__(message)
        self.error_code = error_code
        self.message = message

    def to_dict(self) -> dict[str, str]:
        """Возвращает ошибку в виде словаря для сериализации в JSON."""

        return {
            "status": "error",
            "error_code": self.error_code,
            "message": self.message,
        }


def validate_ast(sql_string: str) -> str:
    """Проверяет SQL через AST и разрешает только read-only SELECT/CTE."""

    statements = _parse_statements(sql_string)
    if len(statements) != 1:
        raise GuardrailError(
            "SQL_MULTI_STATEMENT_BLOCKED",
            "Разрешен только один SQL-стейтмент за запрос.",
        )

    statement = statements[0]
    if isinstance(statement, _MUTATION_EXPRESSIONS):
        raise GuardrailError(
            "SQL_MUTATION_BLOCKED",
            "Обнаружена мутационная операция (INSERT/UPDATE/DELETE/DROP).",
        )

    if not isinstance(statement, exp.Select):
        raise GuardrailError(
            "SQL_NOT_READ_ONLY",
            "Разрешены только read-only SELECT/CTE запросы.",
        )

    if statement.find(*_MUTATION_EXPRESSIONS):
        raise GuardrailError(
            "SQL_MUTATION_BLOCKED",
            "Обнаружена мутационная операция (INSERT/UPDATE/DELETE/DROP).",
        )

    return statement.sql(dialect=_POSTGRES_DIALECT)


def check_query_cost(sql_string: str, max_total_cost: float | None = None) -> float:
    """Проверяет стоимость read-only запроса через `EXPLAIN (FORMAT JSON)`."""

    normalized_sql = validate_ast(sql_string)
    explain_sql = f"EXPLAIN (FORMAT JSON) {normalized_sql}"
    limit = max_total_cost if max_total_cost is not None else _resolve_max_total_cost()

    engine = _build_engine()
    try:
        with engine.connect() as connection:
            explain_payload = connection.execute(text(explain_sql)).scalar()
    except SQLAlchemyError as error:
        raise GuardrailError(
            "SQL_EXPLAIN_FAILED",
            f"Не удалось выполнить EXPLAIN: {error}",
        ) from error
    finally:
        engine.dispose()

    total_cost = _extract_total_cost(explain_payload)
    if total_cost > limit:
        raise GuardrailError(
            "SQL_COST_LIMIT_EXCEEDED",
            f"Стоимость запроса {total_cost:.2f} превышает лимит {limit:.2f}.",
        )

    return total_cost


def _parse_statements(sql_string: str) -> list[exp.Expression]:
    """Парсит SQL-строку в список AST-выражений с postgres-диалектом."""

    try:
        return sqlglot.parse(sql_string, read=_POSTGRES_DIALECT)
    except sqlglot.errors.ParseError as error:
        raise GuardrailError(
            "SQL_PARSE_ERROR",
            f"Некорректный SQL: {error}",
        ) from error


def _build_engine() -> Engine:
    """Создает SQLAlchemy engine для PostgreSQL из локального env-конфига."""

    db = get_runtime_db_config()
    db_url = (
        f"postgresql+psycopg2://{db.user}:{db.password}@{db.host}:{db.port}/{db.database}"
    )
    return create_engine(db_url, future=True)


def _resolve_max_total_cost() -> float:
    """Возвращает лимит стоимости запроса из ENV или дефолтное значение."""

    raw_limit = os.getenv("SQL_MAX_TOTAL_COST")
    if raw_limit is None:
        return _DEFAULT_MAX_QUERY_COST

    try:
        parsed = float(raw_limit)
    except ValueError as error:
        raise GuardrailError(
            "SQL_COST_CONFIG_INVALID",
            "Переменная SQL_MAX_TOTAL_COST должна быть числом.",
        ) from error

    if parsed <= 0:
        raise GuardrailError(
            "SQL_COST_CONFIG_INVALID",
            "Переменная SQL_MAX_TOTAL_COST должна быть больше нуля.",
        )
    return parsed


def _extract_total_cost(explain_payload: object) -> float:
    """Извлекает `Total Cost` из результата `EXPLAIN (FORMAT JSON)`."""

    explain_json = _normalize_explain_payload(explain_payload)

    if isinstance(explain_json, dict):
        plan = explain_json.get("Plan")
    elif isinstance(explain_json, list) and explain_json:
        first = explain_json[0]
        plan = first.get("Plan") if isinstance(first, dict) else None
    else:
        plan = None

    if not isinstance(plan, dict) or "Total Cost" not in plan:
        raise GuardrailError(
            "SQL_EXPLAIN_INVALID",
            "EXPLAIN не вернул поле Plan.Total Cost.",
        )

    try:
        return float(plan["Total Cost"])
    except (TypeError, ValueError) as error:
        raise GuardrailError(
            "SQL_EXPLAIN_INVALID",
            "Поле Plan.Total Cost имеет некорректный формат.",
        ) from error


def _normalize_explain_payload(explain_payload: object) -> object:
    """Нормализует payload EXPLAIN к Python-структуре dict/list."""

    if isinstance(explain_payload, str):
        try:
            return json.loads(explain_payload)
        except json.JSONDecodeError as error:
            raise GuardrailError(
                "SQL_EXPLAIN_INVALID",
                "EXPLAIN вернул невалидный JSON.",
            ) from error

    if isinstance(explain_payload, (dict, list)):
        return explain_payload

    raise GuardrailError(
        "SQL_EXPLAIN_INVALID",
        "EXPLAIN вернул неподдерживаемый формат результата.",
    )
