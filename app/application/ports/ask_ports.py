"""Порты use case обработки запроса `/api/ask`."""

from __future__ import annotations

from typing import Any, Protocol


class SqlGeneratorPort(Protocol):
    """Контракт генерации SQL из естественного языка."""

    def generate_sql(self, question: str) -> str:
        """Генерирует SQL-строку по вопросу пользователя."""

    def regenerate_sql(
        self,
        question: str,
        previous_output: str,
        error_message: str,
    ) -> str:
        """Делает repair-генерацию, если первичный ответ невалиден."""


class SqlExplainerPort(Protocol):
    """Контракт генерации Explain-текста по вопросу и SQL."""

    def explain(self, question: str, sql_text: str) -> str:
        """Возвращает объяснение SQL для пользователя."""


class QueryExecutorPort(Protocol):
    """Контракт безопасного исполнения SQL-запроса."""

    def execute(self, sql_text: str) -> dict[str, Any]:
        """Выполняет SQL и возвращает сериализуемый payload результата."""


class IntentClassifierPort(Protocol):
    """Контракт fallback-классификатора неоднозначных intent-запросов."""

    def classify(self, question: str) -> dict[str, Any] | str | None:
        """Возвращает payload уточнения или `None`."""

