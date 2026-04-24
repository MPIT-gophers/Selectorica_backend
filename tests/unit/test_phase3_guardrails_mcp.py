"""Unit-тесты для guardrails стоимости и MCP tool Phase 3."""

from __future__ import annotations

import unittest
from unittest.mock import MagicMock, patch

from backend.app.infrastructure.mcp.query_server import execute_safe_query
from backend.app.infrastructure.security.sql_guardrails import GuardrailError, check_query_cost


class TestQueryCostGuardrails(unittest.TestCase):
    """Проверяет ограничения стоимости SQL-запроса через EXPLAIN JSON."""

    @patch("backend.app.infrastructure.security.sql_guardrails.create_engine")
    def test_check_query_cost_success(self, create_engine_mock: MagicMock) -> None:
        """Функция должна извлекать Total Cost и возвращать его как float."""

        engine = MagicMock()
        connection = MagicMock()
        connection.execute.return_value.scalar.return_value = [
            {"Plan": {"Total Cost": 42.5}}
        ]
        engine.connect.return_value.__enter__.return_value = connection
        create_engine_mock.return_value = engine

        result = check_query_cost("SELECT 1", max_total_cost=100.0)

        self.assertEqual(result, 42.5)
        self.assertIn("EXPLAIN (FORMAT JSON)", str(connection.execute.call_args[0][0]))

    @patch("backend.app.infrastructure.security.sql_guardrails.create_engine")
    def test_check_query_cost_blocks_expensive_query(
        self, create_engine_mock: MagicMock
    ) -> None:
        """Слишком дорогой SQL должен блокироваться с доменным кодом."""

        engine = MagicMock()
        connection = MagicMock()
        connection.execute.return_value.scalar.return_value = [
            {"Plan": {"Total Cost": 999.0}}
        ]
        engine.connect.return_value.__enter__.return_value = connection
        create_engine_mock.return_value = engine

        with self.assertRaises(GuardrailError) as ctx:
            check_query_cost("SELECT 1", max_total_cost=100.0)
        self.assertEqual(ctx.exception.error_code, "SQL_COST_LIMIT_EXCEEDED")

    @patch("backend.app.infrastructure.security.sql_guardrails.create_engine")
    def test_check_query_cost_invalid_explain_payload(
        self, create_engine_mock: MagicMock
    ) -> None:
        """Некорректный payload EXPLAIN должен давать ошибку формата."""

        engine = MagicMock()
        connection = MagicMock()
        connection.execute.return_value.scalar.return_value = "not-json"
        engine.connect.return_value.__enter__.return_value = connection
        create_engine_mock.return_value = engine

        with self.assertRaises(GuardrailError) as ctx:
            check_query_cost("SELECT 1", max_total_cost=100.0)
        self.assertEqual(ctx.exception.error_code, "SQL_EXPLAIN_INVALID")


class TestSafeQueryTool(unittest.TestCase):
    """Проверяет MCP tool execute_safe_query в happy-path и fail-path."""

    @patch("backend.app.infrastructure.mcp.query_server.create_engine")
    @patch("backend.app.infrastructure.mcp.query_server.check_query_cost")
    @patch("backend.app.infrastructure.mcp.query_server.validate_ast")
    def test_execute_safe_query_success(
        self,
        validate_ast_mock: MagicMock,
        check_query_cost_mock: MagicMock,
        create_engine_mock: MagicMock,
    ) -> None:
        """Tool должен вернуть OK-ответ с данными при корректном SQL."""

        validate_ast_mock.return_value = "SELECT 1 AS value"
        check_query_cost_mock.return_value = 12.0

        result = MagicMock()
        row = MagicMock()
        row._mapping = {"value": 1}
        result.fetchmany.return_value = [row]
        result.keys.return_value = ["value"]

        connection = MagicMock()
        connection.execute.return_value = result

        engine = MagicMock()
        engine.connect.return_value.__enter__.return_value = connection
        create_engine_mock.return_value = engine

        payload = execute_safe_query("select 1")
        self.assertEqual(payload["status"], "ok")
        self.assertEqual(payload["normalized_sql"], "SELECT 1 AS value")
        self.assertEqual(payload["estimated_total_cost"], 12.0)
        self.assertEqual(payload["row_count"], 1)
        self.assertEqual(payload["rows"][0]["value"], 1)

    @patch("backend.app.infrastructure.mcp.query_server.validate_ast")
    def test_execute_safe_query_returns_guardrail_error(
        self, validate_ast_mock: MagicMock
    ) -> None:
        """Tool должен возвращать JSON-friendly ошибку при блокировке guardrails."""

        validate_ast_mock.side_effect = GuardrailError(
            "SQL_MUTATION_BLOCKED",
            "blocked",
        )
        payload = execute_safe_query("DROP TABLE orders")
        self.assertEqual(payload["status"], "error")
        self.assertEqual(payload["error_code"], "SQL_MUTATION_BLOCKED")


if __name__ == "__main__":
    unittest.main()
