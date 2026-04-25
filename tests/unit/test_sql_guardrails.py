"""Unit-тесты для AST-валидации SQL guardrails."""

from __future__ import annotations

import unittest

from backend.app.infrastructure.security.sql_guardrails import GuardrailError, validate_ast


class TestSQLGuardrails(unittest.TestCase):
    """Проверяет правила read-only AST-валидации SQL."""

    def test_validate_ast_accepts_simple_select(self) -> None:
        """Простой SELECT должен проходить и нормализоваться."""

        result = validate_ast("select id, total_amount from orders where total_amount > 0")
        self.assertEqual(
            result,
            "SELECT id, total_amount FROM orders WHERE total_amount > 0",
        )

    def test_validate_ast_accepts_cte_select(self) -> None:
        """CTE с финальным SELECT должен считаться read-only."""

        result = validate_ast(
            "with recent as (select id from orders) select id from recent"
        )
        self.assertEqual(result, "WITH recent AS (SELECT id FROM orders) SELECT id FROM recent")

    def test_validate_ast_blocks_mutation_statements(self) -> None:
        """Мутационные операции должны блокироваться с доменным кодом."""

        mutation_queries = (
            "INSERT INTO orders (id) VALUES (1)",
            "UPDATE orders SET status = 'ok'",
            "DELETE FROM orders",
            "DROP TABLE orders",
            "ALTER TABLE orders ADD COLUMN unsafe_flag INT",
            "TRUNCATE orders",
            "GRANT SELECT ON orders TO public",
        )

        for query in mutation_queries:
            with self.subTest(query=query):
                with self.assertRaises(GuardrailError) as ctx:
                    validate_ast(query)
                self.assertEqual(ctx.exception.error_code, "SQL_MUTATION_BLOCKED")

    def test_validate_ast_blocks_multi_statement(self) -> None:
        """Несколько SQL-выражений в одной строке должны блокироваться."""

        with self.assertRaises(GuardrailError) as ctx:
            validate_ast("SELECT 1; DROP TABLE orders")
        self.assertEqual(ctx.exception.error_code, "SQL_MULTI_STATEMENT_BLOCKED")

    def test_validate_ast_raises_parse_error_for_invalid_sql(self) -> None:
        """Синтаксически битый SQL должен отдавать parse-ошибку guardrails."""

        with self.assertRaises(GuardrailError) as ctx:
            validate_ast("SELECT FROM")
        self.assertEqual(ctx.exception.error_code, "SQL_PARSE_ERROR")

    def test_validate_ast_blocks_sleep_function(self) -> None:
        """Resource-abuse функции должны блокироваться до EXPLAIN/исполнения."""

        with self.assertRaises(GuardrailError) as ctx:
            validate_ast("SELECT pg_sleep(20)")
        self.assertEqual(ctx.exception.error_code, "SQL_FUNCTION_BLOCKED")


if __name__ == "__main__":
    unittest.main()
