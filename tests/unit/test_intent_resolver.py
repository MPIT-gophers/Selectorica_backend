"""Unit-тесты deterministic intent clarification resolver."""

from __future__ import annotations

import unittest

from backend.app.domain.services.intent_resolution_policy import IntentResolver


class TestIntentResolver(unittest.TestCase):
    """Проверяет правила уточнения аналитических запросов."""

    def setUp(self) -> None:
        """Создает resolver для каждого теста без внешних зависимостей."""

        self.resolver = IntentResolver()

    def test_sales_without_metric_needs_metric_clarification(self) -> None:
        """Слово `продажи` без явной метрики должно уточнять бизнес-смысл."""

        result = self.resolver.resolve("Покажи продажи по регионам за текущий месяц")

        self.assertTrue(result.needs_clarification)
        self.assertEqual(result.clarification.kind, "metric")
        self.assertEqual(result.clarification.question, "Что считать продажами?")
        self.assertEqual(
            [option.label for option in result.clarification.options],
            ["Выручка", "Поездки", "Средний чек"],
        )

    def test_missing_period_uses_safe_default_without_clarification(self) -> None:
        """Аналитический запрос без периода должен использовать безопасный дефолт."""

        result = self.resolver.resolve("Покажи выручку по городам")

        self.assertFalse(result.needs_clarification)
        self.assertIsNone(result.clarification)
        self.assertEqual(result.resolved_params["date_range"]["value"], "last_7_days")
        self.assertEqual(result.resolved_params["date_range"]["source"], "default")
        self.assertIn("за последние 7 дней", result.effective_question)
        self.assertIn("последние 7 дней", result.assumptions[0])

    def test_missing_period_uses_context_before_default(self) -> None:
        """Если период есть в контексте, resolver должен использовать его вместо дефолта."""

        result = self.resolver.resolve(
            "Покажи отмены по городам",
            context={
                "previous_params": {
                    "date_range": {
                        "value": "last_30_days",
                        "label": "последние 30 дней",
                    }
                }
            },
        )

        self.assertFalse(result.needs_clarification)
        self.assertEqual(result.resolved_params["date_range"]["source"], "context")
        self.assertIn("за последние 30 дней", result.effective_question)
        self.assertIn("из предыдущего запроса", result.assumptions[0])

    def test_metric_ambiguity_has_priority_over_missing_period(self) -> None:
        """Если не хватает и метрики, и периода, первым уточняется смысл метрики."""

        result = self.resolver.resolve("Покажи продажи по регионам")

        self.assertTrue(result.needs_clarification)
        self.assertEqual(result.clarification.kind, "metric")
        self.assertEqual(result.clarification.param_name, "metric")
        self.assertEqual(result.clarification.reason_code, "METRIC_AMBIGUOUS")
        self.assertTrue(result.clarification.allow_free_input)

    def test_clear_question_does_not_need_clarification(self) -> None:
        """Запрос с метрикой и периодом должен идти в обычный SQL-flow."""

        result = self.resolver.resolve("Покажи выручку по городам за текущий месяц")

        self.assertFalse(result.needs_clarification)
        self.assertIsNone(result.clarification)


if __name__ == "__main__":
    unittest.main()
