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

    def test_missing_period_needs_period_clarification(self) -> None:
        """Аналитический запрос без периода должен уточнять временной диапазон."""

        result = self.resolver.resolve("Покажи выручку по городам")

        self.assertTrue(result.needs_clarification)
        self.assertEqual(result.clarification.kind, "period")
        self.assertEqual(result.clarification.question, "За какой период показать данные?")
        self.assertEqual(
            [option.label for option in result.clarification.options],
            ["7 дней", "30 дней", "Текущий месяц"],
        )

    def test_metric_ambiguity_has_priority_over_missing_period(self) -> None:
        """Если не хватает и метрики, и периода, первым уточняется смысл метрики."""

        result = self.resolver.resolve("Покажи продажи по регионам")

        self.assertTrue(result.needs_clarification)
        self.assertEqual(result.clarification.kind, "metric")

    def test_clear_question_does_not_need_clarification(self) -> None:
        """Запрос с метрикой и периодом должен идти в обычный SQL-flow."""

        result = self.resolver.resolve("Покажи выручку по городам за текущий месяц")

        self.assertFalse(result.needs_clarification)
        self.assertIsNone(result.clarification)


if __name__ == "__main__":
    unittest.main()
