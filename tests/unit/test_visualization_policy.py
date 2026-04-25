"""Unit-тесты доменной политики выбора визуализации."""

from __future__ import annotations

import unittest

from backend.app.domain.services.visualization_policy import build_visualization_spec


class TestVisualizationPolicy(unittest.TestCase):
    """Проверяет выбор типа графика по колонкам и данным."""

    def test_returns_line_for_temporal_metric_result(self) -> None:
        """Временной вопрос с датой и метрикой должен давать line chart."""

        result = build_visualization_spec(
            question="Покажи динамику выручки по дням за 7 дней",
            columns=["order_date", "revenue_local"],
            rows=[
                {"order_date": "2026-04-20T00:00:00", "revenue_local": 100.0},
                {"order_date": "2026-04-21T00:00:00", "revenue_local": 120.0},
            ],
        )

        self.assertEqual(result["type"], "line")
        self.assertEqual(result["x_field"], "order_date")
        self.assertEqual(result["y_field"], "revenue_local")

    def test_returns_table_only_when_no_chart_fields(self) -> None:
        """Если нет надежной пары X/Y, должна возвращаться только таблица."""

        result = build_visualization_spec(
            question="Покажи список статусов",
            columns=["city_id", "status_tender"],
            rows=[
                {"city_id": 1, "status_tender": "done"},
                {"city_id": 2, "status_tender": "decline"},
            ],
        )

        self.assertEqual(result["type"], "table_only")

    def test_returns_bar_for_numeric_low_cardinality_category(self) -> None:
        """Числовой разрез с малой кардинальностью подходит для bar chart."""

        result = build_visualization_spec(
            question="Покажи число отмененных заказов по часам",
            columns=["order_hour", "cancelled_orders"],
            rows=[
                {"order_hour": 8, "cancelled_orders": 12},
                {"order_hour": 9, "cancelled_orders": 7},
                {"order_hour": 10, "cancelled_orders": 15},
            ],
        )

        self.assertEqual(result["type"], "bar")
        self.assertEqual(result["x_field"], "order_hour")
        self.assertEqual(result["y_field"], "cancelled_orders")

    def test_keeps_metric_when_name_contains_id_inside_word(self) -> None:
        """Подстрока id внутри слова не должна отбрасывать числовую метрику."""

        result = build_visualization_spec(
            question="Сравни валидные заказы по статусам",
            columns=["status_order", "valid_orders"],
            rows=[
                {"status_order": "done", "valid_orders": 120},
                {"status_order": "cancelled", "valid_orders": 30},
            ],
        )

        self.assertEqual(result["type"], "bar")
        self.assertEqual(result["x_field"], "status_order")
        self.assertEqual(result["y_field"], "valid_orders")

    def test_returns_line_for_days_word_form(self) -> None:
        """Формулировка про дни должна выбирать line chart по временной оси."""

        result = build_visualization_spec(
            question="Какая выручка по дням за 7 дней",
            columns=["order_date", "revenue_local"],
            rows=[
                {"order_date": "2026-04-20", "revenue_local": 100.0},
                {"order_date": "2026-04-21", "revenue_local": 120.0},
            ],
        )

        self.assertEqual(result["type"], "line")
        self.assertEqual(result["x_field"], "order_date")
        self.assertEqual(result["y_field"], "revenue_local")


if __name__ == "__main__":
    unittest.main()
