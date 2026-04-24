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


if __name__ == "__main__":
    unittest.main()

