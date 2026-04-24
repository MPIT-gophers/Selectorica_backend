"""Unit-тесты сервиса KPI-снимка пилота."""

from __future__ import annotations

import unittest
from dataclasses import dataclass

from backend.app.application.services.pilot_kpi_service import PilotKpiService
from backend.app.infrastructure.history.sqlite_report_history_repo import ReportRecord


@dataclass
class StubReportHistoryRepo:
    """Заглушка репозитория истории, возвращающая заранее подготовленные записи."""

    reports: list[ReportRecord]
    last_limit: int | None = None

    def list_reports(self, limit: int = 50) -> list[ReportRecord]:
        """Сохраняет лимит запроса и возвращает фиксированный набор записей."""

        self.last_limit = limit
        return self.reports


class TestPilotKpiService(unittest.TestCase):
    """Проверяет агрегацию KPI из persisted report history."""

    def test_snapshot_aggregates_history_records(self) -> None:
        """Сервис должен считать KPI только из сохраненных отчетов."""

        repo = StubReportHistoryRepo(
            reports=[
                ReportRecord(
                    question="Какой revenue за вчера?",
                    sql_text="SELECT 1",
                    asked_at="2026-04-22T10:00:00+00:00",
                    refinement_trace=[],
                    explain_text="Пользовательский запрос выполнен.",
                    confidence={"score": 0.8, "level": "high"},
                    recommended_actions=["Check revenue trend"],
                ),
                ReportRecord(
                    question="Покажи продажи по регионам",
                    sql_text="SELECT 2",
                    asked_at="2026-04-23T11:00:00+00:00",
                    refinement_trace=[
                        {
                            "question": "Что считать продажами?",
                            "selected_label": "Выручка",
                            "selected_value": "Покажи выручку",
                        }
                    ],
                    explain_text="",
                    confidence={"score": 0.6, "level": "medium"},
                    recommended_actions=[],
                ),
                ReportRecord(
                    question="Какой статус заказов?",
                    sql_text="SELECT 3",
                    asked_at="2026-04-24T12:00:00+00:00",
                    refinement_trace=[],
                    explain_text="Расшифровка результата.",
                    confidence={"score": 1.5},
                    recommended_actions=["Investigate decline"],
                ),
            ]
        )

        snapshot = PilotKpiService(repo).get_snapshot()

        self.assertEqual(repo.last_limit, 1000)
        self.assertEqual(snapshot.report_count, 3)
        self.assertAlmostEqual(snapshot.avg_confidence_score, 0.7)
        self.assertAlmostEqual(snapshot.decision_log_complete_rate, 2 / 3)
        self.assertAlmostEqual(snapshot.clarification_rate, 1 / 3)
        self.assertEqual(snapshot.latest_report_at, "2026-04-24T12:00:00+00:00")
        self.assertEqual(snapshot.sample_question, "Какой статус заказов?")
        self.assertTrue(snapshot.generated_at.endswith("+00:00"))

    def test_snapshot_is_empty_when_history_is_missing(self) -> None:
        """Пустая история должна возвращать безопасные нулевые значения."""

        snapshot = PilotKpiService(StubReportHistoryRepo(reports=[])).get_snapshot()

        self.assertEqual(snapshot.report_count, 0)
        self.assertEqual(snapshot.avg_confidence_score, 0.0)
        self.assertEqual(snapshot.decision_log_complete_rate, 0.0)
        self.assertEqual(snapshot.clarification_rate, 0.0)
        self.assertEqual(snapshot.latest_report_at, "")
        self.assertEqual(snapshot.sample_question, "")


if __name__ == "__main__":
    unittest.main()
