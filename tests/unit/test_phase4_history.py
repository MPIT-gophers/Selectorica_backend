"""Unit tests for phase 4 SQLite report history."""

from __future__ import annotations

import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
TMP_ROOT = REPO_ROOT / ".tmp_test"

from backend.app.infrastructure.history.sqlite_report_history_repo import (
    ReportHistoryRepository,
    ReportRecord,
)


class TestReportHistoryRepository(unittest.TestCase):
    """Checks report history save/list behavior."""

    def test_save_and_list_reports(self) -> None:
        """Repository should save records and return them in reverse order."""

        TMP_ROOT.mkdir(exist_ok=True)
        db_path = TMP_ROOT / "phase4_reports_test.sqlite3"
        if db_path.exists():
            db_path.unlink()

        repo = ReportHistoryRepository(db_path)
        first = ReportRecord(
            question="Сколько завершенных поездок?",
            sql_text="SELECT COUNT(*) FROM orders WHERE status = 'completed'",
            asked_at="2026-04-23T00:00:00+00:00",
            refinement_trace=[],
            explain_text="completed trips",
            confidence={"score": 0.9, "level": "high", "reason": "test"},
            recommended_actions=["Ops: check shift allocation."],
        )
        second = ReportRecord(
            question="Сколько отмен?",
            sql_text="SELECT COUNT(*) FROM orders WHERE status = 'cancelled'",
            asked_at="2026-04-23T01:00:00+00:00",
            refinement_trace=[
                {
                    "question": "Что считать отменами?",
                    "selected_label": "Отмененные заказы",
                    "selected_value": "Покажи отмененные заказы за текущий месяц",
                }
            ],
            explain_text="cancellations by period",
            confidence={"score": 0.42, "level": "low", "reason": "test-low"},
            recommended_actions=["Finance: review cancellation loss segments."],
        )

        repo.save_report(first)
        repo.save_report(second)
        rows = repo.list_reports(limit=10)

        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0].question, second.question)
        self.assertEqual(rows[0].refinement_trace[0]["selected_label"], "Отмененные заказы")
        self.assertEqual(rows[0].explain_text, "cancellations by period")
        self.assertEqual(rows[0].confidence["level"], "low")
        self.assertIn("Finance", rows[0].recommended_actions[0])
        self.assertEqual(rows[1].question, first.question)
        self.assertEqual(rows[1].confidence["level"], "high")


if __name__ == "__main__":
    unittest.main()
