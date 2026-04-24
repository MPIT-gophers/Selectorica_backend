"""Unit-тесты FastAPI endpoint `/api/pilot/kpi`."""

from __future__ import annotations

import unittest
from dataclasses import dataclass

from fastapi.testclient import TestClient

from backend.app.interfaces.http.server import create_app, get_pilot_kpi_service
from backend.app.application.services.pilot_kpi_service import PilotKpiSnapshot


@dataclass
class StubPilotKpiService:
    """Заглушка сервиса KPI для изолированного теста HTTP-слоя."""

    snapshot: PilotKpiSnapshot

    def get_snapshot(self) -> PilotKpiSnapshot:
        """Возвращает заранее подготовленный KPI-снимок."""

        return self.snapshot


class TestPilotKpiApi(unittest.TestCase):
    """Проверяет HTTP-контракт read-only endpoint `/api/pilot/kpi`."""

    def test_pilot_kpi_endpoint_success(self) -> None:
        """Endpoint должен вернуть KPI-снимок с ожидаемыми полями."""

        app = create_app()
        app.dependency_overrides[get_pilot_kpi_service] = lambda: StubPilotKpiService(
            snapshot=PilotKpiSnapshot(
                generated_at="2026-04-24T12:30:00+00:00",
                report_count=5,
                avg_confidence_score=0.65,
                decision_log_complete_rate=0.4,
                clarification_rate=0.2,
                latest_report_at="2026-04-24T12:00:00+00:00",
                sample_question="Какой revenue за вчера?",
            )
        )
        client = TestClient(app)

        response = client.get("/api/pilot/kpi")

        self.assertEqual(response.status_code, 200)
        body = response.json()
        self.assertEqual(body["generated_at"], "2026-04-24T12:30:00+00:00")
        self.assertEqual(body["report_count"], 5)
        self.assertEqual(body["avg_confidence_score"], 0.65)
        self.assertEqual(body["decision_log_complete_rate"], 0.4)
        self.assertEqual(body["clarification_rate"], 0.2)
        self.assertEqual(body["latest_report_at"], "2026-04-24T12:00:00+00:00")
        self.assertEqual(body["sample_question"], "Какой revenue за вчера?")


if __name__ == "__main__":
    unittest.main()
