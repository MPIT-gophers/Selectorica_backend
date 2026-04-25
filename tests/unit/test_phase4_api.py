"""Unit-тесты FastAPI эндпоинта `/api/ask` для фазы 4."""

from __future__ import annotations

import unittest
from dataclasses import dataclass

from fastapi.testclient import TestClient

from backend.app.interfaces.http.server import create_app, get_ask_service
from backend.app.application.services.ask_service import AskResult, AskServiceError


@dataclass
class StubAskService:
    """Заглушка сервиса `/api/ask` для изолированного теста API-слоя."""

    should_fail: bool = False
    should_clarify: bool = False
    should_raise_unexpected: bool = False
    last_refinement_trace: list[dict[str, str]] | None = None
    last_context: dict[str, object] | None = None

    def ask(
        self,
        question: str,
        refinement_trace: list[dict[str, str]] | None = None,
        context: dict[str, object] | None = None,
    ) -> AskResult:
        """Возвращает предсказуемый результат или доменную ошибку."""

        self.last_refinement_trace = refinement_trace
        self.last_context = context

        if self.should_raise_unexpected:
            raise RuntimeError("internal connection password=secret")

        if self.should_clarify:
            return AskResult(
                question=question,
                status="clarification_needed",
                confidence={
                    "score": 0.35,
                    "level": "low",
                    "reason": "Не хватает периода для безопасной генерации SQL.",
                },
                clarification={
                    "kind": "period",
                    "param_name": "date_range",
                    "reason_code": "DATE_RANGE_REQUIRED",
                    "required": True,
                    "reason": "В запросе не указан период.",
                    "question": "За какой период показать данные?",
                    "allow_free_input": True,
                    "free_input_placeholder": "Например: последние 14 дней",
                    "default_value": "last_7_days",
                    "default_label": "последние 7 дней",
                    "options": [
                        {
                            "label": "7 дней",
                            "value": f"{question} за последние 7 дней",
                            "description": "Подходит для оперативного анализа.",
                        }
                    ],
                },
            )

        if self.should_fail:
            raise AskServiceError("SQL_COST_LIMIT_EXCEEDED", "Слишком дорогой запрос.")

        return AskResult(
            question=question,
            generated_sql="SELECT 1 AS value",
            explain="Запрос вернет константу 1 в колонке value.",
            confidence={
                "score": 0.82,
                "level": "high",
                "reason": "Метрика и период определены явно.",
            },
            estimated_total_cost=10.0,
            columns=["value"],
            rows=[{"value": 1}],
            row_count=1,
            report_saved=True,
            report_saved_at="2026-04-23T00:00:00+00:00",
            visualization={
                "type": "bar",
                "x_field": "name",
                "y_field": "value",
                "reason": "Тестовый график.",
                "confidence": 1.0,
            },
            assumptions=["Период не указан, использую последние 7 дней."],
            resolved_params={
                "date_range": {
                    "value": "last_7_days",
                    "label": "последние 7 дней",
                    "source": "default",
                }
            },
            decision_events=[
                {
                    "type": "default_applied",
                    "param_name": "date_range",
                    "reason_code": "DATE_RANGE_DEFAULTED",
                }
            ],
        )


class TestPhase4Api(unittest.TestCase):
    """Проверяет контракты FastAPI endpoint `/api/ask`."""

    def test_ask_endpoint_success(self) -> None:
        """Эндпоинт должен вернуть status=ok и ожидаемые поля payload."""

        app = create_app()
        app.dependency_overrides[get_ask_service] = lambda: StubAskService()
        client = TestClient(app)

        response = client.post("/api/ask", json={"question": "Покажи тестовые данные"})

        self.assertEqual(response.status_code, 200)
        body = response.json()
        self.assertEqual(body["status"], "ok")
        self.assertEqual(body["generated_sql"], "SELECT 1 AS value")
        self.assertEqual(body["row_count"], 1)
        self.assertEqual(body["confidence"]["level"], "high")
        self.assertEqual(body["visualization"]["type"], "bar")
        self.assertEqual(body["resolved_params"]["date_range"]["source"], "default")
        self.assertIn("последние 7 дней", body["assumptions"][0])
        self.assertEqual(body["decision_events"][0]["reason_code"], "DATE_RANGE_DEFAULTED")

    def test_ask_endpoint_domain_error(self) -> None:
        """Доменная ошибка сервиса должна маппиться в HTTP 400."""

        app = create_app()
        app.dependency_overrides[get_ask_service] = lambda: StubAskService(should_fail=True)
        client = TestClient(app)

        response = client.post("/api/ask", json={"question": "Очень тяжелый вопрос"})

        self.assertEqual(response.status_code, 400)
        body = response.json()
        self.assertEqual(body["detail"]["error_code"], "SQL_COST_LIMIT_EXCEEDED")

    def test_ask_endpoint_unexpected_error_hides_internal_details(self) -> None:
        """Непредвиденная ошибка API не должна отдавать repr внутреннего исключения."""

        app = create_app()
        app.dependency_overrides[get_ask_service] = lambda: StubAskService(
            should_raise_unexpected=True
        )
        client = TestClient(app)

        response = client.post("/api/ask", json={"question": "Покажи тестовые данные"})

        self.assertEqual(response.status_code, 500)
        body = response.json()
        self.assertEqual(body["detail"]["error_code"], "UNEXPECTED_ERROR")
        self.assertIn("Непредвиденная ошибка", body["detail"]["message"])
        self.assertNotIn("password=secret", body["detail"]["message"])

    def test_ask_endpoint_requires_question_field(self) -> None:
        """Отсутствие `question` должно приводить к валидационной ошибке FastAPI."""

        app = create_app()
        app.dependency_overrides[get_ask_service] = lambda: StubAskService()
        client = TestClient(app)

        response = client.post("/api/ask", json={})

        self.assertEqual(response.status_code, 422)

    def test_ask_endpoint_clarification_needed(self) -> None:
        """Уточнение intent должно возвращаться как HTTP 200, а не ошибка."""

        app = create_app()
        app.dependency_overrides[get_ask_service] = lambda: StubAskService(should_clarify=True)
        client = TestClient(app)

        response = client.post("/api/ask", json={"question": "Покажи выручку"})

        self.assertEqual(response.status_code, 200)
        body = response.json()
        self.assertEqual(body["status"], "clarification_needed")
        self.assertEqual(body["confidence"]["level"], "low")
        self.assertEqual(body["clarification"]["kind"], "period")
        self.assertEqual(body["clarification"]["param_name"], "date_range")
        self.assertEqual(body["clarification"]["reason_code"], "DATE_RANGE_REQUIRED")
        self.assertTrue(body["clarification"]["allow_free_input"])
        self.assertEqual(body["clarification"]["default_value"], "last_7_days")
        self.assertEqual(body["clarification"]["options"][0]["label"], "7 дней")

    def test_ask_endpoint_passes_refinement_trace(self) -> None:
        """API должен передавать refinement trace в сервисный слой."""

        stub = StubAskService()
        app = create_app()
        app.dependency_overrides[get_ask_service] = lambda: stub
        client = TestClient(app)

        response = client.post(
            "/api/ask",
            json={
                "question": "Покажи выручку",
                "refinement_trace": [
                    {
                        "question": "Что считать продажами?",
                        "selected_label": "Выручка",
                        "selected_value": "Покажи выручку",
                    }
                ],
            },
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(stub.last_refinement_trace[0]["selected_label"], "Выручка")

    def test_ask_endpoint_passes_analysis_context(self) -> None:
        """API должен передавать контекст периода и сценария в сервисный слой."""

        stub = StubAskService()
        app = create_app()
        app.dependency_overrides[get_ask_service] = lambda: stub
        client = TestClient(app)

        response = client.post(
            "/api/ask",
            json={
                "question": "Покажи выручку",
                "context": {
                    "previous_params": {
                        "date_range": {
                            "value": "last_30_days",
                            "label": "последние 30 дней",
                        }
                    },
                    "scenario_id": "FIN-01",
                },
            },
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            stub.last_context["previous_params"]["date_range"]["value"],
            "last_30_days",
        )


if __name__ == "__main__":
    unittest.main()
