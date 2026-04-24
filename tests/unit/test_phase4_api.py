"""Unit-тесты FastAPI эндпоинта `/api/ask` для фазы 4."""

from __future__ import annotations

import unittest
from dataclasses import dataclass

from fastapi.testclient import TestClient

from backend.scripts.api_server import create_app, get_ask_service
from backend.app.application.services.ask_service import AskResult, AskServiceError


@dataclass
class StubAskService:
    """Заглушка сервиса `/api/ask` для изолированного теста API-слоя."""

    should_fail: bool = False
    should_clarify: bool = False
    last_refinement_trace: list[dict[str, str]] | None = None

    def ask(
        self,
        question: str,
        refinement_trace: list[dict[str, str]] | None = None,
    ) -> AskResult:
        """Возвращает предсказуемый результат или доменную ошибку."""

        self.last_refinement_trace = refinement_trace

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
                    "reason": "В запросе не указан период.",
                    "question": "За какой период показать данные?",
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

    def test_ask_endpoint_domain_error(self) -> None:
        """Доменная ошибка сервиса должна маппиться в HTTP 400."""

        app = create_app()
        app.dependency_overrides[get_ask_service] = lambda: StubAskService(should_fail=True)
        client = TestClient(app)

        response = client.post("/api/ask", json={"question": "Очень тяжелый вопрос"})

        self.assertEqual(response.status_code, 400)
        body = response.json()
        self.assertEqual(body["detail"]["error_code"], "SQL_COST_LIMIT_EXCEEDED")

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


if __name__ == "__main__":
    unittest.main()
