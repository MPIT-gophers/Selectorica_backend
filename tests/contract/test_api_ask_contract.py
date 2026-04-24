"""Контрактные тесты endpoint `/api/ask`."""

from __future__ import annotations

import unittest
from dataclasses import dataclass

from fastapi.testclient import TestClient

from backend.app.application.services.ask_service import AskResult, AskServiceError
from backend.app.interfaces.http.server import create_app, get_ask_service


@dataclass
class ContractStubAskService:
    """Заглушка сервиса для проверки внешнего HTTP-контракта."""

    mode: str = "ok"
    error_code: str = "SQL_EXECUTION_FAILED"

    def ask(
        self,
        question: str,
        refinement_trace: list[dict[str, str]] | None = None,
    ) -> AskResult:
        """Возвращает payload в зависимости от выбранного режима."""

        if self.mode == "error":
            raise AskServiceError(self.error_code, "Не удалось выполнить SQL-запрос.")
        if self.mode == "clarification":
            return AskResult(
                question=question,
                status="clarification_needed",
                confidence={"score": 0.35, "level": "low", "reason": "Нужен период."},
                clarification={
                    "kind": "period",
                    "reason": "Период не указан.",
                    "question": "За какой период показать данные?",
                    "options": [
                        {
                            "label": "7 дней",
                            "value": f"{question} за последние 7 дней",
                            "description": "Оперативный анализ.",
                        }
                    ],
                },
            )
        return AskResult(
            question=question,
            status="ok",
            generated_sql="SELECT 1 AS value",
            explain="Тестовый explain.",
            recommended_actions=[
                "Finance: проверить причины снижения revenue_local за выбранный период."
            ],
            estimated_total_cost=1.0,
            columns=["value"],
            rows=[{"value": 1}],
            row_count=1,
            report_saved=True,
            report_saved_at="2026-04-24T00:00:00+00:00",
            visualization={
                "type": "table_only",
                "reason": "Тестовые данные.",
                "confidence": 1.0,
            },
            confidence={"score": 0.9, "level": "high", "reason": "Тестовая уверенность."},
        )


class TestAskApiContract(unittest.TestCase):
    """Проверяет публичный контракт API `/api/ask`."""

    def _make_client(self, service: ContractStubAskService) -> TestClient:
        """Создает клиент с override зависимости AskService."""

        app = create_app()
        app.dependency_overrides[get_ask_service] = lambda: service
        return TestClient(app)

    def test_ok_response_contract(self) -> None:
        """Успешный ответ содержит обязательные поля контракта."""

        client = self._make_client(ContractStubAskService(mode="ok"))
        response = client.post("/api/ask", json={"question": "Покажи тест"})

        self.assertEqual(response.status_code, 200)
        body = response.json()
        self.assertEqual(body["status"], "ok")
        self.assertIn("generated_sql", body)
        self.assertIn("columns", body)
        self.assertIn("rows", body)
        self.assertIn("row_count", body)
        self.assertIn("report_saved", body)
        self.assertIn("report_saved_at", body)
        self.assertEqual(
            body["recommended_actions"],
            ["Finance: проверить причины снижения revenue_local за выбранный период."],
        )

    def test_clarification_response_contract(self) -> None:
        """Ответ уточнения содержит confidence и clarification payload."""

        client = self._make_client(ContractStubAskService(mode="clarification"))
        response = client.post("/api/ask", json={"question": "Покажи выручку"})

        self.assertEqual(response.status_code, 200)
        body = response.json()
        self.assertEqual(body["status"], "clarification_needed")
        self.assertEqual(body["clarification"]["kind"], "period")
        self.assertEqual(body["confidence"]["level"], "low")
        self.assertGreater(len(body["clarification"]["options"]), 0)
        self.assertNotIn("recommended_actions", body)

    def test_domain_error_contract(self) -> None:
        """Доменная ошибка маппится в HTTP 400 с ожидаемой структурой."""

        client = self._make_client(ContractStubAskService(mode="error"))
        response = client.post("/api/ask", json={"question": "Сломанный запрос"})

        self.assertEqual(response.status_code, 400)
        body = response.json()
        self.assertIn("detail", body)
        self.assertEqual(body["detail"]["error_code"], "SQL_EXECUTION_FAILED")
        self.assertIn("message", body["detail"])

    def test_guardrail_error_contract(self) -> None:
        """Guardrail-ошибки возвращаются как структурированные HTTP 400 ответы."""

        for error_code in (
            "SQL_COST_LIMIT_EXCEEDED",
            "SQL_MUTATION_BLOCKED",
            "SQL_MULTI_STATEMENT_BLOCKED",
        ):
            with self.subTest(error_code=error_code):
                client = self._make_client(
                    ContractStubAskService(mode="error", error_code=error_code)
                )
                response = client.post("/api/ask", json={"question": "Show me everything"})

                self.assertEqual(response.status_code, 400)
                body = response.json()
                self.assertEqual(body["detail"]["error_code"], error_code)
                self.assertIn("message", body["detail"])


if __name__ == "__main__":
    unittest.main()
