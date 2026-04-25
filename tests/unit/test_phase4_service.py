"""Unit-тесты нормализации SQL в сервисе фазы 4."""

from __future__ import annotations

import unittest
from typing import Any

from backend.app.infrastructure.history.sqlite_report_history_repo import ReportRecord
from backend.app.application.services.ask_service import AskService, AskServiceError


class StubSqlGenerator:
    """Заглушка генератора SQL с управляемым ответом."""

    def __init__(
        self,
        sql_text: str,
        regenerated_sql_text: str = "SELECT 1;",
    ) -> None:
        """Сохраняет SQL-ответы для первичной и восстановительной генерации."""

        self._sql_text = sql_text
        self._regenerated_sql_text = regenerated_sql_text
        self.last_question = ""
        self.regenerate_called = False

    def generate_sql(self, _question: str) -> str:
        """Возвращает заранее подготовленный SQL-ответ."""

        self.last_question = _question
        return self._sql_text

    def regenerate_sql(
        self,
        question: str,
        previous_output: str,
        error_message: str,
    ) -> str:
        """Возвращает заранее заданный SQL для repair-попытки."""

        self.regenerate_called = True
        self.last_question = question
        _ = previous_output
        _ = error_message
        return self._regenerated_sql_text


class StubSqlExplainer:
    """Заглушка explain-генератора, возвращающая фиксированный текст."""

    def explain(self, _question: str, _sql_text: str) -> str:
        """Возвращает краткий explain без внешних зависимостей."""

        return "ok"


class StubIntentClassifier:
    """Заглушка LLM-classifier для fallback-уточнений."""

    def __init__(self, payload: dict[str, Any] | str | None = None) -> None:
        """Сохраняет ответ классификатора и факт вызова."""

        self.payload = payload
        self.called = False

    def classify(self, _question: str) -> dict[str, Any] | str | None:
        """Возвращает заранее заданный ответ классификатора."""

        self.called = True
        return self.payload


class StubExecutor:
    """Заглушка исполнителя SQL с фиксацией последнего запроса."""

    def __init__(self) -> None:
        """нициализирует контейнер для последнего SQL-запроса."""

        self.last_sql: str = ""
        self.payload: dict[str, Any] = {
            "status": "ok",
            "estimated_total_cost": 1.0,
            "columns": [],
            "rows": [],
            "row_count": 0,
        }

    def execute(self, sql_text: str) -> dict[str, Any]:
        """Запоминает SQL и возвращает успешный пустой результат."""

        self.last_sql = sql_text
        return self.payload


class SequencedExecutor:
    """Заглушка исполнителя, которая возвращает payload-ответы по очереди."""

    def __init__(self, payloads: list[dict[str, Any]]) -> None:
        """Сохраняет сценарий ответов и историю SQL для проверки retry-flow."""

        self.payloads = list(payloads)
        self.sql_history: list[str] = []

    def execute(self, sql_text: str) -> dict[str, Any]:
        """Запоминает SQL и возвращает следующий подготовленный payload."""

        self.sql_history.append(sql_text)
        return self.payloads.pop(0)


class StubHistoryRepo:
    """Заглушка репозитория истории отчетов."""

    def __init__(self) -> None:
        """Сохраняет последнюю запись для последующих проверок теста."""

        self.last_record: ReportRecord | None = None

    def save_report(self, _record: ReportRecord) -> None:
        """митирует успешное сохранение отчета без побочных эффектов."""

        self.last_record = _record
        return None


class TestPhase4Service(unittest.TestCase):
    """Проверяет нормализацию SQL-ответа от LLM перед исполнением."""

    def test_intermediate_sql_is_mapped_to_domain_error(self) -> None:
        """`-- intermediate_sql` должен маппиться в понятную доменную ошибку."""

        service = AskService(
            sql_generator=StubSqlGenerator("-- intermediate_sql\nSELECT DISTINCT city_id FROM orders;"),
            sql_explainer=StubSqlExplainer(),
            executor=StubExecutor(),
            history_repo=StubHistoryRepo(),
        )

        with self.assertRaises(AskServiceError) as context:
            service.ask("Покажи регионы за текущий месяц")

        self.assertEqual(context.exception.error_code, "SQL_CONTEXT_INSUFFICIENT")

    def test_markdown_and_comments_are_trimmed_before_execution(self) -> None:
        """Сервис должен убрать markdown/code-comments и исполнить чистый SELECT."""

        executor = StubExecutor()
        service = AskService(
            sql_generator=StubSqlGenerator(
                "```sql\n-- comment\nSELECT city_id, COUNT(*) AS cnt FROM orders GROUP BY 1;\n```"
            ),
            sql_explainer=StubSqlExplainer(),
            executor=executor,
            history_repo=StubHistoryRepo(),
        )

        result = service.ask("Сколько заказов по городам за текущий месяц?")

        self.assertEqual(
            executor.last_sql,
            "SELECT city_id, COUNT(*) AS cnt FROM orders GROUP BY 1;",
        )
        self.assertEqual(result.generated_sql, executor.last_sql)

    def test_retry_is_used_when_first_output_is_not_sql(self) -> None:
        """Сервис должен делать repair-попытку, если первичный ответ не содержит SQL."""

        executor = StubExecutor()
        generator = StubSqlGenerator(
            sql_text="Не могу построить запрос.",
            regenerated_sql_text="SELECT city_id, COUNT(*) AS cnt FROM orders GROUP BY 1;",
        )
        service = AskService(
            sql_generator=generator,
            sql_explainer=StubSqlExplainer(),
            executor=executor,
            history_repo=StubHistoryRepo(),
        )

        result = service.ask("Сколько заказов по регионам за текущий месяц?")

        self.assertTrue(generator.regenerate_called)
        self.assertEqual(
            executor.last_sql,
            "SELECT city_id, COUNT(*) AS cnt FROM orders GROUP BY 1;",
        )
        self.assertEqual(result.generated_sql, executor.last_sql)

    def test_retry_is_used_when_executor_returns_recoverable_sql_error(self) -> None:
        """Recoverable SQL-ошибка после guardrails/execution должна запускать одну repair-попытку."""

        executor = SequencedExecutor(
            [
                {
                    "status": "error",
                    "error_code": "SQL_PARSE_ERROR",
                    "message": "Некорректный SQL.",
                },
                {
                    "status": "ok",
                    "estimated_total_cost": 1.0,
                    "columns": ["value"],
                    "rows": [{"value": 1}],
                    "row_count": 1,
                },
            ]
        )
        generator = StubSqlGenerator(
            sql_text="SELECT broken",
            regenerated_sql_text="SELECT 1 AS value;",
        )
        service = AskService(
            sql_generator=generator,
            sql_explainer=StubSqlExplainer(),
            executor=executor,
            history_repo=StubHistoryRepo(),
        )

        result = service.ask("Покажи выручку за текущий месяц")

        self.assertEqual(result.status, "ok")
        self.assertTrue(generator.regenerate_called)
        self.assertEqual(executor.sql_history, ["SELECT broken", "SELECT 1 AS value;"])
        self.assertEqual(result.generated_sql, "SELECT 1 AS value;")
        self.assertEqual(result.confidence["level"], "medium")

    def test_safety_sql_errors_do_not_trigger_retry(self) -> None:
        """Safety/cost guardrails должны возвращаться пользователю без repair-попытки."""

        for error_code in (
            "SQL_MUTATION_BLOCKED",
            "SQL_MULTI_STATEMENT_BLOCKED",
            "SQL_COST_LIMIT_EXCEEDED",
        ):
            with self.subTest(error_code=error_code):
                executor = SequencedExecutor(
                    [
                        {
                            "status": "error",
                            "error_code": error_code,
                            "message": "Запрос остановлен guardrails.",
                        }
                    ]
                )
                generator = StubSqlGenerator(sql_text="SELECT risky FROM orders;")
                service = AskService(
                    sql_generator=generator,
                    sql_explainer=StubSqlExplainer(),
                    executor=executor,
                    history_repo=StubHistoryRepo(),
                )

                with self.assertRaises(AskServiceError) as context:
                    service.ask("Покажи выручку за текущий месяц")

                self.assertEqual(context.exception.error_code, error_code)
                self.assertFalse(generator.regenerate_called)
                self.assertEqual(executor.sql_history, ["SELECT risky FROM orders;"])

    def test_region_term_is_normalized_to_city_id(self) -> None:
        """Термин `регион` в вопросе должен быть нормализован к `city_id` перед генерацией."""

        generator = StubSqlGenerator(sql_text="SELECT 1;")
        service = AskService(
            sql_generator=generator,
            sql_explainer=StubSqlExplainer(),
            executor=StubExecutor(),
            history_repo=StubHistoryRepo(),
        )

        service.ask("Покажи выручку по регионам за 7 дней")

        self.assertIn("city_id", generator.last_question)

    def test_line_visualization_for_temporal_question(self) -> None:
        """Временной вопрос с датой и метрикой должен давать line-спецификацию."""

        executor = StubExecutor()
        executor.payload = {
            "status": "ok",
            "estimated_total_cost": 1.0,
            "columns": ["order_date", "revenue_local"],
            "rows": [
                {"order_date": "2026-04-20T00:00:00", "revenue_local": 100.0},
                {"order_date": "2026-04-21T00:00:00", "revenue_local": 120.0},
            ],
            "row_count": 2,
        }
        service = AskService(
            sql_generator=StubSqlGenerator("SELECT order_date, revenue_local FROM x;"),
            sql_explainer=StubSqlExplainer(),
            executor=executor,
            history_repo=StubHistoryRepo(),
        )

        result = service.ask("Покажи динамику выручки по дням за последние 7 дней")

        self.assertEqual(result.visualization["type"], "line")
        self.assertEqual(result.visualization["x_field"], "order_date")
        self.assertGreaterEqual(result.confidence["score"], 0.8)
        self.assertEqual(result.confidence["level"], "high")

    def test_table_only_for_id_and_text_columns(self) -> None:
        """Если нет валидной метрики, сервис должен выбрать table_only."""

        executor = StubExecutor()
        executor.payload = {
            "status": "ok",
            "estimated_total_cost": 1.0,
            "columns": ["city_id", "status_tender"],
            "rows": [
                {"city_id": 67, "status_tender": "done"},
                {"city_id": 67, "status_tender": "decline"},
            ],
            "row_count": 2,
        }
        service = AskService(
            sql_generator=StubSqlGenerator("SELECT city_id, status_tender FROM x;"),
            sql_explainer=StubSqlExplainer(),
            executor=executor,
            history_repo=StubHistoryRepo(),
        )

        result = service.ask("Покажи список статусов за текущий месяц")

        self.assertEqual(result.visualization["type"], "table_only")
        self.assertEqual(result.confidence["score"], 0.9)
        self.assertIn("Визуализация", result.confidence["reason"])

    def test_ask_returns_metric_clarification_before_sql_generation(self) -> None:
        """Неоднозначные продажи должны остановить flow до Vanna и SQL."""

        generator = StubSqlGenerator(sql_text="SELECT 1;")
        executor = StubExecutor()
        service = AskService(
            sql_generator=generator,
            sql_explainer=StubSqlExplainer(),
            executor=executor,
            history_repo=StubHistoryRepo(),
        )

        result = service.ask("Покажи продажи по регионам за текущий месяц")

        self.assertEqual(result.status, "clarification_needed")
        self.assertIsNotNone(result.clarification)
        self.assertEqual(result.clarification["kind"], "metric")
        self.assertLess(result.confidence["score"], 0.5)
        self.assertEqual(result.confidence["level"], "low")
        self.assertEqual(generator.last_question, "")
        self.assertEqual(executor.last_sql, "")

    def test_clear_question_still_uses_sql_flow(self) -> None:
        """Понятный вопрос должен сохранять существующее поведение SQL-flow."""

        generator = StubSqlGenerator(sql_text="SELECT 1;")
        executor = StubExecutor()
        service = AskService(
            sql_generator=generator,
            sql_explainer=StubSqlExplainer(),
            executor=executor,
            history_repo=StubHistoryRepo(),
        )

        result = service.ask("Покажи выручку по регионам за текущий месяц")

        self.assertEqual(result.status, "ok")
        self.assertIn(result.confidence["level"], {"medium", "high"})
        self.assertEqual(executor.last_sql, "SELECT 1;")

    def test_finance_question_gets_recommended_action_for_metric_drop(self) -> None:
        """Финансовый вопрос с просадкой метрики должен вернуть action-подсказку для Finance."""

        executor = StubExecutor()
        executor.payload = {
            "status": "ok",
            "estimated_total_cost": 1.0,
            "columns": ["order_date", "revenue_local"],
            "rows": [
                {"order_date": "2026-04-20T00:00:00", "revenue_local": 120.0},
                {"order_date": "2026-04-21T00:00:00", "revenue_local": 80.0},
            ],
            "row_count": 2,
        }
        service = AskService(
            sql_generator=StubSqlGenerator("SELECT order_date, revenue_local FROM orders;"),
            sql_explainer=StubSqlExplainer(),
            executor=executor,
            history_repo=StubHistoryRepo(),
        )

        result = service.ask("Покажи динамику выручки по дням за последние 7 дней")

        self.assertEqual(
            result.recommended_actions,
            ["Finance: проверить причины снижения revenue_local за выбранный период."],
        )

    def test_ops_question_gets_recommended_action_for_problem_status(self) -> None:
        """Операционный вопрос с проблемным статусом должен вернуть action-подсказку для Ops."""

        executor = StubExecutor()
        executor.payload = {
            "status": "ok",
            "estimated_total_cost": 1.0,
            "columns": ["status_tender", "cnt"],
            "rows": [
                {"status_tender": "decline", "cnt": 12},
                {"status_tender": "done", "cnt": 3},
            ],
            "row_count": 2,
        }
        service = AskService(
            sql_generator=StubSqlGenerator("SELECT status_tender, cnt FROM orders;"),
            sql_explainer=StubSqlExplainer(),
            executor=executor,
            history_repo=StubHistoryRepo(),
        )

        result = service.ask("Покажи отмены по статусам за сегодня")

        self.assertEqual(
            result.recommended_actions,
            ['Ops: проверить причины роста статуса "decline" и очередь обработки.'],
        )

    def test_scenario_action_hint_is_used_when_no_specific_action_exists(self) -> None:
        """Если сигналов в данных нет, action layer должен брать действие из scenario context."""

        executor = StubExecutor()
        executor.payload = {
            "status": "ok",
            "estimated_total_cost": 1.0,
            "columns": ["city_id"],
            "rows": [{"city_id": 67}],
            "row_count": 1,
        }
        service = AskService(
            sql_generator=StubSqlGenerator("SELECT city_id FROM orders LIMIT 1;"),
            sql_explainer=StubSqlExplainer(),
            executor=executor,
            history_repo=StubHistoryRepo(),
        )

        result = service.ask(
            "Покажи список городов за текущий месяц",
            context={"action_hint": "Проверить проблемные часы и покрытие смен."},
        )

        self.assertEqual(
            result.recommended_actions,
            ["Проверить проблемные часы и покрытие смен."],
        )

    def test_success_without_signals_still_gets_safe_action_fallback(self) -> None:
        """Успешный ответ без role/scenario сигналов не должен оставаться без next step."""

        service = AskService(
            sql_generator=StubSqlGenerator("SELECT 1 AS value;"),
            sql_explainer=StubSqlExplainer(),
            executor=StubExecutor(),
            history_repo=StubHistoryRepo(),
        )

        result = service.ask("Покажи данные за текущий месяц")

        self.assertEqual(
            result.recommended_actions,
            [
                (
                    "Сверьте SQL и explain ниже, затем уточните период или разрез, "
                    "если результат нужен для управленческого решения."
                )
            ],
        )

    def test_finance_drop_action_requires_temporal_context(self) -> None:
        """Finance action не должен называть снижением обычную сортировку не по времени."""

        executor = StubExecutor()
        executor.payload = {
            "status": "ok",
            "estimated_total_cost": 1.0,
            "columns": ["city_id", "revenue_local"],
            "rows": [
                {"city_id": 67, "revenue_local": 120.0},
                {"city_id": 21, "revenue_local": 80.0},
            ],
            "row_count": 2,
        }
        service = AskService(
            sql_generator=StubSqlGenerator("SELECT city_id, revenue_local FROM orders;"),
            sql_explainer=StubSqlExplainer(),
            executor=executor,
            history_repo=StubHistoryRepo(),
        )

        result = service.ask("Покажи выручку по городам за текущий месяц")

        self.assertNotIn("снижения", result.recommended_actions[0])
        self.assertEqual(
            result.recommended_actions,
            ["Finance: сверить отклонения по revenue_local и вклад ключевых сегментов."],
        )

    def test_successful_report_saves_refinement_trace(self) -> None:
        """Сервис должен сохранять refinement trace вместе с итоговым отчетом."""

        history_repo = StubHistoryRepo()
        service = AskService(
            sql_generator=StubSqlGenerator(sql_text="SELECT 1;"),
            sql_explainer=StubSqlExplainer(),
            executor=StubExecutor(),
            history_repo=history_repo,
        )

        service.ask(
            "Покажи выручку по регионам за текущий месяц",
            refinement_trace=[
                {
                    "question": "Что считать продажами?",
                    "selected_label": "Выручка",
                    "selected_value": "Покажи выручку по регионам за текущий месяц",
                }
            ],
        )

        self.assertIsNotNone(history_repo.last_record)
        self.assertEqual(
            history_repo.last_record.refinement_trace[0]["selected_label"],
            "Выручка",
        )
        self.assertTrue(history_repo.last_record.explain_text)
        self.assertIn("level", history_repo.last_record.confidence)
        self.assertIsInstance(history_repo.last_record.recommended_actions, list)

    def test_refinement_trace_prevents_repeated_metric_clarification_loop(self) -> None:
        """После выбора метрики сервис не должен повторно вызывать classifier."""

        generator = StubSqlGenerator(sql_text="SELECT 1;")
        executor = StubExecutor()
        classifier = StubIntentClassifier(
            payload={
                "needs_clarification": True,
                "confidence": {"score": 0.2, "level": "low", "reason": "loop"},
                "clarification": {
                    "kind": "metric",
                    "reason": "loop",
                    "question": "loop?",
                    "options": [
                        {"label": "Loop", "value": "Loop", "description": "Loop"}
                    ],
                },
            }
        )
        service = AskService(
            sql_generator=generator,
            sql_explainer=StubSqlExplainer(),
            executor=executor,
            history_repo=StubHistoryRepo(),
            intent_classifier=classifier,
        )

        result = service.ask(
            "Покажи продажи по регионам",
            refinement_trace=[
                {
                    "question": "Что считать продажами?",
                    "selected_label": "Выручка",
                    "selected_value": "Покажи выручку по регионам",
                }
            ],
            context={
                "previous_params": {
                    "date_range": {
                        "value": "last_30_days",
                        "label": "последние 30 дней",
                    }
                }
            },
        )

        self.assertEqual(result.status, "ok")
        self.assertFalse(classifier.called)
        self.assertIn("последние 30 дней", generator.last_question)
        self.assertEqual(result.resolved_params["date_range"]["source"], "context")
        self.assertEqual(executor.last_sql, "SELECT 1;")

    def test_missing_period_returns_period_clarification_before_sql_generation(self) -> None:
        """Ясный вопрос без периода должен вернуть уточнение вместо дефолта."""

        generator = StubSqlGenerator(sql_text="SELECT 1;")
        service = AskService(
            sql_generator=generator,
            sql_explainer=StubSqlExplainer(),
            executor=StubExecutor(),
            history_repo=StubHistoryRepo(),
        )

        result = service.ask("Покажи выручку по регионам")

        self.assertEqual(result.status, "clarification_needed")
        self.assertEqual(result.clarification["kind"], "period")
        self.assertEqual(result.clarification["reason_code"], "DATE_RANGE_REQUIRED")
        self.assertEqual(generator.last_question, "")

    def test_missing_period_uses_context_before_default_in_service(self) -> None:
        """Сервис должен передавать контекст периода в resolver перед SQL-flow."""

        generator = StubSqlGenerator(sql_text="SELECT 1;")
        service = AskService(
            sql_generator=generator,
            sql_explainer=StubSqlExplainer(),
            executor=StubExecutor(),
            history_repo=StubHistoryRepo(),
        )

        result = service.ask(
            "Покажи отмены по регионам",
            context={
                "previous_params": {
                    "date_range": {
                        "value": "last_30_days",
                        "label": "последние 30 дней",
                    }
                }
            },
        )

        self.assertEqual(result.status, "ok")
        self.assertIn("последние 30 дней", generator.last_question)
        self.assertEqual(result.resolved_params["date_range"]["source"], "context")

    def test_classifier_can_request_clarification_for_non_rule_ambiguity(self) -> None:
        """LLM-classifier должен уметь вернуть уточнение, если deterministic rules промолчали."""

        generator = StubSqlGenerator(sql_text="SELECT 1;")
        executor = StubExecutor()
        classifier = StubIntentClassifier(
            payload={
                "needs_clarification": True,
                "confidence": {
                    "score": 0.42,
                    "level": "low",
                    "reason": "Фраза слишком общая для точного выбора бизнес-метрики.",
                },
                "clarification": {
                    "kind": "metric",
                    "reason": "Непонятно, какую именно метрику нужно показать.",
                    "question": "Что именно вы хотите увидеть?",
                    "options": [
                        {
                            "label": "Выручка",
                            "value": "Покажи выручку по клиентам за текущий месяц",
                            "description": "Сумма по завершенным заказам.",
                        }
                    ],
                },
            }
        )
        service = AskService(
            sql_generator=generator,
            sql_explainer=StubSqlExplainer(),
            executor=executor,
            history_repo=StubHistoryRepo(),
            intent_classifier=classifier,
        )

        result = service.ask("Покажи показатели по клиентам за текущий месяц")

        self.assertEqual(result.status, "clarification_needed")
        self.assertEqual(result.clarification["kind"], "metric")
        self.assertTrue(classifier.called)
        self.assertEqual(generator.last_question, "")
        self.assertEqual(executor.last_sql, "")

    def test_classifier_is_not_called_when_rules_already_require_clarification(self) -> None:
        """Fallback-classifier не должен работать, если deterministic rules уже нашли проблему."""

        classifier = StubIntentClassifier(payload={"needs_clarification": False})
        service = AskService(
            sql_generator=StubSqlGenerator(sql_text="SELECT 1;"),
            sql_explainer=StubSqlExplainer(),
            executor=StubExecutor(),
            history_repo=StubHistoryRepo(),
            intent_classifier=classifier,
        )

        result = service.ask("Покажи продажи по регионам за текущий месяц")

        self.assertEqual(result.status, "clarification_needed")
        self.assertFalse(classifier.called)

    def test_classifier_invalid_payload_does_not_break_sql_flow(self) -> None:
        """Некорректный ответ classifier должен тихо игнорироваться."""

        generator = StubSqlGenerator(sql_text="SELECT 1;")
        executor = StubExecutor()
        classifier = StubIntentClassifier(payload="not-json-at-all")
        service = AskService(
            sql_generator=generator,
            sql_explainer=StubSqlExplainer(),
            executor=executor,
            history_repo=StubHistoryRepo(),
            intent_classifier=classifier,
        )

        result = service.ask("Покажи выручку по клиентам за текущий месяц")

        self.assertEqual(result.status, "ok")
        self.assertTrue(classifier.called)
        self.assertEqual(executor.last_sql, "SELECT 1;")

    def test_classifier_region_clarification_is_rejected_as_out_of_scope(self) -> None:
        """Classifier не должен уводить пользователя в выбор несуществующих `регионов`."""

        generator = StubSqlGenerator(sql_text="SELECT 1;")
        executor = StubExecutor()
        classifier = StubIntentClassifier(
            payload={
                "needs_clarification": True,
                "confidence": {
                    "score": 0.65,
                    "level": "medium",
                    "reason": "Неясно, какие регионы нужны.",
                },
                "clarification": {
                    "kind": "region",
                    "reason": "Не указаны конкретные регионы для анализа.",
                    "question": "По каким регионам необходимо показать выручку?",
                    "options": [
                        {
                            "label": "Северный",
                            "value": "Покажи выручку по северному региону",
                            "description": "Северный регион",
                        }
                    ],
                },
            }
        )
        service = AskService(
            sql_generator=generator,
            sql_explainer=StubSqlExplainer(),
            executor=executor,
            history_repo=StubHistoryRepo(),
            intent_classifier=classifier,
        )

        result = service.ask("Покажи выручку по регионам за текущий месяц")

        self.assertEqual(result.status, "ok")
        self.assertTrue(classifier.called)
        self.assertEqual(executor.last_sql, "SELECT 1;")


if __name__ == "__main__":
    unittest.main()
