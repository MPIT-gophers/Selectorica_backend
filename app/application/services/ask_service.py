"""Бизнес-логика фазы 4: NL-вопрос -> SQL -> Explain -> безопасное выполнение."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Protocol

from backend.app.application.dto.clarification_payloads import (
    build_classifier_clarification_payload,
    build_rule_clarification_confidence,
    build_rule_clarification_payload,
)
from backend.app.application.use_cases.ask_question import AskExecutionData, AskQuestionUseCase
from backend.app.domain.services.confidence_policy import build_confidence_payload
from backend.app.domain.services.intent_resolution_policy import IntentResolver
from backend.app.domain.services.question_normalizer import normalize_question_terms
from backend.app.domain.services.visualization_policy import build_visualization_spec
from backend.app.infrastructure.history.sqlite_report_history_repo import (
    ReportHistoryRepository,
    ReportRecord,
)


class AskServiceError(Exception):
    """Доменно-осмысленная ошибка сервиса `/api/ask`."""

    def __init__(self, error_code: str, message: str):
        super().__init__(message)
        self.error_code = error_code
        self.message = message


class SqlGenerator(Protocol):
    """Контракт генератора SQL из естественного языка."""

    def generate_sql(self, question: str) -> str:
        """Генерирует SQL-строку по вопросу пользователя."""

    def regenerate_sql(
        self,
        question: str,
        previous_output: str,
        error_message: str,
    ) -> str:
        """Пытается восстановить валидный SQL после неудачной первичной генерации."""


class SqlExplainer(Protocol):
    """Контракт генерации Explain-текста по вопросу и SQL."""

    def explain(self, question: str, sql_text: str) -> str:
        """Возвращает человекочитаемое объяснение SQL перед выполнением."""


class QueryExecutor(Protocol):
    """Контракт безопасного исполнителя SQL."""

    def execute(self, sql_text: str) -> dict[str, Any]:
        """Выполняет SQL и возвращает сериализуемый payload результата."""


class IntentClassifier(Protocol):
    """Контракт fallback-классификатора неоднозначных intent-запросов."""

    def classify(self, question: str) -> dict[str, Any] | str | None:
        """Возвращает payload уточнения или пустой результат."""


@dataclass(frozen=True)
class AskResult:
    """Результат обработки запроса `/api/ask`."""

    question: str
    status: str = "ok"
    generated_sql: str = ""
    explain: str = ""
    estimated_total_cost: float = 0.0
    columns: list[str] | None = None
    rows: list[dict[str, Any]] | None = None
    row_count: int = 0
    report_saved: bool = False
    report_saved_at: str = ""
    visualization: dict[str, Any] | None = None
    confidence: dict[str, Any] | None = None
    clarification: dict[str, Any] | None = None
    recommended_actions: list[str] = field(default_factory=list)


class AskService:
    """Оркестратор полного пайплайна вопроса пользователя."""

    _ALLOWED_CLASSIFIER_KINDS = {"metric", "period"}
    _FINANCE_KEYWORDS = ("выручк", "доход", "прибыл", "марж", "оборот", "gmv")
    _OPS_KEYWORDS = ("отмен", "статус", "заказ", "достав", "просроч", "sla", "очеред")
    _STATUS_COLUMN_KEYWORDS = ("status", "state", "stage")
    _PROBLEM_STATUS_KEYWORDS = (
        "decline",
        "cancel",
        "cancelled",
        "failed",
        "late",
        "overdue",
        "refund",
        "reject",
    )
    _NON_METRIC_COLUMN_KEYWORDS = ("id", "date", "time", "day", "month", "year")

    def __init__(
        self,
        sql_generator: SqlGenerator,
        sql_explainer: SqlExplainer,
        executor: QueryExecutor,
        history_repo: ReportHistoryRepository,
        intent_resolver: IntentResolver | None = None,
        intent_classifier: IntentClassifier | None = None,
    ) -> None:
        """Инъецирует зависимости сервиса для генерации/выполнения/истории."""

        self._sql_generator = sql_generator
        self._sql_explainer = sql_explainer
        self._executor = executor
        self._history_repo = history_repo
        self._intent_resolver = intent_resolver or IntentResolver()
        self._intent_classifier = intent_classifier
        self._ask_use_case = AskQuestionUseCase(
            clean_question=self._clean_question,
            resolve_intent=self._intent_resolver.resolve,
            classify_with_fallback=self._classify_with_fallback,
            normalize_question_terms=self._normalize_question_terms,
            generate_sql_with_retry=self._generate_sql_with_retry,
            execute_query=self._executor.execute,
            explain_sql=self._sql_explainer.explain,
            build_visualization_spec=self._build_visualization_spec,
            build_confidence_payload=self._build_confidence_payload,
            build_recommended_actions=self._build_recommended_actions,
            utc_now_iso=utc_now_iso,
            save_report=self._save_report_from_use_case,
            create_domain_error=self._create_domain_error,
            build_rule_clarification_result=self._build_rule_clarification_result,
            build_classifier_clarification_result=self._build_classifier_clarification_result,
            build_success_result=self._build_success_result,
        )

    def ask(
        self,
        question: str,
        refinement_trace: list[dict[str, str]] | None = None,
    ) -> AskResult:
        """Обрабатывает вопрос, выполняет SQL и сохраняет запись отчета."""

        return self._ask_use_case.execute(question, refinement_trace=refinement_trace)

    def _clean_question(self, question: str) -> str:
        """Тримминг и валидация пользовательского вопроса."""

        cleaned_question = question.strip()
        if not cleaned_question:
            raise AskServiceError("INVALID_QUESTION", "Поле question не должно быть пустым.")
        return cleaned_question

    def _create_domain_error(self, error_code: str, message: str) -> AskServiceError:
        """Создает доменную ошибку для use case."""

        return AskServiceError(error_code, message)

    def _build_rule_clarification_result(
        self,
        question: str,
        clarification: Any,
    ) -> AskResult:
        """Формирует результат уточнения из deterministic intent-resolver."""

        confidence = build_rule_clarification_confidence()
        clarification_payload = build_rule_clarification_payload(clarification)
        return AskResult(
            question=question,
            status="clarification_needed",
            confidence=confidence,
            clarification=clarification_payload,
        )

    def _build_classifier_clarification_result(
        self,
        question: str,
        classifier_result: dict[str, Any],
    ) -> AskResult:
        """Формирует результат уточнения из fallback-classifier."""

        confidence, clarification_payload = build_classifier_clarification_payload(
            classifier_result
        )
        return AskResult(
            question=question,
            status="clarification_needed",
            confidence=confidence,
            clarification=clarification_payload,
        )

    def _save_report_from_use_case(
        self,
        question: str,
        sql_text: str,
        asked_at: str,
        refinement_trace: list[dict[str, str]],
        explain_text: str,
        confidence: dict[str, Any],
        recommended_actions: list[str],
    ) -> bool:
        """Сохраняет отчет для use case через текущий репозиторий истории."""

        return self._try_save_report(
            ReportRecord(
                question=question,
                sql_text=sql_text,
                asked_at=asked_at,
                refinement_trace=refinement_trace,
                explain_text=explain_text,
                confidence=confidence,
                recommended_actions=recommended_actions,
            )
        )

    def _build_success_result(self, execution_data: AskExecutionData) -> AskResult:
        """Собирает итоговый успешный ответ из вычисленных данных use case."""

        return AskResult(
            question=execution_data.question,
            status="ok",
            generated_sql=execution_data.generated_sql,
            explain=execution_data.explain,
            estimated_total_cost=execution_data.estimated_total_cost,
            columns=execution_data.columns,
            rows=execution_data.rows,
            row_count=execution_data.row_count,
            report_saved=execution_data.report_saved,
            report_saved_at=execution_data.report_saved_at,
            visualization=execution_data.visualization,
            confidence=execution_data.confidence,
            recommended_actions=execution_data.recommended_actions,
        )

    def _build_recommended_actions(
        self,
        question: str,
        columns: list[str],
        rows: list[dict[str, Any]],
    ) -> list[str]:
        """Строит простые action-подсказки для Finance/Ops по вопросу и фактическому результату."""

        normalized_question = question.lower()
        actions: list[str] = []

        if any(keyword in normalized_question for keyword in self._FINANCE_KEYWORDS):
            finance_action = self._build_finance_action(columns=columns, rows=rows)
            if finance_action:
                actions.append(finance_action)

        if any(keyword in normalized_question for keyword in self._OPS_KEYWORDS):
            ops_action = self._build_ops_action(columns=columns, rows=rows)
            if ops_action:
                actions.append(ops_action)

        return actions

    def _build_finance_action(
        self,
        columns: list[str],
        rows: list[dict[str, Any]],
    ) -> str | None:
        """Возвращает Finance-подсказку, если в результате виден финансовый сигнал для проверки."""

        metric_column = self._find_primary_metric_column(columns=columns, rows=rows)
        metric_values = self._extract_numeric_values(metric_column, rows) if metric_column else []

        if len(metric_values) >= 2 and metric_values[-1] < metric_values[0]:
            return f"Finance: проверить причины снижения {metric_column} за выбранный период."
        if not rows:
            return "Finance: проверить период и фильтры, данных по финансовой метрике не найдено."
        if metric_column:
            return f"Finance: сверить отклонения по {metric_column} и вклад ключевых сегментов."
        return None

    def _build_ops_action(
        self,
        columns: list[str],
        rows: list[dict[str, Any]],
    ) -> str | None:
        """Возвращает Ops-подсказку, если результат указывает на проблемный статус или нагрузку."""

        if not rows:
            return "Ops: проверить период и фильтры, событий по операционной метрике не найдено."

        problem_status = self._find_problem_status(rows=rows, columns=columns)
        if problem_status:
            return f'Ops: проверить причины роста статуса "{problem_status}" и очередь обработки.'

        metric_column = self._find_primary_metric_column(columns=columns, rows=rows)
        if metric_column:
            return f"Ops: проверить сегменты с максимальной нагрузкой по {metric_column}."
        return None

    def _find_primary_metric_column(
        self,
        columns: list[str],
        rows: list[dict[str, Any]],
    ) -> str | None:
        """Находит первую числовую колонку, похожую на бизнес-метрику, а не на идентификатор или дату."""

        for column in columns:
            normalized_column = column.lower()
            if any(keyword in normalized_column for keyword in self._NON_METRIC_COLUMN_KEYWORDS):
                continue
            if self._extract_numeric_values(column, rows):
                return column
        return None

    def _extract_numeric_values(
        self,
        column: str,
        rows: list[dict[str, Any]],
    ) -> list[float]:
        """Собирает числовые значения колонки из результата, игнорируя bool и пустые значения."""

        values: list[float] = []
        for row in rows:
            raw_value = row.get(column)
            if isinstance(raw_value, bool) or not isinstance(raw_value, (int, float)):
                continue
            values.append(float(raw_value))
        return values

    def _find_problem_status(
        self,
        rows: list[dict[str, Any]],
        columns: list[str],
    ) -> str | None:
        """Ищет проблемный статус в статусной колонке, чтобы подсветить Ops-следующее действие."""

        status_column = next(
            (
                column
                for column in columns
                if any(keyword in column.lower() for keyword in self._STATUS_COLUMN_KEYWORDS)
            ),
            None,
        )
        if status_column is None:
            return None

        for row in rows:
            raw_status = row.get(status_column)
            if not isinstance(raw_status, str):
                continue
            normalized_status = raw_status.lower()
            if any(keyword in normalized_status for keyword in self._PROBLEM_STATUS_KEYWORDS):
                return raw_status
        return None

    def _normalize_question_terms(self, question: str) -> str:
        """Нормализует пользовательские термины к полям схемы, чтобы снизить шанс невалидного SQL."""

        return normalize_question_terms(question)

    def _classify_with_fallback(self, question: str) -> dict[str, Any] | None:
        """Пытается получить уточнение от fallback-classifier без ломания основного flow."""

        if self._intent_classifier is None:
            return None

        raw_payload = self._intent_classifier.classify(question)
        normalized_payload = self._normalize_classifier_payload(raw_payload)
        if not normalized_payload:
            return None
        if not normalized_payload.get("needs_clarification"):
            return None

        clarification = normalized_payload.get("clarification")
        confidence = normalized_payload.get("confidence")
        if not isinstance(clarification, dict) or not isinstance(confidence, dict):
            return None
        if clarification.get("kind") not in self._ALLOWED_CLASSIFIER_KINDS:
            return None
        if not clarification.get("options"):
            return None

        return {
            "clarification": clarification,
            "confidence": confidence,
        }

    def _normalize_classifier_payload(
        self,
        payload: dict[str, Any] | str | None,
    ) -> dict[str, Any] | None:
        """Нормализует ответ classifier из dict/JSON-строки к словарю."""

        if payload is None:
            return None
        if isinstance(payload, dict):
            return payload
        if not isinstance(payload, str):
            return None

        normalized = self._strip_code_fences(payload.strip())
        normalized = self._extract_json_object(normalized)
        if not normalized:
            return None

        try:
            parsed = json.loads(normalized)
        except json.JSONDecodeError:
            return None
        return parsed if isinstance(parsed, dict) else None

    def _extract_json_object(self, value: str) -> str:
        """Пытается вытащить JSON-объект из текстового ответа LLM."""

        match = re.search(r"(?s)\{.*\}", value)
        if not match:
            return ""
        return match.group(0).strip()

    def _generate_sql_with_retry(self, question: str) -> tuple[str, bool]:
        """Генерирует SQL с одной попыткой repair, если первичный ответ не исполним."""

        first_output = self._sql_generator.generate_sql(question)
        try:
            return self._normalize_generated_sql(first_output), False
        except AskServiceError as error:
            if error.error_code != "SQL_GENERATION_FAILED":
                raise

            repaired_output = self._sql_generator.regenerate_sql(
                question=question,
                previous_output=first_output,
                error_message=error.message,
            )
            return self._normalize_generated_sql(repaired_output), True

    def _normalize_generated_sql(self, generated_sql: str) -> str:
        """Нормализует ответ LLM до исполнимого read-only SQL или поднимает доменную ошибку."""

        normalized = generated_sql.strip()
        if not normalized:
            raise AskServiceError(
                "SQL_GENERATION_FAILED",
                "LLM вернул пустой SQL. Попробуйте переформулировать вопрос.",
            )

        if "intermediate_sql" in normalized.lower():
            raise AskServiceError(
                "SQL_CONTEXT_INSUFFICIENT",
                "Для такого запроса недостаточно контекста схемы. "
                "Уточните поля из доступной модели (например, city_id, channel, order_date).",
            )

        normalized = self._strip_code_fences(normalized)
        normalized = self._drop_leading_sql_comments(normalized)
        normalized = self._extract_select_or_cte(normalized)

        if not normalized:
            raise AskServiceError(
                "SQL_GENERATION_FAILED",
                "LLM не вернул исполнимый SQL. Попробуйте переформулировать вопрос.",
            )
        return normalized

    def _strip_code_fences(self, value: str) -> str:
        """Удаляет markdown-ограждения ```sql ... ``` из текста ответа LLM."""

        stripped = value.strip()
        if stripped.startswith("```"):
            lines = stripped.splitlines()
            if lines:
                lines = lines[1:]
            if lines and lines[-1].strip().startswith("```"):
                lines = lines[:-1]
            return "\n".join(lines).strip()
        return stripped

    def _drop_leading_sql_comments(self, value: str) -> str:
        """Удаляет только начальные SQL-комментарии, оставляя тело запроса без изменений."""

        lines = value.splitlines()
        while lines and (
            not lines[0].strip()
            or lines[0].lstrip().startswith("--")
        ):
            lines.pop(0)
        return "\n".join(lines).strip()

    def _extract_select_or_cte(self, value: str) -> str:
        """Возвращает SQL начиная с первого `SELECT`/`WITH`, если LLM добавил поясняющий пролог."""

        match = re.search(r"(?is)\b(with|select)\b", value)
        if not match:
            return ""
        return value[match.start():].strip()

    def _build_visualization_spec(
        self,
        question: str,
        columns: list[str],
        rows: list[dict[str, Any]],
    ) -> dict[str, Any]:
        """Подбирает безопасную и понятную спецификацию графика по данным результата."""

        return build_visualization_spec(question=question, columns=columns, rows=rows)

    def _try_save_report(self, record: ReportRecord) -> bool:
        """Сохраняет отчет в историю и не ломает запрос при ошибке логирования."""

        try:
            self._history_repo.save_report(record)
            return True
        except Exception:
            return False

    def _build_confidence_payload(
        self,
        used_retry: bool,
        visualization: dict[str, Any],
    ) -> dict[str, Any]:
        """Строит простой confidence score для UI и explainability."""

        return build_confidence_payload(used_retry=used_retry, visualization=visualization)


def utc_now_iso() -> str:
    """Возвращает текущее время UTC в ISO-формате."""

    return datetime.now(timezone.utc).isoformat()
