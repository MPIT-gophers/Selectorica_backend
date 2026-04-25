"""Use case обработки пользовательского запроса `/api/ask`."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Protocol, TypeVar

TResult = TypeVar("TResult")

_RECOVERABLE_SQL_ERROR_CODES = {
    "SQL_PARSE_ERROR",
    "SQL_EXPLAIN_FAILED",
    "SQL_EXECUTION_FAILED",
}


@dataclass(frozen=True)
class AskExecutionData:
    """Результат SQL-ветки до финальной сборки DTO ответа."""

    question: str
    generated_sql: str
    explain: str
    estimated_total_cost: float
    columns: list[str]
    rows: list[dict[str, Any]]
    row_count: int
    truncated: bool
    report_saved: bool
    report_saved_at: str
    visualization: dict[str, Any] | None
    confidence: dict[str, Any] | None
    recommended_actions: list[str]
    assumptions: list[str]
    resolved_params: dict[str, Any]
    decision_events: list[dict[str, Any]]


class IntentResolutionPort(Protocol):
    """Минимальный контракт результата intent-резолвера."""

    needs_clarification: bool
    clarification: Any
    effective_question: str
    resolved_params: dict[str, Any]
    assumptions: list[str]
    decision_events: list[dict[str, Any]]
    intent_confidence: float


class AskQuestionUseCase:
    """Use case оркестрации flow от вопроса до финального результата."""

    def __init__(
        self,
        clean_question: Callable[[str], str],
        resolve_intent: Callable[[str, dict[str, Any] | None], IntentResolutionPort],
        classify_with_fallback: Callable[[str], dict[str, Any] | None],
        normalize_question_terms: Callable[[str], str],
        generate_sql_with_retry: Callable[[str], tuple[str, bool]],
        repair_sql_after_execution_error: Callable[[str, str, str], tuple[str, bool]],
        execute_query: Callable[[str], dict[str, Any]],
        explain_sql: Callable[[str, str], str],
        build_visualization_spec: Callable[[str, list[str], list[dict[str, Any]]], dict[str, Any]],
        build_confidence_payload: Callable[
            [bool, dict[str, Any], float, list[str]],
            dict[str, Any],
        ],
        build_recommended_actions: Callable[
            [str, list[str], list[dict[str, Any]], dict[str, Any]],
            list[str],
        ],
        utc_now_iso: Callable[[], str],
        save_report: Callable[
            [
                str,
                str,
                str,
                list[dict[str, str]],
                str,
                dict[str, Any],
                list[str],
                list[str],
                dict[str, Any],
                list[dict[str, Any]],
            ],
            bool,
        ],
        create_domain_error: Callable[[str, str], Exception],
        build_rule_clarification_result: Callable[[str, Any], TResult],
        build_classifier_clarification_result: Callable[[str, dict[str, Any]], TResult],
        build_success_result: Callable[[AskExecutionData], TResult],
    ) -> None:
        """Инициализирует orchestration через порты и фабрики результата."""

        self._clean_question = clean_question
        self._resolve_intent = resolve_intent
        self._classify_with_fallback = classify_with_fallback
        self._normalize_question_terms = normalize_question_terms
        self._generate_sql_with_retry = generate_sql_with_retry
        self._repair_sql_after_execution_error = repair_sql_after_execution_error
        self._execute_query = execute_query
        self._explain_sql = explain_sql
        self._build_visualization_spec = build_visualization_spec
        self._build_confidence_payload = build_confidence_payload
        self._build_recommended_actions = build_recommended_actions
        self._utc_now_iso = utc_now_iso
        self._save_report = save_report
        self._create_domain_error = create_domain_error
        self._build_rule_clarification_result = build_rule_clarification_result
        self._build_classifier_clarification_result = build_classifier_clarification_result
        self._build_success_result = build_success_result

    def execute(
        self,
        question: str,
        refinement_trace: list[dict[str, str]] | None = None,
        context: dict[str, Any] | None = None,
    ) -> TResult:
        """Запускает полный ask-flow и возвращает DTO результата."""

        trace = refinement_trace or []
        cleaned_question = self._clean_question(
            self._get_latest_refined_question(question, trace)
        )
        intent_resolution = self._resolve_intent(cleaned_question, context or {})

        if intent_resolution.needs_clarification:
            return self._build_rule_clarification_result(
                cleaned_question,
                intent_resolution.clarification,
            )

        # LLM-classifier оставляем только для первого шага и только когда deterministic
        # resolver не применил контекст: так мы не возвращаем пользователя в лишние циклы.
        if not trace and not intent_resolution.decision_events:
            classifier_result = self._classify_with_fallback(cleaned_question)
            if classifier_result is not None:
                return self._build_classifier_clarification_result(
                    cleaned_question,
                    classifier_result,
                )

        generation_question_source = intent_resolution.effective_question or cleaned_question
        generation_question = self._normalize_question_terms(generation_question_source)
        generated_sql, used_retry = self._generate_sql_with_retry(generation_question)
        execution_payload = self._execute_query(generated_sql)
        if self._can_repair_sql_error(execution_payload):
            generated_sql, repair_used_retry = self._repair_sql_after_execution_error(
                generation_question,
                generated_sql,
                execution_payload.get("message", "Не удалось выполнить SQL-запрос."),
            )
            used_retry = used_retry or repair_used_retry
            execution_payload = self._execute_query(generated_sql)

        if execution_payload.get("status") != "ok":
            raise self._create_domain_error(
                execution_payload.get("error_code", "SQL_EXECUTION_FAILED"),
                execution_payload.get("message", "Не удалось выполнить SQL-запрос."),
            )

        columns = list(execution_payload["columns"])
        rows = list(execution_payload["rows"])
        visualization = self._build_visualization_spec(
            question=cleaned_question,
            columns=columns,
            rows=rows,
        )

        asked_at = self._utc_now_iso()
        explain_text = self._explain_sql(cleaned_question, generated_sql).strip()
        confidence = self._build_confidence_payload(
            used_retry=used_retry,
            visualization=visualization,
            intent_confidence=float(intent_resolution.intent_confidence),
            assumptions=list(intent_resolution.assumptions),
        )
        recommended_actions = self._build_recommended_actions(
            cleaned_question,
            columns,
            rows,
            context or {},
        )
        report_saved = self._save_report(
            cleaned_question,
            generated_sql,
            asked_at,
            trace,
            explain_text,
            confidence,
            recommended_actions,
            list(intent_resolution.assumptions),
            dict(intent_resolution.resolved_params),
            list(intent_resolution.decision_events),
        )

        return self._build_success_result(
            AskExecutionData(
                question=cleaned_question,
                generated_sql=generated_sql,
                explain=explain_text,
                estimated_total_cost=float(execution_payload["estimated_total_cost"]),
                columns=columns,
                rows=rows,
                row_count=int(execution_payload["row_count"]),
                truncated=bool(execution_payload.get("truncated", False)),
                report_saved=report_saved,
                report_saved_at=asked_at,
                visualization=visualization,
                confidence=confidence,
                recommended_actions=recommended_actions,
                assumptions=list(intent_resolution.assumptions),
                resolved_params=dict(intent_resolution.resolved_params),
                decision_events=list(intent_resolution.decision_events),
            )
        )

    def _get_latest_refined_question(
        self,
        question: str,
        refinement_trace: list[dict[str, str]],
    ) -> str:
        """Возвращает последний выбранный пользователем вариант как актуальный вопрос."""

        if not refinement_trace:
            return question
        latest_value = refinement_trace[-1].get("selected_value", "").strip()
        return latest_value or question

    def _can_repair_sql_error(self, execution_payload: dict[str, Any]) -> bool:
        """Проверяет, можно ли сделать одну repair-попытку после ошибки SQL."""

        return (
            execution_payload.get("status") != "ok"
            and execution_payload.get("error_code") in _RECOVERABLE_SQL_ERROR_CODES
        )
