"""Use case обработки пользовательского запроса `/api/ask`."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Protocol, TypeVar

TResult = TypeVar("TResult")


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
    report_saved: bool
    report_saved_at: str
    visualization: dict[str, Any] | None
    confidence: dict[str, Any] | None
    recommended_actions: list[str]


class IntentResolutionPort(Protocol):
    """Минимальный контракт результата intent-резолвера."""

    needs_clarification: bool
    clarification: Any


class AskQuestionUseCase:
    """Use case оркестрации flow от вопроса до финального результата."""

    def __init__(
        self,
        clean_question: Callable[[str], str],
        resolve_intent: Callable[[str], IntentResolutionPort],
        classify_with_fallback: Callable[[str], dict[str, Any] | None],
        normalize_question_terms: Callable[[str], str],
        generate_sql_with_retry: Callable[[str], tuple[str, bool]],
        execute_query: Callable[[str], dict[str, Any]],
        explain_sql: Callable[[str, str], str],
        build_visualization_spec: Callable[[str, list[str], list[dict[str, Any]]], dict[str, Any]],
        build_confidence_payload: Callable[[bool, dict[str, Any]], dict[str, Any]],
        build_recommended_actions: Callable[[str, list[str], list[dict[str, Any]]], list[str]],
        utc_now_iso: Callable[[], str],
        save_report: Callable[
            [str, str, str, list[dict[str, str]], str, dict[str, Any], list[str]],
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
    ) -> TResult:
        """Запускает полный ask-flow и возвращает DTO результата."""

        cleaned_question = self._clean_question(question)
        trace = refinement_trace or []

        # После первого пользовательского уточнения продолжаем SQL-flow,
        # чтобы исключить повторные циклы уточнений.
        if not trace:
            intent_resolution = self._resolve_intent(cleaned_question)
            if intent_resolution.needs_clarification:
                return self._build_rule_clarification_result(
                    cleaned_question,
                    intent_resolution.clarification,
                )

            classifier_result = self._classify_with_fallback(cleaned_question)
            if classifier_result is not None:
                return self._build_classifier_clarification_result(
                    cleaned_question,
                    classifier_result,
                )

        generation_question = self._normalize_question_terms(cleaned_question)
        generated_sql, used_retry = self._generate_sql_with_retry(generation_question)
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
        )
        recommended_actions = self._build_recommended_actions(
            cleaned_question,
            columns,
            rows,
        )
        report_saved = self._save_report(
            cleaned_question,
            generated_sql,
            asked_at,
            trace,
            explain_text,
            confidence,
            recommended_actions,
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
                report_saved=report_saved,
                report_saved_at=asked_at,
                visualization=visualization,
                confidence=confidence,
                recommended_actions=recommended_actions,
            )
        )
