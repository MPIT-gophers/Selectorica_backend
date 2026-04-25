"""Router для endpoint `/api/ask`."""

from __future__ import annotations

from typing import Any, Union

from fastapi import APIRouter, Depends, HTTPException, status

from backend.app.application.services.ask_service import AskService
from backend.app.application.services.pilot_kpi_service import PilotKpiService

from .dependencies import get_ask_service, get_pilot_kpi_service
from .schemas import AskRequest, AskResponse, ClarificationResponse, PilotKpiResponse

router = APIRouter(tags=["ask", "pilot"])

_ERROR_RECOMMENDED_ACTIONS: dict[str, list[str]] = {
    "SQL_COST_LIMIT_EXCEEDED": [
        "Сузьте период и повторите запрос на более коротком окне.",
        "Добавьте фильтр по городу, смене или каналу, чтобы уменьшить объем выборки.",
    ],
    "SQL_MUTATION_BLOCKED": [
        "Переформулируйте вопрос как аналитический read-only запрос без изменений данных.",
        "Используйте готовый вопрос из Ops или Finance, если нужен безопасный шаблон.",
    ],
    "SQL_MULTI_STATEMENT_BLOCKED": [
        "Переформулируйте вопрос как один аналитический read-only запрос.",
        "Уберите дополнительные SQL-инструкции и повторите вопрос обычным языком.",
    ],
    "SQL_NOT_READ_ONLY": [
        (
            "Задайте вопрос как аналитический запрос на чтение данных, "
            "без команд изменения схемы или прав."
        ),
        "Сформулируйте ожидаемую метрику, период и разрез вместо SQL-инструкции.",
    ],
    "SQL_FUNCTION_BLOCKED": [
        "Уберите технические SQL-функции из запроса и опишите бизнес-метрику обычным языком.",
        "Используйте безопасный аналитический вопрос по заказам: период, город, статус или сумма.",
    ],
    "SQL_CONTEXT_INSUFFICIENT": [
        "Назовите целевую метрику и период сравнения прямо в вопросе.",
        "Уточните допустимый разрез: город, час, день, канал или смена.",
    ],
    "SQL_EXPLAIN_FAILED": [
        (
            "Сузьте вопрос до одной метрики и одного периода, "
            "чтобы система могла оценить стоимость запроса."
        ),
        "Проверьте, что в вопросе используются доступные поля заказов: город, статус, дата или сумма.",
    ],
    "SQL_EXECUTION_FAILED": [
        "Проверьте, что вопрос не ссылается на отсутствующие поля или нестандартные разрезы.",
        "Попробуйте один из готовых Ops или Finance сценариев и затем уточните его.",
    ],
    "SQL_RUNTIME_CONFIG_INVALID": [
        "Проверьте backend env: runtime должен использовать READONLY_DB_USER и READONLY_DB_PASSWORD.",
        "Для локальной отладки можно явно включить ALLOW_RUNTIME_DB_ADMIN_FALLBACK=1.",
    ],
    "SQL_PARSE_ERROR": [
        "Уберите двусмысленные формулировки и попросите агрегированный срез по orders.",
        "Проверьте, что вопрос не содержит смешанных инструкций и лишних условий.",
    ],
    "INVALID_QUESTION": [
        "Проверьте, что вопрос не пустой и описывает конкретную бизнес-метрику.",
        "Используйте один понятный запрос вместо нескольких инструкций в одном тексте.",
    ],
    "UNEXPECTED_ERROR": [
        "Повторите запрос позже.",
        "Если проблема повторяется, проверьте backend logs.",
    ],
}

_DEFAULT_ERROR_RECOMMENDED_ACTIONS = [
    "Попробуйте переформулировать вопрос короче и конкретнее.",
    "Если проблема повторяется, вернитесь на главный экран и запустите готовый сценарий.",
]


def _build_error_detail(error_code: str, message: str) -> dict[str, Any]:
    """Собирает стабильный error payload, чтобы frontend не додумывал action layer."""

    return {
        "error_code": error_code,
        "message": message,
        "recommended_actions": list(
            _ERROR_RECOMMENDED_ACTIONS.get(
                error_code,
                _DEFAULT_ERROR_RECOMMENDED_ACTIONS,
            )
        ),
    }


@router.post(
    "/api/ask",
    response_model=Union[AskResponse, ClarificationResponse],
    status_code=status.HTTP_200_OK,
)
async def ask_endpoint(
    payload: AskRequest,
    service: AskService = Depends(get_ask_service),
) -> AskResponse | ClarificationResponse:
    """Принимает вопрос, строит SQL/Explain и возвращает данные запроса."""

    try:
        result = service.ask(
            payload.question,
            refinement_trace=payload.refinement_trace,
            context=payload.context,
        )
    except Exception as error:
        if hasattr(error, "error_code") and hasattr(error, "message"):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=_build_error_detail(
                    str(getattr(error, "error_code")),
                    str(getattr(error, "message")),
                ),
            ) from error
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=_build_error_detail(
                "UNEXPECTED_ERROR",
                (
                    "Непредвиденная ошибка API. Повторите запрос позже "
                    "или проверьте backend logs."
                ),
            ),
        ) from error

    if result.status == "clarification_needed":
        return ClarificationResponse(
            status="clarification_needed",
            question=result.question,
            confidence=result.confidence or {},
            clarification=result.clarification,
            assumptions=result.assumptions,
            resolved_params=result.resolved_params,
            decision_events=result.decision_events,
        )

    return AskResponse(
        status="ok",
        question=result.question,
        generated_sql=result.generated_sql,
        explain=result.explain,
        recommended_actions=result.recommended_actions,
        confidence=result.confidence or {},
        estimated_total_cost=result.estimated_total_cost,
        columns=result.columns or [],
        rows=result.rows or [],
        row_count=result.row_count,
        truncated=result.truncated,
        report_saved=result.report_saved,
        report_saved_at=result.report_saved_at,
        visualization=result.visualization,
        assumptions=result.assumptions,
        resolved_params=result.resolved_params,
        decision_events=result.decision_events,
    )


@router.get(
    "/api/pilot/kpi",
    response_model=PilotKpiResponse,
    status_code=status.HTTP_200_OK,
)
async def pilot_kpi_endpoint(
    service: PilotKpiService = Depends(get_pilot_kpi_service),
) -> PilotKpiResponse:
    """Возвращает бизнес-friendly KPI snapshot, derived from persisted report history."""

    snapshot = service.get_snapshot()
    return PilotKpiResponse(
        generated_at=snapshot.generated_at,
        report_count=snapshot.report_count,
        avg_confidence_score=snapshot.avg_confidence_score,
        decision_log_complete_rate=snapshot.decision_log_complete_rate,
        clarification_rate=snapshot.clarification_rate,
        latest_report_at=snapshot.latest_report_at,
        sample_question=snapshot.sample_question,
    )
