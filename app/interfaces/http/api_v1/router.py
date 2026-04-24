"""Router для endpoint `/api/ask`."""

from __future__ import annotations

from typing import Any, Union

from fastapi import APIRouter, Depends, HTTPException, status

from backend.app.application.services.ask_service import AskService
from backend.app.application.services.pilot_kpi_service import PilotKpiService

from .dependencies import get_ask_service, get_pilot_kpi_service
from .schemas import AskRequest, AskResponse, ClarificationResponse, PilotKpiResponse

router = APIRouter(tags=["ask", "pilot"])


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
        )
    except Exception as error:
        if hasattr(error, "error_code") and hasattr(error, "message"):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail={
                    "error_code": str(getattr(error, "error_code")),
                    "message": str(getattr(error, "message")),
                },
            ) from error
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail={
                "error_code": "UNEXPECTED_ERROR",
                "message": repr(error),
            },
        ) from error

    if result.status == "clarification_needed":
        return ClarificationResponse(
            status="clarification_needed",
            question=result.question,
            confidence=result.confidence or {},
            clarification=result.clarification,
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
        report_saved=result.report_saved,
        report_saved_at=result.report_saved_at,
        visualization=result.visualization,
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
