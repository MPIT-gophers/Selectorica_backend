"""Pydantic-схемы HTTP-контракта `/api/ask` и pilot KPI snapshot."""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field


class AskRequest(BaseModel):
    """Схема входного запроса к `/api/ask`."""

    question: str = Field(..., description="Вопрос пользователя на русском языке.")
    refinement_trace: list[dict[str, str]] = Field(
        default_factory=list,
        description="Цепочка выбранных пользователем уточнений перед финальным запросом.",
    )


class AskResponse(BaseModel):
    """Схема успешного ответа `/api/ask`."""

    status: Literal["ok"]
    question: str
    generated_sql: str
    explain: str
    recommended_actions: list[str]
    confidence: dict[str, Any]
    estimated_total_cost: float
    columns: list[str]
    rows: list[dict[str, Any]]
    row_count: int
    report_saved: bool
    report_saved_at: str
    visualization: dict[str, Any] | None = None


class ClarificationOptionResponse(BaseModel):
    """Один вариант уточнения intent для frontend-кнопки."""

    label: str
    value: str
    description: str


class ClarificationPayloadResponse(BaseModel):
    """Payload уточнения неоднозначного пользовательского вопроса."""

    kind: str
    reason: str
    question: str
    options: list[ClarificationOptionResponse]


class ClarificationResponse(BaseModel):
    """Ответ `/api/ask`, когда SQL еще нельзя безопасно строить."""

    status: Literal["clarification_needed"]
    question: str
    confidence: dict[str, Any]
    clarification: ClarificationPayloadResponse


class PilotKpiResponse(BaseModel):
    """Read-only KPI-снимок, вычисленный из persisted report history, а не из live telemetry."""

    generated_at: str = Field(
        ...,
        description="ISO timestamp формирования snapshot. Это backend-generated время, а не метрика истории.",
    )
    report_count: int = Field(
        ...,
        description="Количество сохраненных report records, найденных в persisted report history.",
    )
    avg_confidence_score: float = Field(
        ...,
        description="Средний confidence score по сохраненным отчетам; derived from persisted report history.",
        ge=0.0,
        le=1.0,
    )
    decision_log_complete_rate: float = Field(
        ...,
        description=(
            "Доля отчетов с полным decision log (explanation, confidence, recommended actions), "
            "derived from persisted report history."
        ),
        ge=0.0,
        le=1.0,
    )
    clarification_rate: float = Field(
        ...,
        description="Доля отчетов с non-empty refinement trace, derived from persisted report history.",
        ge=0.0,
        le=1.0,
    )
    latest_report_at: str = Field(
        default="",
        description="Время самой свежей сохраненной записи или пустая строка, если history пуста.",
    )
    sample_question: str = Field(
        default="",
        description="Вопрос из самой свежей сохраненной записи или пустая строка, если history пуста.",
    )

