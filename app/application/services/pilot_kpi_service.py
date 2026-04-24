"""Сервис KPI-снимка пилота на основе persisted report history."""

from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Protocol

from backend.app.infrastructure.history.sqlite_report_history_repo import ReportRecord


class ReportHistoryReader(Protocol):
    """Контракт чтения сохраненной истории отчетов для KPI-агрегации."""

    def list_reports(self, limit: int = 50) -> list[ReportRecord]:
        """Возвращает список сохраненных отчетов."""


@dataclass(frozen=True)
class PilotKpiSnapshot:
    """Структура read-only KPI-снимка для backend API."""

    generated_at: str
    report_count: int
    avg_confidence_score: float
    decision_log_complete_rate: float
    clarification_rate: float
    latest_report_at: str
    sample_question: str


class PilotKpiService:
    """Собирает business-friendly KPI из сохраненной истории отчетов."""

    _HISTORY_LIMIT = 1000

    def __init__(self, history_repo: ReportHistoryReader) -> None:
        """Сохраняет репозиторий истории, из которого строится snapshot."""

        self._history_repo = history_repo

    def get_snapshot(self) -> PilotKpiSnapshot:
        """Агрегирует persisted report history в компактный KPI-снимок."""

        reports = self._history_repo.list_reports(limit=self._HISTORY_LIMIT)
        generated_at = datetime.now(timezone.utc).isoformat()

        if not reports:
            return PilotKpiSnapshot(
                generated_at=generated_at,
                report_count=0,
                avg_confidence_score=0.0,
                decision_log_complete_rate=0.0,
                clarification_rate=0.0,
                latest_report_at="",
                sample_question="",
            )

        report_count = len(reports)
        complete_count = sum(1 for report in reports if self._has_complete_decision_log(report))
        clarification_count = sum(1 for report in reports if bool(report.refinement_trace))
        confidence_scores = [
            score
            for report in reports
            for score in [self._extract_confidence_score(report.confidence)]
            if score is not None
        ]
        latest_report = max(reports, key=self._report_sort_key)

        return PilotKpiSnapshot(
            generated_at=generated_at,
            report_count=report_count,
            avg_confidence_score=self._average(confidence_scores),
            decision_log_complete_rate=complete_count / report_count,
            clarification_rate=clarification_count / report_count,
            latest_report_at=latest_report.asked_at or "",
            sample_question=latest_report.question or "",
        )

    def _has_complete_decision_log(self, report: ReportRecord) -> bool:
        """Проверяет, что отчет содержит полный decision log для бизнес-проверок."""

        return bool(report.explain_text.strip()) and bool(report.confidence) and bool(
            report.recommended_actions
        )

    def _extract_confidence_score(self, confidence: dict[str, Any] | None) -> float | None:
        """Извлекает и нормализует confidence score из сохраненного payload."""

        if not confidence:
            return None

        raw_score = confidence.get("score")
        try:
            score = float(raw_score)
        except (TypeError, ValueError):
            return None

        if not math.isfinite(score):
            return None
        if score < 0.0 or score > 1.0:
            return None
        return score

    def _average(self, values: list[float]) -> float:
        """Возвращает среднее значение или 0.0, если в истории нет валидных скорингов."""

        if not values:
            return 0.0
        return sum(values) / len(values)

    def _report_sort_key(self, report: ReportRecord) -> tuple[datetime, str, str]:
        """Формирует ключ сортировки, чтобы latest_report_at брался из самой свежей записи."""

        parsed_asked_at = self._parse_timestamp(report.asked_at)
        return parsed_asked_at, report.asked_at or "", report.question or ""

    def _parse_timestamp(self, value: str) -> datetime:
        """Парсит ISO timestamp и безопасно откатывается к минимальному времени при ошибке."""

        if not value:
            return datetime.min.replace(tzinfo=timezone.utc)

        try:
            parsed = datetime.fromisoformat(value)
        except ValueError:
            return datetime.min.replace(tzinfo=timezone.utc)

        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)


__all__ = ["PilotKpiService", "PilotKpiSnapshot", "ReportHistoryReader"]
