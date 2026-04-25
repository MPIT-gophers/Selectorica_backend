"""Политика вычисления confidence payload для ответа `/api/ask`."""

from __future__ import annotations

from typing import Any


_BASE_CONFIDENCE_SCORE = 0.9
_RETRY_PENALTY = 0.15
_MIN_CONFIDENCE_SCORE = 0.35
_MAX_CONFIDENCE_SCORE = 0.95
_HIGH_CONFIDENCE_THRESHOLD = 0.8
_MEDIUM_CONFIDENCE_THRESHOLD = 0.6


def build_confidence_payload(
    used_retry: bool,
    visualization: dict[str, Any],
    intent_confidence: float = 0.9,
    assumptions: list[str] | None = None,
) -> dict[str, Any]:
    """Строит confidence score и текстовую причину для frontend."""

    score = min(_BASE_CONFIDENCE_SCORE, float(intent_confidence))
    if used_retry:
        score -= _RETRY_PENALTY

    if assumptions:
        score -= 0.05
        score = min(score, _HIGH_CONFIDENCE_THRESHOLD - 0.01)

    score = max(_MIN_CONFIDENCE_SCORE, min(_MAX_CONFIDENCE_SCORE, round(score, 2)))
    level = _get_confidence_level(score)
    reason = _build_confidence_reason(
        level=level,
        used_retry=used_retry,
        visualization=visualization,
        assumptions=assumptions or [],
    )

    return {
        "score": score,
        "level": level,
        "reason": reason,
    }


def _get_confidence_level(score: float) -> str:
    """Возвращает человекочитаемый уровень по числовому score."""

    if score >= _HIGH_CONFIDENCE_THRESHOLD:
        return "high"
    if score >= _MEDIUM_CONFIDENCE_THRESHOLD:
        return "medium"
    return "low"


def _build_confidence_reason(
    level: str,
    used_retry: bool,
    visualization: dict[str, Any],
    assumptions: list[str],
) -> str:
    """Собирает короткое объяснение факторов, повлиявших на confidence."""

    factors: list[str] = []
    if used_retry:
        factors.append("SQL потребовал повторную генерацию после ошибки.")
    if assumptions:
        factors.append("Применены предположения: " + " ".join(assumptions))

    visualization_reason = str(visualization.get("reason", "")).strip()
    visualization_type = str(visualization.get("type", "")).strip()
    if visualization_type == "table_only" or visualization_reason:
        factors.append(
            "Визуализация оценивается отдельно"
            + (f": {visualization_reason}" if visualization_reason else ".")
        )

    if factors:
        return " ".join(factors)

    if level == "high":
        return "Вопрос содержит достаточно явный аналитический смысл для построения SQL."
    if level == "medium":
        return "SQL построен успешно, но часть интерпретации опирается на эвристики."
    return "Ответ удалось построить, но уверенность в интерпретации ограничена."
