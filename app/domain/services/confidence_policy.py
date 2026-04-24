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
) -> dict[str, Any]:
    """Строит confidence score и текстовую причину для frontend."""

    score = _BASE_CONFIDENCE_SCORE
    if used_retry:
        score -= _RETRY_PENALTY

    visualization_confidence = float(visualization.get("confidence", 0.7))
    score = min(score, visualization_confidence)
    score = max(_MIN_CONFIDENCE_SCORE, min(_MAX_CONFIDENCE_SCORE, round(score, 2)))

    if score >= _HIGH_CONFIDENCE_THRESHOLD:
        level = "high"
        reason = "Вопрос содержит достаточно явный аналитический смысл для построения SQL."
    elif score >= _MEDIUM_CONFIDENCE_THRESHOLD:
        level = "medium"
        reason = "SQL построен успешно, но часть интерпретации опирается на эвристики."
    else:
        level = "low"
        reason = "Ответ удалось построить, но уверенность в интерпретации ограничена."

    return {
        "score": score,
        "level": level,
        "reason": reason,
    }
