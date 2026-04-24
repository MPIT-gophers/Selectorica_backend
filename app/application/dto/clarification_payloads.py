"""Сборка payload уточнений для результата `/api/ask`."""

from __future__ import annotations

from typing import Any


def build_rule_clarification_confidence() -> dict[str, Any]:
    """Возвращает стандартный confidence для deterministic clarification."""

    return {
        "score": 0.35,
        "level": "low",
        "reason": "Для безопасной генерации SQL нужно уточнить смысл запроса.",
    }


def build_rule_clarification_payload(clarification: Any) -> dict[str, Any]:
    """Собирает payload уточнения из доменной структуры intent resolver."""

    return {
        "kind": clarification.kind,
        "reason": clarification.reason,
        "question": clarification.question,
        "options": [
            {
                "label": option.label,
                "value": option.value,
                "description": option.description,
            }
            for option in clarification.options
        ],
    }


def build_classifier_clarification_payload(
    classifier_result: dict[str, Any],
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Возвращает confidence и clarification payload из classifier-результата."""

    return classifier_result["confidence"], classifier_result["clarification"]

