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
        "param_name": clarification.param_name,
        "reason_code": clarification.reason_code,
        "required": clarification.required,
        "reason": clarification.reason,
        "question": clarification.question,
        "allow_free_input": clarification.allow_free_input,
        "free_input_placeholder": clarification.free_input_placeholder,
        "default_value": clarification.default_value,
        "default_label": clarification.default_label,
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

    clarification = dict(classifier_result["clarification"])
    clarification.setdefault("param_name", clarification.get("kind", ""))
    clarification.setdefault(
        "reason_code",
        f"{str(clarification.get('kind', 'intent')).upper()}_AMBIGUOUS",
    )
    clarification.setdefault("required", True)
    clarification.setdefault("allow_free_input", True)
    clarification.setdefault("free_input_placeholder", "")
    clarification.setdefault("default_value", None)
    clarification.setdefault("default_label", "")
    return classifier_result["confidence"], clarification
