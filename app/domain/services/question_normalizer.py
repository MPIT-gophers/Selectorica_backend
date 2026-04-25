"""Нормализация терминов пользовательского вопроса к схеме данных."""

from __future__ import annotations

import re


def normalize_question_terms(question: str) -> str:
    """Заменяет разговорные термины на названия полей из модели данных."""

    normalized = question
    replacements = [
        (r"\bрегион(ы|ов|ам|ами|ах|а|е|у)?\b", "city_id"),
        (r"\bгород(а|ов|ам|ами|ах|е|у)?\b", "city_id"),
        (r"\bканал(ы|ов|ам|ами|ах|а|е|у)?\b", "status_tender"),
    ]

    for pattern, replacement in replacements:
        normalized = re.sub(pattern, replacement, normalized, flags=re.IGNORECASE)

    return normalized
