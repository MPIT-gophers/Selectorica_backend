"""Политика выбора спецификации визуализации по результату запроса."""

from __future__ import annotations

from datetime import datetime
from typing import Any


def build_visualization_spec(
    question: str,
    columns: list[str],
    rows: list[dict[str, Any]],
) -> dict[str, Any]:
    """Возвращает безопасную спецификацию графика для frontend."""

    if not rows or not columns:
        return {
            "type": "table_only",
            "reason": "В результате нет данных для графика.",
            "confidence": 1.0,
        }

    numeric_fields = [field for field in columns if _is_numeric_field(field, rows)]
    time_fields = [field for field in columns if _is_time_field(field, rows)]
    category_fields = [field for field in columns if _is_category_field(field, rows)]

    if time_fields and numeric_fields and _looks_temporal_question(question):
        return {
            "type": "line",
            "x_field": time_fields[0],
            "y_field": numeric_fields[0],
            "reason": "Вопрос про динамику во времени, выбран line chart.",
            "confidence": 0.9,
        }

    if category_fields and numeric_fields:
        return {
            "type": "bar",
            "x_field": category_fields[0],
            "y_field": numeric_fields[0],
            "reason": "Вопрос про сравнение категорий, выбран bar chart.",
            "confidence": 0.8,
        }

    return {
        "type": "table_only",
        "reason": "Недостаточно надежных полей для корректной визуализации.",
        "confidence": 0.7,
    }


def _is_numeric_field(field: str, rows: list[dict[str, Any]]) -> bool:
    """Проверяет пригодность поля как метрики (ось Y)."""

    lowered = field.lower()
    if "id" in lowered:
        return False

    sample_values = [row.get(field) for row in rows[:100]]
    numeric_values = [value for value in sample_values if isinstance(value, (int, float))]
    return len(numeric_values) >= max(1, len(sample_values) // 3)


def _is_time_field(field: str, rows: list[dict[str, Any]]) -> bool:
    """Проверяет пригодность поля как временной оси X."""

    lowered = field.lower()
    if "date" in lowered or "time" in lowered or "timestamp" in lowered:
        return True

    for row in rows[:20]:
        value = row.get(field)
        if not isinstance(value, str):
            continue
        candidate = value.replace("Z", "+00:00")
        try:
            datetime.fromisoformat(candidate)
            return True
        except ValueError:
            continue
    return False


def _is_category_field(field: str, rows: list[dict[str, Any]]) -> bool:
    """Проверяет пригодность поля как категориальной оси X."""

    lowered = field.lower()
    if "id" in lowered:
        return False

    values = [row.get(field) for row in rows[:300]]
    non_null = [value for value in values if value is not None]
    if not non_null:
        return False

    if not all(isinstance(value, str) for value in non_null):
        return False

    cardinality = len(set(non_null))
    return 2 <= cardinality <= 20


def _looks_temporal_question(question: str) -> bool:
    """Эвристика временной аналитики по тексту вопроса."""

    q = question.lower()
    temporal_tokens = (
        "день",
        "дня",
        "дням",
        "недел",
        "месяц",
        "год",
        "динам",
        "тренд",
        "time",
    )
    return any(token in q for token in temporal_tokens)

