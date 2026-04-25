"""Deterministic policy для разрешения неоднозначных NL2SQL intent-запросов."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class ClarificationOption:
    """Один вариант уточнения, который frontend покажет как кнопку."""

    label: str
    value: str
    description: str


@dataclass(frozen=True)
class ParameterSpec:
    """Описание параметра аналитического запроса и политики его уточнения."""

    name: str
    required: bool
    default: str | None = None
    default_label: str = ""
    can_infer_from_context: bool = True
    clarify_if_missing: bool = False
    allow_free_input: bool = True


@dataclass(frozen=True)
class Clarification:
    """Payload уточнения, возвращаемый вместо генерации SQL."""

    kind: str
    reason: str
    question: str
    options: list[ClarificationOption]
    param_name: str = ""
    reason_code: str = ""
    required: bool = True
    allow_free_input: bool = True
    free_input_placeholder: str = ""
    default_value: str | None = None
    default_label: str = ""


@dataclass(frozen=True)
class IntentResolution:
    """Результат проверки intent перед NL2SQL-генерацией."""

    needs_clarification: bool
    clarification: Clarification | None = None
    effective_question: str = ""
    resolved_params: dict[str, Any] = field(default_factory=dict)
    assumptions: list[str] = field(default_factory=list)
    decision_events: list[dict[str, Any]] = field(default_factory=list)
    intent_confidence: float = 0.9


class IntentResolver:
    """Проверяет вопрос на неоднозначность метрики и периода."""

    _PARAMETER_SPECS = {
        "date_range": ParameterSpec(
            name="date_range",
            required=False,
            default="last_7_days",
            default_label="последние 7 дней",
            can_infer_from_context=True,
            clarify_if_missing=False,
            allow_free_input=True,
        )
    }

    _ANALYTIC_TOKENS = (
        "покажи",
        "посчитай",
        "сколько",
        "выручк",
        "продаж",
        "поездк",
        "заказ",
        "отмен",
        "средний чек",
    )
    _PERIOD_TOKENS = (
        "сегодня",
        "вчера",
        "недел",
        "7 дней",
        "30 дней",
        "месяц",
        "квартал",
        "год",
        "за период",
        "последн",
        "текущ",
        "прошл",
        "май",
        "апрел",
        "январ",
        "феврал",
        "март",
        "июн",
        "июл",
        "август",
        "сентябр",
        "октябр",
        "ноябр",
        "декабр",
    )
    _EXPLICIT_METRIC_TOKENS = (
        "выручк",
        "доход",
        "revenue",
        "количество",
        "число",
        "поездк",
        "заказ",
        "средний чек",
    )

    def resolve(
        self,
        question: str,
        context: dict[str, Any] | None = None,
    ) -> IntentResolution:
        """Возвращает уточнение, если вопрос нельзя безопасно интерпретировать."""

        normalized = self._normalize(question)
        if self._has_ambiguous_sales_metric(normalized):
            return IntentResolution(
                True,
                self._build_metric_clarification(question),
                effective_question=question.strip(),
                intent_confidence=0.35,
            )

        if self._is_analytic_question(normalized) and not self._has_period(normalized):
            if self._requires_explicit_period(normalized):
                return IntentResolution(
                    True,
                    self._build_period_clarification(question),
                    effective_question=question.strip(),
                    intent_confidence=0.4,
                )
            return self._resolve_missing_period(question, context or {})

        return IntentResolution(
            needs_clarification=False,
            effective_question=question.strip(),
            resolved_params=self._build_explicit_period_param(normalized),
        )

    def _has_ambiguous_sales_metric(self, question: str) -> bool:
        """Определяет `продажи` без явной бизнес-метрики."""

        return "продаж" in question and not any(
            token in question for token in self._EXPLICIT_METRIC_TOKENS
        )

    def _is_analytic_question(self, question: str) -> bool:
        """Проверяет, похож ли вопрос на аналитический запрос к данным."""

        return any(token in question for token in self._ANALYTIC_TOKENS)

    def _has_period(self, question: str) -> bool:
        """Проверяет наличие явного периода в вопросе."""

        if any(token in question for token in self._PERIOD_TOKENS):
            return True
        return bool(
            re.search(r"\b\d{1,2}[./-]\d{1,2}([./-]\d{2,4})?\b", question)
            or re.search(r"\bq[1-4]\b", question, flags=re.IGNORECASE)
        )

    def _requires_explicit_period(self, question: str) -> bool:
        """Определяет случаи, где дефолтный период может исказить сравнение."""

        return any(
            token in question
            for token in (
                "сравн",
                "относительно",
                "план",
                "day-over-day",
                "week-over-week",
                "dod",
                "wow",
            )
        )

    def _resolve_missing_period(
        self,
        question: str,
        context: dict[str, Any],
    ) -> IntentResolution:
        """Разрешает отсутствующий период через контекст или безопасный дефолт."""

        context_period = self._extract_context_period(context)
        if context_period:
            value = context_period["value"]
            label = context_period["label"]
            phrase = self._date_range_phrase(value=value, label=label)
            return IntentResolution(
                needs_clarification=False,
                effective_question=self._append_period(question, phrase),
                resolved_params={
                    "date_range": {
                        "value": value,
                        "label": label,
                        "source": "context",
                    }
                },
                assumptions=[
                    f"Период не указан, использую {label} из предыдущего запроса."
                ],
                decision_events=[
                    {
                        "type": "context_applied",
                        "param_name": "date_range",
                        "reason_code": "DATE_RANGE_FROM_CONTEXT",
                        "value": value,
                    }
                ],
                intent_confidence=0.8,
            )

        spec = self._PARAMETER_SPECS["date_range"]
        phrase = self._date_range_phrase(value=spec.default or "", label=spec.default_label)
        return IntentResolution(
            needs_clarification=False,
            effective_question=self._append_period(question, phrase),
            resolved_params={
                "date_range": {
                    "value": spec.default,
                    "label": spec.default_label,
                    "source": "default",
                }
            },
            assumptions=[
                f"Период не указан, использую безопасный дефолт: {spec.default_label}."
            ],
            decision_events=[
                {
                    "type": "default_applied",
                    "param_name": "date_range",
                    "reason_code": "DATE_RANGE_DEFAULTED",
                    "value": spec.default,
                }
            ],
            intent_confidence=0.75,
        )

    def _extract_context_period(self, context: dict[str, Any]) -> dict[str, str] | None:
        """Достает период из предыдущего или сценарного контекста запроса."""

        for section_name in ("previous_params", "default_params"):
            section = context.get(section_name)
            if not isinstance(section, dict):
                continue
            candidate = section.get("date_range")
            if not isinstance(candidate, dict):
                continue
            value = candidate.get("value")
            label = candidate.get("label")
            if isinstance(value, str) and value.strip():
                return {
                    "value": value.strip(),
                    "label": str(label or self._date_range_label(value)).strip(),
                }
        return None

    def _build_explicit_period_param(self, question: str) -> dict[str, Any]:
        """Возвращает marker явного периода, если он найден в вопросе."""

        if not self._has_period(question):
            return {}
        return {
            "date_range": {
                "value": "explicit",
                "label": "период из запроса",
                "source": "explicit",
            }
        }

    def _append_period(self, question: str, period_phrase: str) -> str:
        """Добавляет нормализованный период к вопросу для SQL-генератора."""

        cleaned = question.strip().rstrip(".")
        return f"{cleaned} {period_phrase}".strip()

    def _date_range_phrase(self, value: str, label: str) -> str:
        """Преобразует значение периода в русскую фразу для NL2SQL-вопроса."""

        mapping = {
            "today": "за сегодня",
            "yesterday": "за вчера",
            "last_7_days": "за последние 7 дней",
            "last_30_days": "за последние 30 дней",
            "current_month": "за текущий месяц",
        }
        if value in mapping:
            return mapping[value]
        normalized_label = label.strip()
        if normalized_label.lower().startswith("за "):
            return normalized_label
        return f"за {normalized_label}"

    def _date_range_label(self, value: str) -> str:
        """Возвращает человекочитаемый label периода по внутреннему value."""

        labels = {
            "today": "сегодня",
            "yesterday": "вчера",
            "last_7_days": "последние 7 дней",
            "last_30_days": "последние 30 дней",
            "current_month": "текущий месяц",
        }
        return labels.get(value, value)

    def _build_metric_clarification(self, question: str) -> Clarification:
        """Создает варианты уточнения для неоднозначного термина `продажи`."""

        return Clarification(
            kind="metric",
            reason="Термин 'продажи' может означать разные бизнес-метрики.",
            question="Что считать продажами?",
            param_name="metric",
            reason_code="METRIC_AMBIGUOUS",
            required=True,
            allow_free_input=True,
            free_input_placeholder="Например: выручка, завершенные поездки или средний чек",
            options=[
                ClarificationOption(
                    label="Выручка",
                    value=self._replace_sales_term(question, "выручку"),
                    description="Сумма price_order_local по завершенным заказам.",
                ),
                ClarificationOption(
                    label="Поездки",
                    value=self._replace_sales_term(
                        question, "количество завершенных поездок"
                    ),
                    description="Количество заказов со status_order = done.",
                ),
                ClarificationOption(
                    label="Средний чек",
                    value=self._replace_sales_term(question, "средний чек"),
                    description="Средняя выручка на завершенную поездку.",
                ),
            ],
        )

    def _build_period_clarification(self, question: str) -> Clarification:
        """Создает варианты уточнения периода для аналитического запроса."""

        cleaned = question.strip().rstrip(".")
        return Clarification(
            kind="period",
            reason="В аналитическом запросе не указан период, поэтому результат может быть неоднозначным.",
            question="За какой период показать данные?",
            param_name="date_range",
            reason_code="DATE_RANGE_REQUIRED",
            required=True,
            allow_free_input=True,
            free_input_placeholder="Например: последние 14 дней или с 1 по 7 апреля",
            default_value="last_7_days",
            default_label="последние 7 дней",
            options=[
                ClarificationOption(
                    "7 дней",
                    f"{cleaned} за последние 7 дней",
                    "Подходит для оперативного анализа.",
                ),
                ClarificationOption(
                    "30 дней",
                    f"{cleaned} за последние 30 дней",
                    "Подходит для месячного обзора.",
                ),
                ClarificationOption(
                    "Текущий месяц",
                    f"{cleaned} за текущий месяц",
                    "Календарный месяц с начала месяца.",
                ),
            ],
        )

    def _replace_sales_term(self, question: str, replacement: str) -> str:
        """Заменяет первое вхождение `продажи` на выбранную бизнес-метрику."""

        return re.sub(r"продаж[аиу]?", replacement, question, count=1, flags=re.IGNORECASE)

    def _normalize(self, question: str) -> str:
        """Приводит вопрос к виду для простых словарных проверок."""

        return " ".join(question.lower().strip().split())
