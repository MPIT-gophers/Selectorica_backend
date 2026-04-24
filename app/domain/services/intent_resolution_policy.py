"""Deterministic policy для разрешения неоднозначных NL2SQL intent-запросов."""

from __future__ import annotations

import re
from dataclasses import dataclass


@dataclass(frozen=True)
class ClarificationOption:
    """Один вариант уточнения, который frontend покажет как кнопку."""

    label: str
    value: str
    description: str


@dataclass(frozen=True)
class Clarification:
    """Payload уточнения, возвращаемый вместо генерации SQL."""

    kind: str
    reason: str
    question: str
    options: list[ClarificationOption]


@dataclass(frozen=True)
class IntentResolution:
    """Результат проверки intent перед NL2SQL-генерацией."""

    needs_clarification: bool
    clarification: Clarification | None = None


class IntentResolver:
    """Проверяет вопрос на неоднозначность метрики и периода."""

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

    def resolve(self, question: str) -> IntentResolution:
        """Возвращает уточнение, если вопрос нельзя безопасно интерпретировать."""

        normalized = self._normalize(question)
        if self._has_ambiguous_sales_metric(normalized):
            return IntentResolution(True, self._build_metric_clarification(question))
        if self._is_analytic_question(normalized) and not self._has_period(normalized):
            return IntentResolution(True, self._build_period_clarification(question))
        return IntentResolution(False)

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

        return any(token in question for token in self._PERIOD_TOKENS)

    def _build_metric_clarification(self, question: str) -> Clarification:
        """Создает варианты уточнения для неоднозначного термина `продажи`."""

        return Clarification(
            kind="metric",
            reason="Термин 'продажи' может означать разные бизнес-метрики.",
            question="Что считать продажами?",
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

