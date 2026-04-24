"""Unit-тесты доменного сервиса нормализации вопроса."""

from __future__ import annotations

import unittest

from backend.app.domain.services.question_normalizer import normalize_question_terms


class TestQuestionNormalizer(unittest.TestCase):
    """Проверяет замены бизнес-терминов на поля схемы данных."""

    def test_region_term_is_mapped_to_city_id(self) -> None:
        """Термины `регион*` должны нормализоваться в `city_id`."""

        normalized = normalize_question_terms("Покажи выручку по регионам за месяц")
        self.assertIn("city_id", normalized)

    def test_channel_term_is_mapped_to_status_tender(self) -> None:
        """Термины `канал*` должны нормализоваться в `status_tender`."""

        normalized = normalize_question_terms("Покажи поездки по каналам за 7 дней")
        self.assertIn("status_tender", normalized)


if __name__ == "__main__":
    unittest.main()

