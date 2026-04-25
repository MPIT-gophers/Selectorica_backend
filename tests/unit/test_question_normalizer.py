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

    def test_city_term_is_mapped_to_city_id(self) -> None:
        """Термины `город*` должны нормализоваться в `city_id`."""

        normalized = normalize_question_terms("Покажи выручку по городам за месяц")
        self.assertIn("city_id", normalized)

    def test_channel_term_is_mapped_to_status_tender(self) -> None:
        """Термины `канал*` должны нормализоваться в `status_tender`."""

        normalized = normalize_question_terms("Покажи поездки по каналам за 7 дней")
        self.assertIn("status_tender", normalized)

    def test_region_genitive_term_is_mapped_to_city_id(self) -> None:
        """Форма `региона` должна нормализоваться в MVP-разрез `city_id`."""

        normalized = normalize_question_terms("Покажи выручку одного региона")
        self.assertIn("city_id", normalized)

    def test_channel_genitive_term_is_mapped_to_status_tender(self) -> None:
        """Форма `канала` должна нормализоваться в MVP-разрез `status_tender`."""

        normalized = normalize_question_terms("Покажи долю одного канала")
        self.assertIn("status_tender", normalized)

    def test_adjective_like_words_are_not_mapped(self) -> None:
        """Прилагательные рядом с терминами не должны ломаться заменой."""

        question = "Покажи региональную, городскую и канальную динамику"

        normalized = normalize_question_terms(question)

        self.assertEqual(normalized, question)


if __name__ == "__main__":
    unittest.main()
