"""Unit tests for confidence policy."""

from __future__ import annotations

import unittest

from backend.app.domain.services.confidence_policy import build_confidence_payload


class TestConfidencePolicy(unittest.TestCase):
    """Checks confidence score boundaries and retry penalty behavior."""

    def test_high_confidence_is_returned_for_clear_queries(self) -> None:
        """High visual confidence without retry should stay in the high bucket."""

        payload = build_confidence_payload(
            used_retry=False,
            visualization={"confidence": 0.95},
        )

        self.assertEqual(payload["level"], "high")
        self.assertEqual(payload["score"], 0.9)
        self.assertIn("достаточно", payload["reason"])

    def test_medium_confidence_stays_between_thresholds(self) -> None:
        """Scores in the middle bucket should be labeled as medium."""

        payload = build_confidence_payload(
            used_retry=False,
            visualization={"confidence": 0.79},
        )

        self.assertEqual(payload["level"], "medium")
        self.assertEqual(payload["score"], 0.79)

    def test_high_confidence_includes_exact_high_threshold(self) -> None:
        """The high bucket should include the exact 0.80 boundary."""

        payload = build_confidence_payload(
            used_retry=False,
            visualization={"confidence": 0.8},
        )

        self.assertEqual(payload["level"], "high")
        self.assertEqual(payload["score"], 0.8)

    def test_medium_confidence_starts_at_exact_lower_boundary(self) -> None:
        """The medium bucket should start at the exact 0.60 threshold."""

        payload = build_confidence_payload(
            used_retry=False,
            visualization={"confidence": 0.6},
        )

        self.assertEqual(payload["level"], "medium")
        self.assertEqual(payload["score"], 0.6)

    def test_low_confidence_is_returned_below_medium_threshold(self) -> None:
        """Scores below the medium threshold should be labeled as low."""

        payload = build_confidence_payload(
            used_retry=False,
            visualization={"confidence": 0.59},
        )

        self.assertEqual(payload["level"], "low")
        self.assertEqual(payload["score"], 0.59)

    def test_retry_penalty_pushes_score_down_before_bucket_selection(self) -> None:
        """Retry should lower score and can move the result into medium confidence."""

        payload = build_confidence_payload(
            used_retry=True,
            visualization={"confidence": 0.95},
        )

        self.assertEqual(payload["level"], "medium")
        self.assertEqual(payload["score"], 0.75)

    def test_retry_reduces_score_for_same_visualization_confidence(self) -> None:
        """Retry should reduce score for the same visualization confidence."""

        without_retry = build_confidence_payload(
            used_retry=False,
            visualization={"confidence": 0.9},
        )
        with_retry = build_confidence_payload(
            used_retry=True,
            visualization={"confidence": 0.9},
        )

        self.assertEqual(without_retry["score"], 0.9)
        self.assertEqual(with_retry["score"], 0.75)
        self.assertGreater(without_retry["score"], with_retry["score"])

    def test_score_is_clamped_to_minimum(self) -> None:
        """Very low visualization confidence must not drop below the minimum clamp."""

        payload = build_confidence_payload(
            used_retry=True,
            visualization={"confidence": 0.1},
        )

        self.assertEqual(payload["level"], "low")
        self.assertEqual(payload["score"], 0.35)

    def test_score_never_exceeds_maximum_clamp(self) -> None:
        """The resulting score should never exceed the documented maximum bound."""

        payload = build_confidence_payload(
            used_retry=False,
            visualization={"confidence": 1.5},
        )

        self.assertLessEqual(payload["score"], 0.95)
        self.assertEqual(payload["level"], "high")


if __name__ == "__main__":
    unittest.main()
