from __future__ import annotations

import unittest


class SensitiveWordModelTests(unittest.TestCase):
    def test_sensitive_word_model_has_expected_schema(self) -> None:
        from app.services.sensitive_word_service import normalize_sensitive_word
        from app.db.models import SensitiveWordModel

        self.assertEqual(normalize_sensitive_word("  即納  "), "即納")
        self.assertEqual(SensitiveWordModel.__tablename__, "lt_sensitive_words")
        self.assertIn("word", SensitiveWordModel.__table__.columns)
        self.assertIn("enabled", SensitiveWordModel.__table__.columns)
        self.assertEqual(SensitiveWordModel.__table__.columns["word"].type.length, 500)
        self.assertFalse(SensitiveWordModel.__table__.columns["word"].nullable)
        self.assertFalse(SensitiveWordModel.__table__.columns["enabled"].nullable)
        self.assertTrue(
            any(constraint.name == "uq_lt_sensitive_word" for constraint in SensitiveWordModel.__table__.constraints)
        )


class SensitiveWordSanitizerTests(unittest.TestCase):
    def test_removes_normal_words_and_collapses_whitespace(self) -> None:
        from app.services.sensitive_word_service import sanitize_sensitive_text

        self.assertEqual(
            sanitize_sensitive_text(
                "楽天1位  春物  即納",
                ["楽天1位", "即納"],
            ),
            "春物",
        )

    def test_empty_bracket_rule_removes_every_bracketed_segment(self) -> None:
        from app.services.sensitive_word_service import sanitize_sensitive_text

        self.assertEqual(
            sanitize_sensitive_text(
                "【楽天1位】【日本国内発送】 春物",
                ["【】"],
            ),
            "春物",
        )

    def test_payload_sanitizer_updates_title_and_tagline_fields_recursively(self) -> None:
        from app.services.sensitive_word_service import sanitize_product_payload

        payload = {
            "title": "【楽天1位】 春物",
            "itemName": "【楽天1位】 春物",
            "tagline": "即納 おすすめ",
            "item": {"subtitle": "【期間限定】 即納"},
            "description": "【期間限定】 即納",
        }

        cleaned, changed = sanitize_product_payload(payload, ["【】", "即納"])

        self.assertTrue(changed)
        self.assertEqual(cleaned["title"], "春物")
        self.assertEqual(cleaned["itemName"], "春物")
        self.assertEqual(cleaned["tagline"], "おすすめ")
        self.assertEqual(cleaned["item"]["subtitle"], "")
        self.assertEqual(cleaned["description"], "【期間限定】 即納")


if __name__ == "__main__":
    unittest.main()
