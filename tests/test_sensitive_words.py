from __future__ import annotations

import unittest


class SensitiveWordModelTests(unittest.TestCase):
    def test_sensitive_word_model_has_expected_schema(self) -> None:
        from app.services.sensitive_word_service import normalize_sensitive_word
        from app.db.models import SensitiveWordModel

        id_column = SensitiveWordModel.__table__.columns["id"]
        enabled_column = SensitiveWordModel.__table__.columns["enabled"]
        created_at_column = SensitiveWordModel.__table__.columns["created_at"]
        updated_at_column = SensitiveWordModel.__table__.columns["updated_at"]

        self.assertEqual(normalize_sensitive_word("  即納  "), "即納")
        self.assertEqual(SensitiveWordModel.__tablename__, "lt_sensitive_words")
        self.assertIn("id", SensitiveWordModel.__table__.columns)
        self.assertIn("word", SensitiveWordModel.__table__.columns)
        self.assertIn("enabled", SensitiveWordModel.__table__.columns)
        self.assertIn("created_at", SensitiveWordModel.__table__.columns)
        self.assertIn("updated_at", SensitiveWordModel.__table__.columns)
        self.assertTrue(id_column.primary_key)
        self.assertTrue(id_column.autoincrement)
        self.assertEqual(SensitiveWordModel.__table__.columns["word"].type.length, 500)
        self.assertFalse(SensitiveWordModel.__table__.columns["word"].nullable)
        self.assertFalse(enabled_column.nullable)
        self.assertIsNotNone(enabled_column.default)
        self.assertTrue(enabled_column.default.arg)
        self.assertIsNotNone(enabled_column.server_default)
        self.assertEqual(str(enabled_column.server_default.arg), "1")
        self.assertFalse(created_at_column.nullable)
        self.assertIsNotNone(created_at_column.server_default)
        self.assertFalse(updated_at_column.nullable)
        self.assertIsNotNone(updated_at_column.server_default)
        self.assertIsNotNone(updated_at_column.onupdate)
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

    def test_overlapping_literal_words_are_deduplicated_and_applied_longest_first(self) -> None:
        from app.services.sensitive_word_service import sanitize_sensitive_text

        self.assertEqual(
            sanitize_sensitive_text(
                "期間限定",
                [" 限定 ", "期間限定", "限定", "期間限定"],
            ),
            "",
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

    def test_payload_sanitizer_only_cleans_root_name_but_keeps_nested_generic_names(self) -> None:
        from app.services.sensitive_word_service import sanitize_product_payload

        payload = {
            "name": "即納 春物",
            "metadata": {
                "name": "即納 サイズ名",
                "tagline": "即納 おすすめ",
            },
            "attributes": [
                {"name": "即納 カラー", "value": "赤"},
                {"name": "即納 素材", "value": "綿"},
            ],
            "item": {
                "itemName": "即納 ワンピース",
                "subtitle": "即納 人気",
            },
        }

        cleaned, changed = sanitize_product_payload(payload, ["即納"])

        self.assertTrue(changed)
        self.assertEqual(cleaned["name"], "春物")
        self.assertEqual(cleaned["metadata"]["name"], "即納 サイズ名")
        self.assertEqual(cleaned["attributes"][0]["name"], "即納 カラー")
        self.assertEqual(cleaned["attributes"][1]["name"], "即納 素材")
        self.assertEqual(cleaned["metadata"]["tagline"], "おすすめ")
        self.assertEqual(cleaned["item"]["itemName"], "ワンピース")
        self.assertEqual(cleaned["item"]["subtitle"], "人気")


if __name__ == "__main__":
    unittest.main()
