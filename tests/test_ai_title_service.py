from __future__ import annotations

import json
import unittest
from unittest.mock import MagicMock

from app.services import ai_title_service
from app.services.user_service import normalize_permissions


class AiTitleServiceTests(unittest.TestCase):
    def test_ai_manage_is_a_supported_assignable_permission(self) -> None:
        self.assertEqual(normalize_permissions(["ai.manage"]), ["ai.manage"])

    def test_provider_catalog_includes_generic_openai_and_major_providers(self) -> None:
        values = {item["value"] for item in ai_title_service.provider_catalog()}

        self.assertTrue({"custom_openai", "aliyun", "openai", "anthropic", "gemini", "openrouter"} <= values)

    def test_extract_stream_text_reads_openai_delta_content(self) -> None:
        line = 'data: {"choices":[{"delta":{"content":"{\\"title\\":\\"春物\\""}}]}'.encode()

        self.assertEqual(ai_title_service.extract_stream_text(line), '{"title":"春物"')

    def test_parse_generated_result_requires_title_and_returns_subtitle(self) -> None:
        result = ai_title_service.parse_generated_result(
            '前置文本\n{"title":"春の新作 レディースバッグ","subtitle":"軽量で使いやすい"}\n'
        )

        self.assertEqual(result["title"], "春の新作 レディースバッグ")
        self.assertEqual(result["subtitle"], "軽量で使いやすい")

    def test_save_version_updates_product_only_after_explicit_save(self) -> None:
        session = MagicMock()
        product = MagicMock()
        product.owner_username = "operator"
        product.review_status = "pending"
        product.raw_payload_json = json.dumps({"title": "旧标题", "tagline": "旧副标题"}, ensure_ascii=False)
        version = MagicMock()
        version.owner_username = "operator"
        version.product_id = 7
        version.title = "新标题"
        version.subtitle = "新副标题"
        session.get.side_effect = [product, version]

        saved = ai_title_service.save_title_version_in_session(
            session,
            owner_username="operator",
            product_id=7,
            version_id=9,
        )

        self.assertEqual(product.title, "新标题")
        payload = json.loads(product.raw_payload_json)
        self.assertEqual(payload["title"], "新标题")
        self.assertEqual(payload["itemName"], "新标题")
        self.assertEqual(payload["tagline"], "新副标题")
        self.assertIs(saved, version)

    def test_cleanup_versions_keeps_only_selected_content(self) -> None:
        session = MagicMock()
        product = MagicMock()
        product.id = 7
        product.title = "最终标题"
        product.raw_payload_json = json.dumps({"tagline": "最终副标题"}, ensure_ascii=False)
        first = MagicMock()
        first.id = 1
        first.title = "旧标题"
        first.subtitle = "旧副标题"
        second = MagicMock()
        second.id = 2
        second.title = "最终标题"
        second.subtitle = "最终副标题"
        session.scalars.return_value.all.return_value = [first, second]

        ai_title_service.cleanup_title_versions_for_approved_product(session, product)

        session.delete.assert_called_once_with(first)
        self.assertTrue(second.is_selected)

    def test_ensure_current_version_adds_original_and_selects_it(self) -> None:
        session = MagicMock()
        product = MagicMock()
        product.id = 7
        product.owner_username = "operator"
        product.title = "原始标题"
        product.raw_payload_json = json.dumps({"tagline": "原始副标题"}, ensure_ascii=False)
        session.scalars.return_value.all.return_value = []

        version = ai_title_service.ensure_current_title_version_in_session(session, product)

        self.assertEqual(version.source, "original")
        self.assertEqual(version.title, "原始标题")
        self.assertEqual(version.subtitle, "原始副标题")
        self.assertTrue(version.is_selected)
        session.add.assert_called_once_with(version)
