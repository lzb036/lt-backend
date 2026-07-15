from __future__ import annotations

import json
import unittest
from contextlib import contextmanager
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from app.services import crawler_service


@contextmanager
def session_context(session: MagicMock):
    yield session


def listed_product() -> SimpleNamespace:
    return SimpleNamespace(
        id=9,
        owner_username="operator",
        store_id=3,
        review_status="listed",
        rakuten_manage_number="target-manage",
        item_number="target-item",
        source_url="https://www.rakuten.co.jp/target/target-item/",
        title="替换前标题",
        genre_id="100001",
        image_url="https://example.com/old.jpg",
        price=1000,
        currency="JPY",
        raw_payload_json=json.dumps({
            "title": "替换前标题",
            "tagline": "旧副标题",
            "genreId": "100001",
            "images": ["https://example.com/old.jpg"],
            "variants": {"old-sku": {"standardPrice": "1000"}},
        }, ensure_ascii=False),
        listing_task_id=None,
        last_error=None,
    )


class ProductReplacementTests(unittest.TestCase):
    def test_replacement_draft_uses_collected_content_without_source_identity(self) -> None:
        item = {
            "title": "替换后标题",
            "source_url": "https://item.rakuten.co.jp/source-shop/source-item/",
            "genre_id": "200002",
            "price": 2880,
            "image_url": "https://example.com/new.jpg",
            "raw": {
                "title": "替换后标题",
                "tagline": "新副标题",
                "genreId": "200002",
                "manageNumber": "source-manage",
                "itemNumber": "source-item",
                "images": ["https://example.com/new.jpg"],
                "descriptions": [{"label": "商品说明", "value": "新说明"}],
                "variants": {"new-sku": {"standardPrice": "2880"}},
            },
        }

        draft = crawler_service.replacement_draft_from_collected_item(item)

        self.assertEqual(draft["title"], "替换后标题")
        self.assertEqual(draft["genreId"], "200002")
        self.assertNotIn("manageNumber", draft)
        self.assertNotIn("itemNumber", draft)
        self.assertEqual(draft["images"], ["https://example.com/new.jpg"])

    def test_replacement_draft_deduplicates_rakuten_cdn_host_variants(self) -> None:
        item = {
            "title": "商品",
            "genre_id": "200002",
            "raw": {
                "title": "商品",
                "genreId": "200002",
                "images": [
                    "https://image.rakuten.co.jp/shop/cabinet/item/main.jpg",
                    "https://tshop.r10s.jp/shop/cabinet/item/main.jpg",
                    "https://shop.r10s.jp/shop/cabinet/item/main.jpg?_ex=128x128",
                ],
                "variants": {"sku": {"standardPrice": "1000"}},
            },
        }

        draft = crawler_service.replacement_draft_from_collected_item(item)

        self.assertEqual(
            draft["images"],
            ["https://image.rakuten.co.jp/shop/cabinet/item/main.jpg"],
        )

    def test_replacement_difference_marks_changed_sections(self) -> None:
        before = {
            "title": "旧标题",
            "tagline": "旧副标题",
            "genreId": "100001",
            "price": 1000,
            "images": ["old"],
            "variants": [{"variantId": "old"}],
            "descriptions": [{"label": "商品说明", "value": "旧"}],
        }
        after = {
            "title": "新标题",
            "tagline": "新副标题",
            "genreId": "200002",
            "price": 2000,
            "images": ["new", "new2"],
            "variants": [{"variantId": "new"}],
            "descriptions": [{"label": "商品说明", "value": "新"}],
        }

        difference = crawler_service.product_replacement_difference(before, after)

        self.assertTrue(all(difference[key]["changed"] for key in (
            "title", "tagline", "genre", "price", "images", "variants", "descriptions"
        )))
        self.assertEqual(difference["images"]["beforeCount"], 1)
        self.assertEqual(difference["images"]["afterCount"], 2)

    def test_create_preview_keeps_target_unchanged_and_saves_preview_task(self) -> None:
        target = listed_product()
        store = SimpleNamespace(id=3, enabled=True, alias_name="店铺", store_name="店铺")
        session = MagicMock()
        session.get.side_effect = lambda model, key: target if model is crawler_service.ProductModel else store
        session.scalar.return_value = None
        item = {
            "title": "替换后标题",
            "source_url": "https://item.rakuten.co.jp/source-shop/source-item/",
            "genre_id": "200002",
            "price": 2880,
            "image_url": "https://example.com/new.jpg",
            "raw": {
                "title": "替换后标题",
                "genreId": "200002",
                "images": ["https://example.com/new.jpg"],
                "variants": {"new-sku": {"standardPrice": "2880"}},
            },
        }

        with (
            patch.object(crawler_service, "session_scope", side_effect=lambda: session_context(session)),
            patch.object(crawler_service, "collect_product_detail", return_value=item),
            patch.object(crawler_service, "product_detail_to_public", return_value={"id": 9, "title": "替换前标题"}),
            patch.object(crawler_service, "sync_task_to_public", return_value={"id": "task-id", "status": "preview_ready"}),
        ):
            result = crawler_service.create_product_replacement_preview(
                "operator",
                9,
                "https://item.rakuten.co.jp/source-shop/source-item/",
            )

        self.assertEqual(target.title, "替换前标题")
        task = session.add.call_args.args[0]
        self.assertEqual(task.task_type, "product_replace")
        self.assertEqual(task.status, "preview_ready")
        self.assertEqual(result["task"]["status"], "preview_ready")

    def test_confirm_requires_exact_target_manage_number(self) -> None:
        target = listed_product()
        task = SimpleNamespace(
            id="task-id",
            owner_username="operator",
            task_type="product_replace",
            status="preview_ready",
            payload_json=json.dumps({"targetProductId": 9, "draftPayload": {"title": "新标题"}}),
            message="",
            error_detail=None,
            finished_at=None,
        )
        session = MagicMock()
        session.get.side_effect = lambda model, key: task if model is crawler_service.SyncTaskModel else target

        with patch.object(crawler_service, "session_scope", return_value=session_context(session)):
            with self.assertRaisesRegex(RuntimeError, "商品管理编号"):
                crawler_service.confirm_product_replacement("operator", "task-id", "wrong")

        self.assertEqual(task.status, "preview_ready")

    def test_perform_replacement_preserves_target_identity_after_remote_success(self) -> None:
        target = listed_product()
        target.rakuten_listing_status = "listed"
        target.store_last_seen_at = None
        target.currency = "JPY"
        store = SimpleNamespace(
            id=3,
            enabled=True,
            store_code="target-shop",
            store_name="目标店铺",
            alias_name="目标店铺",
            rakuten_service_secret_encrypted="secret",
            rakuten_license_key_encrypted="key",
        )
        session = MagicMock()
        session.get.side_effect = lambda model, key: target if model is crawler_service.ProductModel else store
        payload = {
            "targetProductId": 9,
            "draftPayload": {
                "title": "替换后标题",
                "tagline": "替换后副标题",
                "genreId": "200002",
                "price": 2880,
                "images": ["https://example.com/new.jpg"],
                "descriptions": [],
                "variants": {"new-sku": {"standardPrice": "2880"}},
                "raw": {"variants": {"new-sku": {"standardPrice": "2880"}}},
            },
        }

        with (
            patch.object(crawler_service, "session_scope", side_effect=lambda: session_context(session)),
            patch.object(crawler_service, "decrypt_text", side_effect=lambda value: value),
            patch.object(crawler_service, "raise_if_task_cancelled"),
            patch.object(crawler_service, "update_task_progress"),
            patch.object(
                crawler_service,
                "upload_product_images_to_rakuten",
                return_value=[{"location": "/cabinet/new.jpg", "alt": "替换后标题"}],
            ),
            patch.object(
                crawler_service,
                "upload_product_description_images_to_rakuten",
                return_value={"rawPayload": payload["draftPayload"]["raw"], "uploadedImages": []},
            ),
            patch.object(
                crawler_service,
                "build_rakuten_item_upsert_payload",
                return_value={
                    "itemNumber": "target-item",
                    "title": "替换后标题",
                    "genreId": "200002",
                    "variants": {"new-sku": {"standardPrice": "2880"}},
                },
            ),
            patch.object(
                crawler_service,
                "put_rakuten_item_with_attribute_retry",
                side_effect=lambda _secret, _key, _manage, item_payload: item_payload,
            ),
            patch.object(crawler_service, "build_rakuten_inventory_upsert_payloads", return_value=[]),
            patch.object(crawler_service, "bulk_upsert_rakuten_inventories"),
            patch.object(crawler_service, "patch_rakuten_item_visibility"),
            patch.object(crawler_service, "price_from_rakuten_item", return_value=2880),
            patch.object(crawler_service, "product_detail_to_public", return_value={"id": 9, "title": "替换后标题"}),
        ):
            result = crawler_service.perform_product_replacement("operator", 3, payload, task_id="task-id")

        self.assertEqual(result["product"]["title"], "替换后标题")
        self.assertEqual(target.title, "替换后标题")
        self.assertEqual(target.genre_id, "200002")
        self.assertEqual(target.id, 9)
        self.assertEqual(target.store_id, 3)
        self.assertEqual(target.rakuten_manage_number, "target-manage")
        self.assertEqual(target.item_number, "target-item")
        self.assertEqual(target.source_url, "https://www.rakuten.co.jp/target/target-item/")
        self.assertEqual(target.review_status, "listed")


if __name__ == "__main__":
    unittest.main()
