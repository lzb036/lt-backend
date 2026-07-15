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
        parent_product_id=8,
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


def listed_master_product() -> SimpleNamespace:
    return SimpleNamespace(
        id=8,
        owner_username="operator",
        store_id=None,
        review_status="listed_master",
        title="主商品旧标题",
        genre_id="100001",
        image_url="https://example.com/master-old.jpg",
        price=1000,
        raw_payload_json=json.dumps({
            "title": "主商品旧标题",
            "genreId": "100001",
            "images": ["https://example.com/master-old.jpg"],
            "variants": {"old-sku": {"standardPrice": "1000"}},
            "listedStores": [{
                "storeId": 3,
                "storeName": "目标店铺",
                "manageNumber": "target-manage",
            }],
        }, ensure_ascii=False),
        last_error=None,
    )


def pending_replacement_product() -> SimpleNamespace:
    return SimpleNamespace(
        id=21,
        owner_username="operator",
        store_id=None,
        review_status="pending",
        rakuten_manage_number=None,
        item_number="source-item",
        source_url="https://item.rakuten.co.jp/source-shop/source-item/",
        title="待审核替换标题",
        genre_id="200002",
        image_url="https://example.com/edited.jpg",
        price=3880,
        currency="JPY",
        raw_payload_json=json.dumps({
            "title": "待审核替换标题",
            "tagline": "已编辑副标题",
            "genreId": "200002",
            "images": ["https://example.com/edited.jpg"],
            "variants": {"edited-sku": {"standardPrice": "3880"}},
            "_replacement": {
                "taskId": "task-id",
                "targetProductId": 9,
            },
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

    def test_replacement_draft_builds_default_sku_for_single_product(self) -> None:
        item = {
            "title": "单品商品",
            "genre_id": "200002",
            "price": 1880,
            "raw": {
                "title": "单品商品",
                "genreId": "200002",
                "price": "1880",
                "images": ["https://example.com/product.jpg"],
                "variants": {},
            },
        }

        draft = crawler_service.replacement_draft_from_collected_item(item)

        self.assertEqual(draft["variants"]["default"]["standardPrice"], "1880")

    def test_rakuten_payload_limits_title_and_tagline_by_utf8_bytes(self) -> None:
        product = SimpleNamespace(
            id=9,
            title="主" * 100,
            genre_id="200002",
            price=1880,
            rakuten_manage_number="target-manage",
        )
        raw = {
            "title": "主" * 100,
            "tagline": "副" * 100,
            "genreId": "200002",
            "price": "1880",
            "variants": {},
        }

        payload = crawler_service.build_rakuten_item_upsert_payload(
            product,
            raw,
            [],
            manage_number="target-manage",
        )

        self.assertLessEqual(len(payload["title"].encode("utf-8")), 255)
        self.assertLessEqual(len(payload["tagline"].encode("utf-8")), 174)

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

    def test_create_preview_keeps_target_unchanged_and_creates_pending_replacement_product(self) -> None:
        target = listed_product()
        store = SimpleNamespace(id=3, enabled=True, alias_name="店铺", store_name="店铺")
        session = MagicMock()
        crawl_task = None

        def get_model(model: object, key: object) -> object:
            if model is crawler_service.CrawlTaskModel:
                return crawl_task
            if model is crawler_service.ProductModel:
                return target
            if model is crawler_service.StoreModel:
                return store
            return None

        session.get.side_effect = get_model
        session.scalar.return_value = None

        def capture_added_row(row: object) -> None:
            nonlocal crawl_task
            if isinstance(row, crawler_service.CrawlTaskModel):
                crawl_task = row

        session.add.side_effect = capture_added_row
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
            patch.object(crawler_service, "product_to_public", return_value={"id": 21, "reviewStatus": "pending"}),
            patch.object(crawler_service, "sync_task_to_public", return_value={"id": "task-id", "status": "preview_ready"}),
        ):
            result = crawler_service.create_product_replacement_preview(
                "operator",
                9,
                "https://item.rakuten.co.jp/source-shop/source-item/",
            )

        self.assertEqual(target.title, "替换前标题")
        crawl_task = session.add.call_args_list[0].args[0]
        self.assertEqual(crawl_task.mode, "manual")
        self.assertEqual(crawl_task.source_type, "product_replace")
        self.assertEqual(crawl_task.status, "success")
        self.assertEqual(crawl_task.success_count, 1)
        task = session.add.call_args_list[1].args[0]
        self.assertEqual(task.task_type, "product_replace")
        self.assertEqual(task.status, "preview_ready")
        pending = session.add.call_args_list[2].args[0]
        self.assertEqual(pending.review_status, "pending")
        self.assertIsNone(pending.store_id)
        self.assertEqual(pending.task_id, crawl_task.id)
        self.assertEqual(
            json.loads(pending.raw_payload_json)["_replacement"]["targetProductId"],
            target.id,
        )
        self.assertEqual(
            json.loads(pending.raw_payload_json)["_replacement"]["targetStoreName"],
            "店铺",
        )
        self.assertEqual(
            json.loads(pending.raw_payload_json)["_replacement"]["targetManageNumber"],
            "target-manage",
        )
        self.assertEqual(result["task"]["status"], "preview_ready")
        self.assertEqual(result["pendingProduct"]["reviewStatus"], "pending")

    def test_failed_replacement_collection_is_recorded_as_manual_crawl_failure(self) -> None:
        session = MagicMock()
        crawl_task = None

        def get_model(model: object, key: object) -> object:
            if model is crawler_service.CrawlTaskModel:
                return crawl_task
            return None

        session.get.side_effect = get_model

        def capture_task(row: object) -> None:
            nonlocal crawl_task
            if isinstance(row, crawler_service.CrawlTaskModel):
                crawl_task = row

        session.add.side_effect = capture_task

        with (
            patch.object(crawler_service, "session_scope", side_effect=lambda: session_context(session)),
            patch.object(crawler_service, "collect_product_detail", side_effect=RuntimeError("采集失败")),
        ):
            with self.assertRaisesRegex(RuntimeError, "采集失败"):
                crawler_service.create_product_replacement_preview(
                    "operator",
                    9,
                    "https://item.rakuten.co.jp/source-shop/source-item/",
                )

        self.assertIsNotNone(crawl_task)
        self.assertEqual(crawl_task.status, "failed")
        self.assertEqual(crawl_task.failed_count, 1)
        self.assertIn("采集失败", crawl_task.error_detail)

    def test_preview_replacement_sync_task_is_hidden_until_confirmed(self) -> None:
        self.assertFalse(crawler_service.sync_task_visible_in_list("product_replace", "preview_ready"))
        self.assertTrue(crawler_service.sync_task_visible_in_list("product_replace", "queued"))
        self.assertTrue(crawler_service.sync_task_visible_in_list("product_replace", "running"))
        self.assertTrue(crawler_service.sync_task_visible_in_list("store_sync", "queued"))

    def test_normal_approval_rejects_replacement_pending_product(self) -> None:
        pending = pending_replacement_product()
        session = MagicMock()
        session.scalars.return_value.all.return_value = [pending]

        with patch.object(crawler_service, "session_scope", return_value=session_context(session)):
            with self.assertRaisesRegex(RuntimeError, "确认替换"):
                crawler_service.update_product_status("operator", [pending.id], "approved")

        self.assertEqual(pending.review_status, "pending")

    def test_confirm_requires_exact_target_manage_number(self) -> None:
        target = listed_product()
        pending = pending_replacement_product()
        task = SimpleNamespace(
            id="task-id",
            owner_username="operator",
            task_type="product_replace",
            status="preview_ready",
            payload_json=json.dumps({
                "targetProductId": 9,
                "pendingProductId": 21,
                "draftPayload": {"title": "新标题"},
            }),
            message="",
            error_detail=None,
            finished_at=None,
        )
        session = MagicMock()
        def get_model(model: object, key: object) -> object:
            if model is crawler_service.SyncTaskModel:
                return task
            if key == target.id:
                return target
            if key == pending.id:
                return pending
            return None

        session.get.side_effect = get_model

        with patch.object(crawler_service, "session_scope", return_value=session_context(session)):
            with self.assertRaisesRegex(RuntimeError, "商品管理编号"):
                crawler_service.confirm_product_replacement("operator", "task-id", "wrong")

        self.assertEqual(task.status, "preview_ready")

    def test_confirm_uses_latest_pending_product_content(self) -> None:
        target = listed_product()
        pending = pending_replacement_product()
        task = SimpleNamespace(
            id="task-id",
            owner_username="operator",
            task_type="product_replace",
            status="preview_ready",
            payload_json=json.dumps({
                "targetProductId": 9,
                "pendingProductId": 21,
                "draftPayload": {"title": "旧预览标题"},
            }, ensure_ascii=False),
            message="",
            error_detail=None,
            finished_at=None,
        )
        session = MagicMock()

        def get_model(model: object, key: object) -> object:
            if model is crawler_service.SyncTaskModel:
                return task
            if key == target.id:
                return target
            if key == pending.id:
                return pending
            return None

        session.get.side_effect = get_model

        with (
            patch.object(crawler_service, "session_scope", return_value=session_context(session)),
            patch.object(crawler_service, "dispatch_next_sync_task"),
            patch.object(crawler_service, "rakuten_genre_path", return_value="分类"),
            patch.object(crawler_service, "product_to_public", return_value={"id": 21, "reviewStatus": "pending"}),
            patch.object(crawler_service, "sync_task_to_public", return_value={"id": "task-id", "status": "queued"}),
        ):
            crawler_service.confirm_product_replacement("operator", "task-id", "target-manage")

        payload = json.loads(task.payload_json)
        self.assertEqual(payload["draftPayload"]["title"], "待审核替换标题")
        self.assertEqual(payload["draftPayload"]["images"], ["https://example.com/edited.jpg"])
        self.assertEqual(task.status, "queued")

    def test_confirm_pending_product_creates_new_sync_task_when_old_task_is_missing(self) -> None:
        target = listed_product()
        pending = pending_replacement_product()
        pending.raw_payload_json = json.dumps({
            **json.loads(pending.raw_payload_json),
            "_replacement": {
                "taskId": "deleted-task",
                "targetProductId": target.id,
                "targetManageNumber": "target-manage",
                "targetStoreId": 3,
                "targetStoreName": "目标店铺",
            },
        }, ensure_ascii=False)
        store = SimpleNamespace(id=3, enabled=True, alias_name="目标店铺", store_name="目标店铺")
        session = MagicMock()

        def get_model(model: object, key: object) -> object:
            if model is crawler_service.ProductModel and key == pending.id:
                return pending
            if model is crawler_service.ProductModel and key == target.id:
                return target
            if model is crawler_service.StoreModel and key == store.id:
                return store
            if model is crawler_service.SyncTaskModel:
                return None
            return None

        session.get.side_effect = get_model

        with (
            patch.object(crawler_service, "session_scope", return_value=session_context(session)),
            patch.object(crawler_service, "dispatch_next_sync_task"),
            patch.object(crawler_service, "rakuten_genre_path", return_value="分类"),
            patch.object(crawler_service, "product_detail_to_public", return_value={"id": 9, "title": "替换前标题"}),
            patch.object(crawler_service, "product_to_public", return_value={"id": 21, "reviewStatus": "pending"}),
            patch.object(crawler_service, "sync_task_to_public", return_value={"id": "new-task", "status": "queued"}),
        ):
            result = crawler_service.confirm_pending_product_replacement(
                "operator",
                pending.id,
                "target-manage",
            )

        created_task = next(
            row for row in (call.args[0] for call in session.add.call_args_list)
            if isinstance(row, crawler_service.SyncTaskModel)
        )
        self.assertEqual(created_task.status, "queued")
        self.assertEqual(created_task.task_type, "product_replace")
        self.assertEqual(json.loads(pending.raw_payload_json)["_replacement"]["taskId"], created_task.id)
        self.assertEqual(result["task"]["status"], "queued")

    def test_cancel_replacement_removes_pending_product(self) -> None:
        pending = pending_replacement_product()
        task = SimpleNamespace(
            id="task-id",
            owner_username="operator",
            task_type="product_replace",
            status="preview_ready",
            payload_json=json.dumps({"pendingProductId": pending.id}),
            message="",
            finished_at=None,
        )
        session = MagicMock()
        session.get.side_effect = lambda model, key: task if model is crawler_service.SyncTaskModel else pending

        with (
            patch.object(crawler_service, "session_scope", return_value=session_context(session)),
            patch.object(crawler_service, "sync_task_to_public", return_value={"id": "task-id", "status": "cancelled"}),
        ):
            crawler_service.cancel_product_replacement("operator", "task-id")

        self.assertEqual(task.status, "cancelled")
        session.delete.assert_called_once_with(pending)

    def test_perform_replacement_preserves_target_identity_after_remote_success(self) -> None:
        target = listed_product()
        parent = listed_master_product()
        pending = pending_replacement_product()
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
        def get_model(model: object, key: object) -> object:
            if model is crawler_service.StoreModel:
                return store
            if key == target.id:
                return target
            if key == parent.id:
                return parent
            if key == pending.id:
                return pending
            return None

        session.get.side_effect = get_model
        payload = {
            "targetProductId": 9,
            "pendingProductId": 21,
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

        def upload_product_images(
            _secret: str,
            _key: str,
            _store: object,
            _product: object,
            _manage_number: str,
            *,
            cabinet_context: dict[str, object],
            cancel_check: object,
        ) -> list[dict[str, str]]:
            self.assertEqual(cabinet_context, {})
            self.assertFalse(cancel_check())
            return [{"location": "/cabinet/new.jpg", "alt": "替换后标题"}]

        def upload_description_images(
            _secret: str,
            _key: str,
            _store: object,
            _product: object,
            _manage_number: str,
            raw_payload: dict[str, object],
            *,
            cabinet_context: dict[str, object],
            cancel_check: object,
        ) -> dict[str, object]:
            self.assertEqual(cabinet_context, {})
            self.assertFalse(cancel_check())
            return {"rawPayload": raw_payload, "uploadedImages": []}

        with (
            patch.object(crawler_service, "session_scope", side_effect=lambda: session_context(session)),
            patch.object(crawler_service, "decrypt_text", side_effect=lambda value: value),
            patch.object(crawler_service, "raise_if_task_cancelled"),
            patch.object(crawler_service, "update_task_progress"),
            patch.object(
                crawler_service,
                "upload_product_images_to_rakuten",
                side_effect=upload_product_images,
            ),
            patch.object(
                crawler_service,
                "upload_product_description_images_to_rakuten",
                side_effect=upload_description_images,
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
        self.assertEqual(parent.title, "替换后标题")
        self.assertEqual(parent.genre_id, "200002")
        self.assertEqual(parent.price, 2880)
        self.assertEqual(
            json.loads(parent.raw_payload_json)["listedStores"][0]["manageNumber"],
            "target-manage",
        )
        session.delete.assert_called_once_with(pending)


if __name__ == "__main__":
    unittest.main()
