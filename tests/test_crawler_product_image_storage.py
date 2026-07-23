from __future__ import annotations

import tempfile
import unittest
from contextlib import contextmanager
from io import BytesIO
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

from PIL import Image

from app.services import crawler_service
from app.services.product_image_storage import ObjectFingerprint, StoredObject


class FakeProductImageStorage:
    def __init__(self, *, enabled: bool = True):
        self.enabled = enabled
        self.objects: dict[str, bytes] = {}
        self.last_modified: dict[str, int] = {}
        self.put_calls: list[tuple[str, bytes, str]] = []
        self.copy_calls: list[tuple[str, str]] = []
        self.delete_calls: list[str] = []

    def put_bytes(self, key: str, content: bytes, content_type: str = "") -> None:
        self.objects[key] = bytes(content)
        self.last_modified.setdefault(key, 1_700_000_000)
        self.put_calls.append((key, bytes(content), content_type))

    def read_bytes(self, key: str, *, max_bytes: int) -> bytes:
        content = self.objects[key]
        if len(content) > max_bytes:
            raise RuntimeError("图片大小不能超过允许的限制。")
        return content

    def exists(self, key: str) -> bool:
        return key in self.objects

    def object_size(self, key: str) -> int | None:
        content = self.objects.get(key)
        return len(content) if content is not None else None

    def object_fingerprint(self, key: str) -> ObjectFingerprint | None:
        content = self.objects.get(key)
        if content is None:
            return None
        return ObjectFingerprint(
            size=len(content),
            sha256="",
            last_modified=self.last_modified.get(key, 1_700_000_000),
        )

    def copy(self, source_key: str, target_key: str) -> None:
        self.objects[target_key] = self.objects[source_key]
        self.last_modified[target_key] = self.last_modified.get(source_key, 1_700_000_000)
        self.copy_calls.append((source_key, target_key))

    def delete(self, key: str) -> None:
        self.objects.pop(key, None)
        self.last_modified.pop(key, None)
        self.delete_calls.append(key)

    def list_objects(self, prefix: str) -> list[StoredObject]:
        return [
            StoredObject(
                key=key,
                size=len(content),
                last_modified=self.last_modified.get(key, 1_700_000_000),
            )
            for key, content in sorted(self.objects.items())
            if key.startswith(prefix)
        ]

    def delete_prefix(self, prefix: str) -> int:
        keys = [key for key in self.objects if key.startswith(prefix)]
        for key in keys:
            self.delete(key)
        return len(keys)

    def health_check(self) -> bool:
        return True


class ProductImageStorageIntegrationTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        root = Path(self.temp_dir.name)
        self.product_root = root / "product-images"
        self.draft_root = root / "product-image-drafts"
        self.storage = FakeProductImageStorage()
        self.patches = [
            patch.object(crawler_service, "LOCAL_PRODUCT_IMAGE_DIR", self.product_root),
            patch.object(crawler_service, "LOCAL_PRODUCT_IMAGE_DRAFT_DIR", self.draft_root),
            patch.object(crawler_service, "product_image_storage", self.storage, create=True),
        ]
        for item in self.patches:
            item.start()

    def tearDown(self):
        for item in reversed(self.patches):
            item.stop()
        self.temp_dir.cleanup()

    @staticmethod
    def encoded_test_image(
        *,
        size: tuple[int, int],
        quality: int,
    ) -> bytes:
        image = Image.new("RGB", (320, 240), "white")
        for x in range(40, 280):
            for y in range(30, 210):
                image.putpixel((x, y), ((x * 3) % 255, (y * 5) % 255, ((x + y) * 2) % 255))
        image = image.resize(size, Image.Resampling.LANCZOS)
        output = BytesIO()
        image.save(output, format="JPEG", quality=quality)
        return output.getvalue()

    def test_rakuten_image_identity_ignores_cdn_host_and_resize_query(self):
        full_url = "https://image.rakuten.co.jp/okeya-2020/cabinet/a/b/ac03.jpg"
        thumbnail_url = (
            "https://tshop.r10s.jp/okeya-2020/cabinet/a/b/ac03.jpg"
            "?_ex=200x200&s=0&r=1"
        )

        self.assertEqual(
            crawler_service.rakuten_image_identity_key(full_url),
            crawler_service.rakuten_image_identity_key(thumbnail_url),
        )

    def test_localization_prefers_full_rakuten_image_over_thumbnail_variant(self):
        thumbnail_url = (
            "https://tshop.r10s.jp/okeya-2020/cabinet/a/b/ac03.jpg"
            "?_ex=200x200&s=0&r=1"
        )
        full_url = "https://image.rakuten.co.jp/okeya-2020/cabinet/a/b/ac03.jpg"
        image_data = {
            "content": self.encoded_test_image(size=(800, 1058), quality=92),
            "suffix": ".jpg",
            "contentType": "image/jpeg",
        }

        with patch.object(
            crawler_service,
            "load_product_image_bytes",
            return_value=image_data,
        ) as load_image:
            result = crawler_service.localize_product_image_urls(
                12,
                [thumbnail_url, full_url],
                prefix="p",
            )

        self.assertEqual(len(result["urls"]), 1)
        self.assertEqual(load_image.call_count, 1)
        self.assertEqual(load_image.call_args.args[0], full_url)
        self.assertEqual(result["replacementMap"][thumbnail_url], result["urls"][0])
        self.assertEqual(result["replacementMap"][full_url], result["urls"][0])

    def test_localization_deduplicates_resized_reencoded_visual_copy(self):
        full_url = "https://example.test/full.jpg"
        thumbnail_url = "https://cdn.example.test/thumb.jpg"
        full_image = {
            "content": self.encoded_test_image(size=(800, 600), quality=94),
            "suffix": ".jpg",
            "contentType": "image/jpeg",
        }
        thumbnail_image = {
            "content": self.encoded_test_image(size=(200, 150), quality=76),
            "suffix": ".jpg",
            "contentType": "image/jpeg",
        }

        with patch.object(
            crawler_service,
            "load_product_image_bytes",
            side_effect=[full_image, thumbnail_image],
        ):
            result = crawler_service.localize_product_image_urls(
                12,
                [full_url, thumbnail_url],
                prefix="p",
            )

        self.assertEqual(len(result["urls"]), 1)
        self.assertEqual(result["replacementMap"][thumbnail_url], result["urls"][0])
        self.assertEqual(len(self.storage.put_calls), 1)

    def test_product_image_urls_do_not_scan_embedded_promotions_when_images_are_explicit(self):
        product_url = "https://image.rakuten.co.jp/shop/cabinet/product/main.jpg"
        topic_url = "https://tshop.r10s.jp/shop/cabinet/topics/watch.jpg"
        payload = {
            "images": [product_url],
            "embeddedItem": {
                "embeddedPayload": {
                    "api": {
                        "data": {
                            "topicsList": [{"imageUrl": topic_url}],
                            "mnoPromotion": {
                                "image": {
                                    "url": "https://www.rakuten.co.jp/com/inc/item/mno/img/mobile_pr.png"
                                }
                            },
                        }
                    }
                }
            },
        }

        self.assertEqual(
            crawler_service.product_image_urls(payload, shop_code="shop"),
            [product_url],
        )

    def test_market_item_images_ignore_description_recommendations(self):
        product_url = "https://image.rakuten.co.jp/gadgery/cabinet/06209451/y0219871.jpg"
        recommended_url = "https://image.rakuten.co.jp/gadgery/cabinet/ladys/y14406056.jpg"
        item = {
            "pcFields": {
                "images": [{"location": "/06209451/y0219871.jpg"}],
            },
            "newProductDescription": (
                '<a href="https://item.rakuten.co.jp/gadgery/y14406056/">'
                f'<img src="{recommended_url}"></a>'
            ),
        }

        self.assertEqual(
            crawler_service.market_item_image_urls(
                item,
                shop_code="gadgery",
                item_number="y0219871",
            ),
            [product_url],
        )

    def test_market_descriptions_remove_cross_item_image_links(self):
        shop_banner = "https://image.rakuten.co.jp/gadgery/cabinet/shop/banner.jpg"
        current_image = "https://image.rakuten.co.jp/gadgery/cabinet/06209451/y0219871_1.jpg"
        recommended_image = "https://image.rakuten.co.jp/gadgery/cabinet/ladys/y14406056.jpg"
        item = {
            "newProductDescription": (
                '<a href="https://www.rakuten.ne.jp/gold/gadgery/">'
                f'<img src="{shop_banner}"></a>'
                '<a href="https://item.rakuten.co.jp/gadgery/y0219871/">'
                f'<img src="{current_image}"></a>'
                '<a href="https://item.rakuten.co.jp/gadgery/y14406056/">'
                f'<img src="{recommended_image}"></a>'
            ),
        }

        descriptions = crawler_service.market_product_descriptions(
            {},
            crawler_service.BeautifulSoup("<html></html>", "lxml"),
            item,
            shop_code="gadgery",
            item_number="y0219871",
        )

        description = next(
            row["value"]
            for row in descriptions
            if row["label"] == "スマートフォン用 商品説明文"
        )
        self.assertIn(shop_banner, description)
        self.assertIn(current_image, description)
        self.assertNotIn(recommended_image, description)
        self.assertNotIn("y14406056", description)

    def test_trusted_product_main_images_use_only_whitelisted_embedded_fields(self):
        primary_url = "https://tshop.r10s.jp/shop/cabinet/product/main.jpg"
        sku_url = "https://tshop.r10s.jp/shop/cabinet/product/sku.jpg"
        topic_url = "https://tshop.r10s.jp/shop/cabinet/topics/watch.jpg"
        payload = {
            "embeddedItem": {
                "pcFields": {
                    "images": [{"location": primary_url}],
                },
                "media": {
                    "skuImages": [{"location": sku_url}],
                },
                "embeddedPayload": {
                    "newApi": {
                        "topicsList": [{"imageUrl": topic_url}],
                    }
                },
            }
        }

        self.assertEqual(
            crawler_service.trusted_product_main_image_urls(payload, shop_code="shop"),
            [primary_url, sku_url],
        )

    def test_trusted_product_main_images_fall_back_to_media_when_pc_images_are_missing(self):
        media_url = "https://tshop.r10s.jp/shop/cabinet/product/main.jpg"
        sku_url = "https://tshop.r10s.jp/shop/cabinet/product/sku.jpg"
        payload = {
            "embeddedItem": {
                "media": {
                    "images": [{"location": media_url}],
                    "skuImages": [{"location": sku_url}],
                },
                "embeddedPayload": {
                    "newApi": {
                        "topicsList": [
                            {
                                "imageUrl": "https://tshop.r10s.jp/shop/cabinet/topics/watch.jpg"
                            }
                        ]
                    }
                },
            }
        }

        self.assertEqual(
            crawler_service.trusted_product_main_image_urls(payload, shop_code="shop"),
            [media_url, sku_url],
        )

    def test_product_shop_code_prefers_explicit_payload_over_local_image_url(self):
        product = SimpleNamespace(
            image_url="/api/static/product-images/86975/p01.jpg",
            source_url="https://item.rakuten.co.jp/okeya-2020/20230722001/",
        )

        self.assertEqual(
            crawler_service.product_shop_code(
                product,
                {"shopCode": "okeya-2020"},
            ),
            "okeya-2020",
        )

    def test_trusted_product_main_images_ignore_edited_remote_image_list(self):
        product_url = "https://image.rakuten.co.jp/shop/cabinet/product/main.jpg"
        promotion_url = "https://r.r10s.jp/com/img/item/Normal/direct_2000_pcv1.png"
        payload = {
            "images": [product_url, promotion_url],
            "ltEditedImages": [product_url, promotion_url],
            "embeddedItem": {
                "pcFields": {
                    "images": [{"location": product_url}],
                }
            },
        }

        self.assertEqual(
            crawler_service.trusted_product_main_image_urls(payload, shop_code="shop"),
            [product_url],
        )

    def test_uploaded_image_is_written_to_oss_without_local_file(self):
        upload = SimpleNamespace(
            filename="photo.jpg",
            content_type="image/jpeg",
            file=BytesIO(b"uploaded"),
        )

        image_url = crawler_service.save_uploaded_product_image_file(
            upload,
            self.product_root / "12",
            lambda filename: crawler_service.local_product_image_url(12, filename),
            name_prefix="1",
        )

        stored = crawler_service.parse_product_image_url(image_url)
        self.assertIsNotNone(stored)
        self.assertEqual(self.storage.objects[stored.object_key], b"uploaded")
        self.assertFalse((self.product_root / "12" / stored.filename).exists())

    def test_image_bytes_are_written_to_oss(self):
        image_url = crawler_service.save_product_image_bytes(
            b"edited",
            ".png",
            self.draft_root / "7",
            lambda filename: crawler_service.local_product_image_draft_url(7, filename),
            name_prefix="meitu",
        )

        stored = crawler_service.parse_product_image_url(image_url)
        self.assertEqual(self.storage.objects[stored.object_key], b"edited")
        self.assertEqual(self.storage.put_calls[0][2], "image/png")

    def test_localization_reuses_first_image_when_remote_urls_have_same_content(self):
        first_url = "https://tshop.r10s.jp/example/a.jpg"
        duplicate_url = "https://image.rakuten.co.jp/example/cabinet/a-copy.jpg"
        image_data = {
            "content": b"same-image",
            "suffix": ".jpg",
            "contentType": "image/jpeg",
        }

        with patch.object(
            crawler_service,
            "load_product_image_bytes",
            side_effect=[image_data, image_data],
        ):
            result = crawler_service.localize_product_image_urls(
                12,
                [first_url, duplicate_url],
                prefix="p",
            )

        self.assertEqual(len(result["urls"]), 1)
        self.assertEqual(result["replacementMap"][first_url], result["urls"][0])
        self.assertEqual(result["replacementMap"][duplicate_url], result["urls"][0])
        self.assertEqual(len(self.storage.put_calls), 1)

    def test_description_localization_reuses_matching_main_image_content(self):
        main_url = "https://tshop.r10s.jp/example/main.jpg"
        description_url = "https://tshop.r10s.jp/example/description-copy.jpg"
        image_data = {
            "content": b"same-image",
            "suffix": ".jpg",
            "contentType": "image/jpeg",
        }
        raw_payload = {
            "descriptions": [
                {
                    "label": "PC用 商品説明文",
                    "value": f'<img src="{description_url}">',
                }
            ]
        }
        content_hash_urls: dict[str, str] = {}

        with patch.object(
            crawler_service,
            "load_product_image_bytes",
            side_effect=[image_data, image_data],
        ):
            image_result = crawler_service.localize_product_image_urls(
                13,
                [main_url],
                prefix="p",
                content_hash_urls=content_hash_urls,
            )
            description_result = crawler_service.localize_product_description_images(
                13,
                raw_payload,
                existing_replacements=image_result["replacementMap"],
                content_hash_urls=content_hash_urls,
            )

        self.assertEqual(
            description_result["replacementMap"][description_url],
            image_result["urls"][0],
        )
        self.assertEqual(len(self.storage.put_calls), 1)

    def test_oss_read_precedes_existing_local_fallback(self):
        image_url = crawler_service.local_product_image_url(3, "a.jpg")
        local_path = self.product_root / "3" / "a.jpg"
        local_path.parent.mkdir(parents=True)
        local_path.write_bytes(b"local")
        self.storage.objects["product-images/3/a.jpg"] = b"oss"

        result = crawler_service.load_product_image_bytes(image_url, max_bytes=20)

        self.assertEqual(result["content"], b"oss")

    def test_missing_oss_object_uses_local_fallback(self):
        image_url = crawler_service.local_product_image_url(3, "a.jpg")
        local_path = self.product_root / "3" / "a.jpg"
        local_path.parent.mkdir(parents=True)
        local_path.write_bytes(b"local")

        result = crawler_service.load_product_image_bytes(image_url, max_bytes=20)

        self.assertEqual(result["content"], b"local")

    def test_absolute_application_url_uses_local_fallback_during_migration(self):
        image_url = "https://wujiancm.com/api/static/product-images/3/a.jpg"
        local_path = self.product_root / "3" / "a.jpg"
        local_path.parent.mkdir(parents=True)
        local_path.write_bytes(b"local")

        with patch.object(
            crawler_service.requests,
            "get",
            side_effect=AssertionError("application image URL must not use remote HTTP fallback"),
        ):
            result = crawler_service.load_product_image_bytes(image_url, max_bytes=20)

        self.assertEqual(result["content"], b"local")
        self.assertTrue(crawler_service.is_local_product_image_url(image_url))

    def test_remote_object_prevents_missing_local_result(self):
        image_url = crawler_service.local_product_image_url(3, "a.jpg")
        self.storage.objects["product-images/3/a.jpg"] = b"oss"

        self.assertFalse(crawler_service.is_missing_local_product_image_url(image_url))

    def test_missing_oss_image_error_remains_recoverable(self):
        self.assertTrue(
            crawler_service.is_missing_local_product_image_error(
                RuntimeError("商品图片文件不存在。")
            )
        )

    def test_finalize_draft_defers_source_deletion_until_post_commit_cleanup(self):
        draft_url = crawler_service.local_product_image_draft_url(8, "draft.jpg")
        self.storage.objects["product-image-drafts/8/draft.jpg"] = b"draft"
        cleanup_urls: list[str] = []

        final_url = crawler_service.finalize_product_image_url(
            8,
            draft_url,
            cleanup_urls=cleanup_urls,
        )

        final_ref = crawler_service.parse_product_image_url(final_url)
        self.assertEqual(self.storage.objects[final_ref.object_key], b"draft")
        self.assertIn("product-image-drafts/8/draft.jpg", self.storage.objects)
        self.assertEqual(cleanup_urls, [draft_url])
        self.assertEqual(
            self.storage.copy_calls,
            [("product-image-drafts/8/draft.jpg", final_ref.object_key)],
        )

    def test_localization_cleanup_does_not_delete_unreferenced_oss_objects(self):
        self.storage.objects.update(
            {
                "product-images/5/keep.jpg": b"keep",
                "product-images/5/remove.jpg": b"remove",
            }
        )

        crawler_service.remove_unused_local_product_images(
            5,
            [crawler_service.local_product_image_url(5, "keep.jpg")],
        )

        self.assertIn("product-images/5/keep.jpg", self.storage.objects)
        self.assertIn("product-images/5/remove.jpg", self.storage.objects)

    def test_equivalent_absolute_url_does_not_delete_referenced_object(self):
        key = "product-images/5/keep.jpg"
        self.storage.objects[key] = b"keep"

        crawler_service.remove_local_product_image_if_unused(
            crawler_service.local_product_image_url(5, "keep.jpg"),
            ["https://wujiancm.com/api/static/product-images/5/keep.jpg"],
        )

        self.assertIn(key, self.storage.objects)

    def test_internal_image_url_must_belong_to_current_product(self):
        with self.assertRaisesRegex(RuntimeError, "不属于当前商品"):
            crawler_service.validate_product_image_url_ownership(
                5,
                crawler_service.local_product_image_url(6, "foreign.jpg"),
            )

    def test_post_commit_cleanup_is_not_called_when_commit_fails(self):
        image_url = crawler_service.local_product_image_url(5, "old.jpg")
        product = SimpleNamespace(
            id=5,
            owner_username="owner",
            review_status="pending",
            title="old",
            price=0,
            raw_payload_json="{}",
            image_url=image_url,
            last_error=None,
        )
        session = SimpleNamespace(
            get=lambda _model, _product_id: product,
            flush=lambda: None,
        )

        @contextmanager
        def failing_session_scope():
            yield session
            raise RuntimeError("commit failed")

        payload = SimpleNamespace(
            title="new",
            tagline="",
            variants=[],
            imageChanges=object(),
        )
        cleanup = Mock()
        with (
            patch.object(crawler_service, "session_scope", failing_session_scope),
            patch.object(crawler_service, "patch_local_item_detail", return_value={}),
            patch.object(
                crawler_service,
                "apply_product_image_changes",
                return_value=({}, [image_url]),
            ),
            patch.object(crawler_service, "product_editable_image_urls", return_value=[]),
            patch.object(crawler_service, "product_shop_code", return_value=""),
            patch.object(crawler_service, "product_detail_to_public", return_value={"id": 5}),
            patch.object(crawler_service, "cleanup_product_image_urls", cleanup),
        ):
            with self.assertRaisesRegex(RuntimeError, "commit failed"):
                crawler_service.update_product_local_detail("owner", 5, payload)

        cleanup.assert_not_called()

    def test_localization_local_cleanup_is_not_called_when_commit_fails(self):
        old_url = crawler_service.local_product_image_url(5, "old.jpg")
        product = SimpleNamespace(
            id=5,
            owner_username="owner",
            review_status="pending",
            title="product",
            source_url="https://example.com/product",
            raw_payload_json="{}",
            image_url=old_url,
            last_error=None,
        )
        first_session = SimpleNamespace(
            get=lambda _model, _product_id: product,
        )
        second_session = SimpleNamespace(
            get=lambda _model, _product_id: product,
            flush=lambda: None,
        )
        sessions = iter((first_session, second_session))

        @contextmanager
        def failing_second_session_scope():
            session = next(sessions)
            yield session
            if session is second_session:
                raise RuntimeError("commit failed")

        cleanup = Mock()
        with (
            patch.object(crawler_service, "session_scope", failing_second_session_scope),
            patch.object(crawler_service, "product_raw_payload", return_value={}),
            patch.object(crawler_service, "product_shop_code", return_value=""),
            patch.object(crawler_service, "product_editable_image_urls", return_value=[old_url]),
            patch.object(
                crawler_service,
                "localize_product_image_urls",
                return_value={"urls": [old_url], "replacementMap": {}, "errors": []},
            ),
            patch.object(
                crawler_service,
                "localize_product_description_images",
                return_value={"replacementMap": {}, "errors": [], "warnings": [], "removedUrls": []},
            ),
            patch.object(crawler_service, "set_product_image_urls", return_value={}),
            patch.object(crawler_service, "collect_local_product_image_urls", return_value=[old_url]),
            patch.object(crawler_service, "remove_unused_local_product_images", cleanup),
        ):
            with self.assertRaisesRegex(RuntimeError, "commit failed"):
                crawler_service.localize_collected_product_images("owner", 5)

        cleanup.assert_not_called()

    def test_localization_local_cleanup_runs_after_commit(self):
        old_url = crawler_service.local_product_image_url(5, "old.jpg")
        product = SimpleNamespace(
            id=5,
            owner_username="owner",
            review_status="pending",
            title="product",
            source_url="https://example.com/product",
            raw_payload_json="{}",
            image_url=old_url,
            last_error=None,
        )
        first_session = SimpleNamespace(
            get=lambda _model, _product_id: product,
        )
        second_session = SimpleNamespace(
            get=lambda _model, _product_id: product,
            flush=lambda: None,
        )
        sessions = iter((first_session, second_session))
        second_commit_completed = False

        @contextmanager
        def successful_session_scope():
            nonlocal second_commit_completed
            session = next(sessions)
            yield session
            if session is second_session:
                second_commit_completed = True

        def assert_committed(_product_id, _referenced_urls):
            self.assertTrue(second_commit_completed)

        with (
            patch.object(crawler_service, "session_scope", successful_session_scope),
            patch.object(crawler_service, "product_raw_payload", return_value={}),
            patch.object(crawler_service, "product_shop_code", return_value=""),
            patch.object(crawler_service, "product_editable_image_urls", return_value=[old_url]),
            patch.object(
                crawler_service,
                "localize_product_image_urls",
                return_value={"urls": [old_url], "replacementMap": {}, "errors": []},
            ),
            patch.object(
                crawler_service,
                "localize_product_description_images",
                return_value={"replacementMap": {}, "errors": [], "warnings": [], "removedUrls": []},
            ),
            patch.object(crawler_service, "set_product_image_urls", return_value={}),
            patch.object(crawler_service, "collect_local_product_image_urls", return_value=[old_url]),
            patch.object(
                crawler_service,
                "remove_unused_local_product_images",
                side_effect=assert_committed,
            ),
        ):
            crawler_service.localize_collected_product_images("owner", 5)

    def test_clear_product_files_deletes_both_oss_prefixes_and_local_fallback(self):
        self.storage.objects.update(
            {
                "product-images/6/a.jpg": b"a",
                "product-image-drafts/6/b.jpg": b"b",
            }
        )
        (self.product_root / "6").mkdir(parents=True)
        (self.product_root / "6" / "a.jpg").write_bytes(b"local")

        crawler_service.clear_product_temp_image_files(6)

        self.assertEqual(self.storage.objects, {})
        self.assertFalse((self.product_root / "6").exists())

    def test_cleanup_expired_drafts_deletes_only_old_oss_objects(self):
        old_key = "product-image-drafts/7/old.jpg"
        recent_key = "product-image-drafts/7/recent.jpg"
        self.storage.objects.update({old_key: b"old", recent_key: b"recent"})
        self.storage.last_modified[old_key] = 1_700_000_000
        self.storage.last_modified[recent_key] = 1_799_900_000

        with (
            patch.object(crawler_service.time, "time", return_value=1_800_000_000),
            patch.object(crawler_service.settings, "product_image_draft_retention_days", 7),
        ):
            deleted = crawler_service.cleanup_expired_product_image_drafts()

        self.assertEqual(deleted, 1)
        self.assertNotIn(old_key, self.storage.objects)
        self.assertIn(recent_key, self.storage.objects)

    def test_collect_product_ids_includes_oss_only_prefixes(self):
        self.storage.objects.update(
            {
                "product-images/77/a.jpg": b"a",
                "product-image-drafts/78/b.jpg": b"b",
            }
        )

        self.assertEqual(
            set(crawler_service.collect_product_image_dir_ids()),
            {77, 78},
        )

    def test_recent_oss_only_prefix_is_not_ready_for_orphan_cleanup(self):
        ready = crawler_service.orphan_product_ids_ready_for_cleanup(
            product_ids=[77, 78],
            existing_ids=set(),
            oss_last_modified={77: 1_799_900_000, 78: 1_700_000_000},
            cutoff=1_799_000_000,
        )

        self.assertEqual(ready, [78])

    def test_orphan_cleanup_does_not_delete_object_created_after_snapshot(self):
        old_key = "product-images/77/old.jpg"
        new_key = "product-images/77/new.jpg"
        self.storage.objects[old_key] = b"old"
        self.storage.last_modified[old_key] = 1_700_000_000
        inserted_new_object = False

        class FakeScalars:
            @staticmethod
            def all():
                return []

        class FakeSession:
            def scalars(inner_self, _query):
                nonlocal inserted_new_object
                if not inserted_new_object:
                    inserted_new_object = True
                    self.storage.objects[new_key] = b"new"
                    self.storage.last_modified[new_key] = 1_799_900_000
                return FakeScalars()

        @contextmanager
        def fake_session_scope():
            yield FakeSession()

        with (
            patch.object(crawler_service, "session_scope", fake_session_scope),
            patch.object(crawler_service.time, "time", return_value=1_800_000_000),
            patch.object(crawler_service.settings, "product_image_orphan_retention_days", 7),
        ):
            deleted = crawler_service.cleanup_orphan_product_image_dirs()

        self.assertEqual(deleted, 1)
        self.assertNotIn(old_key, self.storage.objects)
        self.assertIn(new_key, self.storage.objects)

    def test_product_image_content_info_reads_oss_and_preserves_media_type(self):
        image_url = crawler_service.local_product_image_url(10, "photo.png")
        self.storage.objects["product-images/10/photo.png"] = b"png-content"

        info = crawler_service.product_image_content_info(image_url)

        self.assertEqual(info["content"], b"png-content")
        self.assertEqual(info["mediaType"], "image/png")


if __name__ == "__main__":
    unittest.main()
