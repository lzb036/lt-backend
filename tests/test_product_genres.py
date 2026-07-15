from __future__ import annotations

import json
import unittest
from contextlib import contextmanager
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from fastapi import HTTPException
from fastapi.routing import APIRoute

from app.core.auth import require_permission
from app.services import crawler_service


def sample_genre() -> tuple[str, str]:
    genres = crawler_service.load_rakuten_attribute_rules()["genres"]
    genre_id, genre = next(iter(genres.items()))
    return genre_id, genre["genrePath"]


def product(
    product_id: int,
    *,
    owner: str = "operator",
    status: str = "pending",
    genre_id: str = "",
    title: str = "测试商品",
) -> SimpleNamespace:
    return SimpleNamespace(
        id=product_id,
        owner_username=owner,
        review_status=status,
        genre_id=genre_id,
        title=title,
        raw_payload_json=json.dumps({"title": title}, ensure_ascii=False),
    )


@contextmanager
def session_context(session: MagicMock):
    yield session


class ProductGenreServiceTests(unittest.TestCase):
    def test_fixed_chinese_map_covers_every_genre_segment(self) -> None:
        genres = crawler_service.load_rakuten_attribute_rules()["genres"]
        translations = crawler_service.load_rakuten_genre_zh_map()
        segments = {
            segment.strip()
            for genre in genres.values()
            if isinstance(genre, dict)
            for segment in str(genre.get("genrePath") or "").split(">")
            if segment.strip()
        }

        self.assertEqual(segments - translations.keys(), set())
        self.assertTrue(all(translations[segment].strip() for segment in segments))

    def test_rakuten_genre_path_returns_complete_path_for_known_id(self) -> None:
        genre_id, expected_path = sample_genre()

        self.assertEqual(crawler_service.rakuten_genre_path(genre_id), expected_path)

    def test_rakuten_genre_zh_path_maps_each_segment_and_falls_back_to_source(self) -> None:
        with patch.object(
            crawler_service,
            "load_rakuten_genre_zh_map",
            return_value={"鞋": "鞋靴", "女士鞋": "女鞋", "凉鞋": "凉鞋"},
        ):
            self.assertEqual(
                crawler_service.rakuten_genre_zh_path("鞋>女士鞋>凉鞋"),
                "鞋靴>女鞋>凉鞋",
            )
            self.assertEqual(crawler_service.rakuten_genre_zh_path("未知分类"), "未知分类")

    def test_search_rakuten_genres_matches_id_and_path_and_respects_limit(self) -> None:
        genre_id, genre_path = sample_genre()
        path_keyword = genre_path.split(">")[-1]

        id_results = crawler_service.search_rakuten_genres(genre_id, limit=5)
        path_results = crawler_service.search_rakuten_genres(path_keyword, limit=2)

        self.assertEqual(
            id_results[0],
            {
                "genreId": genre_id,
                "genrePath": genre_path,
                "genrePathZh": crawler_service.rakuten_genre_zh_path(genre_path),
            },
        )
        self.assertLessEqual(len(path_results), 2)
        self.assertTrue(any(path_keyword.casefold() in item["genrePath"].casefold() for item in path_results))

    def test_search_rakuten_genres_matches_chinese_path(self) -> None:
        genres = {"100001": {"genrePath": "靴>レディース靴>サンダル"}}
        translations = {"靴": "鞋靴", "レディース靴": "女鞋", "サンダル": "凉鞋"}

        with (
            patch.object(crawler_service, "load_rakuten_attribute_rules", return_value={"genres": genres}),
            patch.object(crawler_service, "load_rakuten_genre_zh_map", return_value=translations),
        ):
            results = crawler_service.search_rakuten_genres("凉鞋")

        self.assertEqual(results, [{
            "genreId": "100001",
            "genrePath": "靴>レディース靴>サンダル",
            "genrePathZh": "鞋靴>女鞋>凉鞋",
        }])

    def test_list_rakuten_genre_children_returns_sorted_direct_children(self) -> None:
        genres = {
            "100001": {"genrePath": "鞋>女士鞋>凉鞋"},
            "100002": {"genrePath": "鞋>女士鞋>靴子"},
            "100003": {"genrePath": "服饰>女士服饰"},
        }

        with patch.object(
            crawler_service,
            "load_rakuten_attribute_rules",
            return_value={"genres": genres},
        ), patch.object(
            crawler_service,
            "load_rakuten_genre_zh_map",
            return_value={"鞋": "鞋靴", "女士鞋": "女鞋", "凉鞋": "凉鞋", "靴子": "靴子", "服饰": "服饰"},
        ):
            roots = crawler_service.list_rakuten_genre_children("")
            shoe_children = crawler_service.list_rakuten_genre_children("鞋")
            women_shoe_children = crawler_service.list_rakuten_genre_children("鞋>女士鞋")

        self.assertEqual([item["label"] for item in roots], ["服饰", "鞋"])
        self.assertEqual(shoe_children, [{
            "label": "女士鞋",
            "labelZh": "女鞋",
            "genrePath": "鞋>女士鞋",
            "genrePathZh": "鞋靴>女鞋",
            "genreId": "",
            "leaf": False,
        }])
        self.assertEqual(
            women_shoe_children,
            [
                {
                    "label": "凉鞋",
                    "labelZh": "凉鞋",
                    "genrePath": "鞋>女士鞋>凉鞋",
                    "genrePathZh": "鞋靴>女鞋>凉鞋",
                    "genreId": "100001",
                    "leaf": True,
                },
                {
                    "label": "靴子",
                    "labelZh": "靴子",
                    "genrePath": "鞋>女士鞋>靴子",
                    "genrePathZh": "鞋靴>女鞋>靴子",
                    "genreId": "100002",
                    "leaf": True,
                },
            ],
        )

    def test_product_to_public_includes_derived_genre_path(self) -> None:
        genre_id, genre_path = sample_genre()
        row = product(1, genre_id=genre_id)
        row.task_id = None
        row.parent_product_id = None
        row.listing_task_id = None
        row.store_id = None
        row.rakuten_manage_number = None
        row.store_product_status = ""
        row.rakuten_listing_status = ""
        row.store_last_seen_at = None
        row.tagline = ""
        row.source_url = "https://example.com/item"
        row.rakuten_item_url = ""
        row.item_number = ""
        row.shop_name = ""
        row.image_url = ""
        row.images = []
        row.price = None
        row.currency = "JPY"
        row.last_error = None
        row.listed_at = None
        row.created_at = None
        row.updated_at = None

        public = crawler_service.product_to_public(row)

        self.assertEqual(public["genrePath"], genre_path)
        self.assertTrue(public["genrePathZh"])

    def test_update_pending_product_genre_persists_id_and_raw_payload(self) -> None:
        genre_id, genre_path = sample_genre()
        row = product(7)
        session = MagicMock()
        session.scalars.return_value.first.return_value = row

        with (
            patch.object(crawler_service, "session_scope", return_value=session_context(session)),
            patch.object(
                crawler_service,
                "product_to_public",
                side_effect=lambda value: {
                    "id": value.id,
                    "genreId": value.genre_id,
                    "genrePath": crawler_service.rakuten_genre_path(value.genre_id),
                },
            ),
        ):
            updated = crawler_service.update_pending_product_genre("operator", 7, genre_id)

        self.assertEqual(row.genre_id, genre_id)
        self.assertEqual(json.loads(row.raw_payload_json)["genreId"], genre_id)
        self.assertEqual(updated["genrePath"], genre_path)
        session.flush.assert_called_once()

    def test_update_pending_product_genre_rejects_invalid_or_non_pending_product(self) -> None:
        genre_id, _ = sample_genre()
        session = MagicMock()
        session.scalars.return_value.first.return_value = product(7, status="approved")

        with patch.object(crawler_service, "session_scope", return_value=session_context(session)):
            with self.assertRaisesRegex(RuntimeError, "只有待审核商品"):
                crawler_service.update_pending_product_genre("operator", 7, genre_id)

        with self.assertRaisesRegex(RuntimeError, "有效品类"):
            crawler_service.update_pending_product_genre("operator", 7, "123")

        with self.assertRaisesRegex(RuntimeError, "有效品类"):
            crawler_service.update_pending_product_genre("operator", 7, "999999")

    def test_local_detail_save_persists_drafted_genre_for_pending_product(self) -> None:
        genre_id, _ = sample_genre()
        row = product(7)
        row.price = 1000
        row.image_url = ""
        row.last_error = None
        session = MagicMock()
        session.get.return_value = row
        payload = SimpleNamespace(
            title="更新标题",
            tagline="更新副标题",
            variants=[],
            imageChanges=None,
            genreId=genre_id,
        )

        with (
            patch.object(crawler_service, "session_scope", return_value=session_context(session)),
            patch.object(
                crawler_service,
                "patch_local_item_detail",
                return_value={"itemName": "更新标题", "genreId": genre_id},
            ),
            patch.object(crawler_service, "price_from_rakuten_item", return_value=1000),
            patch.object(crawler_service, "product_detail_to_public", return_value={"id": 7, "genreId": genre_id}),
            patch.object(crawler_service, "cleanup_product_image_urls"),
        ):
            result = crawler_service.update_product_local_detail("operator", 7, payload)

        self.assertEqual(row.genre_id, genre_id)
        self.assertEqual(json.loads(row.raw_payload_json)["genreId"], genre_id)
        self.assertEqual(result["genreId"], genre_id)

    def test_local_detail_save_persists_drafted_genre_for_listed_master(self) -> None:
        genre_id, _ = sample_genre()
        row = product(7, status="listed_master")
        row.price = 1000
        row.image_url = ""
        row.last_error = None
        session = MagicMock()
        session.get.return_value = row
        payload = SimpleNamespace(
            title="更新标题",
            tagline="更新副标题",
            variants=[],
            imageChanges=None,
            genreId=genre_id,
        )

        with (
            patch.object(crawler_service, "session_scope", return_value=session_context(session)),
            patch.object(
                crawler_service,
                "patch_local_item_detail",
                return_value={"itemName": "更新标题", "genreId": genre_id},
            ),
            patch.object(crawler_service, "price_from_rakuten_item", return_value=1000),
            patch.object(crawler_service, "product_detail_to_public", return_value={"id": 7, "genreId": genre_id}),
            patch.object(crawler_service, "cleanup_product_image_urls"),
        ):
            result = crawler_service.update_product_local_detail("operator", 7, payload)

        self.assertEqual(row.genre_id, genre_id)
        self.assertEqual(json.loads(row.raw_payload_json)["genreId"], genre_id)
        self.assertEqual(result["genreId"], genre_id)

    def test_local_detail_save_rejects_invalid_genre_without_mutating_product(self) -> None:
        row = product(7, genre_id="")
        row.price = 1000
        row.image_url = ""
        row.last_error = None
        session = MagicMock()
        session.get.return_value = row
        payload = SimpleNamespace(
            title="更新标题",
            tagline="",
            variants=[],
            imageChanges=None,
            genreId="999999",
        )

        with patch.object(crawler_service, "session_scope", return_value=session_context(session)):
            with self.assertRaisesRegex(RuntimeError, "有效品类"):
                crawler_service.update_product_local_detail("operator", 7, payload)

        self.assertEqual(row.genre_id, "")

    def test_store_detail_patch_sends_and_persists_genre(self) -> None:
        genre_id, _ = sample_genre()
        response = MagicMock()
        response.raise_for_status.return_value = None
        raw_payload = {
            "title": "旧标题",
            "tagline": "旧副标题",
            "genreId": "100001",
            "variants": {
                "sku-1": {
                    "standardPrice": "1000",
                    "hidden": False,
                },
            },
        }
        variant = SimpleNamespace(
            variantId="sku-1",
            standardPrice=Decimal("1200"),
            hidden=False,
        )

        with patch.object(crawler_service.requests, "patch", return_value=response) as request:
            updated = crawler_service.patch_rakuten_item_detail(
                "secret",
                "key",
                "manage-number",
                raw_payload,
                title="更新标题",
                tagline="更新副标题",
                genre_id=genre_id,
                variants=[variant],
            )

        sent_payload = request.call_args.kwargs["json"]
        self.assertEqual(sent_payload["genreId"], genre_id)
        self.assertEqual(updated["genreId"], genre_id)

    def test_approval_rejects_invalid_genre_before_mutating_any_product(self) -> None:
        genre_id, _ = sample_genre()
        valid = product(1, genre_id=genre_id, title="有效商品")
        invalid = product(2, genre_id="", title="缺少品类商品")
        session = MagicMock()
        session.scalars.return_value.all.return_value = [valid, invalid]

        with patch.object(crawler_service, "session_scope", return_value=session_context(session)):
            with self.assertRaisesRegex(RuntimeError, "1 个商品缺少有效品类"):
                crawler_service.update_product_status("operator", [1, 2], "approved")

        self.assertEqual(valid.review_status, "pending")
        self.assertEqual(invalid.review_status, "pending")

    def test_approval_accepts_product_with_valid_genre(self) -> None:
        genre_id, _ = sample_genre()
        row = product(1, genre_id=genre_id)
        session = MagicMock()
        session.scalars.return_value.all.return_value = [row]

        with (
            patch.object(crawler_service, "session_scope", return_value=session_context(session)),
            patch.object(crawler_service, "product_to_public", return_value={"id": 1, "reviewStatus": "approved"}),
            patch("app.services.ai_title_service.cleanup_title_versions_for_approved_product"),
        ):
            result = crawler_service.update_product_status("operator", [1], "approved")

        self.assertEqual(row.review_status, "approved")
        self.assertEqual(result, [{"id": 1, "reviewStatus": "approved"}])


class ProductGenreApiTests(unittest.TestCase):
    def setUp(self) -> None:
        from app.api import crawler as crawler_api

        self.api = crawler_api
        self.user = {"username": "operator", "permissions": ["products.manage"]}

    def test_genre_routes_require_products_permission(self) -> None:
        expected = {
            ("GET", "/crawler/products/genres"),
            ("PUT", "/crawler/products/{product_id}/genre"),
        }
        routes = {
            (method, route.path)
            for route in self.api.router.routes
            if isinstance(route, APIRoute)
            for method in route.methods
            if (method, route.path) in expected
        }

        self.assertEqual(routes, expected)
        products_dependency = require_permission("products.manage")
        for method, path in expected:
            route = next(
                route
                for route in self.api.router.routes
                if isinstance(route, APIRoute) and route.path == path and method in route.methods
            )
            dependency_calls = [dependency.call for dependency in route.dependant.dependencies]
            self.assertTrue(
                any(getattr(call, "__name__", "") == getattr(products_dependency, "__name__", "") for call in dependency_calls)
            )

    def test_search_and_update_routes_wrap_service_results(self) -> None:
        genre_id, genre_path = sample_genre()
        payload = self.api.ProductGenrePayload(genreId=genre_id)
        updated = {"id": 7, "genreId": genre_id, "genrePath": genre_path}

        with patch.object(
            self.api.crawler_service,
            "search_rakuten_genres",
            return_value=[{"genreId": genre_id, "genrePath": genre_path}],
        ) as search_mock:
            search_result = self.api.search_product_genres(keyword=genre_id, limit=10, user=self.user)

        with patch.object(
            self.api.crawler_service,
            "update_pending_product_genre",
            return_value=updated,
        ) as update_mock:
            update_result = self.api.update_product_genre(7, payload, user=self.user)

        self.assertEqual(search_result, {"genres": [{"genreId": genre_id, "genrePath": genre_path}]})
        self.assertEqual(update_result, {"product": updated})
        search_mock.assert_called_once_with(genre_id, 10)
        update_mock.assert_called_once_with("operator", 7, genre_id)

    def test_genre_children_route_wraps_service_result(self) -> None:
        children = [{
            "label": "女士鞋",
            "genrePath": "鞋>女士鞋",
            "genreId": "",
            "leaf": False,
        }]

        with patch.object(
            self.api.crawler_service,
            "list_rakuten_genre_children",
            return_value=children,
        ) as mock_children:
            result = self.api.list_product_genre_children(parentPath="鞋", user=self.user)

        self.assertEqual(result, {"genres": children})
        mock_children.assert_called_once_with("鞋")

    def test_update_route_maps_service_error_to_bad_request(self) -> None:
        genre_id, _ = sample_genre()
        payload = self.api.ProductGenrePayload(genreId=genre_id)

        with patch.object(
            self.api.crawler_service,
            "update_pending_product_genre",
            side_effect=RuntimeError("只有待审核商品可以修改品类。"),
        ):
            with self.assertRaises(HTTPException) as context:
                self.api.update_product_genre(7, payload, user=self.user)

        self.assertEqual(context.exception.status_code, 400)
        self.assertEqual(context.exception.detail, "只有待审核商品可以修改品类。")


if __name__ == "__main__":
    unittest.main()
