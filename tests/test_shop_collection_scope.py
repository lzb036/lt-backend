from __future__ import annotations

import unittest
from unittest.mock import patch

from app.services import crawler_service


class ShopCollectionScopeTests(unittest.TestCase):
    def test_fallback_shop_url_is_used_as_the_only_collection_target(self) -> None:
        target = (
            "店铺:モンドセレクション 日榜 全部\n"
            "__LT_FALLBACK_SHOP_URL__:https://www.rakuten.co.jp/mondeselection/"
        )

        with patch.object(
            crawler_service,
            "collect_item_plan_for_target",
            return_value=crawler_service.CollectedItemPlan(0, ()),
        ) as collect:
            result = crawler_service.collect_items("shop", target, task_id="task-1")

        self.assertEqual(result, [])
        collect.assert_called_once_with(
            "shop",
            "店铺:mondeselection 日榜 全部",
            task_id="task-1",
        )

    def test_fallback_search_url_prefers_sid_over_shop_display_name(self) -> None:
        target = (
            "店铺:CUTE NAILS TOWN 周榜 前 3000\n"
            "__LT_FALLBACK_SHOP_URL__:"
            "https://search.rakuten.co.jp/search/mall/?sn=CUTE%20NAILS%20TOWN&sid=403301"
        )

        with patch.object(
            crawler_service,
            "collect_item_plan_for_target",
            return_value=crawler_service.CollectedItemPlan(0, ()),
        ) as collect:
            result = crawler_service.collect_items("shop", target, task_id="task-2")

        self.assertEqual(result, [])
        collect.assert_called_once_with(
            "shop",
            "店铺:403301 周榜 前 3000",
            task_id="task-2",
        )

    def test_fallback_search_url_prefers_valid_shop_code_over_sid(self) -> None:
        normalized = crawler_service.normalize_rakuten_shop_target(
            "https://search.rakuten.co.jp/search/mall/"
            "?sn=GlobalTime%20楽天市場店&su=global-time&sid=403277"
        )

        self.assertEqual(normalized, "global-time")

    def test_fallback_search_url_uses_sid_when_shop_code_is_malformed(self) -> None:
        normalized = crawler_service.normalize_rakuten_shop_target(
            "https://search.rakuten.co.jp/search/mall/"
            "?sn=OKEYA楽天市場店&su=https%3A%2F%2Fexample.com%2Fbad&sid=403302"
        )

        self.assertEqual(normalized, "403302")

    def test_display_name_only_shop_target_is_rejected(self) -> None:
        with (
            patch.object(crawler_service, "crawl_price_rule_for_task", return_value={}),
            patch.object(crawler_service, "collect_listing_items") as collect_listing,
        ):
            with self.assertRaisesRegex(RuntimeError, "不能只填写店铺展示名称"):
                crawler_service.collect_items_for_target(
                    "shop",
                    "店铺:モンドセレクション 日榜 全部",
                )

        collect_listing.assert_not_called()

    def test_explicit_shop_code_filters_out_other_shops(self) -> None:
        listing_items = [
            {
                "title": "correct",
                "source_url": "https://item.rakuten.co.jp/mondeselection/item-1/",
            },
            {
                "title": "wrong",
                "source_url": "https://item.rakuten.co.jp/other-shop/item-2/",
            },
        ]

        with (
            patch.object(crawler_service, "crawl_price_rule_for_task", return_value={}),
            patch.object(
                crawler_service,
                "resolve_rakuten_shop_search_keyword",
                return_value="モンドセレクション",
            ),
            patch.object(
                crawler_service,
                "collect_listing_items",
                return_value=listing_items,
            ) as collect_listing,
            patch.object(
                crawler_service,
                "existing_collected_source_hashes_for_task",
                return_value=set(),
            ),
            patch.object(
                crawler_service,
                "iter_enriched_collected_items_with_detail",
                side_effect=lambda items, **_: iter(items),
            ),
        ):
            result = crawler_service.collect_items_for_target(
                "shop",
                "店铺:mondeselection 日榜 全部",
            )

        collect_listing.assert_called_once()
        self.assertIsNone(collect_listing.call_args.args[1])
        self.assertEqual(
            [item["source_url"] for item in result],
            ["https://item.rakuten.co.jp/mondeselection/item-1/"],
        )

    def test_numeric_sid_uses_sid_scoped_search(self) -> None:
        with (
            patch.object(crawler_service, "crawl_price_rule_for_task", return_value={}),
            patch.object(
                crawler_service,
                "collect_listing_items",
                return_value=[],
            ) as collect_listing,
            patch.object(
                crawler_service,
                "existing_collected_source_hashes_for_task",
                return_value=set(),
            ),
            patch.object(
                crawler_service,
                "iter_enriched_collected_items_with_detail",
                return_value=iter(()),
            ),
        ):
            result = crawler_service.collect_items_for_target(
                "shop",
                "店铺:123456 日榜 前 30",
            )

        self.assertEqual(result, [])
        collect_listing.assert_called_once_with(
            "https://search.rakuten.co.jp/search/mall/?sid=123456",
            30,
            task_id=None,
        )


if __name__ == "__main__":
    unittest.main()
