from __future__ import annotations

import unittest

from app.services import crawler_service


class CrawlMinPriceFilterTests(unittest.TestCase):
    def test_price_below_threshold_is_filtered(self) -> None:
        self.assertTrue(
            crawler_service.should_filter_collected_item_by_price(
                {"price": 2499},
                {"operator": "gte", "value": 2500},
            )
        )

    def test_price_equal_to_threshold_is_kept(self) -> None:
        self.assertFalse(
            crawler_service.should_filter_collected_item_by_price(
                {"price": 2500},
                {"operator": "gte", "value": 2500},
            )
        )

    def test_unknown_price_is_kept_for_detail_collection(self) -> None:
        self.assertFalse(
            crawler_service.should_filter_collected_item_by_price(
                {"price": None},
                {"operator": "gte", "value": 3800},
            )
        )


if __name__ == "__main__":
    unittest.main()
