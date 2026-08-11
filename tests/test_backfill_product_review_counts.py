from __future__ import annotations

from scripts import backfill_product_review_counts


def test_listing_review_counts_normalizes_urls_and_keeps_zero() -> None:
    counts = backfill_product_review_counts.listing_review_counts(
        [
            {
                "source_url": "https://item.rakuten.co.jp/shop/item-1/?variantId=1",
                "raw": {"reviewCount": 12},
            },
            {
                "source_url": "https://item.rakuten.co.jp/shop/item-2/",
                "raw": {"reviewCount": 0},
            },
            {
                "source_url": "https://item.rakuten.co.jp/shop/item-3/",
                "raw": {},
            },
        ]
    )

    assert counts == {
        "https://item.rakuten.co.jp/shop/item-1/": 12,
        "https://item.rakuten.co.jp/shop/item-2/": 0,
    }
