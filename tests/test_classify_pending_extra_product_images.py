from __future__ import annotations

import unittest

from scripts import classify_pending_extra_product_images


class PendingExtraProductImageClassificationTests(unittest.TestCase):
    def test_shop_home_image_is_safe_to_remove_from_main_images(self):
        classification = classify_pending_extra_product_images.classify_extra_image(
            hrefs=["https://www.rakuten.ne.jp/gold/gadgery/"],
            source_url="https://image.rakuten.co.jp/gadgery/cabinet/shop/banner.jpg",
            shop_code="gadgery",
            item_number="y0219871",
        )

        self.assertEqual(classification, "safe_remove")

    def test_unlinked_description_image_is_kept(self):
        classification = classify_pending_extra_product_images.classify_extra_image(
            hrefs=[""],
            source_url="https://image.rakuten.co.jp/shop/cabinet/detail/image01.jpg",
            shop_code="shop",
            item_number="item-001",
        )

        self.assertEqual(classification, "keep")

    def test_source_matching_current_item_number_is_kept(self):
        classification = classify_pending_extra_product_images.classify_extra_image(
            hrefs=[],
            source_url="https://image.rakuten.co.jp/shop/cabinet/items/item-001-detail.jpg",
            shop_code="shop",
            item_number="item-001",
        )

        self.assertEqual(classification, "keep")

    def test_unmapped_image_requires_manual_review(self):
        classification = classify_pending_extra_product_images.classify_extra_image(
            hrefs=[],
            source_url="https://image.rakuten.co.jp/shop/cabinet/misc/image01.jpg",
            shop_code="shop",
            item_number="item-001",
        )

        self.assertEqual(classification, "manual_review")

    def test_description_contexts_capture_linked_and_unlinked_images(self):
        payload = {
            "descriptions": [
                {
                    "value": (
                        '<a href="https://www.rakuten.ne.jp/gold/shop/">'
                        '<img src="/api/static/product-images/1/banner.jpg"></a>'
                        '<img src="/api/static/product-images/1/detail.jpg">'
                    ),
                },
            ],
        }

        contexts = classify_pending_extra_product_images.description_image_link_contexts(payload)

        self.assertEqual(
            contexts["/api/static/product-images/1/banner.jpg"],
            ["https://www.rakuten.ne.jp/gold/shop/"],
        )
        self.assertEqual(
            contexts["/api/static/product-images/1/detail.jpg"],
            [""],
        )


if __name__ == "__main__":
    unittest.main()
