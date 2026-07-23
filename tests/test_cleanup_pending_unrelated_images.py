from __future__ import annotations

import unittest

from scripts import cleanup_pending_unrelated_product_images


class PendingUnrelatedImageCleanupTests(unittest.TestCase):
    def test_cleanup_keeps_trusted_product_images_and_removes_appended_promotions(self):
        trusted_sources = [
            "https://image.rakuten.co.jp/shop/cabinet/product/a.jpg",
            "https://image.rakuten.co.jp/shop/cabinet/product/b.jpg",
        ]
        current_images = [
            "/api/static/product-images/1/p01.jpg",
            "/api/static/product-images/1/p02.jpg",
            "/api/static/product-images/1/topic.jpg",
            "/api/static/product-images/1/banner.jpg",
        ]
        payload = {
            "images": current_images,
            "ltEditedImages": current_images,
            "embeddedItem": {
                "pcFields": {
                    "images": [{"location": url} for url in trusted_sources],
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
            },
        }

        kept, removed, trusted = cleanup_pending_unrelated_product_images.cleanup_image_references(
            payload,
            current_images,
            shop_code="shop",
        )

        self.assertEqual(kept, current_images[:2])
        self.assertEqual(removed, current_images[2:])
        self.assertEqual(trusted, trusted_sources)

    def test_cleanup_skips_product_when_no_trusted_source_images_exist(self):
        current_images = ["/api/static/product-images/1/p01.jpg"]

        kept, removed, trusted = cleanup_pending_unrelated_product_images.cleanup_image_references(
            {"images": current_images, "ltEditedImages": current_images},
            current_images,
            shop_code="shop",
        )

        self.assertEqual(kept, current_images)
        self.assertEqual(removed, [])
        self.assertEqual(trusted, [])

    def test_payload_cleanup_removes_other_item_recommendations_only(self):
        payload = {
            "itemNumber": "y0219871",
            "embeddedItem": {
                "newProductDescription": (
                    '<a href="https://www.rakuten.ne.jp/gold/gadgery/">'
                    '<img src="shop-notice.jpg"></a>'
                    '<a href="https://item.rakuten.co.jp/gadgery/y14406056/">'
                    '<img src="other-product.jpg"></a>'
                ),
            },
        }

        cleaned = (
            cleanup_pending_unrelated_product_images.crawler_service
            .remove_cross_item_rakuten_image_links_from_payload(
                payload,
                shop_code="gadgery",
                item_number="y0219871",
            )
        )

        description = cleaned["embeddedItem"]["newProductDescription"]
        self.assertIn("shop-notice.jpg", description)
        self.assertNotIn("other-product.jpg", description)
        self.assertNotEqual(cleaned, payload)


if __name__ == "__main__":
    unittest.main()
