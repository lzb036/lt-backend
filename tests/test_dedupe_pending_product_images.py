from __future__ import annotations

import unittest
from unittest.mock import patch

from app.services.crawler_service import ProductImageVisualSignature
from scripts import dedupe_pending_product_images


class PendingProductImageDeduplicationTests(unittest.TestCase):
    def test_build_deduplication_merges_visually_equal_reencoded_images(self):
        first_url = "/api/static/product-images/1/full.jpg"
        duplicate_url = "/api/static/product-images/1/reencoded.jpg"
        pixels = bytes([10, 20, 30] * (32 * 32))
        first_signature = ProductImageVisualSignature(800, 600, pixels)
        duplicate_signature = ProductImageVisualSignature(
            200,
            150,
            bytes(min(255, value + 2) for value in pixels),
        )

        with patch.object(
            dedupe_pending_product_images,
            "image_content_fingerprint",
            side_effect=[
                ("sha-full", first_signature),
                ("sha-reencoded", duplicate_signature),
            ],
        ):
            deduplicated, replacements, errors = dedupe_pending_product_images.build_deduplication(
                [first_url, duplicate_url],
                [],
                workers=2,
            )

        self.assertEqual(deduplicated, [first_url])
        self.assertEqual(replacements, {duplicate_url: first_url})
        self.assertEqual(errors, [])


if __name__ == "__main__":
    unittest.main()
