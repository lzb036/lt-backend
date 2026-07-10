from __future__ import annotations

import asyncio
import unittest
from unittest.mock import patch

from app import main
from app.api import crawler as crawler_api


class ProductImageHttpResponseTests(unittest.TestCase):
    def test_get_response_returns_image_content_and_cache_headers(self):
        with patch.object(
            main.crawler_service,
            "product_image_http_info",
            return_value={
                "type": "stream",
                "body": iter([b"image-", b"data"]),
                "size": 10,
                "mediaType": "image/png",
            },
        ):
            response = main.build_product_image_response(
                "/api/static/product-images/1/photo.png",
                method="GET",
            )

        async def collect_body() -> bytes:
            chunks = [chunk async for chunk in response.body_iterator]
            return b"".join(chunks)

        self.assertEqual(response.status_code, 200)
        self.assertEqual(asyncio.run(collect_body()), b"image-data")
        self.assertEqual(response.media_type, "image/png")
        self.assertEqual(response.headers["content-length"], "10")
        self.assertIn("max-age", response.headers["cache-control"])

    def test_head_response_returns_length_without_body(self):
        with patch.object(
            main.crawler_service,
            "product_image_http_info",
            return_value={
                "type": "metadata",
                "size": 10,
                "mediaType": "image/jpeg",
            },
        ):
            response = main.build_product_image_response(
                "/api/static/product-images/1/photo.jpg",
                method="HEAD",
            )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.body, b"")
        self.assertEqual(response.headers["content-length"], "10")

    def test_missing_image_response_is_404(self):
        with patch.object(
            main.crawler_service,
            "product_image_http_info",
            side_effect=RuntimeError("商品图片文件不存在。"),
        ):
            response = main.build_product_image_response(
                "/api/static/product-images/1/missing.jpg",
                method="GET",
            )

        self.assertEqual(response.status_code, 404)

    def test_authenticated_download_streams_stored_content(self):
        with (
            patch.object(crawler_api.crawler_service, "product_review_statuses", return_value={"pending"}),
            patch.object(crawler_api, "has_permission", return_value=True),
            patch.object(
                crawler_api.crawler_service,
                "product_image_download_info",
                return_value={
                    "type": "stream",
                    "body": iter([b"download-", b"content"]),
                    "filename": "product-1-1.jpg",
                    "mediaType": "image/jpeg",
                },
            ),
        ):
            response = crawler_api.download_product_image(1, 0, user={"username": "owner"})

        async def collect_body() -> bytes:
            chunks = [chunk async for chunk in response.body_iterator]
            return b"".join(chunks)

        self.assertEqual(asyncio.run(collect_body()), b"download-content")
        self.assertIn("product-1-1.jpg", response.headers["content-disposition"])


if __name__ == "__main__":
    unittest.main()
