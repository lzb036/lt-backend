from __future__ import annotations

import unittest
from unittest.mock import Mock, patch

import requests

from app.services import crawler_service


def image_response(status_code: int = 200, content: bytes = b"image") -> Mock:
    response = Mock()
    response.status_code = status_code
    response.headers = {"Content-Type": "image/jpeg"}
    response.iter_content.return_value = [content]
    response.close.return_value = None
    if status_code >= 400:
        response.raise_for_status.side_effect = requests.HTTPError(response=response)
    else:
        response.raise_for_status.return_value = None
    return response


class ProductImageDirectFallbackTests(unittest.TestCase):
    def test_direct_success_does_not_use_proxy(self) -> None:
        direct = image_response()
        with patch.object(crawler_service.requests, "get", return_value=direct) as request:
            result = crawler_service.download_remote_product_image(
                "https://tshop.r10s.jp/example/image.jpg",
                max_bytes=1024,
                size_error_message="too large",
            )

        self.assertEqual(result["content"], b"image")
        self.assertIsNone(request.call_args.kwargs["proxies"])
        request.assert_called_once()

    def test_connection_error_retries_through_proxy(self) -> None:
        proxied = image_response()
        with (
            patch.object(
                crawler_service.requests,
                "get",
                side_effect=[requests.ConnectionError("direct failed"), proxied],
            ) as request,
            patch.object(
                crawler_service,
                "crawler_request_proxies",
                return_value={"https": "http://proxy"},
            ),
        ):
            result = crawler_service.download_remote_product_image(
                "https://tshop.r10s.jp/example/image.jpg",
                max_bytes=1024,
                size_error_message="too large",
            )

        self.assertEqual(result["content"], b"image")
        self.assertEqual(request.call_args_list[0].kwargs["proxies"], None)
        self.assertEqual(request.call_args_list[1].kwargs["proxies"], {"https": "http://proxy"})

    def test_not_found_does_not_retry_through_proxy(self) -> None:
        missing = image_response(404)
        with (
            patch.object(crawler_service.requests, "get", return_value=missing) as request,
            self.assertRaises(crawler_service.ProductImageUnavailableError),
        ):
            crawler_service.download_remote_product_image(
                "https://tshop.r10s.jp/example/missing.jpg",
                max_bytes=1024,
                size_error_message="too large",
            )

        request.assert_called_once()


if __name__ == "__main__":
    unittest.main()
