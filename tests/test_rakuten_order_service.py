from __future__ import annotations

from datetime import datetime
import unittest
from unittest.mock import Mock, patch

import requests

from app.core.config import settings
from app.services import rakuten_order_service


def json_response(payload: object, *, status_code: int = 200, text: str = "") -> Mock:
    response = Mock()
    response.status_code = status_code
    response.text = text
    response.json.return_value = payload
    return response


class RakutenOrderServiceTests(unittest.TestCase):
    def test_search_order_numbers_paginates_and_uses_esa_authorization(self) -> None:
        first_page = json_response({
            "orderNumberList": ["1001", "1002"],
            "PaginationResponseModel": {
                "totalPages": 2,
                "requestPage": 1,
            },
        })
        second_page = json_response({
            "orderNumberList": ["1003"],
            "PaginationResponseModel": {
                "totalPages": 2,
                "requestPage": 2,
            },
        })

        with (
            patch.object(
                rakuten_order_service,
                "crawler_request_proxies",
                return_value={"http": "http://proxy", "https": "http://proxy"},
            ),
            patch.object(
                rakuten_order_service.requests.Session,
                "post",
                side_effect=[first_page, second_page],
            ) as post,
        ):
            result = rakuten_order_service.search_order_numbers(
                "secret-123",
                "key-456",
                datetime(2026, 7, 1, 0, 0, 0),
                datetime(2026, 7, 2, 0, 0, 0),
                [100, 200],
            )

        self.assertEqual(result, ["1001", "1002", "1003"])
        self.assertEqual(post.call_count, 2)
        self.assertEqual(
            post.call_args_list[0].kwargs["headers"]["Authorization"],
            "ESA c2VjcmV0LTEyMzprZXktNDU2",
        )
        self.assertEqual(post.call_args_list[0].kwargs["timeout"], settings.crawler_timeout_seconds)
        self.assertEqual(
            post.call_args_list[0].kwargs["proxies"],
            {"http": "http://proxy", "https": "http://proxy"},
        )
        self.assertEqual(
            post.call_args_list[0].kwargs["json"]["PaginationRequestModel"]["requestPage"],
            1,
        )
        self.assertEqual(
            post.call_args_list[1].kwargs["json"]["PaginationRequestModel"]["requestPage"],
            2,
        )
        self.assertEqual(post.call_args_list[0].args[0], rakuten_order_service.RAKUTEN_ORDER_SEARCH_URL)

    def test_get_orders_uses_version_7_and_rms_safe_batches(self) -> None:
        order_numbers = [f"ORDER-{index:03d}" for index in range(205)]
        pages = [
            json_response({"OrderModelList": [{"orderNumber": batch[0]}]})
            for batch in (
                order_numbers[:100],
                order_numbers[100:200],
                order_numbers[200:],
            )
        ]

        with patch.object(
            rakuten_order_service.requests.Session,
            "post",
            side_effect=pages,
        ) as post:
            result = rakuten_order_service.get_orders("secret-123", "key-456", order_numbers)

        self.assertEqual(len(result), 3)
        self.assertEqual(post.call_count, 3)
        for call in post.call_args_list:
            self.assertEqual(call.args[0], rakuten_order_service.RAKUTEN_ORDER_GET_URL)
            self.assertEqual(call.kwargs["json"]["version"], 7)
            self.assertLessEqual(
                len(call.kwargs["json"]["orderNumberList"]),
                rakuten_order_service.RMS_SAFE_ORDER_BATCH_SIZE,
            )

    def test_iter_order_items_normalizes_package_items(self) -> None:
        order = {
            "orderNumber": "1001",
            "PackageModelList": [
                {
                    "ItemModelList": [
                        {
                            "itemDetailId": "detail-1",
                            "manageNumber": "manage-1",
                            "itemNumber": "item-1",
                            "SkuModelList": [{"variantId": "sku-1"}],
                            "units": "2",
                            "price": 1200,
                            "priceTaxIncl": 1320,
                            "deleteItemFlag": 0,
                            "restoreInventoryFlag": 1,
                        }
                    ]
                },
                {
                    "ItemModelList": [
                        {
                            "itemDetailId": "detail-2",
                            "manageNumber": "manage-2",
                            "itemNumber": "item-2",
                            "SkuModelList": None,
                            "units": None,
                            "price": "900",
                            "priceTaxIncl": "990",
                            "deleteItemFlag": True,
                            "restoreInventoryFlag": False,
                        }
                    ]
                },
            ],
        }

        result = list(rakuten_order_service.iter_order_items(order))

        self.assertEqual(result, [
            {
                "orderNumber": "1001",
                "packagePosition": 1,
                "itemDetailId": "detail-1",
                "manageNumber": "manage-1",
                "itemNumber": "item-1",
                "SkuModelList": [{"variantId": "sku-1"}],
                "units": 2,
                "price": 1200,
                "priceTaxIncl": 1320,
                "deleteItemFlag": False,
                "restoreInventoryFlag": True,
            },
            {
                "orderNumber": "1001",
                "packagePosition": 2,
                "itemDetailId": "detail-2",
                "manageNumber": "manage-2",
                "itemNumber": "item-2",
                "SkuModelList": [],
                "units": 0,
                "price": "900",
                "priceTaxIncl": "990",
                "deleteItemFlag": True,
                "restoreInventoryFlag": False,
            },
        ])

    def test_search_order_numbers_rejects_credential_failure_without_leaking_data(self) -> None:
        response = json_response(
            {"error": "bad auth"},
            status_code=403,
            text='{"message":"secret-123 key-456 should not leak"}',
        )

        with patch.object(rakuten_order_service.requests.Session, "post", return_value=response):
            with self.assertRaises(RuntimeError) as exc_info:
                rakuten_order_service.search_order_numbers(
                    "secret-123",
                    "key-456",
                    datetime(2026, 7, 1, 0, 0, 0),
                    datetime(2026, 7, 2, 0, 0, 0),
                    [100],
                )

        message = str(exc_info.exception)
        self.assertIn("认证失败", message)
        self.assertNotIn("secret-123", message)
        self.assertNotIn("key-456", message)
        self.assertNotIn("should not leak", message)

    def test_search_order_numbers_rejects_missing_pagination(self) -> None:
        response = json_response({"orderNumberList": ["1001"]})

        with (
            patch.object(rakuten_order_service.requests.Session, "post", return_value=response),
            self.assertRaisesRegex(RuntimeError, "分页信息缺失"),
        ):
            rakuten_order_service.search_order_numbers(
                "secret-123",
                "key-456",
                datetime(2026, 7, 1, 0, 0, 0),
                datetime(2026, 7, 2, 0, 0, 0),
                [100],
            )

    def test_get_orders_rejects_rate_limiting_without_echoing_response_body(self) -> None:
        response = json_response(
            {"error": "too many requests"},
            status_code=429,
            text='{"details":"remote body should stay remote"}',
        )

        with patch.object(rakuten_order_service.requests.Session, "post", return_value=response):
            with self.assertRaises(RuntimeError) as exc_info:
                rakuten_order_service.get_orders("secret-123", "key-456", ["1001"])

        message = str(exc_info.exception)
        self.assertIn("限流", message)
        self.assertNotIn("remote body", message)

    def test_get_orders_rejects_malformed_json(self) -> None:
        response = Mock()
        response.status_code = 200
        response.text = "{not-json}"
        response.json.side_effect = ValueError("broken json")

        with (
            patch.object(rakuten_order_service.requests.Session, "post", return_value=response),
            self.assertRaisesRegex(RuntimeError, "返回格式无法解析"),
        ):
            rakuten_order_service.get_orders("secret-123", "key-456", ["1001"])

    def test_search_order_numbers_wraps_network_failure(self) -> None:
        with patch.object(
            rakuten_order_service.requests.Session,
            "post",
            side_effect=requests.ConnectionError("network down"),
        ):
            with self.assertRaises(RuntimeError) as exc_info:
                rakuten_order_service.search_order_numbers(
                    "secret-123",
                    "key-456",
                    datetime(2026, 7, 1, 0, 0, 0),
                    datetime(2026, 7, 2, 0, 0, 0),
                    [100],
                )

        self.assertIn("请求失败", str(exc_info.exception))


if __name__ == "__main__":
    unittest.main()
