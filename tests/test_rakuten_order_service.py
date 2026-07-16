from __future__ import annotations

from datetime import datetime
import traceback
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


def traceback_text(exc: BaseException) -> str:
    return "".join(traceback.TracebackException.from_exception(exc).format())


class RakutenOrderServiceTests(unittest.TestCase):
    def assert_redacted_exception(self, exc: BaseException, *forbidden: str) -> None:
        self.assertIsNone(exc.__cause__)
        self.assertIsNone(exc.__context__)
        rendered = traceback_text(exc)
        for text in forbidden:
            self.assertNotIn(text, str(exc))
            self.assertNotIn(text, rendered)

    def test_search_order_numbers_paginates_with_local_counter_and_nested_numeric_sort(self) -> None:
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
        self.assertNotIn("SortModelList", post.call_args_list[0].kwargs["json"])
        self.assertEqual(
            post.call_args_list[0].kwargs["json"]["PaginationRequestModel"]["SortModelList"],
            [{"sortColumn": 1, "sortDirection": 2}],
        )
        self.assertEqual(
            [call.kwargs["json"]["PaginationRequestModel"]["requestPage"] for call in post.call_args_list],
            [1, 2],
        )
        self.assertEqual(post.call_args_list[0].args[0], rakuten_order_service.RAKUTEN_ORDER_SEARCH_URL)

    def test_search_order_numbers_accepts_total_pages_zero_only_for_first_requested_page(self) -> None:
        response = json_response({
            "orderNumberList": [],
            "PaginationResponseModel": {
                "totalPages": 0,
                "requestPage": 0,
            },
        })

        with patch.object(rakuten_order_service.requests.Session, "post", return_value=response) as post:
            result = rakuten_order_service.search_order_numbers(
                "secret-123",
                "key-456",
                datetime(2026, 7, 1, 0, 0, 0),
                datetime(2026, 7, 2, 0, 0, 0),
                [100],
            )

        self.assertEqual(result, [])
        self.assertEqual(post.call_count, 1)

    def test_search_order_numbers_rejects_zero_total_pages_after_prior_results(self) -> None:
        first_page = json_response({
            "orderNumberList": ["1001"],
            "PaginationResponseModel": {
                "totalPages": 2,
                "requestPage": 1,
            },
        })
        malformed_second_page = json_response({
            "orderNumberList": [],
            "PaginationResponseModel": {
                "totalPages": 0,
                "requestPage": 0,
            },
        })

        with patch.object(
            rakuten_order_service.requests.Session,
            "post",
            side_effect=[first_page, malformed_second_page],
        ) as post:
            with self.assertRaisesRegex(RuntimeError, "分页响应无效") as exc_info:
                rakuten_order_service.search_order_numbers(
                    "secret-123",
                    "key-456",
                    datetime(2026, 7, 1, 0, 0, 0),
                    datetime(2026, 7, 2, 0, 0, 0),
                    [100],
                )

        self.assertEqual(post.call_count, 2)
        self.assert_redacted_exception(exc_info.exception, "secret-123", "key-456")

    def test_search_order_numbers_rejects_stale_response_page_without_looping(self) -> None:
        first_page = json_response({
            "orderNumberList": ["1001"],
            "PaginationResponseModel": {
                "totalPages": 2,
                "requestPage": 1,
            },
        })
        stale_second_page = json_response({
            "orderNumberList": ["1002"],
            "PaginationResponseModel": {
                "totalPages": 2,
                "requestPage": 1,
            },
        })

        with patch.object(
            rakuten_order_service.requests.Session,
            "post",
            side_effect=[first_page, stale_second_page],
        ) as post:
            with self.assertRaisesRegex(RuntimeError, "分页响应无效") as exc_info:
                rakuten_order_service.search_order_numbers(
                    "secret-123",
                    "key-456",
                    datetime(2026, 7, 1, 0, 0, 0),
                    datetime(2026, 7, 2, 0, 0, 0),
                    [100],
                )

        self.assertEqual(post.call_count, 2)
        self.assert_redacted_exception(exc_info.exception, "secret-123", "key-456")

    def test_search_order_numbers_rejects_total_pages_lower_than_requested_page(self) -> None:
        response = json_response({
            "orderNumberList": ["1002"],
            "PaginationResponseModel": {
                "totalPages": 1,
                "requestPage": 2,
            },
        })

        with patch.object(rakuten_order_service.requests.Session, "post", return_value=response):
            with self.assertRaisesRegex(RuntimeError, "分页响应无效") as exc_info:
                rakuten_order_service.search_order_numbers(
                    "secret-123",
                    "key-456",
                    datetime(2026, 7, 1, 0, 0, 0),
                    datetime(2026, 7, 2, 0, 0, 0),
                    [100],
                )

        self.assert_redacted_exception(exc_info.exception, "secret-123", "key-456")

    def test_get_orders_uses_version_7_and_literal_30_batch_limit(self) -> None:
        order_numbers = [f"ORDER-{index:03d}" for index in range(65)]
        responses = [
            json_response({"OrderModelList": [{"orderNumber": "first"}]}),
            json_response({"OrderModelList": [{"orderNumber": "second"}]}),
            json_response({"OrderModelList": [{"orderNumber": "third"}]}),
        ]

        with patch.object(
            rakuten_order_service.requests.Session,
            "post",
            side_effect=responses,
        ) as post:
            result = rakuten_order_service.get_orders("secret-123", "key-456", order_numbers)

        self.assertEqual(len(result), 3)
        self.assertEqual(post.call_count, 3)
        self.assertEqual(rakuten_order_service.RMS_SAFE_ORDER_BATCH_SIZE, 30)
        self.assertEqual(
            [len(call.kwargs["json"]["orderNumberList"]) for call in post.call_args_list],
            [30, 30, 5],
        )
        for call in post.call_args_list:
            self.assertEqual(call.args[0], rakuten_order_service.RAKUTEN_ORDER_GET_URL)
            self.assertEqual(call.kwargs["json"]["version"], 7)
            self.assertEqual(call.kwargs["timeout"], settings.crawler_timeout_seconds)

    def test_iter_order_items_keeps_item_detail_id_and_item_id_separate(self) -> None:
        order = {
            "orderNumber": "1001",
            "PackageModelList": [
                {
                    "ItemModelList": [
                        {
                            "itemDetailId": "detail-1",
                            "itemId": "item-id-1",
                            "manageNumber": "manage-1",
                            "itemNumber": "item-1",
                            "SkuModelList": [{"variantId": "sku-1"}],
                            "units": "2",
                            "price": 1200,
                            "priceTaxIncl": 1320,
                            "deleteItemFlag": 0,
                            "restoreInventoryFlag": 1,
                        },
                        {
                            "itemId": "item-id-2",
                            "manageNumber": "manage-2",
                            "itemNumber": "item-2",
                            "SkuModelList": None,
                            "units": None,
                            "price": "900",
                            "priceTaxIncl": "990",
                            "deleteItemFlag": True,
                            "restoreInventoryFlag": False,
                        },
                    ]
                },
            ],
        }

        result = list(rakuten_order_service.iter_order_items(order))

        self.assertEqual(result[0]["itemDetailId"], "detail-1")
        self.assertEqual(result[0]["itemId"], "item-id-1")
        self.assertEqual(result[1]["itemDetailId"], "")
        self.assertEqual(result[1]["itemId"], "item-id-2")
        self.assertEqual(result[1]["packagePosition"], 1)
        self.assertEqual(result[1]["SkuModelList"], [])

    def test_iter_order_items_emits_canonical_fingerprint_when_detail_id_missing(self) -> None:
        order = {
            "orderNumber": "1001",
            "PackageModelList": [
                {
                    "ItemModelList": [
                        {
                            "itemId": "item-id-2",
                            "manageNumber": "manage-2",
                            "itemNumber": "item-2",
                            "SkuModelList": [{"variantName": "Blue", "variantId": "sku-z"}],
                            "units": 3,
                            "price": 900,
                            "priceTaxIncl": 990,
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
                "itemDetailId": "",
                "itemId": "item-id-2",
                "manageNumber": "manage-2",
                "itemNumber": "item-2",
                "SkuModelList": [{"variantName": "Blue", "variantId": "sku-z"}],
                "units": 3,
                "price": 900,
                "priceTaxIncl": 990,
                "deleteItemFlag": True,
                "restoreInventoryFlag": False,
                "lineFingerprintInputs": {
                    "canonicalSku": '[{"variantId":"sku-z","variantName":"Blue"}]',
                    "itemId": "item-id-2",
                    "itemNumber": "item-2",
                    "linePosition": 1,
                    "manageNumber": "manage-2",
                    "packagePosition": 1,
                    "price": "900",
                    "priceTaxIncl": "990",
                },
                "lineFingerprint": '{"canonicalSku":"[{\\"variantId\\":\\"sku-z\\",\\"variantName\\":\\"Blue\\"}]","itemId":"item-id-2","itemNumber":"item-2","linePosition":1,"manageNumber":"manage-2","packagePosition":1,"price":"900","priceTaxIncl":"990"}',
            }
        ])

    def test_iter_order_items_fingerprint_ignores_units_and_status_flags(self) -> None:
        base_order = {
            "orderNumber": "1001",
            "PackageModelList": [
                {
                    "ItemModelList": [
                        {
                            "itemId": "item-id-2",
                            "manageNumber": "manage-2",
                            "itemNumber": "item-2",
                            "SkuModelList": [{"variantB": ["2", "1"], "variantA": {"y": 2, "x": 1}}],
                            "units": 3,
                            "price": 900,
                            "priceTaxIncl": 990,
                            "deleteItemFlag": True,
                            "restoreInventoryFlag": False,
                        }
                    ]
                },
            ],
        }
        mutated_order = {
            "orderNumber": "1001",
            "PackageModelList": [
                {
                    "ItemModelList": [
                        {
                            "itemId": "item-id-2",
                            "manageNumber": "manage-2",
                            "itemNumber": "item-2",
                            "SkuModelList": [{"variantA": {"x": 1, "y": 2}, "variantB": ["2", "1"]}],
                            "units": 99,
                            "price": 900,
                            "priceTaxIncl": 990,
                            "deleteItemFlag": False,
                            "restoreInventoryFlag": True,
                        }
                    ]
                },
            ],
        }

        base_item = list(rakuten_order_service.iter_order_items(base_order))[0]
        mutated_item = list(rakuten_order_service.iter_order_items(mutated_order))[0]

        self.assertEqual(
            base_item["lineFingerprintInputs"]["canonicalSku"],
            '[{"variantA":{"x":1,"y":2},"variantB":["2","1"]}]',
        )
        self.assertEqual(base_item["lineFingerprint"], mutated_item["lineFingerprint"])
        self.assertNotIn("units", base_item["lineFingerprintInputs"])
        self.assertNotIn("deleteItemFlag", base_item["lineFingerprintInputs"])
        self.assertNotIn("restoreInventoryFlag", base_item["lineFingerprintInputs"])

    def test_search_order_numbers_http_status_precedence_over_body_messages(self) -> None:
        response = json_response({
            "MessageModelList": [
                {
                    "messageType": "ERROR",
                    "messageCode": "RATE-001",
                    "message": "Too many requests for secret-123",
                }
            ],
            "PaginationResponseModel": {"totalPages": 1, "requestPage": 1},
            "orderNumberList": [],
        }, status_code=401)

        with patch.object(rakuten_order_service.requests.Session, "post", return_value=response):
            with self.assertRaisesRegex(RuntimeError, "认证失败") as exc_info:
                rakuten_order_service.search_order_numbers(
                    "secret-123",
                    "key-456",
                    datetime(2026, 7, 1, 0, 0, 0),
                    datetime(2026, 7, 2, 0, 0, 0),
                    [100],
                )

        self.assert_redacted_exception(exc_info.exception, "secret-123", "Too many requests")

    def test_get_orders_http_rate_limit_precedence_over_body_messages(self) -> None:
        response = json_response({
            "MessageModelList": [
                {
                    "messageType": "ERROR",
                    "messageCode": "AUTH-001",
                    "message": "invalid license key secret-123 key-456",
                }
            ],
            "OrderModelList": [],
        }, status_code=429)

        with patch.object(rakuten_order_service.requests.Session, "post", return_value=response):
            with self.assertRaisesRegex(RuntimeError, "限流") as exc_info:
                rakuten_order_service.get_orders("secret-123", "key-456", ["1001"])

        self.assert_redacted_exception(exc_info.exception, "secret-123", "invalid license key")

    def test_search_order_numbers_categorizes_message_model_credential_error_without_leaks(self) -> None:
        response = json_response({
            "MessageModelList": [
                {
                    "messageType": "ERROR",
                    "messageCode": "AUTH-001",
                    "message": "invalid license key secret-123 key-456",
                }
            ],
            "PaginationResponseModel": {"totalPages": 1, "requestPage": 1},
            "orderNumberList": [],
        })

        with patch.object(rakuten_order_service.requests.Session, "post", return_value=response):
            with self.assertRaisesRegex(RuntimeError, "认证失败") as exc_info:
                rakuten_order_service.search_order_numbers(
                    "secret-123",
                    "key-456",
                    datetime(2026, 7, 1, 0, 0, 0),
                    datetime(2026, 7, 2, 0, 0, 0),
                    [100],
                )

        self.assert_redacted_exception(exc_info.exception, "secret-123", "key-456", "invalid license key")

    def test_get_orders_categorizes_message_model_rate_limit_error_without_leaks(self) -> None:
        response = json_response({
            "MessageModelList": [
                {
                    "messageType": "ERROR",
                    "messageCode": "RATE-001",
                    "message": "Too many requests for secret-123",
                }
            ],
            "OrderModelList": [],
        })

        with patch.object(rakuten_order_service.requests.Session, "post", return_value=response):
            with self.assertRaisesRegex(RuntimeError, "限流") as exc_info:
                rakuten_order_service.get_orders("secret-123", "key-456", ["1001"])

        self.assert_redacted_exception(exc_info.exception, "secret-123", "Too many requests")

    def test_get_orders_categorizes_message_model_general_api_error_without_leaks(self) -> None:
        response = json_response({
            "MessageModelList": [
                {
                    "messageType": "ERROR",
                    "messageCode": "ORDER-500",
                    "message": "remote body should stay remote",
                }
            ],
            "OrderModelList": [],
        })

        with patch.object(rakuten_order_service.requests.Session, "post", return_value=response):
            with self.assertRaisesRegex(RuntimeError, "接口错误") as exc_info:
                rakuten_order_service.get_orders("secret-123", "key-456", ["1001"])

        self.assert_redacted_exception(exc_info.exception, "secret-123", "remote body should stay remote")

    def test_search_order_numbers_rejects_missing_pagination(self) -> None:
        response = json_response({"orderNumberList": ["1001"]})

        with patch.object(rakuten_order_service.requests.Session, "post", return_value=response):
            with self.assertRaisesRegex(RuntimeError, "分页信息缺失") as exc_info:
                rakuten_order_service.search_order_numbers(
                    "secret-123",
                    "key-456",
                    datetime(2026, 7, 1, 0, 0, 0),
                    datetime(2026, 7, 2, 0, 0, 0),
                    [100],
                )

        self.assert_redacted_exception(exc_info.exception, "secret-123", "key-456")

    def test_get_orders_rejects_malformed_json(self) -> None:
        response = Mock()
        response.status_code = 200
        response.text = "{not-json}"
        response.json.side_effect = ValueError("broken json with secret-123")

        with patch.object(rakuten_order_service.requests.Session, "post", return_value=response):
            with self.assertRaisesRegex(RuntimeError, "返回格式无法解析") as exc_info:
                rakuten_order_service.get_orders("secret-123", "key-456", ["1001"])

        self.assert_redacted_exception(exc_info.exception, "secret-123", "broken json")

    def test_search_order_numbers_wraps_network_failure_without_chained_cause(self) -> None:
        with patch.object(
            rakuten_order_service.requests.Session,
            "post",
            side_effect=requests.ConnectionError("network down secret-123 key-456"),
        ):
            with self.assertRaisesRegex(RuntimeError, "请求失败") as exc_info:
                rakuten_order_service.search_order_numbers(
                    "secret-123",
                    "key-456",
                    datetime(2026, 7, 1, 0, 0, 0),
                    datetime(2026, 7, 2, 0, 0, 0),
                    [100],
                )

        self.assert_redacted_exception(exc_info.exception, "secret-123", "key-456", "network down")


if __name__ == "__main__":
    unittest.main()
