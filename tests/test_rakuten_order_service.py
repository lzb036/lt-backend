from __future__ import annotations

from datetime import datetime, timedelta, timezone
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
        self.assertEqual(
            post.call_args_list[0].kwargs["json"]["startDatetime"],
            "2026-07-01T01:00:00+0900",
        )
        self.assertEqual(
            post.call_args_list[0].kwargs["json"]["endDatetime"],
            "2026-07-02T01:00:00+0900",
        )

    def test_search_order_numbers_splits_ranges_longer_than_rakuten_limit(self) -> None:
        empty_page = {
            "orderNumberList": [],
            "PaginationResponseModel": {
                "totalPages": 0,
                "requestPage": 0,
            },
        }
        with patch.object(
            rakuten_order_service.requests.Session,
            "post",
            side_effect=[
                json_response(empty_page),
                json_response(empty_page),
            ],
        ) as post:
            result = rakuten_order_service.search_order_numbers(
                "secret-123",
                "key-456",
                datetime(2026, 4, 1, 0, 0, 0),
                datetime(2026, 6, 30, 0, 0, 0),
                [100],
            )

        self.assertEqual(result, [])
        self.assertEqual(post.call_count, 2)
        first_payload = post.call_args_list[0].kwargs["json"]
        second_payload = post.call_args_list[1].kwargs["json"]
        self.assertEqual(first_payload["startDatetime"], "2026-04-01T01:00:00+0900")
        self.assertEqual(first_payload["endDatetime"], "2026-06-03T01:00:00+0900")
        self.assertEqual(second_payload["startDatetime"], first_payload["endDatetime"])
        self.assertEqual(second_payload["endDatetime"], "2026-06-30T01:00:00+0900")

    def test_format_api_datetime_converts_aware_values_to_japan_time(self) -> None:
        value = datetime(
            2026,
            7,
            17,
            15,
            30,
            tzinfo=timezone(timedelta(hours=8)),
        )
        self.assertEqual(
            rakuten_order_service._format_api_datetime(value),
            "2026-07-17T16:30:00+0900",
        )

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

    def test_search_order_numbers_accepts_rakuten_null_pagination_for_zero_results(self) -> None:
        response = json_response({
            "MessageModelList": [{
                "messageType": "INFO",
                "messageCode": "ORDER_EXT_API_SEARCH_ORDER_INFO_102",
                "message": "注文検索に成功しました。(検索結果０件)",
            }],
            "orderNumberList": [],
            "PaginationResponseModel": {
                "totalPages": None,
                "requestPage": None,
                "totalRecordsAmount": None,
            },
        })

        with patch.object(
            rakuten_order_service.requests.Session,
            "post",
            return_value=response,
        ) as post:
            result = rakuten_order_service.search_order_numbers(
                "secret-123",
                "key-456",
                datetime(2025, 1, 1, 0, 0, 0),
                datetime(2025, 2, 1, 0, 0, 0),
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

    def test_search_order_numbers_rejects_zero_total_pages_with_contradictory_nonempty_list(self) -> None:
        response = json_response({
            "orderNumberList": ["1001"],
            "PaginationResponseModel": {
                "totalPages": 0,
                "requestPage": 0,
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

    def test_get_orders_rejects_non_object_order_details(self) -> None:
        response = json_response(
            {
                "OrderModelList": [
                    {"orderNumber": "1001"},
                    "malformed-order-detail",
                ]
            }
        )

        with patch.object(
            rakuten_order_service.requests.Session,
            "post",
            return_value=response,
        ):
            with self.assertRaisesRegex(
                RuntimeError,
                "乐天订单详情读取返回格式无法解析",
            ):
                rakuten_order_service.get_orders(
                    "secret-123",
                    "key-456",
                    ["1001"],
                )

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

    def test_iter_order_items_emits_versioned_digest_fingerprint_when_detail_id_missing(self) -> None:
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

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["orderNumber"], "1001")
        self.assertEqual(result[0]["packagePosition"], 1)
        self.assertEqual(result[0]["itemDetailId"], "")
        self.assertEqual(result[0]["itemId"], "item-id-2")
        self.assertEqual(
            result[0]["lineFingerprintInputs"],
            {
                "canonicalIdentity": {
                    "canonicalSku": '[{"variantId":"sku-z","variantName":"Blue"}]',
                    "itemId": "item-id-2",
                    "itemNumber": "item-2",
                    "manageNumber": "manage-2",
                    "price": "900",
                    "priceTaxIncl": "990",
                },
                "occurrenceIndex": 1,
                "packagePosition": 1,
            },
        )
        self.assertEqual(result[0]["identityOccurrenceIndex"], 1)
        self.assertRegex(result[0]["lineFingerprint"], r"^v1:[0-9a-f]{64}$")

    def test_iter_order_items_fingerprint_is_bounded_and_stable_on_non_identity_changes(self) -> None:
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
            base_item["lineFingerprintInputs"]["canonicalIdentity"][
                "canonicalSku"
            ],
            '[{"variantA":{"x":1,"y":2},"variantB":["2","1"]}]',
        )
        self.assertRegex(base_item["lineFingerprint"], r"^v1:[0-9a-f]{64}$")
        self.assertLessEqual(len(base_item["lineFingerprint"]), 255)
        self.assertEqual(base_item["lineFingerprint"], mutated_item["lineFingerprint"])
        self.assertNotIn("units", base_item["lineFingerprintInputs"])
        self.assertNotIn("deleteItemFlag", base_item["lineFingerprintInputs"])
        self.assertNotIn("restoreInventoryFlag", base_item["lineFingerprintInputs"])

    def test_iter_order_items_fingerprint_changes_for_true_identity_change(self) -> None:
        first_order = {
            "orderNumber": "1001",
            "PackageModelList": [
                {
                    "ItemModelList": [
                        {
                            "itemId": "item-id-2",
                            "manageNumber": "manage-2",
                            "itemNumber": "item-2",
                            "SkuModelList": [{"variantId": "sku-z", "variantName": "Blue"}],
                            "units": 3,
                            "price": 900,
                            "priceTaxIncl": 990,
                        }
                    ]
                },
            ],
        }
        second_order = {
            "orderNumber": "1001",
            "PackageModelList": [
                {
                    "ItemModelList": [
                        {
                            "itemId": "item-id-2",
                            "manageNumber": "manage-CHANGED",
                            "itemNumber": "item-2",
                            "SkuModelList": [{"variantId": "sku-z", "variantName": "Blue"}],
                            "units": 3,
                            "price": 900,
                            "priceTaxIncl": 990,
                        }
                    ]
                },
            ],
        }

        first_item = list(rakuten_order_service.iter_order_items(first_order))[0]
        second_item = list(rakuten_order_service.iter_order_items(second_order))[0]

        self.assertNotEqual(first_item["lineFingerprint"], second_item["lineFingerprint"])

    def test_iter_order_items_fingerprint_is_stable_when_different_lines_reorder(
        self,
    ) -> None:
        first_order = {
            "orderNumber": "1001",
            "PackageModelList": [
                {
                    "ItemModelList": [
                        {
                            "itemId": "item-a",
                            "manageNumber": "MN-A",
                            "itemNumber": "ITEM-A",
                            "SkuModelList": [{"variantId": "blue"}],
                            "units": 1,
                            "price": 100,
                        },
                        {
                            "itemId": "item-b",
                            "manageNumber": "MN-B",
                            "itemNumber": "ITEM-B",
                            "SkuModelList": [{"variantId": "red"}],
                            "units": 2,
                            "price": 200,
                        },
                    ]
                }
            ],
        }
        reordered = {
            **first_order,
            "PackageModelList": [
                {
                    "ItemModelList": list(
                        reversed(
                            first_order["PackageModelList"][0][
                                "ItemModelList"
                            ]
                        )
                    )
                }
            ],
        }

        first = {
            row["itemNumber"]: row["lineFingerprint"]
            for row in rakuten_order_service.iter_order_items(first_order)
        }
        second = {
            row["itemNumber"]: row["lineFingerprint"]
            for row in rakuten_order_service.iter_order_items(reordered)
        }

        self.assertEqual(first, second)

    def test_iter_order_items_uses_occurrence_index_for_identical_lines(
        self,
    ) -> None:
        identical = {
            "itemId": "item-a",
            "manageNumber": "MN-A",
            "itemNumber": "ITEM-A",
            "SkuModelList": [{"variantId": "blue"}],
            "units": 1,
            "price": 100,
        }
        order = {
            "orderNumber": "1001",
            "PackageModelList": [
                {
                    "ItemModelList": [
                        dict(identical),
                        dict(identical),
                    ]
                }
            ],
        }

        rows = list(rakuten_order_service.iter_order_items(order))

        self.assertEqual(
            [row["identityOccurrenceIndex"] for row in rows],
            [1, 2],
        )
        self.assertEqual(len({row["lineFingerprint"] for row in rows}), 2)
        self.assertTrue(
            all(
                "linePosition" not in row["lineFingerprintInputs"]
                for row in rows
            )
        )

    def test_iter_order_items_fingerprint_stays_bounded_for_long_sku_payload(self) -> None:
        long_value = "x" * 5000
        order = {
            "orderNumber": "1001",
            "PackageModelList": [
                {
                    "ItemModelList": [
                        {
                            "itemId": "item-id-2",
                            "manageNumber": "manage-2",
                            "itemNumber": "item-2",
                            "SkuModelList": [
                                {"variantId": "sku-z", "variantName": long_value},
                                {"variantId": "sku-y", "variantMeta": {"long": long_value}},
                            ],
                            "units": 3,
                            "price": 900,
                            "priceTaxIncl": 990,
                        }
                    ]
                },
            ],
        }

        item = list(rakuten_order_service.iter_order_items(order))[0]

        self.assertLessEqual(len(item["lineFingerprint"]), 255)
        self.assertRegex(item["lineFingerprint"], r"^v1:[0-9a-f]{64}$")

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
