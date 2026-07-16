from __future__ import annotations

from datetime import datetime
import hashlib
import json
from typing import Any, Iterable, NoReturn

import requests

from app.core.config import settings
from app.services.crawler_service import (
    build_rakuten_authorization_header,
    crawler_request_proxies,
    first_text_from_keys,
    normalize_text,
)


RAKUTEN_ORDER_SEARCH_URL = "https://api.rms.rakuten.co.jp/es/2.0/order/searchOrder/"
RAKUTEN_ORDER_GET_URL = "https://api.rms.rakuten.co.jp/es/2.0/order/getOrder/"
RAKUTEN_ORDER_SEARCH_PAGE_SIZE = 1000
RMS_SAFE_ORDER_BATCH_SIZE = 30
RAKUTEN_ORDER_DETAIL_VERSION = 7
RAKUTEN_ORDER_SORT_MODEL_LIST = [{"sortColumn": 1, "sortDirection": 2}]


def search_order_numbers(
    service_secret: str,
    license_key: str,
    start_at: datetime,
    end_at: datetime,
    statuses: list[int],
) -> list[str]:
    headers = _rakuten_order_headers(service_secret, license_key)
    session = requests.Session()
    seen: set[str] = set()
    order_numbers: list[str] = []
    expected_page = 1

    try:
        while True:
            payload = _post_order_json(
                session,
                RAKUTEN_ORDER_SEARCH_URL,
                operation="乐天订单查询",
                headers=headers,
                payload={
                    "dateType": 1,
                    "startDatetime": _format_api_datetime(start_at),
                    "endDatetime": _format_api_datetime(end_at),
                    "orderProgressList": [int(status) for status in statuses],
                    "PaginationRequestModel": {
                        "requestRecordsAmount": RAKUTEN_ORDER_SEARCH_PAGE_SIZE,
                        "requestPage": expected_page,
                        "SortModelList": list(RAKUTEN_ORDER_SORT_MODEL_LIST),
                    },
                },
            )
            pagination = payload.get("PaginationResponseModel")
            if not isinstance(pagination, dict):
                _raise_sanitized_runtime_error("乐天订单查询分页信息缺失。")

            total_pages = _int_or_none(pagination.get("totalPages"))
            response_page = _int_or_none(pagination.get("requestPage"))
            if total_pages is None:
                _raise_sanitized_runtime_error("乐天订单查询分页信息缺失。")
            if total_pages == 0:
                normalized_page_orders = _normalize_text_list(payload.get("orderNumberList"))
                if expected_page != 1 or order_numbers or normalized_page_orders or response_page not in {0, 1, None}:
                    _raise_sanitized_runtime_error("乐天订单查询分页响应无效。")
                return []
            if total_pages < expected_page or response_page != expected_page:
                _raise_sanitized_runtime_error("乐天订单查询分页响应无效。")

            for order_number in _normalize_text_list(payload.get("orderNumberList")):
                if order_number in seen:
                    continue
                seen.add(order_number)
                order_numbers.append(order_number)

            if expected_page >= total_pages:
                return order_numbers
            expected_page += 1
    finally:
        session.close()


def get_orders(service_secret: str, license_key: str, order_numbers: list[str]) -> list[dict[str, Any]]:
    normalized_numbers = _normalize_text_list(order_numbers)
    if not normalized_numbers:
        return []

    headers = _rakuten_order_headers(service_secret, license_key)
    session = requests.Session()
    orders: list[dict[str, Any]] = []

    try:
        for start in range(0, len(normalized_numbers), RMS_SAFE_ORDER_BATCH_SIZE):
            batch = normalized_numbers[start:start + RMS_SAFE_ORDER_BATCH_SIZE]
            payload = _post_order_json(
                session,
                RAKUTEN_ORDER_GET_URL,
                operation="乐天订单详情读取",
                headers=headers,
                payload={
                    "version": RAKUTEN_ORDER_DETAIL_VERSION,
                    "orderNumberList": batch,
                },
            )
            order_models = payload.get("OrderModelList")
            if not isinstance(order_models, list):
                _raise_sanitized_runtime_error("乐天订单详情读取返回格式无法解析。")
            if any(not isinstance(order, dict) for order in order_models):
                _raise_sanitized_runtime_error("乐天订单详情读取返回格式无法解析。")
            orders.extend(order_models)
        return orders
    finally:
        session.close()


def iter_order_items(order: dict[str, Any]) -> Iterable[dict[str, Any]]:
    package_models = order.get("PackageModelList")
    if not isinstance(package_models, list):
        return

    for package_position, package in enumerate(package_models, start=1):
        if not isinstance(package, dict):
            continue
        item_models = package.get("ItemModelList")
        if not isinstance(item_models, list):
            continue
        identity_occurrences: dict[str, int] = {}
        for item in item_models:
            if not isinstance(item, dict):
                continue
            normalized_sku_models = item.get("SkuModelList") if isinstance(item.get("SkuModelList"), list) else []
            price = item.get("price")
            price_tax_incl = item.get("priceTaxIncl")
            item_detail_id = first_text_from_keys(item, ("itemDetailId", "itemDetailID"))
            item_id = first_text_from_keys(item, ("itemId", "itemID"))
            normalized_item = {
                "orderNumber": normalize_text(order.get("orderNumber")),
                "packagePosition": package_position,
                "itemDetailId": item_detail_id,
                "itemId": item_id,
                "manageNumber": first_text_from_keys(item, ("manageNumber",)),
                "itemNumber": first_text_from_keys(item, ("itemNumber",)),
                "SkuModelList": normalized_sku_models,
                "units": _non_negative_int(item.get("units")),
                "price": price,
                "priceTaxIncl": price_tax_incl,
                "deleteItemFlag": _to_bool(item.get("deleteItemFlag")),
                "restoreInventoryFlag": _to_bool(item.get("restoreInventoryFlag")),
            }
            if not item_detail_id:
                canonical_identity = {
                    "canonicalSku": _canonical_json(normalized_sku_models),
                    "itemId": item_id,
                    "itemNumber": normalized_item["itemNumber"],
                    "manageNumber": normalized_item["manageNumber"],
                    "price": normalize_text(price),
                    "priceTaxIncl": normalize_text(price_tax_incl),
                }
                identity_key = _canonical_json(canonical_identity)
                occurrence_index = (
                    identity_occurrences.get(identity_key, 0) + 1
                )
                identity_occurrences[identity_key] = occurrence_index
                fingerprint_inputs = {
                    "canonicalIdentity": canonical_identity,
                    "packagePosition": package_position,
                    "occurrenceIndex": occurrence_index,
                }
                normalized_item["identityOccurrenceIndex"] = (
                    occurrence_index
                )
                normalized_item["lineFingerprintInputs"] = fingerprint_inputs
                normalized_item["lineFingerprint"] = _versioned_digest(fingerprint_inputs)
            yield normalized_item


def _rakuten_order_headers(service_secret: str, license_key: str) -> dict[str, str]:
    normalized_secret = normalize_text(service_secret)
    normalized_key = normalize_text(license_key)
    if not normalized_secret or not normalized_key:
        _raise_sanitized_runtime_error("乐天 Secret 或乐天 Key 未配置。")
    return {
        "Authorization": build_rakuten_authorization_header(normalized_secret, normalized_key),
        "Accept": "application/json",
        "Content-Type": "application/json; charset=utf-8",
    }


def _post_order_json(
    session: requests.Session,
    url: str,
    *,
    operation: str,
    headers: dict[str, str],
    payload: dict[str, Any],
) -> dict[str, Any]:
    request_failed = False
    try:
        response = session.post(
            url,
            headers=headers,
            json=payload,
            timeout=settings.crawler_timeout_seconds,
            proxies=crawler_request_proxies(),
        )
    except requests.RequestException:
        request_failed = True
        response = None
    if request_failed or response is None:
        _raise_sanitized_runtime_error(f"{operation}请求失败，请检查网络或稍后重试。")

    status_code = int(response.status_code or 0)
    if status_code in {401, 403}:
        _raise_sanitized_runtime_error("乐天订单接口认证失败，请检查 Secret / Key 权限。")
    if status_code == 429:
        _raise_sanitized_runtime_error("乐天订单接口触发限流，请稍后重试。")

    parsed_payload, parse_failed = _parse_json_dict(response)
    if status_code >= 400:
        if parsed_payload is not None:
            _raise_for_api_messages(parsed_payload, operation)
        _raise_sanitized_runtime_error(f"{operation}失败，远程状态码 {status_code}。")
    if parse_failed or parsed_payload is None:
        _raise_sanitized_runtime_error(f"{operation}返回格式无法解析。")

    _raise_for_api_messages(parsed_payload, operation)
    return parsed_payload


def _raise_for_api_messages(payload: dict[str, Any], operation: str) -> None:
    message_models = payload.get("MessageModelList")
    if not isinstance(message_models, list):
        return

    error_codes: list[str] = []
    error_texts: list[str] = []
    for entry in message_models:
        if not isinstance(entry, dict):
            continue
        if normalize_text(entry.get("messageType")).upper() != "ERROR":
            continue
        error_codes.append(normalize_text(entry.get("messageCode")).upper())
        error_texts.append(normalize_text(entry.get("message")).upper())

    if not error_codes and not error_texts:
        return

    joined = " ".join([*error_codes, *error_texts])
    if any(token in joined for token in ("RATE", "TOO MANY", "LIMIT", "THROTTLE", "429")):
        _raise_sanitized_runtime_error("乐天订单接口触发限流，请稍后重试。")
    if any(token in joined for token in ("AUTH", "LICENSE", "SECRET", "KEY", "UNAUTHORIZED", "FORBIDDEN")):
        _raise_sanitized_runtime_error("乐天订单接口认证失败，请检查 Secret / Key 权限。")
    _raise_sanitized_runtime_error(f"{operation}返回接口错误。")


def _parse_json_dict(response: requests.Response) -> tuple[dict[str, Any] | None, bool]:
    parse_failed = False
    try:
        candidate = response.json()
    except ValueError:
        parse_failed = True
        candidate = None
    if isinstance(candidate, dict):
        return candidate, False
    return None, parse_failed or candidate is not None


def _format_api_datetime(value: datetime) -> str:
    if value.tzinfo is None or value.utcoffset() is None:
        return value.strftime("%Y-%m-%dT%H:%M:%S")
    return value.strftime("%Y-%m-%dT%H:%M:%S%z")


def _normalize_text_list(values: Any) -> list[str]:
    if not isinstance(values, list):
        return []
    result: list[str] = []
    for value in values:
        normalized = normalize_text(value)
        if normalized:
            result.append(normalized)
    return result


def _int_or_none(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _non_negative_int(value: Any) -> int:
    try:
        normalized = int(value)
    except (TypeError, ValueError):
        return 0
    return max(0, normalized)


def _canonicalize(value: Any) -> Any:
    if isinstance(value, dict):
        return {key: _canonicalize(value[key]) for key in sorted(value)}
    if isinstance(value, list):
        return [_canonicalize(item) for item in value]
    return value


def _canonical_json(value: Any) -> str:
    return json.dumps(
        _canonicalize(value),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )


def _versioned_digest(value: Any) -> str:
    canonical_text = _canonical_json(value)
    digest = hashlib.sha256(canonical_text.encode("utf-8")).hexdigest()
    return f"v1:{digest}"


def _raise_sanitized_runtime_error(message: str) -> NoReturn:
    error = RuntimeError(message)
    error.__cause__ = None
    error.__context__ = None
    error.__suppress_context__ = True
    raise error


def _to_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    text = normalize_text(value).lower()
    if text in {"1", "true", "yes", "on"}:
        return True
    if text in {"0", "false", "no", "off", ""}:
        return False
    try:
        return bool(int(text))
    except (TypeError, ValueError):
        return bool(value)
