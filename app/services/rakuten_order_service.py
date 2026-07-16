from __future__ import annotations

from datetime import datetime
from typing import Any, Iterable

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
RAKUTEN_ORDER_SORT_MODEL_LIST = [{"sortColumn": "orderNumber", "sortDirection": "asc"}]


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
                    "SortModelList": list(RAKUTEN_ORDER_SORT_MODEL_LIST),
                    "PaginationRequestModel": {
                        "requestRecordsAmount": RAKUTEN_ORDER_SEARCH_PAGE_SIZE,
                        "requestPage": expected_page,
                    },
                },
            )
            pagination = payload.get("PaginationResponseModel")
            if not isinstance(pagination, dict):
                raise RuntimeError("乐天订单查询分页信息缺失。")

            total_pages = _int_or_none(pagination.get("totalPages"))
            response_page = _int_or_none(pagination.get("requestPage"))
            if total_pages is None:
                raise RuntimeError("乐天订单查询分页信息缺失。")
            if total_pages == 0:
                if response_page not in {0, 1, None}:
                    raise RuntimeError("乐天订单查询分页响应无效。")
                return _normalize_text_list(payload.get("orderNumberList"))
            if total_pages < expected_page or response_page != expected_page:
                raise RuntimeError("乐天订单查询分页响应无效。")

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
                raise RuntimeError("乐天订单详情读取返回格式无法解析。")
            orders.extend(order for order in order_models if isinstance(order, dict))
        return orders
    finally:
        session.close()


def iter_order_items(order: dict[str, Any]) -> Iterable[dict[str, Any]]:
    order_number = normalize_text(order.get("orderNumber"))
    package_models = order.get("PackageModelList")
    if not isinstance(package_models, list):
        return

    for package_position, package in enumerate(package_models, start=1):
        if not isinstance(package, dict):
            continue
        item_models = package.get("ItemModelList")
        if not isinstance(item_models, list):
            continue
        for item in item_models:
            if not isinstance(item, dict):
                continue
            sku_models = item.get("SkuModelList")
            normalized_sku_models = sku_models if isinstance(sku_models, list) else []
            price = item.get("price")
            price_tax_incl = item.get("priceTaxIncl")
            item_detail_id = first_text_from_keys(item, ("itemDetailId", "itemDetailID"))
            item_id = first_text_from_keys(item, ("itemId", "itemID"))
            normalized_item = {
                "orderNumber": order_number,
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
                fingerprint_inputs = {
                    "orderNumber": order_number,
                    "packagePosition": package_position,
                    "itemId": item_id,
                    "manageNumber": normalized_item["manageNumber"],
                    "itemNumber": normalized_item["itemNumber"],
                    "skuSignature": _sku_signature(normalized_sku_models),
                    "units": normalized_item["units"],
                    "price": normalize_text(price),
                    "priceTaxIncl": normalize_text(price_tax_incl),
                }
                normalized_item["lineFingerprintInputs"] = fingerprint_inputs
                normalized_item["lineFingerprint"] = "|".join([
                    fingerprint_inputs["orderNumber"],
                    str(fingerprint_inputs["packagePosition"]),
                    fingerprint_inputs["itemId"],
                    fingerprint_inputs["manageNumber"],
                    fingerprint_inputs["itemNumber"],
                    fingerprint_inputs["skuSignature"],
                    str(fingerprint_inputs["units"]),
                    fingerprint_inputs["price"],
                    fingerprint_inputs["priceTaxIncl"],
                ])
            yield normalized_item


def _rakuten_order_headers(service_secret: str, license_key: str) -> dict[str, str]:
    normalized_secret = normalize_text(service_secret)
    normalized_key = normalize_text(license_key)
    if not normalized_secret or not normalized_key:
        raise RuntimeError("乐天 Secret 或乐天 Key 未配置。")
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
    try:
        response = session.post(
            url,
            headers=headers,
            json=payload,
            timeout=settings.crawler_timeout_seconds,
            proxies=crawler_request_proxies(),
        )
    except requests.RequestException:
        raise RuntimeError(f"{operation}请求失败，请检查网络或稍后重试。") from None

    parsed_payload: dict[str, Any] | None = None
    if _response_might_have_json(response):
        try:
            candidate = response.json()
        except ValueError:
            candidate = None
        if isinstance(candidate, dict):
            parsed_payload = candidate

    if parsed_payload is not None:
        _raise_for_api_messages(parsed_payload, operation)

    status_code = int(response.status_code or 0)
    if status_code in {401, 403}:
        raise RuntimeError("乐天订单接口认证失败，请检查 Secret / Key 权限。") from None
    if status_code == 429:
        raise RuntimeError("乐天订单接口触发限流，请稍后重试。") from None
    if status_code >= 400:
        raise RuntimeError(f"{operation}失败，远程状态码 {status_code}。") from None

    if parsed_payload is None:
        try:
            candidate = response.json()
        except ValueError:
            raise RuntimeError(f"{operation}返回格式无法解析。") from None
        if not isinstance(candidate, dict):
            raise RuntimeError(f"{operation}返回格式无法解析。") from None
        parsed_payload = candidate
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
        message_type = normalize_text(entry.get("messageType")).upper()
        if message_type != "ERROR":
            continue
        error_codes.append(normalize_text(entry.get("messageCode")).upper())
        error_texts.append(normalize_text(entry.get("message")).upper())

    if not error_codes and not error_texts:
        return

    joined = " ".join([*error_codes, *error_texts])
    if any(token in joined for token in ("RATE", "TOO MANY", "LIMIT", "THROTTLE", "429")):
        raise RuntimeError("乐天订单接口触发限流，请稍后重试。") from None
    if any(token in joined for token in ("AUTH", "LICENSE", "SECRET", "KEY", "UNAUTHORIZED", "FORBIDDEN")):
        raise RuntimeError("乐天订单接口认证失败，请检查 Secret / Key 权限。") from None
    raise RuntimeError(f"{operation}返回接口错误。") from None


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


def _response_might_have_json(response: requests.Response) -> bool:
    text = normalize_text(getattr(response, "text", ""))
    return text.startswith("{") or text.startswith("[") or bool(getattr(response, "json", None))


def _sku_signature(sku_models: list[Any]) -> str:
    parts: list[str] = []
    for sku_model in sku_models:
        if isinstance(sku_model, dict):
            for key in sorted(sku_model):
                value = normalize_text(sku_model.get(key))
                if value:
                    parts.append(f"{key}={value}")
        else:
            value = normalize_text(sku_model)
            if value:
                parts.append(value)
    return ",".join(parts)


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
