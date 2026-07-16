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
RMS_SAFE_ORDER_BATCH_SIZE = 100
RAKUTEN_ORDER_DETAIL_VERSION = 7


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
    page = 1

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
                        "requestPage": page,
                    },
                },
            )
            pagination = payload.get("PaginationResponseModel")
            if not isinstance(pagination, dict):
                raise RuntimeError("乐天订单查询分页信息缺失。")
            total_pages = _positive_int(pagination.get("totalPages"))
            current_page = _positive_int(pagination.get("requestPage"), default=page)
            if total_pages <= 0 or current_page <= 0:
                raise RuntimeError("乐天订单查询分页信息缺失。")

            for order_number in _normalize_text_list(payload.get("orderNumberList")):
                if order_number in seen:
                    continue
                seen.add(order_number)
                order_numbers.append(order_number)

            if current_page >= total_pages:
                return order_numbers
            page = current_page + 1
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
                raise RuntimeError("乐天订单详情返回格式无法解析。")
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
            yield {
                "orderNumber": order_number,
                "packagePosition": package_position,
                "itemDetailId": first_text_from_keys(item, ("itemDetailId", "itemDetailID", "itemId")),
                "manageNumber": first_text_from_keys(item, ("manageNumber",)),
                "itemNumber": first_text_from_keys(item, ("itemNumber",)),
                "SkuModelList": sku_models if isinstance(sku_models, list) else [],
                "units": _non_negative_int(item.get("units")),
                "price": item.get("price"),
                "priceTaxIncl": item.get("priceTaxIncl"),
                "deleteItemFlag": _to_bool(item.get("deleteItemFlag")),
                "restoreInventoryFlag": _to_bool(item.get("restoreInventoryFlag")),
            }


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
    except requests.RequestException as exc:
        raise RuntimeError(f"{operation}请求失败，请检查网络或稍后重试。") from exc

    status_code = int(response.status_code or 0)
    if status_code in {401, 403}:
        raise RuntimeError("乐天订单接口认证失败，请检查 Secret / Key 权限。")
    if status_code == 429:
        raise RuntimeError("乐天订单接口触发限流，请稍后重试。")
    if status_code >= 400:
        raise RuntimeError(f"{operation}失败，远程状态码 {status_code}。")

    try:
        payload = response.json()
    except ValueError as exc:
        raise RuntimeError(f"{operation}返回格式无法解析。") from exc
    if not isinstance(payload, dict):
        raise RuntimeError(f"{operation}返回格式无法解析。")
    return payload


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


def _positive_int(value: Any, *, default: int = 0) -> int:
    try:
        normalized = int(value)
    except (TypeError, ValueError):
        return default
    return normalized if normalized > 0 else default


def _non_negative_int(value: Any) -> int:
    try:
        normalized = int(value)
    except (TypeError, ValueError):
        return 0
    return max(0, normalized)


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
