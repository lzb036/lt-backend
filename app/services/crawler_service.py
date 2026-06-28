from __future__ import annotations

import json
import re
import uuid
import base64
import xml.etree.ElementTree as ET
import time
import threading
from datetime import datetime, timedelta
from decimal import Decimal
from html import unescape
from typing import Any
from urllib.parse import parse_qs, quote, unquote, urljoin, urlsplit

import requests
from bs4 import BeautifulSoup
from sqlalchemy import func, select

from app.core.config import settings
from app.core.secure_storage import decrypt_text, encrypt_text, mask_secret
from app.db.database import session_scope
from app.db.models import (
    CrawlLogModel,
    CrawlSourceModel,
    CrawlTaskModel,
    ListingTaskModel,
    ProductModel,
    RoleModel,
    ScheduledCrawlModel,
    StoreModel,
    SyncTaskModel,
    make_source_url_hash,
)

RAKUTEN_SEARCH_BASE = "https://search.rakuten.co.jp/search/mall/"
RAKUTEN_RANKING_BASE = "https://ranking.rakuten.co.jp/search"
RAKUTEN_REALTIME_RANKING_URL = "https://ranking.rakuten.co.jp/realtime/"
RAKUTEN_SHOP_MASTER_URL = "https://api.rms.rakuten.co.jp/es/1.0/shop/shopMaster"
RAKUTEN_CABINET_USAGE_URL = "https://api.rms.rakuten.co.jp/es/1.0/cabinet/usage/get"
RAKUTEN_ITEM_SEARCH_URL = "https://api.rms.rakuten.co.jp/es/2.0/items/search"
RAKUTEN_ITEM_PATCH_URL = "https://api.rms.rakuten.co.jp/es/2.0/items/manage-numbers/{manageNumber}"
RAKUTEN_CABINET_FILE_SEARCH_URL = "https://api.rms.rakuten.co.jp/es/1.0/cabinet/files/search"
RAKUTEN_CABINET_FILE_DELETE_URL = "https://api.rms.rakuten.co.jp/es/1.0/cabinet/file/delete"
RAKUTEN_ITEM_SEARCH_HITS = 100
RAKUTEN_ITEM_SEARCH_MAX_RETRIES = 4
DEFAULT_PAGE_SIZE = 30
MAX_PAGE_SIZE = 500
IGNORED_CABINET_IMAGE_FILENAMES = {"bg_logo.gif", "bg_logo2.gif", "bg_logo3.gif", "spacer.gif", "blank.gif"}
RAKUTEN_PRODUCT_TARGET_ERROR = "单个商品采集支持普通乐天商品链接、Rakuten Fashion 商品链接、带参数链接、店铺编码/商品编号。"
RAKUTEN_SHOP_TARGET_ERROR = "店铺采集请输入店铺展示名称、店铺url代码、店铺url或sid。"
RAKUTEN_FASHION_IMAGE_BASE = "https://tshop.r10s.jp/stylife/cabinet/item"
SCHEDULE_RUN_LOCK = threading.Lock()
SCHEDULE_RUNNER_STARTED = False


def dispatch_crawl_task(task_id: str) -> None:
    worker = threading.Thread(target=run_task, args=(task_id,), daemon=True)
    worker.start()


def log_event(owner_username: str, task_id: str | None, level: str, message: str) -> None:
    with session_scope() as session:
        session.add(CrawlLogModel(owner_username=owner_username, task_id=task_id, level=level, message=message))


def normalize_page_params(page: int | None, page_size: int | None) -> tuple[int, int | None]:
    normalized_page = max(1, int(page or 1))
    normalized_page_size = min(MAX_PAGE_SIZE, max(1, int(page_size or 0))) if page_size else None
    return normalized_page, normalized_page_size


def paginate_query(
    session: Any,
    query: Any,
    *,
    order_by: Any | tuple[Any, ...],
    page: int | None,
    page_size: int | None,
    response_key: str,
    serializer: Any,
) -> list[dict[str, Any]] | dict[str, Any]:
    normalized_page, normalized_page_size = normalize_page_params(page, page_size)
    order_values = order_by if isinstance(order_by, tuple) else (order_by,)
    if not normalized_page_size:
        rows = session.scalars(query.order_by(*order_values)).all()
        return [serializer(row) for row in rows]

    total = int(session.scalar(select(func.count()).select_from(query.order_by(None).subquery())) or 0)
    if total:
        max_page = max(1, (total + normalized_page_size - 1) // normalized_page_size)
        normalized_page = min(normalized_page, max_page)
    rows = session.scalars(
        query.order_by(*order_values)
        .offset((normalized_page - 1) * normalized_page_size)
        .limit(normalized_page_size)
    ).all()
    return {
        response_key: [serializer(row) for row in rows],
        "total": total,
        "page": normalized_page,
        "pageSize": normalized_page_size,
    }


def source_to_public(row: CrawlSourceModel) -> dict[str, Any]:
    return {
        "id": row.id,
        "ownerUsername": row.owner_username,
        "name": row.name,
        "sourceType": row.source_type,
        "target": row.target,
        "enabled": bool(row.enabled),
        "scheduleEnabled": bool(row.schedule_enabled),
        "intervalMinutes": row.interval_minutes,
        "lastRunAt": row.last_run_at.isoformat(sep=" ") if row.last_run_at else None,
        "notes": row.notes,
        "createdAt": row.created_at.isoformat(sep=" ") if row.created_at else None,
        "updatedAt": row.updated_at.isoformat(sep=" ") if row.updated_at else None,
    }


def task_to_public(row: CrawlTaskModel) -> dict[str, Any]:
    return {
        "id": row.id,
        "ownerUsername": row.owner_username,
        "sourceId": row.source_id,
        "sourceType": row.source_type,
        "target": row.target,
        "mode": row.mode,
        "status": row.status,
        "totalCount": row.total_count,
        "successCount": row.success_count,
        "failedCount": row.failed_count,
        "message": row.message,
        "errorDetail": row.error_detail,
        "startedAt": row.started_at.isoformat(sep=" ") if row.started_at else None,
        "finishedAt": row.finished_at.isoformat(sep=" ") if row.finished_at else None,
        "createdAt": row.created_at.isoformat(sep=" ") if row.created_at else None,
    }


def product_to_public(row: ProductModel) -> dict[str, Any]:
    listed_at = product_listed_at_text(row)
    raw_payload = product_raw_payload(row)
    price_range = price_range_from_rakuten_item(raw_payload)
    fallback_price = float(row.price) if row.price is not None else None
    price_min = price_range[0] if price_range else fallback_price
    price_max = price_range[1] if price_range else fallback_price
    return {
        "id": row.id,
        "ownerUsername": row.owner_username,
        "taskId": row.task_id,
        "storeId": row.store_id,
        "rakutenManageNumber": row.rakuten_manage_number,
        "storeProductStatus": row.store_product_status,
        "rakutenListingStatus": row.rakuten_listing_status,
        "storeLastSeenAt": row.store_last_seen_at.isoformat(sep=" ") if row.store_last_seen_at else None,
        "title": row.title,
        "sourceUrl": row.source_url,
        "itemNumber": row.item_number,
        "shopName": row.shop_name,
        "imageUrl": row.image_url,
        "price": price_min,
        "priceMin": price_min,
        "priceMax": price_max,
        "currency": row.currency,
        "salesCount": product_sales_count(raw_payload),
        "genreId": row.genre_id,
        "reviewStatus": row.review_status,
        "lastError": row.last_error,
        "listedAt": listed_at,
        "createdAt": row.created_at.isoformat(sep=" ") if row.created_at else None,
        "updatedAt": row.updated_at.isoformat(sep=" ") if row.updated_at else None,
    }


def product_raw_payload(row: ProductModel) -> dict[str, Any]:
    try:
        payload = json.loads(row.raw_payload_json or "{}")
    except ValueError:
        return {}
    return payload if isinstance(payload, dict) else {}


def parse_rakuten_datetime_value(value: Any) -> datetime | None:
    text = first_text_value(value)
    if not text:
        return None
    normalized = text.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return None
    return parsed.replace(tzinfo=None) if parsed.tzinfo else parsed


def product_rakuten_created_at(row: ProductModel) -> datetime | None:
    return row.listed_at or parse_rakuten_datetime_value(product_raw_payload(row).get("created"))


def product_listed_at_text(row: ProductModel) -> str | None:
    value = row.listed_at or product_rakuten_created_at(row)
    return value.isoformat(sep=" ", timespec="seconds") if value else None


def product_sales_count(raw_payload: dict[str, Any]) -> int | None:
    for key in ("salesCount", "salesQuantity", "soldCount", "orderCount", "sales_count", "sold_count"):
        value = raw_payload.get(key)
        if value is None:
            continue
        try:
            return int(float(str(value).replace(",", "")))
        except (TypeError, ValueError):
            continue
    return None


def product_detail_to_public(row: ProductModel) -> dict[str, Any]:
    raw_payload = product_raw_payload(row)
    shop_code = product_shop_code(row, raw_payload)
    public = product_to_public(row)
    public["detail"] = {
        "manageNumber": first_text_from_keys(raw_payload, ("manageNumber", "itemNumber")) or row.rakuten_manage_number,
        "itemNumber": first_text_from_keys(raw_payload, ("itemNumber", "manageNumber")) or row.item_number,
        "title": first_text_from_keys(raw_payload, ("itemName", "title", "name")) or row.title,
        "tagline": first_text_from_keys(raw_payload, ("tagline", "catchcopy", "catchCopy")),
        "genreId": first_text_from_keys(raw_payload, ("genreId", "genre_id", "genre")) or row.genre_id,
        "shopName": row.shop_name,
        "sourceUrl": row.source_url,
        "listingStatus": row.rakuten_listing_status,
        "salesCount": product_sales_count(raw_payload),
        "created": first_text_from_keys(raw_payload, ("created",)),
        "updated": first_text_from_keys(raw_payload, ("updated",)),
        "descriptions": product_descriptions(raw_payload),
        "images": product_image_urls(raw_payload, shop_code=shop_code),
        "variantSelectors": product_variant_selectors(raw_payload),
        "variants": product_variants(raw_payload),
        "raw": raw_payload,
    }
    return public


def product_descriptions(raw_payload: dict[str, Any]) -> list[dict[str, str]]:
    descriptions: list[dict[str, str]] = []
    seen_values: set[str] = set()

    def append(label: str, value: Any) -> None:
        text = first_text_value(value)
        if not text or text in seen_values:
            return
        seen_values.add(text)
        descriptions.append({"label": label, "value": text})

    product_description = raw_payload.get("productDescription")
    if isinstance(product_description, dict):
        append("PC商品说明", product_description.get("pc"))
        append("移动端商品说明", product_description.get("sp"))
        append("智能手机商品说明", product_description.get("smartphone"))
        append("商品说明", product_description.get("value"))
    else:
        append("商品说明", product_description)

    raw_descriptions = raw_payload.get("descriptions")
    if isinstance(raw_descriptions, list):
        for index, item in enumerate(raw_descriptions, start=1):
            if isinstance(item, dict):
                append(first_text_from_keys(item, ("label", "name")) or f"商品说明 {index}", item.get("value"))
            else:
                append(f"商品说明 {index}", item)

    fields = (
        ("商品说明", "description"),
        ("PC用商品说明", "pcDescription"),
        ("移动端商品说明", "spDescription"),
        ("智能手机商品说明", "smartphoneDescription"),
    )
    for label, key in fields:
        append(label, raw_payload.get(key))
    if not descriptions:
        append("销售说明", raw_payload.get("salesDescription"))
    return descriptions


def product_shop_code(row: ProductModel, raw_payload: dict[str, Any]) -> str:
    for value in (
        row.image_url,
        row.source_url,
        first_text_from_keys(raw_payload, ("shopCode", "shop_code", "shopUrl", "shop_url")),
    ):
        shop_code = normalize_shop_code(value)
        if shop_code:
            return shop_code
    return ""


def product_image_urls(raw_payload: dict[str, Any], *, shop_code: str = "") -> list[str]:
    urls: list[str] = []

    def remember(value: Any) -> None:
        url = normalize_product_image_url(value, shop_code=shop_code)
        if url and url not in urls:
            urls.append(url)

    def collect(value: Any) -> None:
        if isinstance(value, str):
            text = unescape(str(value or "")).replace("\\/", "/")
            remember(text)
            for match in re.findall(r"https?://[^\s\"'<>)]+'?", text):
                remember(match)
            return
        if isinstance(value, dict):
            for key in ("url", "imageUrl", "location", "value"):
                collect(value.get(key))
            for child in value.values():
                collect(child)
        elif isinstance(value, list):
            for child in value:
                collect(child)

    collect(raw_payload)
    return urls


def normalize_product_image_url(value: Any, *, shop_code: str = "") -> str:
    text = unescape(str(value or "")).replace("\\/", "/").strip().strip("'\"")
    if not text.startswith(("http://", "https://")):
        if not shop_code:
            return ""
        if not re.search(r"\.(apng|avif|bmp|gif|jpe?g|png|webp)(?:$|[?#])", text, flags=re.I):
            return ""
        normalized_location = text.lstrip("/")
        if normalized_location.startswith("cabinet/"):
            normalized_location = normalized_location.removeprefix("cabinet/")
        text = build_rakuten_cabinet_image_url(shop_code, normalized_location)
    text = text.rstrip(".,;")
    try:
        parsed = urlsplit(text)
        path = parsed.path.lower()
    except Exception:
        return ""
    if not re.search(r"\.(apng|avif|bmp|gif|jpe?g|png|webp)$", path):
        return ""
    filename = path.rsplit("/", 1)[-1]
    if is_ignored_cabinet_image_filename(filename):
        return ""
    return f"{parsed.scheme}://{parsed.netloc}{parsed.path}"


def is_ignored_cabinet_image_filename(value: str) -> bool:
    return value.strip().lower().rsplit("/", 1)[-1] in IGNORED_CABINET_IMAGE_FILENAMES


def product_cabinet_file_targets(raw_payload: dict[str, Any], shop_code: str) -> list[dict[str, str]]:
    normalized_shop_code = normalize_shop_code(shop_code)
    targets: list[dict[str, str]] = []
    seen: set[str] = set()

    def remember(value: Any) -> None:
        for path in cabinet_paths_from_text(value, normalized_shop_code):
            if not path or path in seen:
                continue
            if is_ignored_cabinet_image_filename(path):
                continue
            seen.add(path)
            targets.append({"filePath": path, "fileName": path.rsplit("/", 1)[-1]})

    def walk(value: Any) -> None:
        if isinstance(value, str):
            remember(value)
            return
        if isinstance(value, dict):
            for child in value.values():
                walk(child)
        elif isinstance(value, list):
            for child in value:
                walk(child)

    walk(raw_payload)
    for url in product_image_urls(raw_payload, shop_code=shop_code):
        remember(url)
    return targets


def cabinet_paths_from_text(value: Any, shop_code: str) -> list[str]:
    text = str(value or "").replace("\\/", "/")
    if not text:
        return []

    patterns = [
        r"https?://image\.rakuten\.co\.jp/([^/]+)/cabinet/[^\"'\s<>]+?\.(?:jpg|jpeg|png|webp|gif)",
        r"https?://(?:shop|tshop)\.r10s\.jp/([^/]+)/cabinet/[^\"'\s<>]+?\.(?:jpg|jpeg|png|webp|gif)",
        r"https?://thumbnail\.image\.rakuten\.co\.jp/@0_mall/([^/]+)/cabinet/[^\"'\s<>]+?\.(?:jpg|jpeg|png|webp|gif)",
        r"@0_mall/([^/]+)/cabinet/[^\"'\s<>]+?\.(?:jpg|jpeg|png|webp|gif)",
    ]
    paths: list[str] = []
    for pattern in patterns:
        for match in re.finditer(pattern, text, flags=re.IGNORECASE):
            matched_shop = normalize_shop_code(match.group(1))
            if shop_code and matched_shop and matched_shop != shop_code:
                continue
            full_match = match.group(0)
            cabinet_index = full_match.lower().find("/cabinet/")
            if cabinet_index < 0:
                cabinet_index = full_match.lower().find("cabinet/")
            if cabinet_index < 0:
                continue
            path = "/" + full_match[cabinet_index:].lstrip("/")
            path = path.split("?", 1)[0].split("#", 1)[0]
            if path not in paths:
                paths.append(path)

    for match in re.finditer(r"(?<![\w/])cabinet/[^\"'\s<>]+?\.(?:jpg|jpeg|png|webp|gif)", text, flags=re.IGNORECASE):
        path = "/" + match.group(0).split("?", 1)[0].split("#", 1)[0].lstrip("/")
        if path not in paths:
            paths.append(path)
    return paths


def product_variant_selectors(raw_payload: dict[str, Any]) -> list[dict[str, Any]]:
    selectors = raw_payload.get("variantSelectors")
    if not isinstance(selectors, list):
        return []
    result: list[dict[str, Any]] = []
    for selector in selectors:
        if not isinstance(selector, dict):
            continue
        result.append(
            {
                "key": first_text_from_keys(selector, ("key", "id", "selectorId")),
                "name": first_text_from_keys(selector, ("name", "displayName", "label")),
                "values": selector_values_to_public(selector.get("values")),
            }
        )
    return result


def product_variants(raw_payload: dict[str, Any]) -> list[dict[str, Any]]:
    variants = raw_payload.get("variants")
    if isinstance(variants, dict):
        variant_items = variants.items()
    elif isinstance(variants, list):
        variant_items = ((first_text_from_keys(item, ("variantId", "skuId", "merchantDefinedSkuId")) if isinstance(item, dict) else "", item) for item in variants)
    else:
        return []

    result: list[dict[str, Any]] = []
    for variant_id, variant in variant_items:
        if not isinstance(variant, dict):
            continue
        selector_values = variant.get("selectorValues")
        result.append(
            {
                "variantId": normalize_text(variant_id) or first_text_from_keys(variant, ("variantId", "skuId")),
                "merchantDefinedSkuId": first_text_from_keys(variant, ("merchantDefinedSkuId",)),
                "articleNumber": first_text_value(variant.get("articleNumber")),
                "standardPrice": first_text_from_keys(variant, ("standardPrice", "price", "displayPrice")),
                "hidden": variant.get("hidden"),
                "selectorValues": selector_values if isinstance(selector_values, dict) else {},
                "specs": variant.get("specs") if isinstance(variant.get("specs"), list) else [],
                "attributes": variant.get("attributes") if isinstance(variant.get("attributes"), list) else [],
                "material": first_text_from_keys(variant, ("material",)),
            }
        )
    return result


def selector_values_to_public(values: Any) -> list[Any]:
    if not isinstance(values, list):
        return []
    result: list[Any] = []
    for item in values:
        if isinstance(item, dict):
            result.append(first_text_from_keys(item, ("label", "value", "name")) or item)
        else:
            result.append(item)
    return result


def parse_datetime_filter(value: str | None) -> datetime | None:
    text = normalize_text(value)
    if not text:
        return None
    normalized = text.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return None
    return parsed.replace(tzinfo=None) if parsed.tzinfo else parsed


def normalize_schedule_time(value: Any) -> str:
    text = normalize_text(value) or "09:00"
    match = re.fullmatch(r"([0-9]{1,2}):([0-9]{1,2})(?::[0-9]{1,2})?", text)
    if not match:
        raise RuntimeError("定时执行时间格式不正确。")
    hour = int(match.group(1))
    minute = int(match.group(2))
    if hour > 23 or minute > 59:
        raise RuntimeError("定时执行时间不合法。")
    return f"{hour:02d}:{minute:02d}"


def next_daily_run_at(schedule_time: str, *, now: datetime | None = None) -> datetime:
    reference = now or datetime.now()
    normalized = normalize_schedule_time(schedule_time)
    hour, minute = [int(part) for part in normalized.split(":", 1)]
    candidate = reference.replace(hour=hour, minute=minute, second=0, microsecond=0)
    if candidate <= reference:
        candidate += timedelta(days=1)
    return candidate


def ranking_period_label(value: Any) -> str:
    period = normalize_text(value) or "daily"
    return {
        "realtime": "实时",
        "daily": "日榜",
        "weekly": "周榜",
        "monthly": "月榜",
    }.get(period, "日榜")


def store_to_public(row: StoreModel, *, reveal: bool = False) -> dict[str, Any]:
    service_secret = decrypt_text(row.rakuten_service_secret_encrypted)
    license_key = decrypt_text(row.rakuten_license_key_encrypted)
    availability_status = "unchecked"
    if row.last_error:
        availability_status = "error"
    elif row.last_synced_at:
        availability_status = "available"
    return {
        "id": row.id,
        "ownerUsername": row.owner_username,
        "storeCode": row.store_code,
        "storeName": row.store_name,
        "aliasName": row.alias_name,
        "platform": row.platform,
        "storeUrl": row.store_url,
        "enabled": bool(row.enabled),
        "description": row.description,
        "rakutenServiceSecret": service_secret if reveal else "",
        "rakutenLicenseKey": license_key if reveal else "",
        "masked": {
            "rakutenServiceSecret": mask_secret(service_secret),
            "rakutenLicenseKey": mask_secret(license_key),
        },
        "cabinetUsedFolderCount": row.cabinet_used_folder_count,
        "cabinetRemainingFolderCount": row.cabinet_remaining_folder_count,
        "cabinetUsageCheckedAt": row.cabinet_usage_checked_at.isoformat(sep=" ") if row.cabinet_usage_checked_at else None,
        "lastSyncedAt": row.last_synced_at.isoformat(sep=" ") if row.last_synced_at else None,
        "lastError": row.last_error,
        "availabilityStatus": availability_status,
        "createdAt": row.created_at.isoformat(sep=" ") if row.created_at else None,
        "updatedAt": row.updated_at.isoformat(sep=" ") if row.updated_at else None,
    }


def scheduled_crawl_to_public(row: ScheduledCrawlModel) -> dict[str, Any]:
    return {
        "id": row.id,
        "ownerUsername": row.owner_username,
        "sourceId": row.source_id,
        "name": row.name,
        "crawlContent": row.crawl_content,
        "crawlCondition": row.crawl_condition,
        "sourceType": row.source_type,
        "target": row.target,
        "enabled": bool(row.enabled),
        "intervalMinutes": row.interval_minutes,
        "scheduleTime": row.schedule_time,
        "lastRunAt": row.last_run_at.isoformat(sep=" ") if row.last_run_at else None,
        "nextRunAt": row.next_run_at.isoformat(sep=" ") if row.next_run_at else None,
        "status": row.status,
        "notes": row.notes,
        "createdAt": row.created_at.isoformat(sep=" ") if row.created_at else None,
        "updatedAt": row.updated_at.isoformat(sep=" ") if row.updated_at else None,
    }


def listing_task_to_public(row: ListingTaskModel) -> dict[str, Any]:
    try:
        product_ids = json.loads(row.product_ids_json or "[]")
    except ValueError:
        product_ids = []
    return {
        "id": row.id,
        "ownerUsername": row.owner_username,
        "storeId": row.store_id,
        "taskName": row.task_name,
        "status": row.status,
        "totalCount": row.total_count,
        "successCount": row.success_count,
        "failedCount": row.failed_count,
        "productIds": product_ids if isinstance(product_ids, list) else [],
        "message": row.message,
        "errorDetail": row.error_detail,
        "startedAt": row.started_at.isoformat(sep=" ") if row.started_at else None,
        "finishedAt": row.finished_at.isoformat(sep=" ") if row.finished_at else None,
        "createdAt": row.created_at.isoformat(sep=" ") if row.created_at else None,
        "updatedAt": row.updated_at.isoformat(sep=" ") if row.updated_at else None,
    }


def sync_task_to_public(row: SyncTaskModel) -> dict[str, Any]:
    return {
        "id": row.id,
        "ownerUsername": row.owner_username,
        "storeId": row.store_id,
        "storeName": row.store_name,
        "taskName": row.task_name,
        "status": row.status,
        "totalCount": row.total_count,
        "successCount": row.success_count,
        "failedCount": row.failed_count,
        "message": row.message,
        "errorDetail": row.error_detail,
        "startedAt": row.started_at.isoformat(sep=" ") if row.started_at else None,
        "finishedAt": row.finished_at.isoformat(sep=" ") if row.finished_at else None,
        "createdAt": row.created_at.isoformat(sep=" ") if row.created_at else None,
        "updatedAt": row.updated_at.isoformat(sep=" ") if row.updated_at else None,
    }


def role_to_public(row: RoleModel) -> dict[str, Any]:
    try:
        permissions = json.loads(row.permissions_json or "[]")
    except ValueError:
        permissions = []
    return {
        "id": row.id,
        "name": row.name,
        "code": row.code,
        "scope": row.scope,
        "enabled": bool(row.enabled),
        "permissions": permissions if isinstance(permissions, list) else [],
        "notes": row.notes,
        "createdAt": row.created_at.isoformat(sep=" ") if row.created_at else None,
        "updatedAt": row.updated_at.isoformat(sep=" ") if row.updated_at else None,
    }


def _product_status_filter(status: str | None) -> str | None:
    if not status:
        return None
    status_map = {
        "pending": "pending",
        "approved": "approved",
        "error": "error",
        "listed": "listed",
        "rejected": "rejected",
    }
    return status_map.get(status, status)


def normalize_text(value: Any) -> str:
    return " ".join(str(value or "").strip().split())


def normalize_shop_code(value: Any) -> str:
    normalized = normalize_text(value)
    if not normalized:
        return ""
    if normalized.startswith(("http://", "https://")):
        try:
            path_parts = [part for part in urlsplit(normalized).path.split("/") if part]
        except Exception:
            path_parts = []
        return normalize_text(path_parts[0])
    return normalized.strip("/")


def build_rakuten_store_url(shop_code: str) -> str:
    normalized_shop_code = normalize_shop_code(shop_code)
    if not normalized_shop_code:
        return ""
    return f"https://www.rakuten.co.jp/{normalized_shop_code}/"


def build_public_item_page_url(shop_code: str, item_number: str) -> str:
    normalized_shop_code = normalize_shop_code(shop_code)
    normalized_item_number = normalize_text(item_number)
    if not normalized_shop_code or not normalized_item_number:
        return ""
    return f"https://item.rakuten.co.jp/{quote(normalized_shop_code, safe='')}/{quote(normalized_item_number, safe='')}/"


def is_rakuten_product_url(value: str) -> bool:
    normalized = normalize_text(value)
    if not normalized.startswith(("http://", "https://")):
        return False
    try:
        parsed = urlsplit(normalized)
    except Exception:
        return False
    hostname = parsed.netloc.lower()
    if hostname == "item.rakuten.co.jp":
        return parse_rakuten_product_target(normalized) is not None
    if hostname == "brandavenue.rakuten.co.jp":
        return parse_rakuten_fashion_product_code(normalized) != ""
    return False


def parse_rakuten_fashion_product_code(target: str) -> str:
    normalized = normalize_text(target)
    if not normalized.startswith(("http://", "https://")):
        return ""
    try:
        parsed = urlsplit(normalized)
    except Exception:
        return ""
    if parsed.netloc.lower() != "brandavenue.rakuten.co.jp":
        return ""
    parts = [unquote(part.strip()) for part in parsed.path.split("/") if part.strip()]
    if len(parts) >= 2 and parts[0] == "item":
        return normalize_text(parts[1])
    return ""


def parse_rakuten_product_target(target: str) -> tuple[str, str] | None:
    normalized = normalize_text(target)
    if not normalized:
        return None
    if normalized.startswith(("http://", "https://")):
        try:
            parsed = urlsplit(normalized)
        except Exception:
            return None
        if parsed.netloc.lower() != "item.rakuten.co.jp":
            return None
        parts = [unquote(part.strip()) for part in parsed.path.split("/") if part.strip()]
        if len(parts) < 2:
            return None
        shop_code, item_number = parts[0], parts[1]
    else:
        parts = [unquote(part.strip()) for part in normalized.strip("/").split("/") if part.strip()]
        if len(parts) != 2:
            return None
        shop_code, item_number = parts
    if not shop_code or not item_number:
        return None
    if any(part.startswith(("http:", "https:")) for part in (shop_code, item_number)):
        return None
    return shop_code, item_number


def normalize_rakuten_product_target(target: str) -> str:
    normalized = normalize_text(target)
    fashion_code = parse_rakuten_fashion_product_code(normalized)
    if fashion_code:
        return f"https://brandavenue.rakuten.co.jp/item/{quote(fashion_code, safe='')}/"
    parsed = parse_rakuten_product_target(target)
    if parsed is None:
        raise RuntimeError(RAKUTEN_PRODUCT_TARGET_ERROR)
    shop_code, item_number = parsed
    return build_public_item_page_url(shop_code, item_number)


def normalize_rakuten_shop_target(target: str) -> str:
    normalized = normalize_text(target)
    if re.fullmatch(r"[0-9]+", normalized):
        return normalized
    if not normalized.startswith(("http://", "https://")):
        return normalized
    try:
        parsed = urlsplit(normalized)
    except Exception as exc:
        raise RuntimeError(RAKUTEN_SHOP_TARGET_ERROR) from exc
    if parsed.netloc.lower() == "search.rakuten.co.jp" and parsed.path.rstrip("/").endswith("/search/mall"):
        params = parse_qs(parsed.query)
        return (
            normalize_text((params.get("sn") or [""])[0])
            or normalize_text((params.get("su") or [""])[0])
            or normalize_text((params.get("sid") or [""])[0])
        )
    if parsed.netloc.lower() in {"www.rakuten.co.jp", "item.rakuten.co.jp"}:
        parts = [unquote(part.strip()) for part in parsed.path.split("/") if part.strip()]
        if parts:
            return normalize_text(parts[0])
    raise RuntimeError(RAKUTEN_SHOP_TARGET_ERROR)


def resolve_rakuten_shop_search_keyword(target: str) -> str:
    normalized = normalize_rakuten_shop_target(target)
    if not normalized:
        raise RuntimeError(RAKUTEN_SHOP_TARGET_ERROR)
    if re.fullmatch(r"[0-9]+", normalized):
        display_name = fetch_rakuten_shop_display_name_by_sid(normalized)
        return display_name or normalized
    if looks_like_rakuten_shop_code(normalized):
        display_name = fetch_rakuten_shop_display_name_by_code(normalized)
        return display_name or normalized
    return normalized


def looks_like_rakuten_shop_code(value: str) -> bool:
    return bool(re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]{1,80}", normalize_text(value)))


def fetch_rakuten_shop_display_name_by_code(shop_code: str) -> str:
    normalized_shop_code = normalize_shop_code(shop_code)
    if not normalized_shop_code:
        return ""
    try:
        html = fetch_html(build_rakuten_store_url(normalized_shop_code))
    except requests.RequestException:
        return ""
    return parse_rakuten_shop_display_name(html)


def fetch_rakuten_shop_display_name_by_sid(sid: str) -> str:
    normalized_sid = normalize_text(sid)
    if not normalized_sid:
        return ""
    try:
        html = fetch_html(f"{RAKUTEN_SEARCH_BASE}?sid={quote(normalized_sid)}")
    except requests.RequestException:
        return ""
    return parse_rakuten_search_shop_name(html) or parse_rakuten_shop_display_name(html)


def parse_rakuten_shop_display_name(html: str) -> str:
    soup = BeautifulSoup(html, "lxml")
    h1 = soup.select_one("h1")
    if h1:
        display_name = normalize_text(h1.get_text(" ", strip=True))
        if display_name:
            return display_name
    og_title = soup.select_one("meta[property='og:title'], meta[name='og:title']")
    if og_title:
        display_name = shop_name_from_title(str(og_title.get("content") or ""))
        if display_name:
            return display_name
    if soup.title:
        display_name = shop_name_from_title(soup.title.get_text(" ", strip=True))
        if display_name:
            return display_name
    return ""


def parse_rakuten_search_shop_name(html: str) -> str:
    soup = BeautifulSoup(html, "lxml")
    for selector in ("h1, h2", "a"):
        for node in soup.select(selector):
            text = normalize_text(node.get_text(" ", strip=True))
            if text and not is_generic_rakuten_shop_label(text) and len(text) <= 80:
                return text
    return ""


def is_generic_rakuten_shop_label(value: str) -> bool:
    normalized = normalize_text(value)
    return normalized in {
        "ログイン",
        "会員登録",
        "買い物かご",
        "閲覧履歴",
        "お気に入り",
        "ショップへ問い合わせ",
        "すべてのショップ",
        "ショップ内から探す",
    }


def shop_name_from_title(title: str) -> str:
    normalized = normalize_text(title)
    if not normalized:
        return ""
    match = re.search(r"楽天市場\s*[|｜]\s*(.+?)(?:\s*[-－|｜]\s*.+)?$", normalized)
    if match:
        return normalize_text(match.group(1))
    return ""


def build_rakuten_authorization_header(service_secret: str, license_key: str) -> str:
    authorization = base64.b64encode(f"{service_secret}:{license_key}".encode("utf-8")).decode("ascii")
    return f"ESA {authorization}"


def fetch_rakuten_shop_meta(service_secret: str, license_key: str) -> dict[str, str]:
    if not service_secret or not license_key:
        raise RuntimeError("乐天 Secret 和乐天 Key 不能为空。")
    try:
        response = requests.get(
            RAKUTEN_SHOP_MASTER_URL,
            timeout=settings.crawler_timeout_seconds,
            headers={
                "Authorization": build_rakuten_authorization_header(service_secret, license_key),
                "Accept": "application/xml, text/xml",
            },
        )
        response.raise_for_status()
    except requests.RequestException as exc:
        raise RuntimeError("乐天密钥检测失败，请检查 Secret / Key 是否正确。") from exc
    meta = parse_rakuten_shop_master_xml(response.text)
    if not meta.get("shopCode") or not meta.get("shopName"):
        raise RuntimeError("未能从乐天接口读取到店铺编号和店铺名称。")
    return meta


def fetch_rakuten_cabinet_usage(service_secret: str, license_key: str) -> dict[str, int]:
    if not service_secret or not license_key:
        raise RuntimeError("乐天 Secret 和乐天 Key 不能为空。")
    try:
        response = requests.get(
            RAKUTEN_CABINET_USAGE_URL,
            timeout=settings.crawler_timeout_seconds,
            headers={
                "Authorization": build_rakuten_authorization_header(service_secret, license_key),
                "Accept": "application/xml, text/xml",
            },
        )
        response.raise_for_status()
    except requests.RequestException as exc:
        raise RuntimeError("乐天 Cabinet 使用量读取失败，请检查 Secret / Key 权限。") from exc
    return parse_rakuten_cabinet_usage_xml(response.text)


def parse_rakuten_cabinet_usage_xml(xml_text: str) -> dict[str, int]:
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError as exc:
        raise RuntimeError("乐天 Cabinet 使用量返回格式无法解析。") from exc

    values = {
        "usedFolderCount": 0,
        "remainingFolderCount": 0,
    }
    tag_map = {
        "UseFolderCount": "usedFolderCount",
        "useFolderCount": "usedFolderCount",
        "AvailFolderCount": "remainingFolderCount",
        "availFolderCount": "remainingFolderCount",
    }
    for element in root.iter():
        local_name = element.tag.split("}", 1)[-1]
        target_key = tag_map.get(local_name)
        if not target_key:
            continue
        try:
            values[target_key] = int(float(normalize_text(element.text) or 0))
        except ValueError:
            values[target_key] = 0
    return values


def parse_rakuten_shop_master_xml(xml_text: str) -> dict[str, str]:
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError as exc:
        raise RuntimeError("乐天店铺信息返回格式无法解析。") from exc

    shop_meta = {
        "shopId": "",
        "shopCode": "",
        "shopName": "",
    }
    shop_name_tags = {"shopName", "shopname", "shop_name", "storeName", "storename", "name", "title"}
    shop_code_tags = {"url", "shopUrl", "shopURL", "shop_url", "shopCode", "shopcode", "shop_code"}
    shop_id_tags = {"shopId", "shopid", "shop_id"}

    for element in root.iter():
        local_name = element.tag.split("}", 1)[-1]
        text_value = normalize_text(element.text)
        if not text_value:
            continue
        if not shop_meta["shopName"] and local_name in shop_name_tags:
            shop_meta["shopName"] = text_value
        if not shop_meta["shopCode"] and local_name in shop_code_tags:
            shop_meta["shopCode"] = normalize_shop_code(text_value)
        if not shop_meta["shopId"] and local_name in shop_id_tags:
            shop_meta["shopId"] = text_value
    return shop_meta


def fetch_rakuten_store_items(service_secret: str, license_key: str) -> list[dict[str, Any]]:
    if not service_secret or not license_key:
        raise RuntimeError("乐天 Secret 或乐天 Key 未配置。")
    headers = {
        "Authorization": build_rakuten_authorization_header(service_secret, license_key),
        "Accept": "application/json",
    }
    items: list[dict[str, Any]] = []
    seen: set[str] = set()
    offset = 0
    total_count: int | None = None
    while True:
        payload = request_rakuten_items_page(headers, offset)
        total_count = total_count if total_count is not None else parse_rakuten_total_count(payload)

        page_items = extract_rakuten_item_candidates(payload)
        new_count = 0
        for item in page_items:
            item_key = normalize_text(
                first_text_from_keys(item, ("manageNumber", "itemNumber", "itemUrl", "itemPageUrl"))
            )
            if not item_key or item_key in seen:
                continue
            seen.add(item_key)
            items.append(item)
            new_count += 1
        offset += RAKUTEN_ITEM_SEARCH_HITS
        if not page_items:
            break
        if total_count is not None and offset >= total_count:
            break
        if len(page_items) < RAKUTEN_ITEM_SEARCH_HITS:
            break
    return items


def request_rakuten_items_page(headers: dict[str, str], offset: int) -> dict[str, Any]:
    last_error: Exception | None = None
    for attempt in range(RAKUTEN_ITEM_SEARCH_MAX_RETRIES):
        try:
            response = requests.get(
                RAKUTEN_ITEM_SEARCH_URL,
                timeout=settings.crawler_timeout_seconds,
                headers=headers,
                params={"hits": RAKUTEN_ITEM_SEARCH_HITS, "offset": offset},
            )
            if response.status_code == 429 and attempt < RAKUTEN_ITEM_SEARCH_MAX_RETRIES - 1:
                retry_after = response.headers.get("Retry-After")
                wait_seconds = float(retry_after) if retry_after and retry_after.isdecimal() else 1.5 * (attempt + 1)
                time.sleep(wait_seconds)
                continue
            response.raise_for_status()
            payload = response.json()
            if not isinstance(payload, dict):
                raise RuntimeError("乐天商品接口返回格式无法解析。")
            return payload
        except ValueError as exc:
            raise RuntimeError("乐天商品接口返回格式无法解析。") from exc
        except requests.RequestException as exc:
            last_error = exc
            if attempt < RAKUTEN_ITEM_SEARCH_MAX_RETRIES - 1:
                time.sleep(1.5 * (attempt + 1))
                continue
            raise RuntimeError(f"乐天商品更新失败，读取 offset={offset} 分页时失败，请检查店铺密钥权限或稍后重试。") from exc
    raise RuntimeError(f"乐天商品更新失败，读取 offset={offset} 分页时失败：{last_error}")


def patch_rakuten_item_listing_status(
    service_secret: str,
    license_key: str,
    manage_number: str,
    *,
    listing_status: str,
) -> None:
    if not service_secret or not license_key:
        raise RuntimeError("乐天 Secret 或乐天 Key 未配置。")
    normalized_manage_number = normalize_text(manage_number)
    if not normalized_manage_number:
        raise RuntimeError("商品管理编号为空，不能更新上架状态。")
    if listing_status not in {"listed", "unlisted"}:
        raise RuntimeError("上架状态不合法。")

    visible = listing_status == "listed"
    payload = {
        "hideItem": not visible,
        "features": {
            "searchVisibility": "ALWAYS_VISIBLE" if visible else "ALWAYS_HIDDEN",
        },
    }
    response = requests.patch(
        RAKUTEN_ITEM_PATCH_URL.format(manageNumber=quote(normalized_manage_number, safe="")),
        timeout=settings.crawler_timeout_seconds,
        headers={
            "Authorization": build_rakuten_authorization_header(service_secret, license_key),
            "Accept": "application/json",
            "Content-Type": "application/json; charset=utf-8",
        },
        json=payload,
    )
    try:
        response.raise_for_status()
    except requests.RequestException as exc:
        detail = normalize_text(response.text)
        message = f"乐天商品 {normalized_manage_number} 状态更新失败"
        if detail:
            message = f"{message}：{detail[:500]}"
        raise RuntimeError(message) from exc


def patch_rakuten_item_price(
    service_secret: str,
    license_key: str,
    manage_number: str,
    raw_payload: dict[str, Any],
    price: Decimal,
) -> dict[str, Any]:
    if not service_secret or not license_key:
        raise RuntimeError("乐天 Secret 或乐天 Key 未配置。")
    normalized_manage_number = normalize_text(manage_number)
    if not normalized_manage_number:
        raise RuntimeError("商品管理编号为空，不能修改价格。")

    variants = raw_payload.get("variants")
    if not isinstance(variants, dict) or not variants:
        raise RuntimeError("当前商品没有可修改的 SKU 款式价格，不能同步到乐天。")

    price_text = str(int(price)) if price == price.to_integral_value() else format(price, "f")
    patch_variants: dict[str, dict[str, str]] = {}
    for variant_id, variant in variants.items():
        if not isinstance(variant, dict):
            continue
        normalized_variant_id = normalize_text(variant_id)
        if normalized_variant_id:
            patch_variants[normalized_variant_id] = {"standardPrice": price_text}
    if not patch_variants:
        raise RuntimeError("当前商品没有可修改的 SKU 款式价格，不能同步到乐天。")

    response = requests.patch(
        RAKUTEN_ITEM_PATCH_URL.format(manageNumber=quote(normalized_manage_number, safe="")),
        timeout=settings.crawler_timeout_seconds,
        headers={
            "Authorization": build_rakuten_authorization_header(service_secret, license_key),
            "Accept": "application/json",
            "Content-Type": "application/json; charset=utf-8",
        },
        json={"variants": patch_variants},
    )
    try:
        response.raise_for_status()
    except requests.RequestException as exc:
        detail = normalize_text(response.text)
        message = f"乐天商品 {normalized_manage_number} 价格更新失败"
        if detail:
            message = f"{message}：{detail[:500]}"
        raise RuntimeError(message) from exc

    updated_payload = dict(raw_payload)
    updated_variants = dict(variants)
    for variant_id, variant in updated_variants.items():
        if isinstance(variant, dict):
            next_variant = dict(variant)
            next_variant["standardPrice"] = price_text
            updated_variants[variant_id] = next_variant
    updated_payload["variants"] = updated_variants
    updated_payload["updated"] = datetime.now().isoformat(timespec="seconds")
    return updated_payload


def patch_rakuten_item_detail(
    service_secret: str,
    license_key: str,
    manage_number: str,
    raw_payload: dict[str, Any],
    *,
    title: str,
    tagline: str,
    variants: list[Any],
) -> dict[str, Any]:
    if not service_secret or not license_key:
        raise RuntimeError("乐天 Secret 或乐天 Key 未配置。")
    normalized_manage_number = normalize_text(manage_number)
    if not normalized_manage_number:
        raise RuntimeError("商品管理编号为空，不能同步修改乐天商品。")

    normalized_title = normalize_text(title)
    if not normalized_title:
        raise RuntimeError("商品标题不能为空。")

    raw_variants = raw_payload.get("variants")
    if not isinstance(raw_variants, dict) or not raw_variants:
        raise RuntimeError("当前商品没有可修改的 SKU 款式，不能同步到乐天。")

    patch_variants: dict[str, dict[str, Any]] = {}
    for variant in variants:
        variant_id = normalize_text(getattr(variant, "variantId", ""))
        if not variant_id or variant_id not in raw_variants:
            raise RuntimeError(f"SKU {variant_id or '-'} 不存在，不能同步修改。")
        standard_price = getattr(variant, "standardPrice", None)
        if standard_price is None or standard_price <= 0:
            raise RuntimeError(f"SKU {variant_id} 价格必须大于 0。")
        if standard_price != standard_price.to_integral_value():
            raise RuntimeError(f"SKU {variant_id} 价格必须为日元整数。")
        patch_variants[variant_id] = {
            "standardPrice": str(int(standard_price)),
            "hidden": bool(getattr(variant, "hidden", False)),
        }

    if not patch_variants:
        raise RuntimeError("请至少保留一个可修改的 SKU 款式。")

    payload = {
        "title": normalized_title,
        "tagline": str(tagline or "").strip(),
        "variants": patch_variants,
    }
    response = requests.patch(
        RAKUTEN_ITEM_PATCH_URL.format(manageNumber=quote(normalized_manage_number, safe="")),
        timeout=settings.crawler_timeout_seconds,
        headers={
            "Authorization": build_rakuten_authorization_header(service_secret, license_key),
            "Accept": "application/json",
            "Content-Type": "application/json; charset=utf-8",
        },
        json=payload,
    )
    try:
        response.raise_for_status()
    except requests.RequestException as exc:
        detail = normalize_text(response.text)
        message = f"乐天商品 {normalized_manage_number} 详情更新失败"
        if detail:
            message = f"{message}：{detail[:500]}"
        raise RuntimeError(message) from exc

    updated_payload = dict(raw_payload)
    updated_payload["title"] = normalized_title
    updated_payload["tagline"] = str(tagline or "").strip()
    updated_variants = dict(raw_variants)
    for variant_id, variant_patch in patch_variants.items():
        current_variant = raw_variants.get(variant_id)
        if isinstance(current_variant, dict):
            next_variant = dict(current_variant)
            next_variant.update(variant_patch)
            updated_variants[variant_id] = next_variant
    updated_payload["variants"] = updated_variants
    updated_payload["updated"] = datetime.now().isoformat(timespec="seconds")
    return updated_payload


def patch_local_item_detail(raw_payload: dict[str, Any], *, title: str, tagline: str, variants: list[Any]) -> dict[str, Any]:
    normalized_title = normalize_text(title)
    if not normalized_title:
        raise RuntimeError("商品标题不能为空。")

    updated_payload = dict(raw_payload)
    updated_payload["title"] = normalized_title
    updated_payload["itemName"] = normalized_title
    updated_payload["tagline"] = str(tagline or "").strip()

    raw_variants = updated_payload.get("variants")
    variant_updates: dict[str, Any] = {}
    for variant in variants:
        variant_id = normalize_text(getattr(variant, "variantId", ""))
        standard_price = getattr(variant, "standardPrice", None)
        if not variant_id:
            continue
        if standard_price is None or standard_price <= 0:
            raise RuntimeError(f"SKU {variant_id} 价格必须大于 0。")
        if standard_price != standard_price.to_integral_value():
            raise RuntimeError(f"SKU {variant_id} 价格必须为日元整数。")
        variant_updates[variant_id] = {
            "standardPrice": str(int(standard_price)),
            "price": str(int(standard_price)),
            "hidden": bool(getattr(variant, "hidden", False)),
        }

    if isinstance(raw_variants, dict):
        updated_variants = dict(raw_variants)
        for variant_id, variant_patch in variant_updates.items():
            current_variant = updated_variants.get(variant_id)
            if isinstance(current_variant, dict):
                next_variant = dict(current_variant)
                next_variant.update(variant_patch)
                updated_variants[variant_id] = next_variant
        updated_payload["variants"] = updated_variants
    elif isinstance(raw_variants, list):
        updated_variants = []
        for index, current_variant in enumerate(raw_variants):
            if not isinstance(current_variant, dict):
                updated_variants.append(current_variant)
                continue
            variant_id = first_text_from_keys(current_variant, ("variantId", "skuId", "merchantDefinedSkuId")) or f"sku-{index + 1}"
            next_variant = dict(current_variant)
            if variant_id in variant_updates:
                next_variant.update(variant_updates[variant_id])
            updated_variants.append(next_variant)
        updated_payload["variants"] = updated_variants
    elif variant_updates:
        updated_payload["variants"] = {
            variant_id: {"variantId": variant_id, **variant_patch}
            for variant_id, variant_patch in variant_updates.items()
        }

    updated_payload["updated"] = datetime.now().isoformat(timespec="seconds")
    return updated_payload


def delete_rakuten_item(service_secret: str, license_key: str, manage_number: str) -> None:
    if not service_secret or not license_key:
        raise RuntimeError("乐天 Secret 或乐天 Key 未配置。")
    normalized_manage_number = normalize_text(manage_number)
    if not normalized_manage_number:
        raise RuntimeError("商品管理编号为空，不能删除乐天商品。")

    response = requests.delete(
        RAKUTEN_ITEM_PATCH_URL.format(manageNumber=quote(normalized_manage_number, safe="")),
        timeout=settings.crawler_timeout_seconds,
        headers={
            "Authorization": build_rakuten_authorization_header(service_secret, license_key),
            "Accept": "application/json",
        },
    )
    try:
        response.raise_for_status()
    except requests.RequestException as exc:
        detail = normalize_text(response.text)
        message = f"乐天商品 {normalized_manage_number} 删除失败"
        if detail:
            message = f"{message}：{detail[:500]}"
        raise RuntimeError(message) from exc


def delete_rakuten_cabinet_file(service_secret: str, license_key: str, file_id: int) -> None:
    xml_body = (
        '<?xml version="1.0" encoding="UTF-8"?>'
        "<request>"
        "<fileDeleteRequest>"
        "<file>"
        f"<fileId>{int(file_id)}</fileId>"
        "</file>"
        "</fileDeleteRequest>"
        "</request>"
    )
    response = requests.post(
        RAKUTEN_CABINET_FILE_DELETE_URL,
        timeout=settings.crawler_timeout_seconds,
        headers={
            "Authorization": build_rakuten_authorization_header(service_secret, license_key),
            "Accept": "application/xml, text/xml",
            "Content-Type": "application/xml; charset=utf-8",
        },
        data=xml_body.encode("utf-8"),
    )
    try:
        response.raise_for_status()
    except requests.RequestException as exc:
        detail = normalize_text(response.text)
        message = f"R-Cabinet 图片 {file_id} 删除失败"
        if detail:
            message = f"{message}：{detail[:500]}"
        raise RuntimeError(message) from exc


def search_rakuten_cabinet_file_ids(
    service_secret: str,
    license_key: str,
    *,
    file_path: str = "",
    file_name: str = "",
) -> list[int]:
    params: dict[str, Any] = {"offset": 1, "limit": 100}
    if file_path:
        params["filePath"] = file_path
    if file_name:
        params["fileName"] = file_name
    response = requests.get(
        RAKUTEN_CABINET_FILE_SEARCH_URL,
        timeout=settings.crawler_timeout_seconds,
        headers={
            "Authorization": build_rakuten_authorization_header(service_secret, license_key),
            "Accept": "application/xml, text/xml",
        },
        params=params,
    )
    try:
        response.raise_for_status()
    except requests.RequestException as exc:
        detail = normalize_text(response.text)
        message = "R-Cabinet 图片搜索失败"
        if detail:
            message = f"{message}：{detail[:500]}"
        raise RuntimeError(message) from exc
    return [record["fileId"] for record in parse_cabinet_files_xml(response.text) if record.get("fileId")]


def parse_cabinet_file_ids_xml(xml_text: str) -> list[int]:
    return [record["fileId"] for record in parse_cabinet_files_xml(xml_text) if record.get("fileId")]


def search_rakuten_cabinet_files(
    service_secret: str,
    license_key: str,
    *,
    file_path: str = "",
    file_name: str = "",
) -> list[dict[str, Any]]:
    params: dict[str, Any] = {"offset": 1, "limit": 100}
    if file_path:
        params["filePath"] = file_path
    if file_name:
        params["fileName"] = file_name
    response = requests.get(
        RAKUTEN_CABINET_FILE_SEARCH_URL,
        timeout=settings.crawler_timeout_seconds,
        headers={
            "Authorization": build_rakuten_authorization_header(service_secret, license_key),
            "Accept": "application/xml, text/xml",
        },
        params=params,
    )
    try:
        response.raise_for_status()
    except requests.RequestException as exc:
        detail = normalize_text(response.text)
        message = "R-Cabinet 图片搜索失败"
        if detail:
            message = f"{message}：{detail[:500]}"
        raise RuntimeError(message) from exc
    return parse_cabinet_files_xml(response.text)


def parse_cabinet_files_xml(xml_text: str) -> list[dict[str, Any]]:
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError as exc:
        raise RuntimeError("R-Cabinet 图片搜索返回格式无法解析。") from exc
    records: list[dict[str, Any]] = []
    seen_ids: set[int] = set()
    for element in root.iter():
        children = list(element)
        if not children:
            continue
        values: dict[str, str] = {}
        for child in children:
            local_name = child.tag.split("}", 1)[-1].lower()
            if local_name in {"fileid", "filename", "filepath", "fileurl", "folderpath"}:
                values[local_name] = normalize_text(child.text)
        raw_file_id = values.get("fileid", "")
        if not raw_file_id:
            continue
        try:
            file_id = int(float(raw_file_id))
        except ValueError:
            continue
        if file_id in seen_ids:
            continue
        seen_ids.add(file_id)
        records.append(
            {
                "fileId": file_id,
                "fileName": values.get("filename", ""),
                "filePath": values.get("filepath", ""),
                "fileUrl": values.get("fileurl", ""),
                "folderPath": values.get("folderpath", ""),
            }
        )
    return records


def parse_rakuten_total_count(payload: dict[str, Any]) -> int | None:
    for key in ("numFound", "totalCount", "total", "count"):
        value = payload.get(key)
        if value is None:
            continue
        try:
            return int(value)
        except (TypeError, ValueError):
            continue
    return None


def extract_rakuten_item_candidates(payload: Any) -> list[dict[str, Any]]:
    candidates: list[dict[str, Any]] = []

    def walk(value: Any) -> None:
        if isinstance(value, dict):
            if is_rakuten_item_candidate(value):
                candidates.append(value)
                return
            for child in value.values():
                walk(child)
        elif isinstance(value, list):
            for child in value:
                walk(child)

    walk(payload)
    return candidates


def is_rakuten_item_candidate(value: dict[str, Any]) -> bool:
    identity = first_text_from_keys(value, ("manageNumber", "itemNumber", "itemUrl", "itemPageUrl"))
    title = first_text_from_keys(value, ("itemName", "title", "name"))
    return bool(identity and title)


def first_text_from_keys(source: dict[str, Any], keys: tuple[str, ...]) -> str:
    for key in keys:
        text = first_text_value(source.get(key))
        if text:
            return text
    return ""


def first_text_value(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return normalize_text(value)
    if isinstance(value, (int, float, Decimal)):
        return normalize_text(value)
    if isinstance(value, dict):
        for key in ("value", "text", "name", "title", "url"):
            text = first_text_value(value.get(key))
            if text:
                return text
    if isinstance(value, list):
        for item in value:
            text = first_text_value(item)
            if text:
                return text
    return ""


def first_url_from_keys(source: dict[str, Any], keys: tuple[str, ...], *, shop_code: str = "") -> str:
    for key in keys:
        url = first_url_value(source.get(key), shop_code=shop_code)
        if url:
            return url
    return ""


def first_url_value(value: Any, *, shop_code: str = "") -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        text = normalize_text(value)
        if text.startswith(("http://", "https://")):
            return text
        if shop_code and text.startswith("/"):
            return build_rakuten_cabinet_image_url(shop_code, text)
        return ""
    if isinstance(value, dict):
        for key in ("url", "imageUrl", "itemUrl", "itemPageUrl", "location", "value"):
            url = first_url_value(value.get(key), shop_code=shop_code)
            if url:
                return url
        for child in value.values():
            url = first_url_value(child, shop_code=shop_code)
            if url:
                return url
    if isinstance(value, list):
        for item in value:
            url = first_url_value(item, shop_code=shop_code)
            if url:
                return url
    return ""


def first_rakuten_image_url(item: dict[str, Any], shop_code: str) -> str:
    for key in ("images", "imageUrl", "imageUrls", "mediumImageUrls", "smallImageUrls"):
        url = first_url_value(item.get(key), shop_code=shop_code)
        if url:
            return url
    return ""


def build_rakuten_cabinet_image_url(shop_code: str, location: str) -> str:
    normalized_shop_code = normalize_shop_code(shop_code)
    normalized_location = normalize_text(location).lstrip("/")
    if not normalized_shop_code or not normalized_location:
        return ""
    return f"https://image.rakuten.co.jp/{quote(normalized_shop_code, safe='')}/cabinet/{quote(normalized_location, safe='/')}"


def price_from_rakuten_item(item: dict[str, Any]) -> float | None:
    value = first_text_from_keys(item, ("itemPrice", "price", "standardPrice", "displayPrice")) or first_variant_price(item)
    if not value:
        return None
    normalized = re.sub(r"[^0-9.]", "", value)
    if not normalized:
        return None
    try:
        return float(normalized)
    except ValueError:
        return None


def price_range_from_rakuten_item(item: dict[str, Any]) -> tuple[float, float] | None:
    prices = variant_prices(item)
    if prices:
        return min(prices), max(prices)
    price = price_from_rakuten_item(item)
    if price is None:
        return None
    return price, price


def variant_prices(item: dict[str, Any]) -> list[float]:
    variants = item.get("variants")
    if isinstance(variants, dict):
        variant_items = variants.values()
    elif isinstance(variants, list):
        variant_items = variants
    else:
        variant_items = []

    prices: list[float] = []
    for variant in variant_items:
        if not isinstance(variant, dict):
            continue
        value = first_text_from_keys(variant, ("standardPrice", "price", "displayPrice"))
        normalized = re.sub(r"[^0-9.]", "", value)
        if not normalized:
            continue
        try:
            prices.append(float(normalized))
        except ValueError:
            continue
    return prices


def first_variant_price(item: dict[str, Any]) -> str:
    prices = variant_prices(item)
    if not prices:
        return ""
    return str(min(prices))


def rakuten_listing_status_from_item(item: dict[str, Any]) -> str:
    features = item.get("features")
    if isinstance(features, dict):
        search_visibility = normalize_text(features.get("searchVisibility")).upper()
        if "HIDDEN" in search_visibility:
            return "unlisted"
    hide_item = item.get("hideItem")
    if isinstance(hide_item, str):
        return "unlisted" if hide_item.strip().lower() in {"1", "true", "yes", "on"} else "listed"
    return "unlisted" if bool(hide_item) else "listed"


def list_sources(owner_username: str, *, page: int | None = None, page_size: int | None = None) -> list[dict[str, Any]] | dict[str, Any]:
    with session_scope() as session:
        query = select(CrawlSourceModel).where(CrawlSourceModel.owner_username == owner_username)
        return paginate_query(
            session,
            query,
            order_by=CrawlSourceModel.created_at.desc(),
            page=page,
            page_size=page_size,
            response_key="sources",
            serializer=source_to_public,
        )


def save_source(owner_username: str, payload: Any, source_id: int | None = None) -> dict[str, Any]:
    with session_scope() as session:
        row = session.get(CrawlSourceModel, source_id) if source_id else None
        if row is None:
            row = CrawlSourceModel(owner_username=owner_username)
            session.add(row)
        if row.owner_username != owner_username:
            raise RuntimeError("不能修改其他用户的采集源。")
        row.name = str(getattr(payload, "name", "") or "").strip()
        row.source_type = str(getattr(payload, "sourceType", "") or "keyword").strip()
        row.target = str(getattr(payload, "target", "") or "").strip()
        if row.source_type == "product_url":
            row.target = normalize_rakuten_product_target(row.target)
        row.enabled = bool(getattr(payload, "enabled", True))
        row.schedule_enabled = bool(getattr(payload, "scheduleEnabled", False))
        row.interval_minutes = int(getattr(payload, "intervalMinutes", 60) or 60)
        row.notes = str(getattr(payload, "notes", "") or "").strip()
        if not row.name or not row.target:
            raise RuntimeError("采集源名称和目标不能为空。")
        session.flush()
        return source_to_public(row)


def delete_source(owner_username: str, source_id: int) -> None:
    with session_scope() as session:
        row = session.get(CrawlSourceModel, source_id)
        if row is None:
            return
        if row.owner_username != owner_username:
            raise RuntimeError("不能删除其他用户的采集源。")
        session.delete(row)


def list_tasks(
    owner_username: str,
    *,
    page: int | None = None,
    page_size: int | None = None,
    target: str | None = None,
    status: str | None = None,
    source_type: str | None = None,
    mode: str | None = None,
    created_at_from: str | None = None,
    created_at_to: str | None = None,
) -> list[dict[str, Any]] | dict[str, Any]:
    with session_scope() as session:
        query = select(CrawlTaskModel).where(CrawlTaskModel.owner_username == owner_username)
        created_at_from_value = parse_datetime_filter(created_at_from)
        created_at_to_value = parse_datetime_filter(created_at_to)
        if target:
            query = query.where(CrawlTaskModel.target.like(f"%{target}%"))
        if status:
            query = query.where(CrawlTaskModel.status == status)
        if source_type:
            query = query.where(CrawlTaskModel.source_type == source_type)
        if mode:
            query = query.where(CrawlTaskModel.mode == mode)
        if created_at_from_value is not None:
            query = query.where(CrawlTaskModel.created_at >= created_at_from_value)
        if created_at_to_value is not None:
            query = query.where(CrawlTaskModel.created_at <= created_at_to_value)
        return paginate_query(
            session,
            query,
            order_by=CrawlTaskModel.created_at.desc(),
            page=page,
            page_size=page_size,
            response_key="tasks",
            serializer=task_to_public,
        )


def normalize_task_ids(task_ids: list[str]) -> list[str]:
    normalized: list[str] = []
    for value in task_ids or []:
        task_id = str(value or "").strip()
        if task_id and task_id not in normalized:
            normalized.append(task_id)
    if not normalized:
        raise RuntimeError("请选择要删除的任务。")
    return normalized


def delete_tasks(owner_username: str, task_ids: list[str]) -> dict[str, Any]:
    normalized_ids = normalize_task_ids(task_ids)
    with session_scope() as session:
        rows = session.scalars(
            select(CrawlTaskModel).where(
                CrawlTaskModel.owner_username == owner_username,
                CrawlTaskModel.id.in_(normalized_ids),
            )
        ).all()
        found_ids = {row.id for row in rows}
        products = session.scalars(select(ProductModel).where(ProductModel.task_id.in_(list(found_ids)))).all() if found_ids else []
        for product in products:
            product.task_id = None
        logs = session.scalars(
            select(CrawlLogModel).where(
                CrawlLogModel.owner_username == owner_username,
                CrawlLogModel.task_id.in_(list(found_ids)),
            )
        ).all() if found_ids else []
        for log in logs:
            log.task_id = None
        for row in rows:
            session.delete(row)
        deleted_ids = [row.id for row in rows]
        return {
            "deletedIds": deleted_ids,
            "failedIds": [task_id for task_id in normalized_ids if task_id not in found_ids],
            "deletedCount": len(deleted_ids),
        }


def list_products(
    owner_username: str,
    *,
    status: str | None = None,
    keyword: str | None = None,
    store_id: int | None = None,
    listing_status: str | None = None,
    listed_at_from: str | None = None,
    listed_at_to: str | None = None,
    price_min: Decimal | None = None,
    price_max: Decimal | None = None,
    collected_at_from: str | None = None,
    collected_at_to: str | None = None,
    page: int | None = None,
    page_size: int | None = None,
) -> list[dict[str, Any]] | dict[str, Any]:
    with session_scope() as session:
        query = select(ProductModel).where(ProductModel.owner_username == owner_username)
        listed_at_from_value = parse_datetime_filter(listed_at_from)
        listed_at_to_value = parse_datetime_filter(listed_at_to)
        collected_at_from_value = parse_datetime_filter(collected_at_from)
        collected_at_to_value = parse_datetime_filter(collected_at_to)
        normalized_page = max(1, int(page or 1))
        normalized_page_size = min(500, max(1, int(page_size or 0))) if page_size else None
        product_status = _product_status_filter(status)
        if product_status:
            query = query.where(ProductModel.review_status == product_status)
        if store_id is not None:
            query = query.where(ProductModel.store_id == store_id)
        if listing_status in {"listed", "unlisted"}:
            query = query.where(ProductModel.rakuten_listing_status == listing_status)
        if keyword:
            if product_status == "listed":
                query = query.where(
                    ProductModel.title.like(f"%{keyword}%")
                    | ProductModel.item_number.like(f"%{keyword}%")
                    | ProductModel.rakuten_manage_number.like(f"%{keyword}%")
                )
            else:
                query = query.where(ProductModel.title.like(f"%{keyword}%"))
        if price_min is not None:
            query = query.where(ProductModel.price >= price_min)
        if price_max is not None:
            query = query.where(ProductModel.price <= price_max)
        if collected_at_from_value is not None:
            query = query.where(ProductModel.created_at >= collected_at_from_value)
        if collected_at_to_value is not None:
            query = query.where(ProductModel.created_at <= collected_at_to_value)
        if listed_at_from_value is not None:
            query = query.where(ProductModel.listed_at >= listed_at_from_value)
        if listed_at_to_value is not None:
            query = query.where(ProductModel.listed_at <= listed_at_to_value)
        if normalized_page_size:
            total = session.scalar(select(func.count()).select_from(query.subquery())) or 0
            if total:
                max_page = max(1, (int(total) + normalized_page_size - 1) // normalized_page_size)
                normalized_page = min(normalized_page, max_page)
            rows = session.scalars(
                query.order_by(ProductModel.created_at.desc())
                .offset((normalized_page - 1) * normalized_page_size)
                .limit(normalized_page_size)
            ).all()
            return {
                "products": [product_to_public(row) for row in rows],
                "total": int(total),
                "page": normalized_page,
                "pageSize": normalized_page_size,
            }

        rows = session.scalars(query.order_by(ProductModel.created_at.desc())).all()
        return [product_to_public(row) for row in rows]


def list_stores(*, page: int | None = None, page_size: int | None = None) -> list[dict[str, Any]] | dict[str, Any]:
    with session_scope() as session:
        query = select(StoreModel)
        return paginate_query(
            session,
            query,
            order_by=StoreModel.created_at.desc(),
            page=page,
            page_size=page_size,
            response_key="stores",
            serializer=store_to_public,
        )


def save_store(owner_username: str, payload: Any, store_id: int | None = None) -> dict[str, Any]:
    with session_scope() as session:
        row = session.get(StoreModel, store_id) if store_id else None
        if row is None:
            row = StoreModel(owner_username=owner_username)
            session.add(row)

        row.alias_name = str(getattr(payload, "aliasName", "") or "").strip()
        row.platform = str(getattr(payload, "platform", "") or "rakuten").strip()
        row.enabled = bool(getattr(payload, "enabled", True))
        row.description = str(getattr(payload, "description", "") or "").strip()

        incoming_service_secret = str(getattr(payload, "rakutenServiceSecret", "") or "").strip()
        incoming_license_key = str(getattr(payload, "rakutenLicenseKey", "") or "").strip()
        service_secret = incoming_service_secret or decrypt_text(row.rakuten_service_secret_encrypted)
        license_key = incoming_license_key or decrypt_text(row.rakuten_license_key_encrypted)

        if row.id is None and (not incoming_service_secret or not incoming_license_key):
            raise RuntimeError("新增店铺时必须填写乐天 Secret 和乐天 Key。")
        shop_meta = fetch_rakuten_shop_meta(service_secret, license_key)
        row.store_code = shop_meta["shopCode"]
        row.store_name = shop_meta["shopName"]
        if not row.alias_name:
            row.alias_name = row.store_name
        row.store_url = build_rakuten_store_url(row.store_code)
        if incoming_service_secret:
            row.rakuten_service_secret_encrypted = encrypt_text(incoming_service_secret)
        if incoming_license_key:
            row.rakuten_license_key_encrypted = encrypt_text(incoming_license_key)
        with session.no_autoflush:
            duplicated_query = select(StoreModel).where(StoreModel.store_code == row.store_code)
            if row.id is not None:
                duplicated_query = duplicated_query.where(StoreModel.id != row.id)
            duplicated_store = session.scalar(duplicated_query)
        if duplicated_store is not None:
            raise RuntimeError("店铺编号已存在。")
        session.flush()
        return store_to_public(row)


def delete_store(store_id: int) -> None:
    with session_scope() as session:
        row = session.get(StoreModel, store_id)
        if row is None:
            return
        session.delete(row)


def verify_store_credentials(row: StoreModel) -> None:
    service_secret = decrypt_text(row.rakuten_service_secret_encrypted)
    license_key = decrypt_text(row.rakuten_license_key_encrypted)
    if not service_secret or not license_key:
        raise RuntimeError("乐天 Secret 或乐天 Key 未配置。")
    shop_meta = fetch_rakuten_shop_meta(service_secret, license_key)
    row.store_code = shop_meta["shopCode"]
    row.store_name = shop_meta["shopName"]
    if not row.alias_name:
        row.alias_name = row.store_name
    row.store_url = build_rakuten_store_url(row.store_code)
    update_store_cabinet_usage(row, service_secret, license_key)
    row.last_synced_at = datetime.now()
    row.last_error = None


def update_store_cabinet_usage(row: StoreModel, service_secret: str, license_key: str) -> None:
    usage = fetch_rakuten_cabinet_usage(service_secret, license_key)
    row.cabinet_used_folder_count = usage["usedFolderCount"]
    row.cabinet_remaining_folder_count = usage["remainingFolderCount"]
    row.cabinet_usage_checked_at = datetime.now()


def sync_store(owner_username: str, store_id: int) -> dict[str, Any]:
    task = create_sync_task(owner_username, store_id)
    return {
        "store": task.get("store"),
        "syncTask": task.get("syncTask"),
        "syncedCount": task.get("syncedCount", 0),
    }


def perform_store_sync(owner_username: str, store_id: int) -> dict[str, Any]:
    with session_scope() as session:
        row = session.get(StoreModel, store_id)
        if row is None:
            raise RuntimeError("店铺不存在。")
        if not row.enabled:
            raise RuntimeError("店铺已停用，不能更新商品。")
        synced_count = 0
        failed_count = 0
        service_secret = decrypt_text(row.rakuten_service_secret_encrypted)
        license_key = decrypt_text(row.rakuten_license_key_encrypted)
        verify_store_credentials(row)
        items = fetch_rakuten_store_items(service_secret, license_key)
        seen_manage_numbers: set[str] = set()
        for item in items:
            manage_number = first_text_from_keys(item, ("manageNumber", "itemNumber"))
            if manage_number:
                seen_manage_numbers.add(manage_number)
            if upsert_store_product(session, owner_username, row, item):
                synced_count += 1
            else:
                failed_count += 1
        mark_missing_store_products_removed(session, row, seen_manage_numbers)
        session.flush()
        return {
            "store": store_to_public(row),
            "totalCount": len(items),
            "syncedCount": synced_count,
            "failedCount": failed_count,
        }


def list_sync_tasks(owner_username: str, *, page: int | None = None, page_size: int | None = None) -> list[dict[str, Any]] | dict[str, Any]:
    with session_scope() as session:
        query = select(SyncTaskModel).where(SyncTaskModel.owner_username == owner_username)
        return paginate_query(
            session,
            query,
            order_by=SyncTaskModel.created_at.desc(),
            page=page,
            page_size=page_size,
            response_key="syncTasks",
            serializer=sync_task_to_public,
        )


def delete_sync_tasks(owner_username: str, task_ids: list[str]) -> dict[str, Any]:
    normalized_ids = normalize_task_ids(task_ids)
    with session_scope() as session:
        rows = session.scalars(
            select(SyncTaskModel).where(
                SyncTaskModel.owner_username == owner_username,
                SyncTaskModel.id.in_(normalized_ids),
            )
        ).all()
        found_ids = {row.id for row in rows}
        for row in rows:
            session.delete(row)
        deleted_ids = [row.id for row in rows]
        return {
            "deletedIds": deleted_ids,
            "failedIds": [task_id for task_id in normalized_ids if task_id not in found_ids],
            "deletedCount": len(deleted_ids),
        }


def create_sync_task(owner_username: str, store_id: int) -> dict[str, Any]:
    with session_scope() as session:
        store = session.get(StoreModel, store_id)
        if store is None:
            raise RuntimeError("店铺不存在。")
        if not store.enabled:
            raise RuntimeError("店铺已停用，不能同步商品。")
        task = SyncTaskModel(
            id=uuid.uuid4().hex,
            owner_username=owner_username,
            store_id=store.id,
            store_name=store.alias_name or store.store_name,
            task_name=f"商品同步 {store.alias_name or store.store_name} {datetime.now():%Y-%m-%d %H:%M}",
            status="running",
            message="正在同步店铺商品",
            started_at=datetime.now(),
        )
        session.add(task)
        session.flush()
        task_id = task.id

    run_sync_task(owner_username, task_id)
    with session_scope() as session:
        task = session.get(SyncTaskModel, task_id)
        store = session.get(StoreModel, task.store_id) if task and task.store_id else None
        return {
            "syncTask": sync_task_to_public(task) if task else {"id": task_id},
            "store": store_to_public(store) if store else None,
            "syncedCount": task.success_count if task else 0,
        }


def run_sync_task(owner_username: str, task_id: str) -> None:
    with session_scope() as session:
        task = session.get(SyncTaskModel, task_id)
        if task is None:
            return
        if task.owner_username != owner_username:
            raise RuntimeError("不能执行其他用户的同步任务。")
        task.status = "running"
        task.message = "正在同步店铺商品"
        task.error_detail = None
        task.started_at = datetime.now()
        task.finished_at = None
        store_id = task.store_id

    try:
        if store_id is None:
            raise RuntimeError("同步任务没有关联店铺。")
        result = perform_store_sync(owner_username, store_id)
        total_count = int(result.get("totalCount") or 0)
        success_count = int(result.get("syncedCount") or 0)
        failed_count = int(result.get("failedCount") or 0)
        status = "success" if failed_count == 0 else "partial"
        message = f"完成，同步 {success_count} 条，异常 {failed_count} 条"
        error_detail = None
    except Exception as exc:
        total_count = 0
        success_count = 0
        failed_count = 1
        status = "failed"
        message = "同步失败"
        error_detail = str(exc)
        with session_scope() as session:
            task = session.get(SyncTaskModel, task_id)
            store = session.get(StoreModel, task.store_id) if task and task.store_id else None
            if store is not None:
                store.last_synced_at = datetime.now()
                store.last_error = error_detail

    with session_scope() as session:
        task = session.get(SyncTaskModel, task_id)
        if task is None:
            return
        task.total_count = total_count
        task.success_count = success_count
        task.failed_count = failed_count
        task.status = status
        task.message = message
        task.error_detail = error_detail
        task.finished_at = datetime.now()


def retry_sync_task(owner_username: str, task_id: str) -> dict[str, Any]:
    with session_scope() as session:
        task = session.get(SyncTaskModel, task_id)
        if task is None:
            raise RuntimeError("同步任务不存在。")
        if task.owner_username != owner_username:
            raise RuntimeError("不能重试其他用户的同步任务。")
        task.status = "running"
        task.message = "重新同步中"
        task.error_detail = None
        task.started_at = datetime.now()
        task.finished_at = None
    run_sync_task(owner_username, task_id)
    with session_scope() as session:
        task = session.get(SyncTaskModel, task_id)
        return sync_task_to_public(task) if task else {"id": task_id}


def verify_all_stores() -> dict[str, Any]:
    with session_scope() as session:
        rows = session.scalars(select(StoreModel).order_by(StoreModel.created_at.desc())).all()
        for row in rows:
            try:
                verify_store_credentials(row)
            except Exception as exc:
                row.last_synced_at = datetime.now()
                row.last_error = str(exc)
        session.flush()
        stores = [store_to_public(row) for row in rows]
        return {
            "stores": stores,
            "summary": {
                "total": len(stores),
                "available": sum(1 for store in stores if store["availabilityStatus"] == "available"),
                "error": sum(1 for store in stores if store["availabilityStatus"] == "error"),
                "unchecked": sum(1 for store in stores if store["availabilityStatus"] == "unchecked"),
            },
        }


def list_scheduled_crawls(owner_username: str, *, page: int | None = None, page_size: int | None = None) -> list[dict[str, Any]] | dict[str, Any]:
    with session_scope() as session:
        query = select(ScheduledCrawlModel).where(
            ScheduledCrawlModel.owner_username == owner_username,
            ScheduledCrawlModel.source_type == "shop",
        )
        return paginate_query(
            session,
            query,
            order_by=ScheduledCrawlModel.created_at.desc(),
            page=page,
            page_size=page_size,
            response_key="schedules",
            serializer=scheduled_crawl_to_public,
        )


def save_scheduled_crawl(owner_username: str, payload: Any, schedule_id: int | None = None) -> dict[str, Any]:
    with session_scope() as session:
        row = session.get(ScheduledCrawlModel, schedule_id) if schedule_id else None
        if row is None:
            row = ScheduledCrawlModel(owner_username=owner_username)
            session.add(row)
        if row.owner_username != owner_username:
            raise RuntimeError("不能修改其他用户的定时任务。")

        raw_target = str(getattr(payload, "target", "") or "").strip()
        normalized_target = normalize_rakuten_shop_target(raw_target)
        if not normalized_target:
            raise RuntimeError(RAKUTEN_SHOP_TARGET_ERROR)
        schedule_time = normalize_schedule_time(getattr(payload, "scheduleTime", "09:00"))
        period_label = ranking_period_label(getattr(payload, "rankingPeriod", "daily"))

        row.source_id = None
        row.source_type = "shop"
        row.target = f"店铺:{normalized_target} {period_label} 全部"
        row.name = str(getattr(payload, "name", "") or "").strip() or f"{normalized_target} 每日定时采集"
        row.crawl_content = normalized_target
        row.crawl_condition = "店铺采集"
        row.enabled = bool(getattr(payload, "enabled", True))
        row.interval_minutes = 1440
        row.schedule_time = schedule_time
        row.notes = str(getattr(payload, "notes", "") or "").strip()
        row.status = "idle" if row.enabled else "disabled"
        row.next_run_at = next_daily_run_at(row.schedule_time) if row.enabled else None
        if not row.name or not row.target:
            raise RuntimeError("定时任务名称和采集目标不能为空。")
        session.flush()
        return scheduled_crawl_to_public(row)


def delete_scheduled_crawl(owner_username: str, schedule_id: int) -> None:
    with session_scope() as session:
        row = session.get(ScheduledCrawlModel, schedule_id)
        if row is None:
            return
        if row.owner_username != owner_username:
            raise RuntimeError("不能删除其他用户的定时任务。")
        session.delete(row)


def run_scheduled_crawl(owner_username: str, schedule_id: int) -> dict[str, Any]:
    with session_scope() as session:
        row = session.get(ScheduledCrawlModel, schedule_id)
        if row is None:
            raise RuntimeError("定时任务不存在。")
        if row.owner_username != owner_username:
            raise RuntimeError("不能执行其他用户的定时任务。")
        row.status = "running"
        row.last_run_at = datetime.now()
        session.flush()
        source_type = row.source_type
        target = row.target

    task_payload = type("TaskPayload", (), {"sourceId": None, "sourceType": source_type, "target": target, "mode": "scheduled"})()
    create_task(owner_username, task_payload)

    with session_scope() as session:
        row = session.get(ScheduledCrawlModel, schedule_id)
        if row is None:
            raise RuntimeError("定时任务不存在。")
        row.status = "idle" if row.enabled else "disabled"
        row.next_run_at = next_daily_run_at(row.schedule_time) if row.enabled else None
        session.flush()
        return scheduled_crawl_to_public(row)


def run_due_scheduled_crawls_once() -> int:
    if not SCHEDULE_RUN_LOCK.acquire(blocking=False):
        return 0
    try:
        now = datetime.now()
        with session_scope() as session:
            rows = session.scalars(
                select(ScheduledCrawlModel).where(
                    ScheduledCrawlModel.enabled.is_(True),
                    ScheduledCrawlModel.source_type == "shop",
                    ScheduledCrawlModel.next_run_at.is_not(None),
                    ScheduledCrawlModel.next_run_at <= now,
                    ScheduledCrawlModel.status != "running",
                )
            ).all()
            due_items = [(row.owner_username, row.id) for row in rows]
            for row in rows:
                row.status = "running"
                row.last_run_at = now
                row.next_run_at = next_daily_run_at(row.schedule_time, now=now)

        for owner_username, schedule_id in due_items:
            try:
                run_scheduled_crawl(owner_username, schedule_id)
            except Exception as exc:
                with session_scope() as session:
                    row = session.get(ScheduledCrawlModel, schedule_id)
                    if row is not None:
                        row.status = "failed"
                        row.notes = str(exc)
            time.sleep(0.1)
        return len(due_items)
    finally:
        SCHEDULE_RUN_LOCK.release()


def start_schedule_runner(interval_seconds: int = 60) -> None:
    global SCHEDULE_RUNNER_STARTED
    if SCHEDULE_RUNNER_STARTED:
        return
    SCHEDULE_RUNNER_STARTED = True

    def loop() -> None:
        while True:
            try:
                run_due_scheduled_crawls_once()
            except Exception:
                pass
            time.sleep(max(10, interval_seconds))

    threading.Thread(target=loop, name="lt-schedule-runner", daemon=True).start()


def update_product_status(owner_username: str, product_ids: list[int], status: str, *, message: str = "") -> list[dict[str, Any]]:
    if status not in {"pending", "approved", "error", "listed", "rejected"}:
        raise RuntimeError("商品状态不合法。")
    with session_scope() as session:
        rows = session.scalars(
            select(ProductModel).where(
                ProductModel.owner_username == owner_username,
                ProductModel.id.in_(product_ids or [-1]),
            )
        ).all()
        if not rows:
            raise RuntimeError("没有找到可操作的商品。")
        if status == "error":
            invalid_rows = [row for row in rows if row.review_status != "pending"]
            if invalid_rows:
                raise RuntimeError("只有待审核商品可以标记异常。")
        for row in rows:
            row.review_status = status
            if message:
                row.last_error = message if status in {"error", "rejected"} else None
        session.flush()
        return [product_to_public(row) for row in rows]


def delete_products(owner_username: str, product_ids: list[int]) -> dict[str, Any]:
    normalized_ids = [int(value) for value in (product_ids or [])]
    if not normalized_ids:
        raise RuntimeError("请先选择商品。")
    with session_scope() as session:
        rows = session.scalars(
            select(ProductModel).where(
                ProductModel.owner_username == owner_username,
                ProductModel.id.in_(normalized_ids),
            )
        ).all()
        if not rows:
            raise RuntimeError("没有找到可删除的商品。")

        success_count = 0
        failed_count = 0
        cabinet_deleted_count = 0
        deleted_ids: list[int] = []
        failed_ids: list[int] = []
        failed_products: list[dict[str, Any]] = []
        errors: list[str] = []
        warnings: list[str] = []
        credential_cache: dict[int, tuple[StoreModel, str, str]] = {}
        for row in rows:
            if row.review_status == "listed":
                try:
                    delete_store_product_from_rakuten(session, row, credential_cache)
                    cabinet_deleted_count += int(getattr(row, "_deleted_cabinet_count", 0) or 0)
                except Exception as exc:
                    failed_count += 1
                    failed_ids.append(row.id)
                    error_text = str(exc)
                    row.last_error = error_text
                    errors.append(f"{productCodeForError(row)}: {error_text}")
                    failed_products.append(product_to_public(row))
                    continue
                warnings.extend(getattr(row, "_delete_warnings", []) or [])
            deleted_ids.append(row.id)
            session.delete(row)
            success_count += 1
        session.flush()
        message = f"完成，成功删除 {success_count} 个，失败 {failed_count} 个"
        if cabinet_deleted_count:
            message = f"{message}，同步删除图片 {cabinet_deleted_count} 个"
        return {
            "deletedIds": deleted_ids,
            "failedIds": failed_ids,
            "products": failed_products,
            "summary": {
                "total": len(rows),
                "successCount": success_count,
                "failedCount": failed_count,
                "cabinetDeletedCount": cabinet_deleted_count,
                "message": message,
                "errors": errors[:20],
                "warnings": warnings[:20],
            }
        }


def productCodeForError(row: ProductModel) -> str:
    return normalize_text(row.rakuten_manage_number or row.item_number or row.title or row.id)


def delete_store_product_from_rakuten(
    session: Any,
    product: ProductModel,
    credential_cache: dict[int, tuple[StoreModel, str, str]],
) -> None:
    if not product.store_id:
        raise RuntimeError("商品未关联店铺，不能删除乐天商品。")
    manage_number = normalize_text(product.rakuten_manage_number or product.item_number)
    if not manage_number:
        raise RuntimeError("商品缺少商品管理编号，不能删除乐天商品。")

    credentials = credential_cache.get(product.store_id)
    if credentials is None:
        store = session.get(StoreModel, product.store_id)
        if store is None:
            raise RuntimeError("商品关联店铺不存在。")
        if not store.enabled:
            raise RuntimeError("商品关联店铺已停用，不能删除乐天商品。")
        credentials = (
            store,
            decrypt_text(store.rakuten_service_secret_encrypted),
            decrypt_text(store.rakuten_license_key_encrypted),
        )
        credential_cache[product.store_id] = credentials

    store, service_secret, license_key = credentials
    raw_payload = product_raw_payload(product)
    delete_rakuten_item(service_secret, license_key, manage_number)
    deleted_count, warnings = delete_product_cabinet_images(service_secret, license_key, raw_payload, store.store_code)
    setattr(product, "_deleted_cabinet_count", deleted_count)
    setattr(product, "_delete_warnings", warnings)


def delete_product_cabinet_images(
    service_secret: str,
    license_key: str,
    raw_payload: dict[str, Any],
    shop_code: str,
) -> tuple[int, list[str]]:
    targets = product_cabinet_file_targets(raw_payload, shop_code)
    deleted_ids: set[int] = set()
    warnings: list[str] = []
    for target in targets:
        try:
            file_ids = resolve_cabinet_file_ids(service_secret, license_key, target)
        except Exception as exc:
            warnings.append(f"{target.get('filePath') or target.get('fileName')}: {exc}")
            continue
        for file_id in file_ids:
            if file_id in deleted_ids:
                continue
            try:
                delete_rakuten_cabinet_file(service_secret, license_key, file_id)
                deleted_ids.add(file_id)
            except Exception as exc:
                warnings.append(f"R-Cabinet 图片 {file_id}: {exc}")
    return len(deleted_ids), warnings


def resolve_cabinet_file_ids(service_secret: str, license_key: str, target: dict[str, str]) -> list[int]:
    file_path = normalize_text(target.get("filePath"))
    file_name = normalize_text(target.get("fileName"))
    if file_name:
        records = search_rakuten_cabinet_files(service_secret, license_key, file_name=file_name)
        if file_path:
            exact_ids = [int(record["fileId"]) for record in records if cabinet_record_matches_target(record, file_path)]
            if exact_ids:
                return exact_ids
        if len(records) == 1:
            return [int(records[0]["fileId"])]
    if file_path:
        try:
            records = search_rakuten_cabinet_files(service_secret, license_key, file_path=file_path)
        except RuntimeError:
            records = []
        exact_ids = [int(record["fileId"]) for record in records if cabinet_record_matches_target(record, file_path)]
        if exact_ids:
            return exact_ids
    return []


def cabinet_record_matches_target(record: dict[str, Any], file_path: str) -> bool:
    expected = normalize_cabinet_path(file_path)
    for value in (
        record.get("fileUrl"),
        cabinet_record_path(record),
        record.get("filePath"),
    ):
        if normalize_cabinet_path(value) == expected:
            return True
    return False


def cabinet_record_path(record: dict[str, Any]) -> str:
    file_name = normalize_text(record.get("fileName"))
    folder_path = normalize_text(record.get("folderPath"))
    if folder_path and file_name:
        return f"/cabinet/{folder_path.strip('/')}/{file_name}"
    return normalize_text(record.get("filePath"))


def normalize_cabinet_path(value: Any) -> str:
    text = normalize_text(value).replace("\\/", "/")
    if not text:
        return ""
    if text.startswith(("http://", "https://")):
        try:
            text = urlsplit(text).path
        except Exception:
            return ""
    cabinet_index = text.lower().find("/cabinet/")
    if cabinet_index >= 0:
        text = text[cabinet_index:]
    elif text.lower().startswith("cabinet/"):
        text = "/" + text
    elif not text.startswith("/"):
        text = "/" + text
    return "/" + text.lstrip("/").split("?", 1)[0].split("#", 1)[0].lower()


def get_product_detail(owner_username: str, product_id: int) -> dict[str, Any]:
    with session_scope() as session:
        row = session.get(ProductModel, product_id)
        if row is None or row.owner_username != owner_username:
            raise RuntimeError("商品不存在。")
        return product_detail_to_public(row)


def update_store_product_price(owner_username: str, product_id: int, price: Decimal) -> dict[str, Any]:
    if price <= 0:
        raise RuntimeError("商品价格必须大于 0。")
    if price != price.to_integral_value():
        raise RuntimeError("乐天商品价格必须为日元整数，不能包含小数。")
    normalized_price = price.to_integral_value()
    with session_scope() as session:
        product = session.get(ProductModel, product_id)
        if product is None or product.owner_username != owner_username:
            raise RuntimeError("商品不存在。")
        if product.review_status != "listed":
            raise RuntimeError("只有店铺商品可以同步修改乐天价格。")
        if not product.store_id:
            raise RuntimeError("商品未关联店铺，不能同步修改乐天价格。")
        manage_number = normalize_text(product.rakuten_manage_number or product.item_number)
        if not manage_number:
            raise RuntimeError("商品缺少商品管理编号，不能同步修改乐天价格。")

        store = session.get(StoreModel, product.store_id)
        if store is None:
            raise RuntimeError("商品关联店铺不存在。")
        if not store.enabled:
            raise RuntimeError("商品关联店铺已停用，不能同步修改乐天价格。")

        raw_payload = product_raw_payload(product)
        try:
            updated_payload = patch_rakuten_item_price(
                decrypt_text(store.rakuten_service_secret_encrypted),
                decrypt_text(store.rakuten_license_key_encrypted),
                manage_number,
                raw_payload,
                normalized_price,
            )
        except Exception as exc:
            product.last_error = str(exc)
            raise

        product.price = normalized_price
        product.listed_at = parse_rakuten_datetime_value(updated_payload.get("created")) or product.listed_at
        product.raw_payload_json = json.dumps(updated_payload, ensure_ascii=False)
        product.store_last_seen_at = datetime.now()
        product.last_error = None
        session.flush()
        return product_to_public(product)


def update_store_product_detail(owner_username: str, product_id: int, payload: Any) -> dict[str, Any]:
    with session_scope() as session:
        product = session.get(ProductModel, product_id)
        if product is None or product.owner_username != owner_username:
            raise RuntimeError("商品不存在。")
        if product.review_status != "listed":
            raise RuntimeError("只有店铺商品可以同步修改乐天商品详情。")
        if not product.store_id:
            raise RuntimeError("商品未关联店铺，不能同步修改乐天商品详情。")
        manage_number = normalize_text(product.rakuten_manage_number or product.item_number)
        if not manage_number:
            raise RuntimeError("商品缺少商品管理编号，不能同步修改乐天商品详情。")

        store = session.get(StoreModel, product.store_id)
        if store is None:
            raise RuntimeError("商品关联店铺不存在。")
        if not store.enabled:
            raise RuntimeError("商品关联店铺已停用，不能同步修改乐天商品详情。")

        raw_payload = product_raw_payload(product)
        try:
            updated_payload = patch_rakuten_item_detail(
                decrypt_text(store.rakuten_service_secret_encrypted),
                decrypt_text(store.rakuten_license_key_encrypted),
                manage_number,
                raw_payload,
                title=getattr(payload, "title", ""),
                tagline=getattr(payload, "tagline", ""),
                variants=list(getattr(payload, "variants", []) or []),
            )
        except Exception as exc:
            product.last_error = str(exc)
            raise

        product.title = first_text_from_keys(updated_payload, ("itemName", "title", "name")) or product.title
        product.price = price_from_rakuten_item(updated_payload)
        product.listed_at = parse_rakuten_datetime_value(updated_payload.get("created")) or product.listed_at
        product.raw_payload_json = json.dumps(updated_payload, ensure_ascii=False)
        product.store_last_seen_at = datetime.now()
        product.last_error = None
        session.flush()
        return product_detail_to_public(product)


def update_product_local_detail(owner_username: str, product_id: int, payload: Any) -> dict[str, Any]:
    with session_scope() as session:
        product = session.get(ProductModel, product_id)
        if product is None or product.owner_username != owner_username:
            raise RuntimeError("商品不存在。")
        if product.review_status == "listed":
            raise RuntimeError("店铺商品请使用同步修改。")

        updated_payload = patch_local_item_detail(
            product_raw_payload(product),
            title=getattr(payload, "title", ""),
            tagline=getattr(payload, "tagline", ""),
            variants=list(getattr(payload, "variants", []) or []),
        )
        product.title = first_text_from_keys(updated_payload, ("itemName", "title", "name")) or product.title
        product.price = price_from_rakuten_item(updated_payload)
        product.raw_payload_json = json.dumps(updated_payload, ensure_ascii=False)
        product.last_error = None
        session.flush()
        return product_detail_to_public(product)


def update_store_products_listing_status(
    owner_username: str,
    product_ids: list[int],
    listing_status: str,
) -> dict[str, Any]:
    if listing_status not in {"listed", "unlisted"}:
        raise RuntimeError("上架状态不合法。")
    normalized_ids = [int(value) for value in (product_ids or [])]
    if not normalized_ids:
        raise RuntimeError("请先选择商品。")

    with session_scope() as session:
        products = session.scalars(
            select(ProductModel).where(
                ProductModel.owner_username == owner_username,
                ProductModel.id.in_(normalized_ids),
                ProductModel.review_status == "listed",
            )
        ).all()
        if not products:
            raise RuntimeError("没有找到可操作的店铺商品。")

        success_ids: list[int] = []
        errors: list[str] = []
        credential_cache: dict[int, tuple[str, str]] = {}
        for product in products:
            manage_number = normalize_text(product.rakuten_manage_number or product.item_number)
            if not product.store_id:
                errors.append(f"{product.title} 未关联店铺")
                product.last_error = "未关联店铺，不能更新乐天上架状态。"
                continue
            if not manage_number:
                errors.append(f"{product.title} 缺少商品管理编号")
                product.last_error = "缺少商品管理编号，不能更新乐天上架状态。"
                continue

            credentials = credential_cache.get(product.store_id)
            if credentials is None:
                store = session.get(StoreModel, product.store_id)
                if store is None:
                    errors.append(f"{product.title} 关联店铺不存在")
                    product.last_error = "关联店铺不存在，不能更新乐天上架状态。"
                    continue
                if not store.enabled:
                    errors.append(f"{store.alias_name or store.store_name} 已停用")
                    product.last_error = "关联店铺已停用，不能更新乐天上架状态。"
                    continue
                credentials = (
                    decrypt_text(store.rakuten_service_secret_encrypted),
                    decrypt_text(store.rakuten_license_key_encrypted),
                )
                credential_cache[product.store_id] = credentials

            try:
                patch_rakuten_item_listing_status(
                    credentials[0],
                    credentials[1],
                    manage_number,
                    listing_status=listing_status,
                )
            except Exception as exc:
                error_text = str(exc)
                product.last_error = error_text
                errors.append(f"{manage_number}: {error_text}")
                continue

            product.rakuten_listing_status = listing_status
            product.last_error = None
            product.store_product_status = "active"
            product.store_last_seen_at = datetime.now()
            success_ids.append(product.id)

        session.flush()
        rows = session.scalars(
            select(ProductModel).where(
                ProductModel.owner_username == owner_username,
                ProductModel.id.in_(normalized_ids),
            )
        ).all()
        success_count = len(success_ids)
        failed_count = len(products) - success_count
        message = f"完成，成功 {success_count} 个，失败 {failed_count} 个"
        return {
            "products": [product_to_public(row) for row in rows],
            "summary": {
                "total": len(products),
                "successCount": success_count,
                "failedCount": failed_count,
                "message": message,
                "errors": errors[:20],
            },
        }


def list_listing_tasks(owner_username: str, *, page: int | None = None, page_size: int | None = None) -> list[dict[str, Any]] | dict[str, Any]:
    with session_scope() as session:
        query = select(ListingTaskModel).where(ListingTaskModel.owner_username == owner_username)
        return paginate_query(
            session,
            query,
            order_by=ListingTaskModel.created_at.desc(),
            page=page,
            page_size=page_size,
            response_key="listingTasks",
            serializer=listing_task_to_public,
        )


def delete_listing_tasks(owner_username: str, task_ids: list[str]) -> dict[str, Any]:
    normalized_ids = normalize_task_ids(task_ids)
    with session_scope() as session:
        rows = session.scalars(
            select(ListingTaskModel).where(
                ListingTaskModel.owner_username == owner_username,
                ListingTaskModel.id.in_(normalized_ids),
            )
        ).all()
        found_ids = {row.id for row in rows}
        for row in rows:
            session.delete(row)
        deleted_ids = [row.id for row in rows]
        return {
            "deletedIds": deleted_ids,
            "failedIds": [task_id for task_id in normalized_ids if task_id not in found_ids],
            "deletedCount": len(deleted_ids),
        }


def create_listing_task(owner_username: str, payload: Any) -> dict[str, Any]:
    product_ids = [int(value) for value in (getattr(payload, "productIds", None) or [])]
    store_id = getattr(payload, "storeId", None)
    task_name = str(getattr(payload, "taskName", "") or "").strip()
    if not product_ids:
        raise RuntimeError("请选择要上架的商品。")
    with session_scope() as session:
        store = session.get(StoreModel, store_id) if store_id else None
        products = session.scalars(
            select(ProductModel).where(
                ProductModel.owner_username == owner_username,
                ProductModel.id.in_(product_ids),
            )
        ).all()
        if not products:
            raise RuntimeError("没有找到可上架的商品。")
        task = ListingTaskModel(
            id=uuid.uuid4().hex,
            owner_username=owner_username,
            store_id=store.id if store else None,
            task_name=task_name or f"上架任务 {datetime.now():%Y-%m-%d %H:%M}",
            status="running",
            total_count=len(products),
            success_count=0,
            failed_count=0,
            product_ids_json=json.dumps([product.id for product in products], ensure_ascii=False),
            message="正在加入商品池",
            started_at=datetime.now(),
        )
        session.add(task)
        session.flush()
        task_id = task.id

    run_listing_task(owner_username, task_id)
    with session_scope() as session:
        task = session.get(ListingTaskModel, task_id)
        return listing_task_to_public(task) if task else {"id": task_id}


def run_listing_task(owner_username: str, task_id: str) -> None:
    with session_scope() as session:
        task = session.get(ListingTaskModel, task_id)
        if task is None:
            return
        if task.owner_username != owner_username:
            raise RuntimeError("不能执行其他用户的上架任务。")
        try:
            product_ids = json.loads(task.product_ids_json or "[]")
        except ValueError:
            product_ids = []
        products = session.scalars(
            select(ProductModel).where(
                ProductModel.owner_username == owner_username,
                ProductModel.id.in_(product_ids or [-1]),
            )
        ).all()
        success_count = 0
        failed_count = 0
        for product in products:
            if product.review_status in {"approved", "listed"}:
                product.review_status = "listed"
                product.last_error = None
                success_count += 1
            else:
                product.review_status = "error"
                product.last_error = "商品未审核通过，不能上架。"
                failed_count += 1
        task.total_count = len(products)
        task.success_count = success_count
        task.failed_count = failed_count
        task.status = "success" if failed_count == 0 else "partial"
        task.message = f"完成，上架 {success_count} 条，异常 {failed_count} 条"
        task.finished_at = datetime.now()


def retry_listing_task(owner_username: str, task_id: str) -> dict[str, Any]:
    with session_scope() as session:
        task = session.get(ListingTaskModel, task_id)
        if task is None:
            raise RuntimeError("上架任务不存在。")
        if task.owner_username != owner_username:
            raise RuntimeError("不能重试其他用户的上架任务。")
        task.status = "running"
        task.message = "重新执行中"
        task.error_detail = None
        task.started_at = datetime.now()
        task.finished_at = None
    run_listing_task(owner_username, task_id)
    with session_scope() as session:
        task = session.get(ListingTaskModel, task_id)
        return listing_task_to_public(task) if task else {"id": task_id}


def ensure_default_roles() -> None:
    defaults = [
        {
            "name": "超级管理员",
            "code": "superadmin",
            "scope": "all",
            "permissions": ["users.manage", "roles.manage", "crawler.manage", "products.manage", "stores.manage"],
            "notes": "系统内置角色，拥有全部管理权限。",
        },
        {
            "name": "运营用户",
            "code": "operator",
            "scope": "own",
            "permissions": ["secrets.manage", "crawler.manage", "products.manage", "stores.manage"],
            "notes": "默认业务角色，可使用公司共享店铺，处理自己的采集任务和商品。",
        },
    ]
    with session_scope() as session:
        for item in defaults:
            row = session.scalar(select(RoleModel).where(RoleModel.code == item["code"]))
            if row is None:
                row = RoleModel(code=item["code"])
                session.add(row)
            row.name = item["name"]
            row.scope = item["scope"]
            row.enabled = True
            row.permissions_json = json.dumps(item["permissions"], ensure_ascii=False)
            row.notes = item["notes"]


def list_roles(*, page: int | None = None, page_size: int | None = None) -> list[dict[str, Any]] | dict[str, Any]:
    ensure_default_roles()
    with session_scope() as session:
        query = select(RoleModel)
        return paginate_query(
            session,
            query,
            order_by=RoleModel.id.asc(),
            page=page,
            page_size=page_size,
            response_key="roles",
            serializer=role_to_public,
        )


def save_role(payload: Any, role_id: int | None = None) -> dict[str, Any]:
    ensure_default_roles()
    with session_scope() as session:
        row = session.get(RoleModel, role_id) if role_id else None
        if row is None:
            row = RoleModel()
            session.add(row)
        code = str(getattr(payload, "code", "") or "").strip()
        if not code:
            raise RuntimeError("角色编码不能为空。")
        if row.code in {"superadmin", "operator"} and code != row.code:
            raise RuntimeError("内置角色编码不能修改。")
        row.name = str(getattr(payload, "name", "") or "").strip()
        row.code = code
        row.scope = str(getattr(payload, "scope", "") or "own").strip()
        row.enabled = bool(getattr(payload, "enabled", True))
        row.permissions_json = json.dumps(getattr(payload, "permissions", None) or [], ensure_ascii=False)
        row.notes = str(getattr(payload, "notes", "") or "").strip()
        if not row.name:
            raise RuntimeError("角色名称不能为空。")
        session.flush()
        return role_to_public(row)


def delete_role(role_id: int) -> None:
    ensure_default_roles()
    with session_scope() as session:
        row = session.get(RoleModel, role_id)
        if row is None:
            return
        if row.code in {"superadmin", "operator"}:
            raise RuntimeError("内置角色不能删除。")
        session.delete(row)


def create_task(owner_username: str, payload: Any) -> dict[str, Any]:
    source_id = getattr(payload, "sourceId", None)
    source_type = str(getattr(payload, "sourceType", "") or "").strip()
    target = str(getattr(payload, "target", "") or "").strip()
    with session_scope() as session:
        source = session.get(CrawlSourceModel, source_id) if source_id else None
        if source is not None:
            if source.owner_username != owner_username:
                raise RuntimeError("不能使用其他用户的采集源。")
            source_type = source.source_type
            target = source.target
        if not source_type or not target:
            raise RuntimeError("采集类型和目标不能为空。")
        if source_type == "product_url":
            target = normalize_rakuten_product_target(target)
        task = CrawlTaskModel(
            id=uuid.uuid4().hex,
            owner_username=owner_username,
            source_id=source.id if source else None,
            source_type=source_type,
            target=target,
            mode=str(getattr(payload, "mode", "") or "manual"),
            status="queued",
            message="等待执行",
        )
        session.add(task)
        session.flush()
        task_public = task_to_public(task)

    dispatch_crawl_task(task_public["id"])
    return task_public


def run_existing_task(owner_username: str, task_id: str) -> dict[str, Any]:
    with session_scope() as session:
        task = session.get(CrawlTaskModel, task_id)
        if task is None:
            raise RuntimeError("采集任务不存在。")
        if task.owner_username != owner_username:
            raise RuntimeError("不能重启其他用户的采集任务。")
        task.status = "queued"
        task.total_count = 0
        task.success_count = 0
        task.failed_count = 0
        task.message = "等待重新执行"
        task.error_detail = None
        task.started_at = None
        task.finished_at = None
    dispatch_crawl_task(task_id)
    with session_scope() as session:
        task = session.get(CrawlTaskModel, task_id)
        return task_to_public(task) if task else {"id": task_id}


def collected_item_error(item: dict[str, Any]) -> str | None:
    raw = item.get("raw") if isinstance(item.get("raw"), dict) else {}
    detail_error = str(raw.get("detailError") or "").strip()
    if detail_error:
        name = str(item.get("title") or item.get("source_url") or "商品").strip()
        return f"{name}: {detail_error}"
    if raw.get("detailCollected") is False:
        name = str(item.get("title") or item.get("source_url") or "商品").strip()
        return f"{name}: 商品详情采集失败。"
    return None


def summarize_task_errors(errors: list[str], limit: int = 20) -> str | None:
    unique_errors: list[str] = []
    seen: set[str] = set()
    for error in errors:
        normalized = str(error or "").strip()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        unique_errors.append(normalized)
    if not unique_errors:
        return None
    visible_errors = unique_errors[:limit]
    if len(unique_errors) > limit:
        visible_errors.append(f"另有 {len(unique_errors) - limit} 条错误未显示。")
    return "\n".join(visible_errors)


def run_task(task_id: str) -> None:
    with session_scope() as session:
        task = session.get(CrawlTaskModel, task_id)
        if task is None:
            return
        task.status = "running"
        task.started_at = datetime.now()
        task.message = "采集中"
        owner_username = task.owner_username
        source_type = task.source_type
        target = task.target

    try:
        items = collect_items(source_type, target)
        with session_scope() as session:
            task = session.get(CrawlTaskModel, task_id)
            if task is None:
                return
            task.total_count = len(items)
            success_count = 0
            failed_count = 0
            saved_count = 0
            errors: list[str] = []
            if not items:
                errors.append("未采集到商品，请检查采集内容、榜单时间或乐天页面结构。")
            for item in items:
                item_error = collected_item_error(item)
                saved = upsert_product(session, owner_username, task_id, item)
                if saved:
                    saved_count += 1
                if saved and item_error is None:
                    success_count += 1
                    continue
                failed_count += 1
                if item_error:
                    errors.append(item_error)
                elif not saved:
                    name = str(item.get("title") or item.get("source_url") or "商品").strip()
                    errors.append(f"{name}: 商品未保存，可能缺少商品标题、商品链接，或已存在于店铺商品中。")
            task.success_count = success_count
            task.failed_count = failed_count
            if not items:
                task.status = "failed"
            else:
                task.status = "success" if failed_count == 0 else "partial"
            task.finished_at = datetime.now()
            task.message = f"完成，采集 {len(items)} 条，完整 {success_count} 条，异常 {failed_count} 条，入库 {saved_count} 条"
            task.error_detail = summarize_task_errors(errors)
        log_event(owner_username, task_id, "info", f"任务完成，完整 {success_count} 条，异常 {failed_count} 条，入库 {saved_count} 条商品")
    except Exception as exc:
        with session_scope() as session:
            task = session.get(CrawlTaskModel, task_id)
            if task is None:
                return
            task.status = "failed"
            task.failed_count = 1
            task.finished_at = datetime.now()
            task.message = "采集失败"
            task.error_detail = str(exc)
        log_event(owner_username, task_id, "error", str(exc))


def collect_items(source_type: str, target: str) -> list[dict[str, Any]]:
    if source_type == "product_url":
        return [collect_product_detail(normalize_rakuten_product_target(target))]
    limit: int | None = 30
    if source_type == "shop":
        target, limit, period = parse_ranking_target(strip_shop_ranking_prefix(target))
        target = resolve_rakuten_shop_search_keyword(target)
    elif source_type == "ranking":
        target, limit, period = parse_ranking_target(target)
    else:
        period = "daily"
    url = build_source_url(source_type, target)
    if source_type in {"ranking", "shop"}:
        url = build_ranking_source_url(target, period)
    html = fetch_html(url)
    items = parse_search_items(html, url)
    if source_type in {"ranking", "shop"} and period == "realtime":
        keyword = normalize_text(target).lower()
        items = [item for item in items if keyword in normalize_text(item.get("title")).lower()]
    limited_items = items if limit is None else items[:limit]
    return enrich_collected_items_with_detail(limited_items)


def enrich_collected_items_with_detail(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    enriched_items: list[dict[str, Any]] = []
    for item in items:
        source_url = normalize_text(item.get("source_url"))
        if not source_url:
            enriched_items.append(item)
            continue
        try:
            detail = collect_product_detail(source_url)
        except Exception as exc:
            fallback = dict(item)
            raw = fallback.get("raw") if isinstance(fallback.get("raw"), dict) else {}
            fallback["raw"] = {**raw, "detailError": str(exc), "detailCollected": False}
            enriched_items.append(fallback)
            continue
        raw = detail.get("raw") if isinstance(detail.get("raw"), dict) else {}
        list_raw = item.get("raw") if isinstance(item.get("raw"), dict) else {}
        detail["raw"] = {**raw, "listPage": list_raw.get("pageUrl"), "detailCollected": True}
        if not detail.get("price"):
            detail["price"] = item.get("price")
        if not detail.get("image_url"):
            detail["image_url"] = item.get("image_url")
        enriched_items.append(detail)
    return enriched_items


def strip_shop_ranking_prefix(target: str) -> str:
    normalized = normalize_text(target)
    if normalized.startswith("店铺:") or normalized.startswith("店铺："):
        return normalized.split(":", 1)[1] if ":" in normalized else normalized.split("：", 1)[1]
    return normalized


def parse_ranking_target(target: str) -> tuple[str, int | None, str]:
    normalized = normalize_text(target)
    limit: int | None = 30
    all_match = re.search(r"(?:^|\s)(全部|全量)\s*$", normalized)
    if all_match:
        limit = None
        normalized = normalized[: all_match.start()].strip()
    match = re.search(r"(?:^|\s)前\s*([0-9]{1,3})\s*$", normalized)
    if match:
        limit = int(match.group(1))
        normalized = normalized[: match.start()].strip()
    period = "daily"
    period_match = re.search(r"(?:^|\s)(实时|实时榜|日榜|每日|每日榜|周榜|週間|週間榜|月榜|月間|月間榜)\s*$", normalized)
    if period_match:
        period_label = period_match.group(1)
        normalized = normalized[: period_match.start()].strip()
        if period_label in {"实时", "实时榜"}:
            period = "realtime"
        elif period_label in {"周榜", "週間", "週間榜"}:
            period = "weekly"
        elif period_label in {"月榜", "月間", "月間榜"}:
            period = "monthly"
        else:
            period = "daily"
    return normalized, None if limit is None else min(100, max(1, limit)), period


def build_ranking_source_url(keyword: str, period: str) -> str:
    normalized_keyword = normalize_text(keyword)
    if period == "realtime":
        return RAKUTEN_REALTIME_RANKING_URL
    if period == "monthly":
        ptn = "3"
    elif period == "weekly":
        ptn = "2"
    else:
        ptn = "1"
    return f"{RAKUTEN_RANKING_BASE}?stx={quote(normalized_keyword)}&srt=1&ptn={ptn}"


def build_source_url(source_type: str, target: str) -> str:
    target = target.strip()
    if target.startswith("http://") or target.startswith("https://"):
        return target
    if source_type == "shop":
        if target.isdigit():
            return f"{RAKUTEN_SEARCH_BASE}?sid={quote(target)}"
        return f"{RAKUTEN_SEARCH_BASE}{quote(target)}/"
    if source_type == "ranking":
        return f"{RAKUTEN_RANKING_BASE}?stx={quote(target)}&srt=1"
    return f"{RAKUTEN_SEARCH_BASE}{quote(target)}/"


def fetch_html(url: str) -> str:
    response = requests.get(
        url,
        timeout=settings.crawler_timeout_seconds,
        headers={"User-Agent": settings.crawler_user_agent},
    )
    response.raise_for_status()
    response.encoding = response.encoding or response.apparent_encoding
    return response.text


def parse_search_items(html: str, page_url: str) -> list[dict[str, Any]]:
    soup = BeautifulSoup(html, "lxml")
    items: list[dict[str, Any]] = []
    seen: set[str] = set()
    for link in soup.select("a[href*='item.rakuten.co.jp'], a[href*='brandavenue.rakuten.co.jp/item/']"):
        href = normalize_product_href(str(link.get("href") or ""), page_url)
        title = " ".join(link.get_text(" ", strip=True).split())
        if not href or href in seen:
            continue
        seen.add(href)
        container = link.find_parent(["div", "li", "article"]) or link
        image = ""
        image_node = container.select_one("img")
        if image_node:
            image = str(image_node.get("src") or image_node.get("data-src") or "")
        if not title:
            title = normalize_text(image_node.get("alt") if image_node else "")
        price = extract_price(container.get_text(" ", strip=True))
        items.append(
            {
                "title": (title or href)[:500],
                "source_url": href,
                "image_url": image,
                "price": price,
                "shop_name": "",
                "item_number": extract_item_number(href),
                "genre_id": "",
                "raw": {"pageUrl": page_url},
            }
        )
    return items


def normalize_product_href(href: str, page_url: str) -> str:
    normalized = normalize_text(href)
    if not normalized:
        return ""
    absolute = urljoin(page_url, normalized)
    if is_rakuten_product_url(absolute):
        try:
            return normalize_rakuten_product_target(absolute)
        except RuntimeError:
            return absolute.split("?", 1)[0]
    return ""


def collect_product_detail(url: str) -> dict[str, Any]:
    normalized_url = normalize_rakuten_product_target(url)
    html = fetch_html(normalized_url)
    soup = BeautifulSoup(html, "lxml")
    if parse_rakuten_fashion_product_code(normalized_url):
        return collect_rakuten_fashion_product_detail(normalized_url, html, soup)
    return collect_rakuten_market_product_detail(normalized_url, html, soup)


def collect_rakuten_market_product_detail(normalized_url: str, html: str, soup: BeautifulSoup) -> dict[str, Any]:
    if is_blocked_or_empty_rakuten_html(html):
        raise RuntimeError("乐天商品详情页返回拦截页，无法通过后端 HTTP 直接采集。")
    json_ld = extract_json_ld_objects(soup)
    product_json = first_json_ld_by_type(json_ld, "Product")
    embedded_item = extract_rakuten_market_item_info(soup)
    breadcrumbs = extract_breadcrumbs_from_json_ld(json_ld)
    if isinstance(embedded_item.get("breadcrumbs"), list):
        breadcrumbs = embedded_item["breadcrumbs"] or breadcrumbs
    meta = extract_page_meta(soup)
    json_title = first_text_from_keys(product_json, ("name", "itemName", "title")) if product_json else ""
    title = (
        first_text_from_keys(embedded_item, ("title", "itemName", "name"))
        or json_title
        or first_meta_text(meta, "og:title")
        or page_title(soup)
    )
    parsed_target = parse_rakuten_product_target(normalized_url)
    shop_code = parsed_target[0] if parsed_target else ""
    item_number = extract_item_number(normalized_url)
    image_urls = market_item_image_urls(embedded_item, shop_code=shop_code, item_number=item_number)
    if not image_urls and product_json:
        image_urls = product_image_urls(product_json)
    if not image_urls:
        image_urls = extract_image_urls_from_soup(soup, shop_code=shop_code, item_number=item_number)
    image_urls = unique_texts(image_urls)
    descriptions = market_product_descriptions(product_json, soup, embedded_item)
    offers = product_json.get("offers") if isinstance(product_json, dict) else None
    variants = market_item_variants(embedded_item) or variants_from_json_ld_offers(offers)
    price = price_from_rakuten_item({"variants": variants}) or price_from_rakuten_item(embedded_item)
    if price is None and isinstance(product_json, dict):
        price = price_from_rakuten_item(product_json)
    if price is None:
        price = extract_price(soup.get_text(" ", strip=True))
    if not title or title == normalized_url:
        raise RuntimeError("未能从乐天商品详情页解析到商品标题，可能页面被拦截或页面模板不支持。")
    if not image_urls and price is None and not descriptions:
        raise RuntimeError("未能从乐天商品详情页解析到有效商品数据，可能页面被拦截或页面模板不支持。")
    raw = {
        "sourceType": "rakuten_market_public",
        "url": normalized_url,
        "canonicalUrl": canonical_url(soup) or normalized_url,
        "title": title,
        "name": title,
        "itemName": title,
        "itemNumber": extract_item_number(normalized_url),
        "manageNumber": first_text_from_keys(embedded_item, ("manageNumber",)) or item_number,
        "shopCode": shop_code,
        "shopName": infer_market_shop_name(soup, embedded_item, shop_code=shop_code),
        "genreId": first_text_from_keys(embedded_item, ("rCategoryId", "genreId")),
        "price": price,
        "standardPrice": price,
        "images": image_urls,
        "productDescription": descriptions[0]["value"] if descriptions else "",
        "descriptions": descriptions,
        "variantSelectors": market_variant_selectors(embedded_item) or variant_selectors_from_variants(variants),
        "variants": variants,
        "embeddedItem": embedded_item,
        "jsonLd": json_ld,
        "breadcrumbs": breadcrumbs,
        "meta": meta,
        "collectedAt": datetime.now().isoformat(timespec="seconds"),
    }
    return {
        "title": title[:500] or normalized_url,
        "source_url": normalized_url,
        "image_url": image_urls[0] if image_urls else "",
        "price": price,
        "shop_name": raw["shopName"],
        "item_number": raw["itemNumber"],
        "genre_id": raw["genreId"],
        "raw": raw,
    }


def collect_rakuten_fashion_product_detail(normalized_url: str, html: str, soup: BeautifulSoup) -> dict[str, Any]:
    json_ld = extract_json_ld_objects(soup)
    product_json = first_json_ld_by_type(json_ld, "Product")
    breadcrumbs = extract_breadcrumbs_from_json_ld(json_ld)
    meta = extract_page_meta(soup)
    state = extract_initial_state(html)
    product = {}
    if isinstance(state.get("itemDetail"), dict):
        item_data = state["itemDetail"].get("data")
        if isinstance(item_data, dict) and isinstance(item_data.get("product"), dict):
            product = item_data["product"]
    brand_info = state.get("brandInfo", {}).get("data") if isinstance(state.get("brandInfo"), dict) else None
    brand_info = brand_info if isinstance(brand_info, dict) else {}
    model_code = first_text_from_keys(product, ("model_cd",)) or parse_rakuten_fashion_product_code(normalized_url)
    title = (
        first_text_from_keys(product, ("product_name", "itemName", "title", "name"))
        or first_text_from_keys(product_json, ("name",)) if isinstance(product_json, dict) else ""
    ) or page_title(soup)
    image_urls = rakuten_fashion_image_urls(product)
    if isinstance(product_json, dict):
        image_urls.extend(product_image_urls(product_json))
    image_urls.extend(extract_image_urls_from_soup(soup))
    image_urls = unique_texts(image_urls)
    descriptions = rakuten_fashion_descriptions(product, brand_info, product_json)
    variants = rakuten_fashion_variants(product)
    price = (
        numeric_price(first_text_from_keys(product, ("selling_price_no_format", "selling_price")))
        or price_from_rakuten_item({"variants": variants})
        or price_from_rakuten_item(product)
        or extract_price(soup.get_text(" ", strip=True))
    )
    genre_id = first_text_from_keys(product.get("rms_info", {}) if isinstance(product.get("rms_info"), dict) else {}, ("genre_id", "genreId"))
    raw = {
        "sourceType": "rakuten_fashion_public",
        "url": normalized_url,
        "canonicalUrl": canonical_url(soup) or normalized_url,
        "title": title,
        "name": title,
        "itemName": title,
        "modelCode": model_code,
        "itemNumber": model_code,
        "manageNumber": model_code,
        "brandNo": first_text_from_keys(product, ("brand_no",)),
        "externalCode": first_text_from_keys(product, ("external_cd",)),
        "brand": first_text_from_keys(product, ("brand_name",)) or first_text_from_keys(brand_info, ("brand_name",)),
        "brandKana": first_text_from_keys(product, ("brand_name_kana",)) or first_text_from_keys(brand_info, ("brand_name_kana",)),
        "makerName": first_text_from_keys(product, ("maker_name",)),
        "shopName": first_text_from_keys(product, ("site_name",)) or "Rakuten Fashion",
        "genreId": genre_id,
        "categoryLName": first_text_from_keys(product, ("category_l_name",)),
        "categoryMName": first_text_from_keys(product, ("category_m_name",)),
        "categoryLCode": first_text_from_keys(product, ("category_l_cd",)),
        "categoryMCode": first_text_from_keys(product, ("category_m_cd",)),
        "price": price,
        "fixedPrice": first_text_from_keys(product, ("fixed_price_no_format", "fixed_price")),
        "sellingPrice": first_text_from_keys(product, ("selling_price_no_format", "selling_price")),
        "discountRate": product.get("discount_rate"),
        "currency": "JPY",
        "images": image_urls,
        "productDescription": {"pc": first_text_from_keys(product, ("product_exp",))},
        "descriptions": descriptions,
        "variantSelectors": variant_selectors_from_variants(variants),
        "variants": variants,
        "inventory": product.get("rms_info", {}).get("inventory_list") if isinstance(product.get("rms_info"), dict) else [],
        "favoriteCount": first_text_from_keys(product, ("favorite_count",)),
        "saleStatus": product.get("sale_status"),
        "saleComment": first_text_from_keys(product, ("sale_comment",)),
        "soldout": product.get("soldout_flg"),
        "soldoutPart": product.get("soldout_part_flg"),
        "preorder": product.get("preorder_flg"),
        "material": rakuten_fashion_first_sku_value(product, "material"),
        "origin": first_text_from_keys(product, ("natives",)),
        "rmsInfo": product.get("rms_info") if isinstance(product.get("rms_info"), dict) else {},
        "coupons": product.get("coupon_list") if isinstance(product.get("coupon_list"), list) else [],
        "brandInfo": brand_info,
        "jsonLd": json_ld,
        "breadcrumbs": breadcrumbs,
        "meta": meta,
        "sourceProduct": product,
        "collectedAt": datetime.now().isoformat(timespec="seconds"),
    }
    return {
        "title": title[:500] or normalized_url,
        "source_url": normalized_url,
        "image_url": image_urls[0] if image_urls else "",
        "price": price,
        "shop_name": raw["brand"] or raw["shopName"],
        "item_number": model_code,
        "genre_id": genre_id,
        "raw": raw,
    }


def extract_initial_state(html: str) -> dict[str, Any]:
    match = re.search(r"window\.__INITIAL_STATE__\s*=\s*(\{.*?\});\s*\nwindow\.__REWIRED_SCHEMAS__", html, re.S)
    if not match:
        return {}
    try:
        state = json.loads(match.group(1))
    except ValueError:
        return {}
    return state if isinstance(state, dict) else {}


def is_blocked_or_empty_rakuten_html(html: str) -> bool:
    text = normalize_text(BeautifulSoup(html or "", "lxml").get_text(" ", strip=True))
    if not text:
        return True
    if len(html or "") < 300 and re.fullmatch(r"Reference\s+#.+", text):
        return True
    return False


def extract_rakuten_market_item_info(soup: BeautifulSoup) -> dict[str, Any]:
    for script in soup.find_all("script"):
        text = script.string or script.get_text() or ""
        if '"itemInfoSku"' not in text or '"variantSelectors"' not in text:
            continue
        stripped = text.strip()
        if not stripped.startswith("{"):
            continue
        try:
            payload = json.loads(stripped)
        except ValueError:
            continue
        for path in (("api", "data", "itemInfoSku"), ("newApi", "itemInfoSku")):
            value: Any = payload
            for key in path:
                if not isinstance(value, dict):
                    value = None
                    break
                value = value.get(key)
            if isinstance(value, dict):
                return value
    return {}


def extract_json_ld_objects(soup: BeautifulSoup) -> list[dict[str, Any]]:
    objects: list[dict[str, Any]] = []
    for script in soup.find_all("script", type="application/ld+json"):
        text = script.string or script.get_text() or ""
        try:
            value = json.loads(text)
        except ValueError:
            continue
        for item in flatten_json_ld(value):
            if isinstance(item, dict):
                objects.append(item)
    return objects


def flatten_json_ld(value: Any) -> list[Any]:
    if isinstance(value, list):
        result: list[Any] = []
        for item in value:
            result.extend(flatten_json_ld(item))
        return result
    if isinstance(value, dict) and isinstance(value.get("@graph"), list):
        return flatten_json_ld(value.get("@graph"))
    return [value]


def first_json_ld_by_type(objects: list[dict[str, Any]], target_type: str) -> dict[str, Any]:
    for item in objects:
        item_type = item.get("@type")
        if isinstance(item_type, list) and target_type in item_type:
            return item
        if item_type == target_type:
            return item
    return {}


def extract_breadcrumbs_from_json_ld(objects: list[dict[str, Any]]) -> list[dict[str, str]]:
    breadcrumb = first_json_ld_by_type(objects, "BreadcrumbList")
    values = breadcrumb.get("itemListElement") if isinstance(breadcrumb, dict) else None
    result: list[dict[str, str]] = []
    if not isinstance(values, list):
        return result
    for item in values:
        if not isinstance(item, dict):
            continue
        child = item.get("item") if isinstance(item.get("item"), dict) else {}
        result.append(
            {
                "name": first_text_from_keys(child, ("name",)) or first_text_from_keys(item, ("name",)),
                "url": first_text_from_keys(child, ("@id", "url")),
            }
        )
    return [item for item in result if item.get("name") or item.get("url")]


def extract_page_meta(soup: BeautifulSoup) -> dict[str, str]:
    meta: dict[str, str] = {}
    for node in soup.select("meta"):
        key = normalize_text(node.get("property") or node.get("name"))
        content = normalize_text(node.get("content"))
        if key and content and key not in meta:
            meta[key] = content
    return meta


def first_meta_text(meta: dict[str, str], key: str) -> str:
    return normalize_text(meta.get(key))


def page_title(soup: BeautifulSoup) -> str:
    title_node = soup.select_one("h1") or soup.select_one("title")
    return normalize_text(title_node.get_text(" ", strip=True) if title_node else "")


def canonical_url(soup: BeautifulSoup) -> str:
    node = soup.find("link", rel="canonical")
    return normalize_text(node.get("href") if node else "")


def infer_market_shop_name(soup: BeautifulSoup, embedded_item: dict[str, Any] | None = None, shop_code: str = "") -> str:
    if embedded_item:
        shop_status = embedded_item.get("shopStatus")
        if isinstance(shop_status, dict):
            name = first_text_from_keys(shop_status, ("shopName", "name"))
            if name:
                return name
    if soup.title:
        title = normalize_text(soup.title.get_text(" ", strip=True))
        if "：" in title:
            candidate = title.rsplit("：", 1)[-1].strip()
            if candidate and candidate != "楽天市場":
                return candidate
    for selector in ("meta[property='og:site_name']", "#shopName", ".shopName"):
        node = soup.select_one(selector)
        value = normalize_text(node.get("content") if node and node.name == "meta" else node.get_text(" ", strip=True) if node else "")
        if value and value != "楽天市場":
            return value
    return normalize_shop_code(shop_code)


def market_product_descriptions(
    product_json: dict[str, Any],
    soup: BeautifulSoup,
    embedded_item: dict[str, Any] | None = None,
) -> list[dict[str, str]]:
    descriptions: list[dict[str, str]] = []
    description = first_text_from_keys(product_json, ("description",)) if isinstance(product_json, dict) else ""
    if description:
        descriptions.append({"label": "商品说明", "value": description})
    if embedded_item:
        embedded_descriptions = (
            ("商品说明", embedded_item.get("newProductDescription")),
            ("PC商品说明", (embedded_item.get("pcFields") or {}).get("productDescription") if isinstance(embedded_item.get("pcFields"), dict) else None),
            ("销售说明", embedded_item.get("salesDescription")),
        )
        for label, value in embedded_descriptions:
            html_value = normalize_detail_html(value)
            if html_value and all(item["value"] != html_value for item in descriptions):
                descriptions.append({"label": label, "value": html_value})
    for selector, label in (
        ("#itemCaption", "商品说明"),
        ("#itemDescription", "商品详情"),
        ("[class*='description']", "商品说明"),
    ):
        node = soup.select_one(selector)
        if node:
            html_value = normalize_detail_html(str(node))
            if html_value and all(item["value"] != html_value for item in descriptions):
                descriptions.append({"label": label, "value": html_value})
    return descriptions


def market_item_image_urls(item: dict[str, Any], *, shop_code: str, item_number: str) -> list[str]:
    urls: list[str] = []
    media = item.get("media") if isinstance(item.get("media"), dict) else {}
    pc_fields = item.get("pcFields") if isinstance(item.get("pcFields"), dict) else {}

    def collect(value: Any) -> None:
        if isinstance(value, str):
            url = normalize_product_image_url(value, shop_code=shop_code)
            if is_relevant_market_item_image(url, shop_code=shop_code, item_number=item_number) and url not in urls:
                urls.append(url)
            return
        if isinstance(value, dict):
            for key in ("location", "url", "imageUrl", "src"):
                collect(value.get(key))
            for child in value.values():
                collect(child)
            return
        if isinstance(value, list):
            for child in value:
                collect(child)

    collect(pc_fields.get("images"))
    collect(media.get("images"))
    collect(media.get("skuImages"))
    collect(item.get("picImageUrl"))
    for sku in item.get("sku") if isinstance(item.get("sku"), list) else []:
        if isinstance(sku, dict):
            collect(sku.get("images"))
    for description in (item.get("newProductDescription"), item.get("salesDescription")):
        for match in re.findall(r"https?://[^\s\"'<>)]*\.(?:apng|avif|bmp|gif|jpe?g|png|webp)(?:\?[^\"'<>)]*)?", str(description or ""), flags=re.I):
            collect(match)
    return urls


def is_relevant_market_item_image(url: str, *, shop_code: str, item_number: str) -> bool:
    if not url:
        return False
    try:
        parsed = urlsplit(url)
    except Exception:
        return False
    host = parsed.netloc.lower()
    path = parsed.path.lower()
    normalized_shop = normalize_shop_code(shop_code).lower()
    if host == "r.r10s.jp":
        return False
    if normalized_shop and normalized_shop not in path and normalized_shop not in host:
        return False
    item_tokens = item_number_image_tokens(item_number)
    if item_tokens:
        return any(token in path for token in item_tokens)
    if "/cabinet/" in path and normalized_shop and (normalized_shop in path or normalized_shop in host):
        return True
    if host in {"image.rakuten.co.jp", "tshop.r10s.jp", "cabinet.rms.rakuten.co.jp"}:
        return True
    return False


def item_number_image_tokens(item_number: str) -> list[str]:
    normalized = normalize_text(item_number).lower()
    tokens = [normalized] if normalized else []
    for part in re.split(r"[^a-z0-9]+", normalized):
        if len(part) >= 4 and part not in tokens:
            tokens.append(part)
    return tokens


def market_item_variants(item: dict[str, Any]) -> dict[str, dict[str, Any]]:
    skus = item.get("sku")
    if not isinstance(skus, list):
        return {}
    selectors = item.get("variantSelectors") if isinstance(item.get("variantSelectors"), list) else []
    purchase_info = item.get("purchaseInfo") if isinstance(item.get("purchaseInfo"), dict) else {}
    purchase_skus = purchase_info.get("sku") if isinstance(purchase_info.get("sku"), list) else []
    purchase_by_variant = {
        first_text_from_keys(row, ("variantId",)): row for row in purchase_skus if isinstance(row, dict)
    }
    inventories = purchase_info.get("variantMappedInventories") if isinstance(purchase_info.get("variantMappedInventories"), list) else []
    inventory_by_variant = {
        first_text_from_keys(row, ("sku",)): row for row in inventories if isinstance(row, dict)
    }
    result: dict[str, dict[str, Any]] = {}
    for index, sku in enumerate(skus, start=1):
        if not isinstance(sku, dict):
            continue
        variant_id = first_text_from_keys(sku, ("variantId",)) or f"sku-{index}"
        purchase_row = purchase_by_variant.get(variant_id, {})
        inventory_row = inventory_by_variant.get(variant_id, {})
        purchase_sku = purchase_row.get("newPurchaseSku") if isinstance(purchase_row.get("newPurchaseSku"), dict) else {}
        selector_values = market_selector_values(sku.get("selectorValues"), selectors)
        price = (
            first_text_from_keys(sku, ("taxIncludedPrice", "standardPrice", "price"))
            or first_text_from_keys(purchase_row, ("taxIncludedPrice", "standardPrice", "price"))
        )
        result[variant_id] = {
            "variantId": variant_id,
            "merchantDefinedSkuId": first_text_from_keys(sku, ("merchantDefinedSkuId",)),
            "articleNumber": first_text_value(sku.get("articleNumber")),
            "standardPrice": price,
            "hidden": bool(sku.get("hidden")),
            "selectorValues": selector_values,
            "specs": market_named_values(sku.get("specs")),
            "attributes": market_named_values(sku.get("attributes")),
            "inventoryId": first_text_from_keys(inventory_row, ("inventoryId",)),
            "material": first_attribute_value(sku.get("attributes"), ("素材", "素材（生地・毛糸）")),
            "images": product_image_urls({"images": sku.get("images")}),
            "referencePrice": first_text_from_keys(sku.get("referencePrice", {}) if isinstance(sku.get("referencePrice"), dict) else {}, ("value",)),
        }
    return result


def market_variant_selectors(item: dict[str, Any]) -> list[dict[str, Any]]:
    selectors = item.get("variantSelectors") if isinstance(item.get("variantSelectors"), list) else []
    result: list[dict[str, Any]] = []
    for index, selector in enumerate(selectors, start=1):
        if not isinstance(selector, dict):
            continue
        key = first_text_from_keys(selector, ("key",)) or f"k{index}"
        result.append(
            {
                "key": key,
                "name": first_text_from_keys(selector, ("label", "name", "displayName")) or key,
                "values": selector_values_to_public(selector.get("values")),
            }
        )
    return result


def market_selector_values(values: Any, selectors: list[Any]) -> dict[str, str]:
    result: dict[str, str] = {}
    if not isinstance(values, list):
        return result
    for index, value in enumerate(values):
        selector = selectors[index] if index < len(selectors) and isinstance(selectors[index], dict) else {}
        key = first_text_from_keys(selector, ("key",)) or f"k{index + 1}"
        result[key] = first_text_value(value)
    return result


def market_named_values(values: Any) -> list[dict[str, str]]:
    if not isinstance(values, list):
        return []
    result: list[dict[str, str]] = []
    for item in values:
        if not isinstance(item, dict):
            continue
        name = first_text_from_keys(item, ("title", "name", "label"))
        value = first_text_from_keys(item, ("value", "text"))
        if name or value:
            result.append({"name": name, "value": value})
    return result


def first_attribute_value(values: Any, labels: tuple[str, ...]) -> str:
    if not isinstance(values, list):
        return ""
    for item in values:
        if not isinstance(item, dict):
            continue
        title = first_text_from_keys(item, ("title", "name", "label"))
        if any(label in title for label in labels):
            return first_text_from_keys(item, ("value", "text"))
    return ""


def variants_from_json_ld_offers(offers: Any) -> dict[str, dict[str, Any]]:
    offer_items = offers if isinstance(offers, list) else [offers] if isinstance(offers, dict) else []
    variants: dict[str, dict[str, Any]] = {}
    for index, offer in enumerate(offer_items, start=1):
        if not isinstance(offer, dict):
            continue
        sku = first_text_from_keys(offer, ("sku", "mpn")) or f"sku-{index}"
        variants[sku] = {
            "variantId": sku,
            "merchantDefinedSkuId": sku,
            "standardPrice": first_text_from_keys(offer, ("price",)),
            "hidden": "OutOfStock" in first_text_from_keys(offer, ("availability",)),
            "selectorValues": {},
            "images": [first_text_from_keys(offer, ("image",))] if first_text_from_keys(offer, ("image",)) else [],
            "availability": first_text_from_keys(offer, ("availability",)),
            "itemCondition": first_text_from_keys(offer, ("itemCondition",)),
        }
    return variants


def rakuten_fashion_image_urls(product: dict[str, Any]) -> list[str]:
    filenames: list[str] = []

    def remember_filename(value: Any) -> None:
        text = normalize_text(value)
        if not text or not re.search(r"\.(?:jpe?g|png|webp|gif)$", text, flags=re.I):
            return
        if text not in filenames:
            filenames.append(text)

    for key in ("product_img_path",):
        remember_filename(product.get(key))
    sku_images = product.get("product_sku_img_path")
    if isinstance(sku_images, dict):
        for value in sku_images.values():
            remember_filename(value)
    sku_sub_images = product.get("product_sku_img_path_sub")
    if isinstance(sku_sub_images, list):
        for value in sku_sub_images:
            remember_filename(value)
    model_info = product.get("product_sku_img_model_info")
    if isinstance(model_info, dict):
        for key in model_info.keys():
            remember_filename(key)
    return unique_texts([rakuten_fashion_image_url(filename) for filename in filenames])


def rakuten_fashion_image_url(filename: str) -> str:
    normalized = normalize_text(filename).lower()
    match = re.search(r"([a-z0-9]+)-", normalized)
    directory = ""
    if match:
        code = match.group(1)
        directory = code[-3:]
    if not directory:
        directory = "000"
    return f"{RAKUTEN_FASHION_IMAGE_BASE}/{directory}/{normalized}"


def rakuten_fashion_descriptions(
    product: dict[str, Any],
    brand_info: dict[str, Any],
    product_json: dict[str, Any] | None,
) -> list[dict[str, str]]:
    descriptions: list[dict[str, str]] = []
    product_exp = first_text_from_keys(product, ("product_exp",))
    if product_exp:
        descriptions.append({"label": "商品说明", "value": normalize_detail_html(product_exp)})
    json_description = first_text_from_keys(product_json or {}, ("description",))
    if json_description and all(item["value"] != json_description for item in descriptions):
        descriptions.append({"label": "结构化商品说明", "value": json_description})
    brand_exp = first_text_from_keys(brand_info, ("brand_exp",))
    if brand_exp:
        descriptions.append({"label": "品牌说明", "value": normalize_detail_html(brand_exp)})
    return descriptions


def rakuten_fashion_variants(product: dict[str, Any]) -> dict[str, dict[str, Any]]:
    skus = product.get("product_sku")
    if not isinstance(skus, list):
        return {}
    variants: dict[str, dict[str, Any]] = {}
    for index, sku in enumerate(skus, start=1):
        if not isinstance(sku, dict):
            continue
        inventory = sku.get("inventory_info") if isinstance(sku.get("inventory_info"), dict) else {}
        variant_id = first_text_from_keys(sku, ("inventory_id",)) or first_text_from_keys(inventory, ("variant_id", "inventory_id")) or f"sku-{index}"
        color = first_text_from_keys(sku, ("rms_v_choise_name", "product_color_name")) or first_text_from_keys(inventory, ("color_name",))
        size = first_text_from_keys(sku, ("rms_h_choise_name", "product_size_name")) or first_text_from_keys(inventory, ("size",))
        image_path = first_text_from_keys(sku, ("product_img_path",))
        variants[variant_id] = {
            "variantId": variant_id,
            "merchantDefinedSkuId": variant_id,
            "articleNumber": "",
            "standardPrice": first_text_from_keys(sku, ("selling_price_with_tax", "selling_price_tax_included", "tax_included_selling_price"))
            or first_text_from_keys(product, ("selling_price_no_format",))
            or first_text_from_keys(sku, ("selling_price", "fixed_price")),
            "displayPrice": first_text_from_keys(product, ("selling_price",)) or first_text_from_keys(sku, ("selling_price",)),
            "fixedPrice": first_text_from_keys(sku, ("fixed_price",)),
            "hidden": first_text_from_keys(sku, ("inventory_exist_flg",)) == "0",
            "selectorValues": {"color": color, "size": size},
            "specs": [
                {"name": "素材", "value": first_text_from_keys(sku, ("material",))},
                {"name": "発送予定", "value": first_text_from_keys(sku, ("inventory_status_message",))},
            ],
            "attributes": [
                {"name": "颜色代码", "value": first_text_from_keys(sku, ("product_color_cd",))},
            ],
            "inventoryId": first_text_from_keys(sku, ("inventory_id",)),
            "material": first_text_from_keys(sku, ("material",)),
            "images": [rakuten_fashion_image_url(image_path)] if image_path else [],
        }
    return variants


def rakuten_fashion_first_sku_value(product: dict[str, Any], key: str) -> str:
    skus = product.get("product_sku")
    if not isinstance(skus, list):
        return ""
    for sku in skus:
        if isinstance(sku, dict):
            value = first_text_from_keys(sku, (key,))
            if value:
                return value
    return ""


def variant_selectors_from_variants(variants: Any) -> list[dict[str, Any]]:
    variant_values = variants.values() if isinstance(variants, dict) else variants if isinstance(variants, list) else []
    selectors: dict[str, list[str]] = {}
    for variant in variant_values:
        if not isinstance(variant, dict):
            continue
        selector_values = variant.get("selectorValues")
        if not isinstance(selector_values, dict):
            continue
        for key, value in selector_values.items():
            text = normalize_text(value)
            if not text:
                continue
            selectors.setdefault(str(key), [])
            if text not in selectors[str(key)]:
                selectors[str(key)].append(text)
    return [{"key": key, "name": selector_display_name(key), "values": values} for key, values in selectors.items()]


def selector_display_name(key: str) -> str:
    return {"color": "颜色", "size": "尺码"}.get(key, key)


def extract_image_urls_from_soup(soup: BeautifulSoup) -> list[str]:
    urls: list[str] = []
    for node in soup.select("img"):
        for attr in ("src", "data-src", "data-original", "data-lazy-src"):
            value = node.get(attr)
            if value:
                normalized = normalize_product_image_url(value)
                if normalized:
                    urls.append(normalized)
    return unique_texts(urls)


def unique_texts(values: list[Any]) -> list[str]:
    result: list[str] = []
    for value in values:
        text = normalize_text(value)
        if text and text not in result:
            result.append(text)
    return result


def normalize_detail_html(value: Any) -> str:
    text = str(value or "").replace("\\/", "/")
    text = unescape(text)
    text = re.sub(r"<script[\s\S]*?</script>", "", text, flags=re.I)
    text = re.sub(r"<style[\s\S]*?</style>", "", text, flags=re.I)
    return text.strip()


def numeric_price(value: Any) -> float | None:
    text = first_text_value(value)
    if not text:
        return None
    normalized = re.sub(r"[^0-9.]", "", text)
    if not normalized:
        return None
    try:
        return float(normalized)
    except ValueError:
        return None


def extract_price(text: str) -> float | None:
    matches = re.findall(r"([0-9][0-9,]{2,})\s*円", text)
    if not matches:
        matches = re.findall(r"￥\s*([0-9][0-9,]{2,})", text)
    if not matches:
        return None
    try:
        return float(matches[0].replace(",", ""))
    except ValueError:
        return None


def extract_item_number(url: str) -> str:
    parsed_target = parse_rakuten_product_target(url)
    if parsed_target is not None:
        return parsed_target[1][:255]
    parts = [part for part in url.rstrip("/").split("/") if part]
    return parts[-1][:255] if parts else ""


def upsert_store_product(session: Any, owner_username: str, store: StoreModel, item: dict[str, Any]) -> bool:
    item_number = first_text_from_keys(item, ("itemNumber", "manageNumber"))
    manage_number = first_text_from_keys(item, ("manageNumber", "itemNumber"))
    source_url = (
        first_url_from_keys(item, ("itemUrl", "itemPageUrl", "url"))
        or build_public_item_page_url(store.store_code, item_number or manage_number)
    )
    title = first_text_from_keys(item, ("itemName", "title", "name"))
    normalized = {
        "title": title,
        "source_url": source_url,
        "image_url": first_rakuten_image_url(item, store.store_code),
        "price": price_from_rakuten_item(item),
        "shop_name": store.store_name,
        "item_number": item_number or manage_number,
        "rakuten_manage_number": manage_number,
        "rakuten_listing_status": rakuten_listing_status_from_item(item),
        "genre_id": first_text_from_keys(item, ("genreId", "genre_id", "genre")),
        "raw": item,
    }
    return upsert_product(session, owner_username, None, normalized, review_status="listed", store_id=store.id)


def mark_missing_store_products_removed(session: Any, store: StoreModel, seen_manage_numbers: set[str]) -> None:
    if not seen_manage_numbers:
        return
    rows = session.scalars(
        select(ProductModel).where(
            ProductModel.store_id == store.id,
            ProductModel.review_status == "listed",
            ProductModel.rakuten_manage_number.is_not(None),
            ProductModel.rakuten_manage_number.not_in(seen_manage_numbers),
        )
    ).all()
    for row in rows:
        row.store_product_status = "removed"
        row.last_error = "本次更新未从乐天店铺后台返回，可能已在乐天下架或删除。"


def upsert_product(
    session: Any,
    owner_username: str,
    task_id: str | None,
    item: dict[str, Any],
    *,
    review_status: str = "pending",
    store_id: int | None = None,
) -> bool:
    source_url = str(item.get("source_url") or "").strip()
    title = str(item.get("title") or "").strip()
    if not source_url or not title:
        return False
    source_url_hash = make_source_url_hash(source_url)
    rakuten_manage_number = str(item.get("rakuten_manage_number") or "").strip() or None
    row = None
    if store_id is not None and rakuten_manage_number:
        row = session.scalar(
            select(ProductModel).where(
                ProductModel.store_id == store_id,
                ProductModel.rakuten_manage_number == rakuten_manage_number,
            )
        )
    if row is None:
        row = session.scalar(
            select(ProductModel).where(
                ProductModel.owner_username == owner_username,
                ProductModel.source_url_hash == source_url_hash,
            )
        )
    if row is None:
        row = ProductModel(owner_username=owner_username, source_url=source_url, source_url_hash=source_url_hash)
        session.add(row)
    elif store_id is None and row.store_id is not None and row.review_status == "listed":
        return False
    row.source_url = source_url
    row.source_url_hash = source_url_hash
    row.task_id = task_id
    row.store_id = store_id
    row.rakuten_manage_number = rakuten_manage_number
    row.rakuten_listing_status = str(item.get("rakuten_listing_status") or row.rakuten_listing_status or "")
    row.title = title[:500]
    row.image_url = str(item.get("image_url") or "")
    row.item_number = str(item.get("item_number") or "")
    row.shop_name = str(item.get("shop_name") or "")
    row.genre_id = str(item.get("genre_id") or "")
    price = item.get("price")
    row.price = Decimal(str(price)) if price is not None else None
    row.currency = "JPY"
    row.review_status = review_status
    if store_id is not None and review_status == "listed":
        row.store_product_status = "active"
        row.store_last_seen_at = datetime.now()
    raw_payload = item.get("raw") or item
    row.listed_at = parse_rakuten_datetime_value(raw_payload.get("created") if isinstance(raw_payload, dict) else None) or row.listed_at
    row.raw_payload_json = json.dumps(raw_payload, ensure_ascii=False)
    row.last_error = None
    return True
