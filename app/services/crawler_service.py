from __future__ import annotations

import json
import re
import uuid
import base64
import hashlib
import mimetypes
import random
import shutil
import xml.etree.ElementTree as ET
import time
import threading
from datetime import datetime, timedelta
from decimal import Decimal
from html import unescape
from io import BytesIO
from pathlib import Path
from typing import Any, Callable
from urllib.parse import parse_qs, quote, unquote, urlencode, urljoin, urlsplit, urlunsplit

import requests
from bs4 import BeautifulSoup, Comment
from sqlalchemy import and_, func, or_, select
from sqlalchemy.exc import OperationalError

from app.core.config import settings
from app.core.secure_storage import decrypt_text, encrypt_text, mask_secret
from app.core.task_queue import enqueue_task
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
RAKUTEN_CABINET_FILE_INSERT_URL = "https://api.rms.rakuten.co.jp/es/1.0/cabinet/file/insert"
RAKUTEN_CABINET_FOLDERS_GET_URL = "https://api.rms.rakuten.co.jp/es/1.0/cabinet/folders/get"
RAKUTEN_CABINET_FOLDER_INSERT_URL = "https://api.rms.rakuten.co.jp/es/1.0/cabinet/folder/insert"
RAKUTEN_INVENTORY_BULK_UPSERT_URL = "https://api.rms.rakuten.co.jp/es/2.1/inventories/bulk-upsert"
RAKUTEN_ITEM_SEARCH_HITS = 100
RAKUTEN_ITEM_SEARCH_MAX_RETRIES = 4
RAKUTEN_WRITE_MAX_RETRIES = 3
RAKUTEN_WRITE_RETRY_STATUS_CODES = {429, 500, 502, 503, 504}
RAKUTEN_INVENTORY_BULK_UPSERT_LIMIT = 400
LOCAL_PRODUCT_IMAGE_URL_PREFIX = "/api/static/product-images"
LOCAL_PRODUCT_IMAGE_DIR = settings.backend_dir / "data" / "product-images"
LOCAL_PRODUCT_IMAGE_DRAFT_URL_PREFIX = "/api/static/product-image-drafts"
LOCAL_PRODUCT_IMAGE_DRAFT_DIR = settings.backend_dir / "data" / "product-image-drafts"
ALLOWED_PRODUCT_IMAGE_EXTENSIONS = {".jpg", ".jpeg", ".png", ".gif"}
ALLOWED_PRODUCT_IMAGE_MIME_TYPES = {"image/jpeg", "image/png", "image/gif"}
MAX_PRODUCT_IMAGE_BYTES = 2 * 1024 * 1024
MAX_PRODUCT_IMAGE_DOWNLOAD_BYTES = 20 * 1024 * 1024
RAKUTEN_CABINET_MAX_IMAGE_BYTES = MAX_PRODUCT_IMAGE_BYTES
RAKUTEN_CABINET_MAX_IMAGE_DIMENSION = 3840
RAKUTEN_LISTING_IMAGE_LIMIT = 20
RAKUTEN_SP_DESCRIPTION_IMAGE_LIMIT = 20
RAKUTEN_SP_DESCRIPTION_ALLOWED_TAGS = {
    "a",
    "b",
    "br",
    "center",
    "font",
    "img",
    "p",
    "table",
    "td",
    "th",
    "tr",
}
RAKUTEN_SP_DESCRIPTION_ALLOWED_ATTRIBUTES = {
    "*": {"align"},
    "a": {"href"},
    "font": {"color", "face", "size"},
    "img": {"alt", "border", "height", "src", "width"},
    "table": {"bgcolor", "border", "cellpadding", "cellspacing", "height", "width"},
    "td": {"align", "bgcolor", "colspan", "height", "rowspan", "valign", "width"},
    "th": {"align", "bgcolor", "colspan", "height", "rowspan", "valign", "width"},
    "tr": {"align", "bgcolor", "valign"},
}
RAKUTEN_SP_DESCRIPTION_DROP_TAGS = {
    "audio",
    "button",
    "canvas",
    "embed",
    "form",
    "iframe",
    "input",
    "link",
    "map",
    "meta",
    "object",
    "script",
    "select",
    "source",
    "style",
    "svg",
    "textarea",
    "video",
}
RAKUTEN_CABINET_FOLDER_PAGE_SIZE = 100
RAKUTEN_CABINET_BATCH_FOLDER_IMAGE_LIMIT = 500
RAKUTEN_CABINET_FOLDER_CREATE_ATTEMPTS = 10
DEFAULT_PAGE_SIZE = 30
MAX_PAGE_SIZE = 500
IGNORED_CABINET_IMAGE_FILENAMES = {"bg_logo.gif", "bg_logo2.gif", "bg_logo3.gif", "spacer.gif", "blank.gif"}
RAKUTEN_PRODUCT_TARGET_ERROR = "单个商品采集支持普通乐天商品链接、Rakuten Fashion 商品链接、带参数链接、店铺编码/商品编号。"
RAKUTEN_SHOP_TARGET_ERROR = "店铺采集请输入店铺展示名称、店铺url代码、店铺url或sid。"
RAKUTEN_FASHION_IMAGE_BASE = "https://tshop.r10s.jp/stylife/cabinet/item"
CRAWLER_HTTP_RETRY_STATUS_CODES = {403, 408, 429, 500, 502, 503, 504}
SCHEDULE_RUN_LOCK = threading.Lock()
CRAWLER_REQUEST_LOCK = threading.Lock()
CRAWLER_SESSION_LOCAL = threading.local()
CRAWLER_LAST_REQUEST_AT = 0.0
SCHEDULE_RUNNER_STARTED = False
DRAFT_IMAGE_CLEANUP_LAST_RUN_AT = 0.0


def dispatch_crawl_task(task_id: str) -> None:
    if should_use_redis_task_queue():
        try:
            enqueue_task(run_task, task_id, description=f"采集任务 {task_id}")
        except Exception as exc:
            mark_background_task_dispatch_failed(CrawlTaskModel, task_id, exc)
            raise
        return
    worker = threading.Thread(target=run_task, args=(task_id,), daemon=True)
    worker.start()


def dispatch_sync_task(owner_username: str, task_id: str) -> None:
    if should_use_redis_task_queue():
        try:
            enqueue_task(
                run_sync_task,
                owner_username,
                task_id,
                description=f"同步任务 {task_id}",
            )
        except Exception as exc:
            mark_background_task_dispatch_failed(SyncTaskModel, task_id, exc)
            raise
        return
    worker = threading.Thread(target=run_sync_task, args=(owner_username, task_id), daemon=True)
    worker.start()


def dispatch_listing_task(owner_username: str, task_id: str) -> None:
    if should_use_redis_task_queue():
        try:
            enqueue_task(
                run_listing_task,
                owner_username,
                task_id,
                description=f"上架任务 {task_id}",
            )
        except Exception as exc:
            mark_background_task_dispatch_failed(ListingTaskModel, task_id, exc)
            raise
        return
    worker = threading.Thread(target=run_listing_task, args=(owner_username, task_id), daemon=True)
    worker.start()


def dispatch_scheduled_crawl(owner_username: str, schedule_id: int) -> None:
    if should_use_redis_task_queue():
        try:
            enqueue_task(
                run_scheduled_crawl_job,
                owner_username,
                schedule_id,
                job_id=f"schedule-{schedule_id}-{uuid.uuid4().hex[:8]}",
                description=f"定时采集 {schedule_id}",
            )
        except Exception as exc:
            mark_scheduled_crawl_dispatch_failed(schedule_id, exc)
            raise
        return
    worker = threading.Thread(target=run_scheduled_crawl_job, args=(owner_username, schedule_id), daemon=True)
    worker.start()


def should_use_redis_task_queue() -> bool:
    return settings.task_queue_mode == "redis"


def mark_background_task_dispatch_failed(model: Any, task_id: str, exc: Exception) -> None:
    with session_scope() as session:
        task = session.get(model, task_id)
        if task is None:
            return
        task.status = "failed"
        task.failed_count = max(1, int(getattr(task, "failed_count", 0) or 0))
        task.message = "任务投递失败"
        task.error_detail = f"Redis 队列投递失败：{exc}"
        task.finished_at = datetime.now()


def mark_scheduled_crawl_dispatch_failed(schedule_id: int, exc: Exception) -> None:
    with session_scope() as session:
        row = session.get(ScheduledCrawlModel, schedule_id)
        if row is None:
            return
        row.status = "failed"
        row.notes = f"Redis 队列投递失败：{exc}"


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
        "status": resolve_crawl_task_status(row.status, row.total_count, row.success_count, row.failed_count),
        "totalCount": row.total_count,
        "successCount": row.success_count,
        "failedCount": row.failed_count,
        "message": row.message,
        "errorDetail": row.error_detail,
        "startedAt": row.started_at.isoformat(sep=" ") if row.started_at else None,
        "finishedAt": row.finished_at.isoformat(sep=" ") if row.finished_at else None,
        "createdAt": row.created_at.isoformat(sep=" ") if row.created_at else None,
    }


def resolve_crawl_task_status(status: str, total_count: int, success_count: int, failed_count: int) -> str:
    if status in {"queued", "running"}:
        return status
    total = max(0, int(total_count or 0))
    success = max(0, int(success_count or 0))
    failed = max(0, int(failed_count or 0))
    if total > 0 and failed >= total and success == 0:
        return "failed"
    if success > 0 and failed > 0:
        return "partial"
    if failed > 0 and success == 0:
        return "failed"
    if success > 0 and failed == 0:
        return "success"
    return status


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
        "parentProductId": row.parent_product_id,
        "listingTaskId": row.listing_task_id,
        "storeId": row.store_id,
        "rakutenManageNumber": row.rakuten_manage_number,
        "storeProductStatus": row.store_product_status,
        "rakutenListingStatus": row.rakuten_listing_status,
        "listedStores": product_listed_stores(raw_payload),
        "storeLastSeenAt": row.store_last_seen_at.isoformat(sep=" ") if row.store_last_seen_at else None,
        "title": row.title,
        "sourceUrl": row.source_url,
        "rakutenItemUrl": product_rakuten_item_url(row, raw_payload),
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


def product_listed_stores(raw_payload: dict[str, Any]) -> list[dict[str, Any]]:
    listed_stores = raw_payload.get("listedStores") if isinstance(raw_payload.get("listedStores"), list) else []
    result: list[dict[str, Any]] = []
    seen: set[int] = set()
    for item in listed_stores:
        if not isinstance(item, dict):
            continue
        try:
            store_id = int(item.get("storeId") or 0)
        except (TypeError, ValueError):
            store_id = 0
        if not store_id or store_id in seen:
            continue
        seen.add(store_id)
        result.append(
            {
                "storeId": store_id,
                "storeCode": normalize_text(item.get("storeCode")),
                "storeName": normalize_text(item.get("storeName")),
                "aliasName": normalize_text(item.get("aliasName")),
                "manageNumber": normalize_text(item.get("manageNumber")),
                "itemNumber": normalize_text(item.get("itemNumber")),
                "productId": int(item.get("productId") or 0) if str(item.get("productId") or "").isdigit() else None,
                "listedAt": normalize_text(item.get("listedAt")),
            }
        )
    return result


def product_rakuten_item_url(row: ProductModel, raw_payload: dict[str, Any]) -> str:
    if row.review_status != "listed":
        return row.source_url
    listing_store = raw_payload.get("listingStore") if isinstance(raw_payload.get("listingStore"), dict) else {}
    shop_code = (
        first_text_from_keys(listing_store, ("storeCode", "shopCode"))
        or normalize_shop_code(row.image_url)
        or normalize_shop_code(first_text_from_keys(raw_payload, ("itemUrl", "itemPageUrl", "url")))
    )
    item_number = (
        normalize_text(row.item_number)
        or first_text_from_keys(raw_payload, ("itemNumber", "manageNumber"))
        or normalize_text(row.rakuten_manage_number)
    )
    return build_public_item_page_url(shop_code, item_number) or row.source_url


def product_raw_payload(row: ProductModel) -> dict[str, Any]:
    try:
        payload = json.loads(row.raw_payload_json or "{}")
    except ValueError:
        return {}
    return payload if isinstance(payload, dict) else {}


RAKUTEN_TAGLINE_KEYS = ("tagline", "catchcopy", "catchCopy", "catchCopyTrans", "subTitle", "subtitle", "saleComment", "sale_comment")


def product_tagline(raw_payload: dict[str, Any]) -> str:
    tagline = first_text_from_keys(raw_payload, RAKUTEN_TAGLINE_KEYS)
    if tagline:
        return tagline
    embedded_item = raw_payload.get("embeddedItem")
    if isinstance(embedded_item, dict):
        tagline = first_text_from_keys(embedded_item, RAKUTEN_TAGLINE_KEYS)
        if tagline:
            return tagline
    source_product = raw_payload.get("sourceProduct")
    if isinstance(source_product, dict):
        return first_text_from_keys(source_product, RAKUTEN_TAGLINE_KEYS)
    return ""


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
        "tagline": product_tagline(raw_payload),
        "genreId": first_text_from_keys(raw_payload, ("genreId", "genre_id", "genre")) or row.genre_id,
        "shopName": row.shop_name,
        "sourceUrl": row.source_url,
        "rakutenItemUrl": product_rakuten_item_url(row, raw_payload),
        "listingStatus": row.rakuten_listing_status,
        "salesCount": product_sales_count(raw_payload),
        "created": first_text_from_keys(raw_payload, ("created",)),
        "updated": first_text_from_keys(raw_payload, ("updated",)),
        "descriptions": product_descriptions(raw_payload),
        "images": product_editable_image_urls(raw_payload, shop_code=shop_code),
        "variantSelectors": product_variant_selectors(raw_payload),
        "variants": product_variants(raw_payload),
        "raw": raw_payload,
    }
    return public


def product_descriptions(raw_payload: dict[str, Any]) -> list[dict[str, str]]:
    descriptions: list[dict[str, str]] = []
    seen_values: set[tuple[str, str]] = set()

    def append(label: str, value: Any, *, keep_empty: bool = False) -> None:
        normalized_label = normalize_text(label) or "商品说明"
        if not has_description_source(value):
            return
        text = normalize_listing_detail_html(value)
        text_key = description_content_key(text)
        seen_key = (normalized_label, text_key)
        if not text_key and not keep_empty:
            return
        if seen_key in seen_values:
            return
        seen_values.add(seen_key)
        descriptions.append({"label": normalized_label, "value": text})

    source_fields = source_rakuten_description_fields(raw_payload)
    append("PC用 商品説明文", source_fields.get("PC用 商品説明文"), keep_empty=True)
    append("スマートフォン用 商品説明文", source_fields.get("スマートフォン用 商品説明文"), keep_empty=True)
    append("PC用 販売説明文", source_fields.get("PC用 販売説明文"), keep_empty=True)

    product_description = raw_payload.get("productDescription")
    if isinstance(product_description, dict):
        append("PC用 商品説明文", product_description.get("pc"), keep_empty=True)
        append("スマートフォン用 商品説明文", product_description.get("sp"), keep_empty=True)
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
        ("スマートフォン用 商品説明文", "spDescription"),
        ("智能手机商品说明", "smartphoneDescription"),
    )
    for label, key in fields:
        append(label, raw_payload.get(key))
    append("PC用 販売説明文", raw_payload.get("salesDescription"), keep_empty=True)
    return normalize_rakuten_description_fields(
        clean_market_product_descriptions(descriptions, keep_empty_labels=RAKUTEN_DESCRIPTION_FIELD_LABELS)
    )


def source_rakuten_description_fields(raw_payload: dict[str, Any]) -> dict[str, str]:
    fields = {label: "" for label in RAKUTEN_STANDARD_DESCRIPTION_LABELS}
    embedded_item = raw_payload.get("embeddedItem") if isinstance(raw_payload.get("embeddedItem"), dict) else {}
    pc_fields = embedded_item.get("pcFields") if isinstance(embedded_item.get("pcFields"), dict) else {}
    source_values = {
        "PC用 商品説明文": pc_fields.get("productDescription"),
        "スマートフォン用 商品説明文": embedded_item.get("newProductDescription"),
        "PC用 販売説明文": embedded_item.get("salesDescription"),
    }
    for label, value in source_values.items():
        if has_description_source(value):
            fields[label] = str(value or "")

    product_description = raw_payload.get("productDescription")
    if isinstance(product_description, dict):
        fallback_values = {
            "PC用 商品説明文": product_description.get("pc"),
            "スマートフォン用 商品説明文": product_description.get("sp") or product_description.get("smartphone"),
            "PC用 販売説明文": raw_payload.get("salesDescription"),
        }
        for label, value in fallback_values.items():
            if not fields[label] and has_description_source(value):
                fields[label] = str(value or "")

    raw_descriptions = raw_payload.get("descriptions")
    if isinstance(raw_descriptions, list):
        for item in raw_descriptions:
            if not isinstance(item, dict):
                continue
            target_label = standard_rakuten_description_label(first_text_from_keys(item, ("label", "name")))
            if target_label and not fields[target_label] and has_description_source(item.get("value")):
                fields[target_label] = str(item.get("value") or "")

    if not fields["PC用 販売説明文"] and has_description_source(raw_payload.get("salesDescription")):
        fields["PC用 販売説明文"] = str(raw_payload.get("salesDescription") or "")
    return fields


def product_descriptions_for_display(raw_payload: dict[str, Any], images: list[str] | None = None, *, shop_code: str = "") -> list[dict[str, str]]:
    return product_descriptions(raw_payload)


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
    skipped_description_keys = {
        "description",
        "descriptions",
        "productdescription",
        "pcdescription",
        "spdescription",
        "smartphonedescription",
        "salesdescription",
        "descriptionimages",
    }

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
            for key, child in value.items():
                if str(key).lower() in skipped_description_keys:
                    continue
                collect(child)
        elif isinstance(value, list):
            for child in value:
                collect(child)

    collect(raw_payload)
    return urls


def product_editable_image_urls(raw_payload: dict[str, Any], *, shop_code: str = "") -> list[str]:
    edited_images = raw_payload.get("ltEditedImages")
    if isinstance(edited_images, list):
        urls = []
        for image in edited_images:
            url = normalize_product_image_url(image, shop_code=shop_code)
            if url and url not in urls:
                urls.append(url)
        return urls
    return product_image_urls(raw_payload, shop_code=shop_code)


def set_product_image_urls(raw_payload: dict[str, Any], images: list[str]) -> dict[str, Any]:
    updated_payload = dict(raw_payload)
    normalized_images = unique_texts([image for image in images if normalize_product_image_url(image)])
    updated_payload["ltEditedImages"] = normalized_images
    updated_payload["images"] = normalized_images
    media = updated_payload.get("media")
    if isinstance(media, dict):
        updated_media = dict(media)
        updated_media["images"] = [{"type": "CABINET" if is_cabinet_image_url(image) else "ABSOLUTE", "location": image} for image in normalized_images]
        updated_payload["media"] = updated_media
    updated_payload["updated"] = datetime.now().isoformat(timespec="seconds")
    return updated_payload


def set_product_image_urls_with_description_updates(
    raw_payload: dict[str, Any],
    images: list[str],
    *,
    replace_map: dict[str, str] | None = None,
    remove_urls: list[str] | None = None,
) -> dict[str, Any]:
    updated_payload = set_product_image_urls(raw_payload, images)
    if replace_map:
        updated_payload = replace_product_description_image_urls(updated_payload, replace_map)
    if remove_urls:
        updated_payload = remove_product_description_image_urls(updated_payload, remove_urls)
    return updated_payload


def localize_collected_product_images(owner_username: str, product_id: int) -> str:
    with session_scope() as session:
        product = session.get(ProductModel, product_id)
        if product is None or product.owner_username != owner_username:
            return ""
        if product.review_status not in {"pending", "approved", "error", "listed_master"}:
            return ""
        raw_payload = product_raw_payload(product)
        shop_code = product_shop_code(product, raw_payload)
        product_title = product.title
        source_images = product_editable_image_urls(raw_payload, shop_code=shop_code)
        if product.image_url and product.image_url not in source_images:
            source_images.insert(0, product.image_url)

    image_result = localize_product_image_urls(product_id, source_images, prefix="p")
    description_result = localize_product_description_images(
        product_id,
        raw_payload,
        existing_replacements=image_result["replacementMap"],
    )
    replacement_map = {**image_result["replacementMap"], **description_result["replacementMap"]}

    with session_scope() as session:
        product = session.get(ProductModel, product_id)
        if product is None or product.owner_username != owner_username:
            return ""
        if product.review_status not in {"pending", "approved", "error", "listed_master"}:
            return ""
        raw_payload = product_raw_payload(product)
        updated_payload = set_product_image_urls(raw_payload, image_result["urls"])
        if replacement_map:
            updated_payload = replace_product_description_image_urls(updated_payload, replacement_map)
            updated_payload = replace_payload_image_url_texts(updated_payload, replacement_map)
        updated_payload["ltLocalImagesReady"] = True
        updated_payload["ltLocalImageUpdatedAt"] = datetime.now().isoformat(timespec="seconds")
        if image_result["errors"] or description_result["errors"]:
            image_errors = [*image_result["errors"], *description_result["errors"]]
            updated_payload["ltLocalImageErrors"] = image_errors[:20]
            product.last_error = summarize_local_image_errors(product_title or product.title, product.source_url, product.id, image_errors)
        else:
            updated_payload.pop("ltLocalImageErrors", None)
            if product.last_error and product.last_error.startswith("图片本地化"):
                product.last_error = None
        product.raw_payload_json = json.dumps(updated_payload, ensure_ascii=False)
        product.image_url = image_result["urls"][0] if image_result["urls"] else ""
        remove_unused_local_product_images(product.id, collect_local_product_image_urls(updated_payload))
        session.flush()
        return product.last_error or ""


def localize_product_image_urls(product_id: int, image_urls: list[str], *, prefix: str) -> dict[str, Any]:
    local_urls: list[str] = []
    replacement_map: dict[str, str] = {}
    errors: list[str] = []
    source_urls = unique_texts(image_urls)
    if not source_urls:
        errors.append("未采集到商品主图。")
    for index, image_url in enumerate(source_urls, start=1):
        if is_product_image_draft_url(image_url):
            continue
        if is_local_product_image_url(image_url):
            if image_url not in local_urls:
                local_urls.append(image_url)
            continue
        try:
            local_url = save_remote_product_image(product_id, image_url, f"{prefix}{index:02d}")
        except Exception as exc:
            errors.append(f"{image_url}: {exc}")
            continue
        replacement_map[image_url] = local_url
        local_urls.append(local_url)
    return {"urls": unique_texts(local_urls), "replacementMap": replacement_map, "errors": errors}


def localize_product_description_images(
    product_id: int,
    raw_payload: dict[str, Any],
    *,
    existing_replacements: dict[str, str] | None = None,
) -> dict[str, Any]:
    description_urls = unique_texts(
        [
            url
            for description in product_descriptions(raw_payload)
            for url in description_image_urls(description.get("value"))
            if not is_product_image_draft_url(url)
        ]
    )
    replacement_map: dict[str, str] = {}
    errors: list[str] = []
    known_replacements = existing_replacements or {}
    for index, image_url in enumerate(description_urls, start=1):
        if is_local_product_image_url(image_url):
            continue
        if image_url in known_replacements:
            replacement_map[image_url] = known_replacements[image_url]
            continue
        try:
            replacement_map[image_url] = save_remote_product_image(product_id, image_url, f"d{index:02d}")
        except Exception as exc:
            errors.append(f"{image_url}: {exc}")
    return {"replacementMap": replacement_map, "errors": errors}


def save_remote_product_image(product_id: int, image_url: str, name_prefix: str) -> str:
    image_data = load_product_image_bytes(
        image_url,
        max_bytes=MAX_PRODUCT_IMAGE_DOWNLOAD_BYTES,
        size_error_message="图片下载大小不能超过 20MB。",
    )
    image_dir = LOCAL_PRODUCT_IMAGE_DIR / str(int(product_id))
    image_dir.mkdir(parents=True, exist_ok=True)
    safe_name = f"{name_prefix}-{uuid.uuid4().hex[:12]}{image_data['suffix']}"
    target_path = image_dir / safe_name
    target_path.write_bytes(image_data["content"])
    return local_product_image_url(product_id, safe_name)


def is_local_product_image_url(image_url: str) -> bool:
    return normalize_text(image_url).startswith(LOCAL_PRODUCT_IMAGE_URL_PREFIX)


def summarize_local_image_errors(product_title: str, source_url: str, product_id: int, errors: list[str]) -> str:
    if not errors:
        return ""
    display_name = normalize_text(product_title or source_url or product_id)
    first_error = errors[0]
    suffix = f"，另有 {len(errors) - 1} 张失败" if len(errors) > 1 else ""
    return f"图片本地化失败：{display_name}: {first_error[:300]}{suffix}"


def mark_product_local_image_error(owner_username: str, product_id: int, message: str) -> None:
    with session_scope() as session:
        product = session.get(ProductModel, product_id)
        if product is None or product.owner_username != owner_username:
            return
        product.last_error = message[:1000]


def replace_payload_image_url_texts(value: Any, replacement_map: dict[str, str]) -> Any:
    if not replacement_map:
        return value
    if isinstance(value, str):
        next_value = replace_payload_image_urls_in_text(value, replacement_map)
        for old_url, new_url in replacement_map.items():
            if old_url and new_url:
                next_value = next_value.replace(old_url, new_url)
        return next_value
    if isinstance(value, list):
        return [replace_payload_image_url_texts(item, replacement_map) for item in value]
    if isinstance(value, dict):
        return {key: replace_payload_image_url_texts(child, replacement_map) for key, child in value.items()}
    return value


def replace_payload_image_urls_in_text(value: str, replacement_map: dict[str, str]) -> str:
    def replace_match(match: re.Match[str]) -> str:
        matched = match.group(0)
        image_url = matched.rstrip(".,;")
        trailing = matched[len(image_url):]
        replacement = (
            replacement_map.get(image_url)
            or replacement_map.get(normalize_product_image_url(image_url))
            or replacement_map.get(normalize_description_image_url(image_url))
        )
        return f"{replacement}{trailing}" if replacement else matched

    return re.sub(r"https?://[^\s\"'<>)]+'?", replace_match, value)


def collect_local_product_image_urls(value: Any) -> list[str]:
    urls: list[str] = []

    def remember(candidate: Any) -> None:
        url = normalize_product_image_url(candidate)
        if is_local_product_image_url(url) and url not in urls:
            urls.append(url)

    def walk(item: Any) -> None:
        if isinstance(item, str):
            text = unescape(item).replace("\\/", "/")
            remember(text)
            pattern = rf"{re.escape(LOCAL_PRODUCT_IMAGE_URL_PREFIX)}/\d+/[^\s\"'<>),]+"
            for match in re.findall(pattern, text):
                remember(match.rstrip(".,;"))
            return
        if isinstance(item, list):
            for child in item:
                walk(child)
            return
        if isinstance(item, dict):
            for child in item.values():
                walk(child)

    walk(value)
    return urls


def remove_unused_local_product_images(product_id: int, referenced_urls: list[str]) -> None:
    image_dir = (LOCAL_PRODUCT_IMAGE_DIR / str(int(product_id))).resolve()
    root = LOCAL_PRODUCT_IMAGE_DIR.resolve()
    try:
        image_dir.relative_to(root)
    except ValueError:
        return
    if not image_dir.exists() or not image_dir.is_dir():
        return
    referenced_paths = {
        path.resolve()
        for path in (local_product_image_path_from_url(url) for url in referenced_urls)
        if path is not None
    }
    for path in image_dir.iterdir():
        if not path.is_file():
            continue
        try:
            if path.resolve() not in referenced_paths:
                path.unlink(missing_ok=True)
        except OSError:
            continue


def normalize_product_image_url(value: Any, *, shop_code: str = "") -> str:
    text = unescape(str(value or "")).replace("\\/", "/").strip().strip("'\"")
    if text.startswith((LOCAL_PRODUCT_IMAGE_URL_PREFIX, LOCAL_PRODUCT_IMAGE_DRAFT_URL_PREFIX)):
        return text.split("?", 1)[0].split("#", 1)[0]
    if not text.startswith(("http://", "https://")):
        if not re.search(r"\.(apng|avif|bmp|gif|jpe?g|png|webp)(?:$|[?#])", text, flags=re.I):
            return ""
        normalized_location = text.lstrip("/")
        thumbnail_match = re.match(r"@0_mall/([^/]+)/cabinet/(.+)", normalized_location, flags=re.I)
        if thumbnail_match:
            matched_shop_code = normalize_shop_code(thumbnail_match.group(1))
            if shop_code and matched_shop_code and matched_shop_code != normalize_shop_code(shop_code):
                return ""
            shop_code = shop_code or matched_shop_code
            normalized_location = thumbnail_match.group(2)
        if not shop_code:
            return ""
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


def is_cabinet_image_url(value: str) -> bool:
    text = normalize_text(value)
    if not text:
        return False
    if text.startswith("/cabinet/") or text.startswith("cabinet/"):
        return True
    try:
        parsed = urlsplit(text)
    except Exception:
        return False
    return "/cabinet/" in parsed.path.lower() and parsed.netloc.lower() in {
        "image.rakuten.co.jp",
        "thumbnail.image.rakuten.co.jp",
        "cabinet.rms.rakuten.co.jp",
    }


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
            target = cabinet_target_from_path(path)
            target_key = "|".join([target.get("folderPath", ""), target.get("filePath", ""), target.get("fileName", "")])
            if target_key in seen:
                continue
            seen.add(target_key)
            targets.append(target)

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


def cabinet_target_from_path(path: str) -> dict[str, str]:
    normalized_path = normalize_cabinet_path(path)
    without_cabinet = normalized_path
    if without_cabinet.lower().startswith("/cabinet/"):
        without_cabinet = without_cabinet[len("/cabinet/") :]
    else:
        without_cabinet = without_cabinet.lstrip("/")
    folder_path, _, file_path = without_cabinet.rpartition("/")
    file_name = file_path or without_cabinet
    return {
        "folderPath": f"/{folder_path}" if folder_path else "",
        "filePath": file_path or file_name,
        "fileName": file_name,
        "cabinetPath": normalized_path,
    }


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


def crawl_limit_label(value: Any, *, default: str = "全部") -> str:
    normalized = normalize_text(value)
    if not normalized:
        return default
    if normalized.lower() in {"all", "none"} or normalized in {"全部", "全量"}:
        return "全部"
    match = re.search(r"([0-9]{1,5})", normalized)
    if not match:
        return default
    return f"前 {max(1, int(match.group(1)))}"


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
        product_ids_payload = json.loads(row.product_ids_json or "[]")
    except ValueError:
        product_ids_payload = []
    if isinstance(product_ids_payload, dict):
        product_ids = product_ids_payload.get("productIds") if isinstance(product_ids_payload.get("productIds"), list) else []
        success_ids = product_ids_payload.get("successIds") if isinstance(product_ids_payload.get("successIds"), list) else []
        failed_ids = product_ids_payload.get("failedIds") if isinstance(product_ids_payload.get("failedIds"), list) else []
    else:
        product_ids = product_ids_payload if isinstance(product_ids_payload, list) else []
        success_ids = []
        failed_ids = []
    return {
        "id": row.id,
        "ownerUsername": row.owner_username,
        "storeId": row.store_id,
        "taskName": row.task_name,
        "status": row.status,
        "totalCount": row.total_count,
        "successCount": row.success_count,
        "failedCount": row.failed_count,
        "productIds": product_ids,
        "successIds": success_ids,
        "failedIds": failed_ids,
        "message": row.message,
        "errorDetail": row.error_detail,
        "startedAt": row.started_at.isoformat(sep=" ") if row.started_at else None,
        "finishedAt": row.finished_at.isoformat(sep=" ") if row.finished_at else None,
        "createdAt": row.created_at.isoformat(sep=" ") if row.created_at else None,
        "updatedAt": row.updated_at.isoformat(sep=" ") if row.updated_at else None,
    }


def sync_task_to_public(row: SyncTaskModel) -> dict[str, Any]:
    try:
        payload = json.loads(row.payload_json or "{}")
    except ValueError:
        payload = {}
    task_result = payload.get("result") if isinstance(payload, dict) and isinstance(payload.get("result"), dict) else {}
    return {
        "id": row.id,
        "ownerUsername": row.owner_username,
        "storeId": row.store_id,
        "storeName": row.store_name,
        "taskName": row.task_name,
        "taskType": row.task_type,
        "status": row.status,
        "totalCount": row.total_count,
        "successCount": row.success_count,
        "failedCount": row.failed_count,
        "payload": payload if isinstance(payload, dict) else {},
        "successIds": task_result.get("successIds") if isinstance(task_result.get("successIds"), list) else [],
        "failedIds": task_result.get("failedIds") if isinstance(task_result.get("failedIds"), list) else [],
        "message": row.message,
        "errorDetail": row.error_detail,
        "startedAt": row.started_at.isoformat(sep=" ") if row.started_at else None,
        "finishedAt": row.finished_at.isoformat(sep=" ") if row.finished_at else None,
        "createdAt": row.created_at.isoformat(sep=" ") if row.created_at else None,
        "updatedAt": row.updated_at.isoformat(sep=" ") if row.updated_at else None,
    }


def update_task_progress(
    model: Any,
    task_id: str,
    *,
    total_count: int | None = None,
    success_count: int | None = None,
    failed_count: int | None = None,
    message: str | None = None,
    status: str | None = None,
) -> None:
    last_error: OperationalError | None = None
    for attempt in range(3):
        try:
            with session_scope() as session:
                task = session.get(model, task_id)
                if task is None:
                    return
                if total_count is not None:
                    task.total_count = max(0, int(total_count))
                if success_count is not None:
                    task.success_count = max(0, int(success_count))
                if failed_count is not None:
                    task.failed_count = max(0, int(failed_count))
                if message is not None:
                    task.message = message
                if status is not None:
                    task.status = status
            return
        except OperationalError as exc:
            last_error = exc
            if not is_mysql_lock_wait_timeout(exc) or attempt >= 2:
                raise
            time.sleep(0.25 * (attempt + 1))
    if last_error is not None:
        raise last_error


def is_mysql_lock_wait_timeout(exc: OperationalError) -> bool:
    original = getattr(exc, "orig", None)
    code = getattr(original, "args", [None])[0] if original is not None else None
    return code == 1205 or "Lock wait timeout exceeded" in str(exc)


def ensure_user_task_capacity(
    session: Any,
    model: Any,
    owner_username: str,
    *,
    limit: int,
    label: str,
    exclude_task_id: str | None = None,
) -> None:
    query = select(func.count()).where(
        model.owner_username == owner_username,
        model.status.in_(("queued", "running")),
    )
    if exclude_task_id:
        query = query.where(model.id != exclude_task_id)
    count = int(session.scalar(query) or 0)
    if count >= limit:
        raise RuntimeError(f"当前用户已有 {count} 个{label}任务正在执行，请等待任务完成后再创建。")


def ensure_store_task_capacity(
    session: Any,
    store_id: int | None,
    *,
    exclude_sync_task_id: str | None = None,
    exclude_listing_task_id: str | None = None,
) -> None:
    if not store_id:
        return
    sync_query = select(func.count()).where(
        SyncTaskModel.store_id == store_id,
        SyncTaskModel.status.in_(("queued", "running")),
    )
    if exclude_sync_task_id:
        sync_query = sync_query.where(SyncTaskModel.id != exclude_sync_task_id)
    listing_query = select(func.count()).where(
        ListingTaskModel.store_id == store_id,
        ListingTaskModel.status.in_(("queued", "running")),
    )
    if exclude_listing_task_id:
        listing_query = listing_query.where(ListingTaskModel.id != exclude_listing_task_id)
    count = int(session.scalar(sync_query) or 0) + int(session.scalar(listing_query) or 0)
    if count > 0:
        raise RuntimeError("该店铺已有同步、上架、上下架或删除任务正在执行，请等待完成后再操作。")


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
        "listed_master": "listed_master",
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
    if item_number.lower() == "c":
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


def fetch_rakuten_cabinet_folders(service_secret: str, license_key: str) -> list[dict[str, Any]]:
    if not service_secret or not license_key:
        raise RuntimeError("乐天 Secret 或乐天 Key 未配置。")
    folders: list[dict[str, Any]] = []
    seen_ids: set[int] = set()
    offset = 1
    while True:
        response = requests.get(
            RAKUTEN_CABINET_FOLDERS_GET_URL,
            timeout=settings.crawler_timeout_seconds,
            headers={
                "Authorization": build_rakuten_authorization_header(service_secret, license_key),
                "Accept": "application/xml, text/xml",
            },
            params={"offset": offset, "limit": RAKUTEN_CABINET_FOLDER_PAGE_SIZE},
        )
        try:
            response.raise_for_status()
        except requests.RequestException as exc:
            detail = normalize_text(response.text)
            message = "R-Cabinet 文件夹列表读取失败"
            if detail:
                message = f"{message}：{detail[:500]}"
            raise RuntimeError(message) from exc

        page_folders = parse_rakuten_cabinet_folders_xml(response.text)
        new_count = 0
        for folder in page_folders:
            folder_id = folder.get("folderId")
            if folder_id is None or folder_id in seen_ids:
                continue
            folders.append(folder)
            seen_ids.add(folder_id)
            new_count += 1
        if len(page_folders) < RAKUTEN_CABINET_FOLDER_PAGE_SIZE or new_count == 0:
            break
        offset += 1
    return folders


def parse_rakuten_cabinet_folders_xml(xml_text: str) -> list[dict[str, Any]]:
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError as exc:
        raise RuntimeError("R-Cabinet 文件夹列表返回格式无法解析。") from exc
    folders: list[dict[str, Any]] = []
    seen_ids: set[int] = set()
    for element in root.iter():
        children = list(element)
        if not children:
            continue
        values: dict[str, str] = {}
        for child in children:
            local_name = child.tag.split("}", 1)[-1].lower()
            if local_name in {
                "folderid",
                "foldername",
                "folderpath",
                "foldernode",
                "directoryname",
                "filecount",
                "imagecount",
                "folderfilecount",
            }:
                values[local_name] = normalize_text(child.text)
        folder_id = parse_optional_int(values.get("folderid"))
        if folder_id is None or folder_id in seen_ids:
            continue
        seen_ids.add(folder_id)
        folder_name = (
            values.get("foldername")
            or values.get("foldernode")
            or values.get("directoryname")
            or Path(values.get("folderpath") or "").name
        )
        folders.append(
            {
                "folderId": folder_id,
                "folderName": folder_name,
                "directoryName": values.get("directoryname", ""),
                "folderPath": values.get("folderpath", ""),
                "fileCount": parse_optional_int(
                    values.get("filecount") or values.get("imagecount") or values.get("folderfilecount")
                )
                or 0,
            }
        )
    return folders


def parse_optional_int(value: Any) -> int | None:
    text = normalize_text(value)
    if not text:
        return None
    try:
        return int(float(text))
    except (TypeError, ValueError):
        return None


def create_rakuten_cabinet_folder(
    service_secret: str,
    license_key: str,
    *,
    folder_name: str,
    directory_name: str,
) -> dict[str, Any]:
    normalized_folder_name = normalize_cabinet_folder_name(folder_name)
    normalized_directory_name = normalize_cabinet_directory_name(directory_name)
    xml_body = (
        '<?xml version="1.0" encoding="UTF-8"?>'
        "<request>"
        "<folderInsertRequest>"
        "<folder>"
        f"<folderName>{xml_escape_text(normalized_folder_name)}</folderName>"
        f"<directoryName>{xml_escape_text(normalized_directory_name)}</directoryName>"
        "</folder>"
        "</folderInsertRequest>"
        "</request>"
    )
    response = requests.post(
        RAKUTEN_CABINET_FOLDER_INSERT_URL,
        timeout=settings.crawler_timeout_seconds,
        headers={
            "Authorization": build_rakuten_authorization_header(service_secret, license_key),
            "Accept": "application/xml, text/xml",
            "Content-Type": "text/xml; charset=utf-8",
        },
        data=xml_body.encode("utf-8"),
    )
    try:
        response.raise_for_status()
    except requests.RequestException as exc:
        detail = normalize_text(response.text)
        result: dict[str, Any] = {}
        if detail:
            try:
                result = parse_rakuten_cabinet_folder_insert_xml(response.text, allow_error=True)
            except RuntimeError:
                result = {}
        if is_cabinet_same_folder_path_result(result):
            raise CabinetFolderAlreadyExistsError(normalized_directory_name, detail) from exc
        message = f"R-Cabinet 文件夹 {normalized_directory_name} 创建失败"
        if detail:
            message = f"{message}：{detail[:500]}"
        raise RuntimeError(message) from exc
    result = parse_rakuten_cabinet_folder_insert_xml(response.text, allow_error=True)
    if is_cabinet_same_folder_path_result(result):
        raise CabinetFolderAlreadyExistsError(normalized_directory_name, response.text)
    if result.get("folderId") is None:
        result_code = normalize_text(result.get("resultCode"))
        result_message = normalize_text(result.get("message"))
        detail_parts = [part for part in [f"resultCode={result_code}" if result_code else "", result_message] if part]
        detail = "，".join(detail_parts) or normalize_text(response.text)[:500]
        raise RuntimeError(f"R-Cabinet 文件夹 {normalized_directory_name} 创建失败：{detail}")
    result["folderName"] = result.get("folderName") or normalized_folder_name
    result["directoryName"] = result.get("directoryName") or normalized_directory_name
    result["folderPath"] = result.get("folderPath") or normalized_directory_name
    result["fileCount"] = int(result.get("fileCount") or 0)
    return result


def parse_rakuten_cabinet_folder_insert_xml(xml_text: str, *, allow_error: bool = False) -> dict[str, Any]:
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError as exc:
        raise RuntimeError("R-Cabinet 文件夹创建返回格式无法解析。") from exc
    result: dict[str, Any] = {}
    for element in root.iter():
        local_name = element.tag.split("}", 1)[-1].lower()
        text = normalize_text(element.text)
        if not text:
            continue
        if local_name in {"systemstatus", "status"}:
            result["systemStatus"] = text
        elif local_name == "message":
            result["message"] = text
        elif local_name in {"resultcode", "code"}:
            result["resultCode"] = text
        elif local_name == "folderid":
            folder_id = parse_optional_int(text)
            if folder_id is not None:
                result["folderId"] = folder_id
        elif local_name in {"foldername", "foldernode"}:
            result["folderName"] = text
        elif local_name == "directoryname":
            result["directoryName"] = text
        elif local_name == "folderpath":
            result["folderPath"] = text
        elif local_name in {"filecount", "imagecount", "folderfilecount"}:
            result["fileCount"] = parse_optional_int(text) or 0
    if result.get("folderId") is None:
        if allow_error:
            return result
        raise RuntimeError("R-Cabinet 文件夹创建成功但未返回 folderId。")
    return result


class CabinetFolderAlreadyExistsError(RuntimeError):
    def __init__(self, directory_name: str, detail: str = "") -> None:
        self.directory_name = normalize_cabinet_directory_name(directory_name)
        self.detail = normalize_text(detail)
        super().__init__(f"R-Cabinet 文件夹 {self.directory_name} 已存在。")


def is_cabinet_same_folder_path_result(result: dict[str, Any]) -> bool:
    result_code = normalize_text(result.get("resultCode"))
    message = normalize_text(result.get("message") or result.get("detail")).lower()
    return result_code == "3015" or "same folder path" in message


def ensure_listing_cabinet_folder(
    service_secret: str,
    license_key: str,
    store: StoreModel,
    required_slots: int,
    *,
    usage: dict[str, int] | None = None,
) -> dict[str, Any]:
    slots = max(1, required_slots)
    folders = fetch_rakuten_cabinet_folders(service_secret, license_key)
    prefix = listing_cabinet_directory_prefix(store)
    candidates = [
        folder
        for folder in folders
        if any(value.lower().startswith(prefix) for value in listing_cabinet_folder_directory_candidates(folder))
    ]
    for folder in sorted(candidates, key=cabinet_listing_folder_sort_key):
        if cabinet_folder_remaining_slots(folder) >= slots:
            return prepare_listing_cabinet_folder(folder)

    usage = usage or fetch_rakuten_cabinet_usage(service_secret, license_key)
    if int(usage.get("remainingFolderCount") or 0) <= 0:
        raise RuntimeError("R-Cabinet 没有可用文件夹数量，不能自动创建新的图片文件夹。")

    next_batch = next_listing_cabinet_batch_number(candidates)
    last_error: Exception | None = None
    for batch in range(next_batch, next_batch + RAKUTEN_CABINET_FOLDER_CREATE_ATTEMPTS):
        directory_name = f"{prefix}{batch:03d}"
        existing = find_listing_cabinet_folder_by_directory(folders, directory_name)
        if existing:
            if cabinet_folder_remaining_slots(existing) >= slots:
                return prepare_listing_cabinet_folder(existing)
            continue
        try:
            folder = create_rakuten_cabinet_folder(
                service_secret,
                license_key,
                folder_name=listing_cabinet_folder_display_name(store, batch),
                directory_name=directory_name,
            )
            folder["directoryName"] = directory_name
            return folder
        except CabinetFolderAlreadyExistsError as exc:
            last_error = exc
            folders = fetch_rakuten_cabinet_folders(service_secret, license_key)
            existing = find_listing_cabinet_folder_by_directory(folders, directory_name)
            if existing and cabinet_folder_remaining_slots(existing) >= slots:
                return prepare_listing_cabinet_folder(existing)
            continue
        except Exception as exc:
            last_error = exc
            raise
    if last_error is not None:
        raise RuntimeError(f"R-Cabinet 自动创建图片文件夹失败：{last_error}") from last_error
    raise RuntimeError("R-Cabinet 自动创建图片文件夹失败。")


def listing_cabinet_folder_directory(folder: dict[str, Any]) -> str:
    for value in listing_cabinet_folder_directory_candidates(folder):
        if value:
            return value
    return ""


def listing_cabinet_folder_directory_candidates(folder: dict[str, Any]) -> list[str]:
    values = [
        normalize_text(folder.get("folderPath")),
        normalize_text(folder.get("directoryName")),
        normalize_text(folder.get("folderName")),
    ]
    return [value for value in values if value]


def prepare_listing_cabinet_folder(folder: dict[str, Any]) -> dict[str, Any]:
    folder["directoryName"] = listing_cabinet_folder_directory(folder)
    return folder


def find_listing_cabinet_folder_by_directory(folders: list[dict[str, Any]], directory_name: str) -> dict[str, Any] | None:
    normalized_directory = normalize_cabinet_directory_name(directory_name)
    for folder in folders:
        if any(
            normalize_cabinet_directory_name(value) == normalized_directory
            for value in listing_cabinet_folder_directory_candidates(folder)
        ):
            return folder
    return None


def listing_cabinet_directory_prefix(store: StoreModel) -> str:
    shop_part = normalize_cabinet_directory_segment(store.store_code or f"store-{store.id}", max_length=6)
    month_part = datetime.now().strftime("%Y%m")
    return f"lt-{shop_part}-{month_part}-"


def listing_cabinet_folder_display_name(store: StoreModel, batch: int) -> str:
    alias = normalize_text(store.alias_name or store.store_name or store.store_code or "store")
    return normalize_cabinet_folder_name(f"LT {alias} {datetime.now():%Y-%m} {batch:03d}")


def cabinet_listing_folder_sort_key(folder: dict[str, Any]) -> tuple[int, int]:
    directory = listing_cabinet_folder_directory(folder)
    match = re.search(r"-(\d{3})$", directory)
    batch = int(match.group(1)) if match else 0
    return (batch, int(folder.get("folderId") or 0))


def next_listing_cabinet_batch_number(folders: list[dict[str, Any]]) -> int:
    max_batch = 0
    for folder in folders:
        directory = listing_cabinet_folder_directory(folder)
        match = re.search(r"-(\d{3})$", directory)
        if match:
            max_batch = max(max_batch, int(match.group(1)))
    return max_batch + 1


def cabinet_folder_remaining_slots(folder: dict[str, Any]) -> int:
    return max(0, RAKUTEN_CABINET_BATCH_FOLDER_IMAGE_LIMIT - int(folder.get("fileCount") or 0))


def normalize_cabinet_folder_name(value: str) -> str:
    text = normalize_text(value) or "LT Images"
    text = re.sub(r"<[^>]*>", "", text).replace("　", " ")
    text = re.sub(r"[\x00-\x1f\x7f]", "", text).strip()
    if not text:
        text = "LT Images"
    encoded = text.encode("utf-8")
    while len(encoded) > 50 and text:
        text = text[:-1]
        encoded = text.encode("utf-8")
    return text or "LT Images"


def normalize_cabinet_directory_name(value: str) -> str:
    text = normalize_text(value).lower()
    text = re.sub(r"[^a-z0-9_-]+", "-", text).strip("-_")
    if not text:
        text = f"lt-{uuid.uuid4().hex[:8]}"
    return text[:20]


def normalize_cabinet_directory_segment(value: str, *, max_length: int) -> str:
    text = normalize_text(value).lower()
    text = re.sub(r"[^a-z0-9_-]+", "-", text).strip("-_")
    if not text:
        text = hashlib.sha1(normalize_text(value).encode("utf-8")).hexdigest()[:max_length]
    return text[:max_length] or uuid.uuid4().hex[:max_length]


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


def patch_rakuten_item_images(
    service_secret: str,
    license_key: str,
    manage_number: str,
    uploaded_images: list[dict[str, str]],
    title: str,
) -> None:
    if not service_secret or not license_key:
        raise RuntimeError("乐天 Secret 或乐天 Key 未配置。")
    normalized_manage_number = normalize_text(manage_number)
    if not normalized_manage_number:
        raise RuntimeError("商品管理编号为空，不能同步修改乐天商品图片。")
    images = build_rakuten_listing_images(uploaded_images, title)
    if not images:
        raise RuntimeError("商品缺少可同步的 R-Cabinet 图片。")
    response = requests.patch(
        RAKUTEN_ITEM_PATCH_URL.format(manageNumber=quote(normalized_manage_number, safe="")),
        timeout=settings.crawler_timeout_seconds,
        headers={
            "Authorization": build_rakuten_authorization_header(service_secret, license_key),
            "Accept": "application/json",
            "Content-Type": "application/json; charset=utf-8",
        },
        json={"images": images},
    )
    try:
        response.raise_for_status()
    except requests.RequestException as exc:
        detail = normalize_text(response.text)
        message = f"乐天商品 {normalized_manage_number} 图片更新失败"
        if detail:
            message = f"{message}：{detail[:500]}"
        raise RuntimeError(message) from exc


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


def bulk_upsert_rakuten_inventories(
    service_secret: str,
    license_key: str,
    inventories: list[dict[str, Any]],
) -> None:
    if not service_secret or not license_key:
        raise RuntimeError("乐天 Secret 或乐天 Key 未配置。")
    if not inventories:
        return
    for offset in range(0, len(inventories), RAKUTEN_INVENTORY_BULK_UPSERT_LIMIT):
        chunk = inventories[offset : offset + RAKUTEN_INVENTORY_BULK_UPSERT_LIMIT]
        response = request_rakuten_write(
            "POST",
            RAKUTEN_INVENTORY_BULK_UPSERT_URL,
            operation="乐天库存/发货信息登记",
            headers={
                "Authorization": build_rakuten_authorization_header(service_secret, license_key),
                "Accept": "application/json",
                "Content-Type": "application/json; charset=utf-8",
            },
            json={"inventories": chunk},
        )
        try:
            response.raise_for_status()
        except requests.RequestException as exc:
            detail = normalize_text(response.text)
            message = "乐天库存/发货信息登记失败"
            if detail:
                message = f"{message}：{detail[:800]}"
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


def insert_rakuten_cabinet_file(
    service_secret: str,
    license_key: str,
    *,
    file_name: str,
    file_path: str,
    content: bytes,
    content_type: str,
    folder_id: int = 0,
    overwrite: bool = True,
) -> dict[str, Any]:
    if not service_secret or not license_key:
        raise RuntimeError("乐天 Secret 或乐天 Key 未配置。")
    if not content:
        raise RuntimeError("图片内容为空，不能上传到 R-Cabinet。")
    normalized_file_name = normalize_cabinet_file_name(file_name)
    normalized_file_path = normalize_cabinet_file_path(file_path)
    xml_body = (
        '<?xml version="1.0" encoding="UTF-8"?>'
        "<request>"
        "<fileInsertRequest>"
        "<file>"
        f"<fileName>{xml_escape_text(normalized_file_name)}</fileName>"
        f"<folderId>{int(folder_id)}</folderId>"
        f"<filePath>{xml_escape_text(normalized_file_path)}</filePath>"
        f"<overWrite>{str(bool(overwrite)).lower()}</overWrite>"
        "</file>"
        "</fileInsertRequest>"
        "</request>"
    )
    response = requests.post(
        RAKUTEN_CABINET_FILE_INSERT_URL,
        timeout=settings.crawler_timeout_seconds,
        headers={
            "Authorization": build_rakuten_authorization_header(service_secret, license_key),
            "Accept": "application/xml, text/xml",
        },
        data={"xml": xml_body},
        files={"file": (normalized_file_path, content, content_type or "application/octet-stream")},
    )
    try:
        response.raise_for_status()
    except requests.RequestException as exc:
        detail = normalize_text(response.text)
        message = f"R-Cabinet 图片 {normalized_file_path} 上传失败"
        if detail:
            message = f"{message}：{detail[:500]}"
        raise RuntimeError(message) from exc
    result = parse_cabinet_insert_xml(response.text)
    result["fileName"] = normalized_file_name
    result["filePath"] = normalized_file_path
    return result


def parse_cabinet_insert_xml(xml_text: str) -> dict[str, Any]:
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError as exc:
        raise RuntimeError("R-Cabinet 图片上传返回格式无法解析。") from exc
    result: dict[str, Any] = {}
    for element in root.iter():
        local_name = element.tag.split("}", 1)[-1].lower()
        text = normalize_text(element.text)
        if local_name == "fileid" and text:
            try:
                result["fileId"] = int(float(text))
            except ValueError:
                pass
        elif local_name in {"fileurl", "fileurlssl", "url"} and text:
            result["fileUrl"] = text
        elif local_name in {"resultcode", "code"} and text:
            result["resultCode"] = text
    return result


def xml_escape_text(value: Any) -> str:
    return (
        str(value or "")
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
        .replace("'", "&apos;")
    )


def normalize_cabinet_file_name(value: str) -> str:
    text = normalize_text(value) or "product"
    text = re.sub(r"<[^>]*>", "", text).replace("　", " ")
    text = re.sub(r"[\x00-\x1f\x7f]", "", text).strip()
    if not text:
        text = "product"
    encoded = text.encode("utf-8")
    while len(encoded) > 50 and text:
        text = text[:-1]
        encoded = text.encode("utf-8")
    return text or "product"


def normalize_cabinet_file_path(value: str) -> str:
    text = normalize_text(value).lower()
    suffix = Path(text).suffix.lower()
    stem = Path(text).stem.lower()
    if suffix == ".jpeg":
        suffix = ".jpg"
    if suffix not in {".jpg", ".png", ".gif"}:
        suffix = ".jpg"
    stem = re.sub(r"[^a-z0-9_-]+", "-", stem).strip("-_")
    if not stem:
        stem = uuid.uuid4().hex[:12]
    stem = stem[: max(1, 20 - len(suffix))]
    if re.fullmatch(r"img\d{8}|imgrc\d{10}", stem):
        stem = f"lt-{stem}"[: max(1, 20 - len(suffix))]
    return f"{stem}{suffix}"


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


def today_range() -> tuple[datetime, datetime]:
    start = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    return start, start + timedelta(days=1)


def _count_grouped_status(
    session: Any,
    model: Any,
    owner_username: str,
    statuses: list[str],
    start_at: datetime,
    end_at: datetime,
    *timestamp_columns: Any,
) -> dict[str, int]:
    if not timestamp_columns:
        timestamp_columns = (model.created_at,)
    time_conditions = [
        and_(column >= start_at, column < end_at)
        for column in timestamp_columns
    ]
    rows = session.execute(
        select(model.status, func.count())
        .where(
            model.owner_username == owner_username,
            or_(*time_conditions),
        )
        .group_by(model.status)
    ).all()
    counts = {status: 0 for status in statuses}
    for status, count in rows:
        if status in counts:
            counts[str(status)] = int(count or 0)
    return counts


def _count_grouped_product_status(
    session: Any,
    owner_username: str,
    statuses: list[str],
    start_at: datetime,
    end_at: datetime,
) -> dict[str, int]:
    rows = session.execute(
        select(ProductModel.review_status, func.count())
        .where(
            ProductModel.owner_username == owner_username,
            or_(
                and_(ProductModel.created_at >= start_at, ProductModel.created_at < end_at),
                and_(ProductModel.updated_at >= start_at, ProductModel.updated_at < end_at),
            ),
        )
        .group_by(ProductModel.review_status)
    ).all()
    counts = {status: 0 for status in statuses}
    for status, count in rows:
        if status in counts:
            counts[str(status)] = int(count or 0)
    return counts


def dashboard_summary(
    owner_username: str,
    *,
    include_stores: bool = True,
    include_crawler: bool = True,
    include_products: bool = True,
    include_sync_tasks: bool = True,
) -> dict[str, Any]:
    start_at, end_at = today_range()
    task_statuses = ["queued", "running", "success", "failed"]
    empty_task_counts = {status: 0 for status in task_statuses}
    empty_product_counts = {"pending": 0, "approved": 0, "error": 0}
    with session_scope() as session:
        total_stores = enabled_stores = error_stores = 0
        if include_stores:
            total_stores = int(session.scalar(
                select(func.count()).select_from(StoreModel).where(StoreModel.owner_username == owner_username)
            ) or 0)
            enabled_stores = int(session.scalar(
                select(func.count()).select_from(StoreModel).where(
                    StoreModel.owner_username == owner_username,
                    StoreModel.enabled.is_(True),
                )
            ) or 0)
            error_stores = int(session.scalar(
                select(func.count()).select_from(StoreModel).where(
                    StoreModel.owner_username == owner_username,
                    StoreModel.last_error.is_not(None),
                    StoreModel.last_error != "",
                )
            ) or 0)
        crawl_tasks = empty_task_counts.copy()
        if include_crawler:
            crawl_tasks = _count_grouped_status(
                session,
                CrawlTaskModel,
                owner_username,
                task_statuses,
                start_at,
                end_at,
                CrawlTaskModel.created_at,
                CrawlTaskModel.started_at,
                CrawlTaskModel.finished_at,
            )
        products = empty_product_counts.copy()
        listing_tasks = empty_task_counts.copy()
        if include_products:
            products = _count_grouped_product_status(
                session,
                owner_username,
                ["pending", "approved", "error"],
                start_at,
                end_at,
            )
            listing_tasks = _count_grouped_status(
                session,
                ListingTaskModel,
                owner_username,
                task_statuses,
                start_at,
                end_at,
                ListingTaskModel.created_at,
                ListingTaskModel.started_at,
                ListingTaskModel.finished_at,
                ListingTaskModel.updated_at,
            )
        sync_tasks = empty_task_counts.copy()
        if include_sync_tasks:
            sync_tasks = _count_grouped_status(
                session,
                SyncTaskModel,
                owner_username,
                task_statuses,
                start_at,
                end_at,
                SyncTaskModel.created_at,
                SyncTaskModel.started_at,
                SyncTaskModel.finished_at,
                SyncTaskModel.updated_at,
            )
    return {
        "dateRange": {
            "from": start_at.isoformat(sep=" "),
            "to": end_at.isoformat(sep=" "),
        },
        "stores": {
            "total": total_stores,
            "enabled": enabled_stores,
            "error": error_stores,
        },
        "products": {
            "pending": products["pending"],
            "approved": products["approved"],
            "error": products["error"],
        },
        "crawlTasks": {
            "queued": crawl_tasks["queued"],
            "running": crawl_tasks["running"],
            "success": crawl_tasks["success"],
            "failed": crawl_tasks["failed"],
        },
        "listingTasks": {
            "queued": listing_tasks["queued"],
            "running": listing_tasks["running"],
            "success": listing_tasks["success"],
            "failed": listing_tasks["failed"],
        },
        "syncTasks": {
            "queued": sync_tasks["queued"],
            "running": sync_tasks["running"],
            "success": sync_tasks["success"],
            "failed": sync_tasks["failed"],
        },
    }


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
            query = query.where(crawl_task_status_filter(status))
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


def crawl_task_status_filter(status: str) -> Any:
    normalized = normalize_text(status)
    if normalized in {"queued", "running"}:
        return CrawlTaskModel.status == normalized
    if normalized == "failed":
        return and_(
            CrawlTaskModel.status.notin_(("queued", "running")),
            CrawlTaskModel.failed_count > 0,
            CrawlTaskModel.success_count == 0,
        )
    if normalized == "partial":
        return and_(
            CrawlTaskModel.status.notin_(("queued", "running")),
            CrawlTaskModel.success_count > 0,
            CrawlTaskModel.failed_count > 0,
        )
    if normalized == "success":
        return and_(
            CrawlTaskModel.status.notin_(("queued", "running")),
            CrawlTaskModel.success_count > 0,
            CrawlTaskModel.failed_count == 0,
        )
    return CrawlTaskModel.status == normalized


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
        order_by = product_list_order_by(product_status)
        if normalized_page_size:
            total = session.scalar(select(func.count()).select_from(query.subquery())) or 0
            if total:
                max_page = max(1, (int(total) + normalized_page_size - 1) // normalized_page_size)
                normalized_page = min(normalized_page, max_page)
            rows = session.scalars(
                query.order_by(*order_by)
                .offset((normalized_page - 1) * normalized_page_size)
                .limit(normalized_page_size)
            ).all()
            return {
                "products": [product_to_public(row) for row in rows],
                "total": int(total),
                "page": normalized_page,
                "pageSize": normalized_page_size,
            }

        rows = session.scalars(query.order_by(*order_by)).all()
        return [product_to_public(row) for row in rows]


def product_list_order_by(status: str | None) -> tuple[Any, ...]:
    if status == "listed":
        return (
            ProductModel.listed_at.desc(),
            ProductModel.updated_at.desc(),
            ProductModel.id.desc(),
        )
    return (ProductModel.created_at.desc(), ProductModel.id.desc())


def ensure_store_owner(owner_username: str, store_id: int) -> StoreModel:
    with session_scope() as session:
        row = session.get(StoreModel, store_id)
        if row is None:
            raise RuntimeError("店铺不存在。")
        if row.owner_username != owner_username:
            raise RuntimeError("不能操作其他用户的店铺。")
        session.expunge(row)
        return row


def list_stores(
    owner_username: str,
    *,
    page: int | None = None,
    page_size: int | None = None,
) -> list[dict[str, Any]] | dict[str, Any]:
    with session_scope() as session:
        query = select(StoreModel).where(StoreModel.owner_username == owner_username)
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
        if row is not None and row.owner_username != owner_username:
            raise RuntimeError("不能操作其他用户的店铺。")
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
            duplicated_query = select(StoreModel).where(
                StoreModel.owner_username == owner_username,
                StoreModel.store_code == row.store_code,
            )
            if row.id is not None:
                duplicated_query = duplicated_query.where(StoreModel.id != row.id)
            duplicated_store = session.scalar(duplicated_query)
        if duplicated_store is not None:
            raise RuntimeError("店铺编号已存在。")
        session.flush()
        return store_to_public(row)


def delete_store(owner_username: str, store_id: int) -> None:
    with session_scope() as session:
        row = session.get(StoreModel, store_id)
        if row is None:
            return
        if row.owner_username != owner_username:
            raise RuntimeError("不能删除其他用户的店铺。")
        product_ids = session.scalars(
            select(ProductModel.id).where(
                ProductModel.owner_username == owner_username,
                ProductModel.store_id == store_id,
            )
        ).all()
        parent_products = session.scalars(
            select(ProductModel).where(
                ProductModel.owner_username == owner_username,
                ProductModel.review_status == "listed_master",
            )
        ).all()
        for product in parent_products:
            remove_product_listed_store_mark(product, store_id)
        if product_ids:
            session.query(ProductModel).filter(
                ProductModel.owner_username == owner_username,
                ProductModel.store_id == store_id,
            ).delete(synchronize_session=False)
        session.delete(row)
    for product_id in product_ids:
        clear_product_temp_image_files(int(product_id))


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
    apply_store_cabinet_usage(row, usage)


def apply_store_cabinet_usage(row: StoreModel, usage: dict[str, int]) -> None:
    row.cabinet_used_folder_count = usage["usedFolderCount"]
    row.cabinet_remaining_folder_count = usage["remainingFolderCount"]
    row.cabinet_usage_checked_at = datetime.now()


def sync_store_cabinet_usage_fields(row: StoreModel, service_secret: str, license_key: str) -> None:
    try:
        update_store_cabinet_usage(row, service_secret, license_key)
    except Exception:
        # R-Cabinet usage is advisory for UI statistics; listing still surfaces hard failures later.
        pass


def sync_store(owner_username: str, store_id: int) -> dict[str, Any]:
    task = create_sync_task(owner_username, store_id)
    return {
        "store": task.get("store"),
        "syncTask": task.get("syncTask"),
        "syncedCount": task.get("syncedCount", 0),
    }


def perform_store_sync(owner_username: str, store_id: int, *, task_id: str | None = None) -> dict[str, Any]:
    with session_scope() as session:
        row = session.get(StoreModel, store_id)
        if row is None:
            raise RuntimeError("店铺不存在。")
        if row.owner_username != owner_username:
            raise RuntimeError("不能同步其他用户的店铺。")
        if not row.enabled:
            raise RuntimeError("店铺已停用，不能更新商品。")
        synced_count = 0
        failed_count = 0
        service_secret = decrypt_text(row.rakuten_service_secret_encrypted)
        license_key = decrypt_text(row.rakuten_license_key_encrypted)
        verify_store_credentials(row)
        items = fetch_rakuten_store_items(service_secret, license_key)
        if task_id:
            update_task_progress(
                SyncTaskModel,
                task_id,
                total_count=len(items),
                success_count=0,
                failed_count=0,
                message=f"同步中，已处理 0 / {len(items)} 条",
            )
        seen_manage_numbers: set[str] = set()
        for index, item in enumerate(items, start=1):
            manage_number = first_text_from_keys(item, ("manageNumber", "itemNumber"))
            if manage_number:
                seen_manage_numbers.add(manage_number)
            item_number = first_text_from_keys(item, ("itemNumber",))
            if item_number:
                seen_manage_numbers.add(item_number)
            if upsert_store_product(session, owner_username, row, item):
                synced_count += 1
            else:
                failed_count += 1
            if task_id:
                update_task_progress(
                    SyncTaskModel,
                    task_id,
                    total_count=len(items),
                    success_count=synced_count,
                    failed_count=failed_count,
                    message=f"同步中，已处理 {index} / {len(items)} 条",
                )
        mark_missing_store_products_removed(session, row, seen_manage_numbers)
        reconcile_listed_master_store_marks_after_store_sync(session, owner_username, row, seen_manage_numbers)
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
    ensure_store_owner(owner_username, store_id)
    task_id = create_sync_task_record(
        owner_username,
        store_id,
        task_type="store_sync",
        task_name_prefix="商品同步",
        message="等待同步店铺商品",
    )
    dispatch_sync_task(owner_username, task_id)
    with session_scope() as session:
        task = session.get(SyncTaskModel, task_id)
        store = session.get(StoreModel, task.store_id) if task and task.store_id else None
        return {
            "syncTask": sync_task_to_public(task) if task else {"id": task_id},
            "store": store_to_public(store) if store else None,
            "syncedCount": 0,
        }


def create_listing_status_sync_task(owner_username: str, store_id: int, listing_status: str) -> dict[str, Any]:
    if listing_status not in {"listed", "unlisted"}:
        raise RuntimeError("上架状态不合法。")
    ensure_store_owner(owner_username, store_id)
    action_label = "全部上架" if listing_status == "listed" else "全部下架"
    task_id = create_sync_task_record(
        owner_username,
        store_id,
        task_type="listing_status",
        task_name_prefix=action_label,
        message=f"等待执行{action_label}",
        payload={"listingStatus": listing_status},
    )
    dispatch_sync_task(owner_username, task_id)
    with session_scope() as session:
        task = session.get(SyncTaskModel, task_id)
        store = session.get(StoreModel, task.store_id) if task and task.store_id else None
        return {
            "syncTask": sync_task_to_public(task) if task else {"id": task_id},
            "store": store_to_public(store) if store else None,
            "summary": {
                "total": 0,
                "successCount": 0,
                "failedCount": 0,
                "message": f"{action_label}任务已创建",
                "errors": [],
            },
        }


def create_product_listing_status_sync_task(owner_username: str, product_ids: list[int], listing_status: str) -> dict[str, Any]:
    if listing_status not in {"listed", "unlisted"}:
        raise RuntimeError("上架状态不合法。")
    normalized_ids = normalize_product_ids(product_ids)
    if not normalized_ids:
        raise RuntimeError("请先选择商品。")
    action_label = "批量上架" if listing_status == "listed" else "批量下架"
    store_id = validate_sync_task_products(owner_username, normalized_ids)
    task_id = create_sync_task_record(
        owner_username,
        store_id,
        task_type="product_listing_status",
        task_name_prefix=action_label,
        message=f"等待执行{action_label}",
        payload={"listingStatus": listing_status, "productIds": normalized_ids},
    )
    dispatch_sync_task(owner_username, task_id)
    return created_sync_task_response(task_id, message=f"{action_label}任务已创建")


def create_product_delete_sync_task(owner_username: str, product_ids: list[int]) -> dict[str, Any]:
    normalized_ids = normalize_product_ids(product_ids)
    if not normalized_ids:
        raise RuntimeError("请先选择商品。")
    store_id = validate_sync_task_products(owner_username, normalized_ids)
    task_id = create_sync_task_record(
        owner_username,
        store_id,
        task_type="product_delete",
        task_name_prefix="批量删除",
        message="等待执行批量删除",
        payload={"productIds": normalized_ids},
    )
    dispatch_sync_task(owner_username, task_id)
    return created_sync_task_response(task_id, message="批量删除任务已创建")


def normalize_product_ids(product_ids: list[int]) -> list[int]:
    result: list[int] = []
    seen: set[int] = set()
    for value in product_ids or []:
        product_id = int(value)
        if product_id in seen:
            continue
        seen.add(product_id)
        result.append(product_id)
    return result


def product_review_statuses(owner_username: str, product_ids: list[int]) -> set[str]:
    normalized_ids = normalize_product_ids(product_ids)
    if not normalized_ids:
        raise RuntimeError("请先选择商品。")
    with session_scope() as session:
        statuses = session.scalars(
            select(ProductModel.review_status).where(
                ProductModel.owner_username == owner_username,
                ProductModel.id.in_(normalized_ids),
            )
        ).all()
    if len(statuses) != len(normalized_ids):
        raise RuntimeError("部分商品不存在，不能执行该操作。")
    return {str(value or "") for value in statuses}


def validate_sync_task_products(owner_username: str, product_ids: list[int]) -> int:
    with session_scope() as session:
        products = session.scalars(
            select(ProductModel).where(
                ProductModel.owner_username == owner_username,
                ProductModel.id.in_(product_ids),
                ProductModel.review_status == "listed",
            )
        ).all()
        found_ids = {product.id for product in products}
        missing_ids = [product_id for product_id in product_ids if product_id not in found_ids]
        if missing_ids:
            raise RuntimeError(f"存在不可操作的店铺商品：{', '.join(str(value) for value in missing_ids[:10])}")
        store_ids = {product.store_id for product in products if product.store_id}
        if not products or len(store_ids) != 1:
            raise RuntimeError("请选择同一个店铺下的店铺商品。")
        store_id = int(next(iter(store_ids)))
        store = session.get(StoreModel, store_id)
        if store is None:
            raise RuntimeError("商品关联店铺不存在。")
        if not store.enabled:
            raise RuntimeError("商品关联店铺已停用，不能创建同步任务。")
        return store_id


def created_sync_task_response(task_id: str, *, message: str) -> dict[str, Any]:
    with session_scope() as session:
        task = session.get(SyncTaskModel, task_id)
        store = session.get(StoreModel, task.store_id) if task and task.store_id else None
        return {
            "syncTask": sync_task_to_public(task) if task else {"id": task_id},
            "store": store_to_public(store) if store else None,
            "summary": {
                "total": 0,
                "successCount": 0,
                "failedCount": 0,
                "message": message,
                "errors": [],
            },
        }


def create_sync_task_record(
    owner_username: str,
    store_id: int,
    *,
    task_type: str,
    task_name_prefix: str,
    message: str,
    payload: dict[str, Any] | None = None,
) -> str:
    with session_scope() as session:
        ensure_user_task_capacity(
            session,
            SyncTaskModel,
            owner_username,
            limit=settings.max_running_sync_tasks_per_user,
            label="同步",
        )
        ensure_store_task_capacity(session, store_id)
        store = session.get(StoreModel, store_id)
        if store is None:
            raise RuntimeError("店铺不存在。")
        if not store.enabled:
            raise RuntimeError("店铺已停用，不能创建同步任务。")
        task = SyncTaskModel(
            id=uuid.uuid4().hex,
            owner_username=owner_username,
            store_id=store.id,
            store_name=store.alias_name or store.store_name,
            task_name=f"{task_name_prefix} {store.alias_name or store.store_name} {datetime.now():%Y-%m-%d %H:%M}",
            task_type=task_type,
            payload_json=json.dumps(payload or {}, ensure_ascii=False),
            status="queued",
            message=message,
        )
        session.add(task)
        session.flush()
        return task.id


def run_sync_task(owner_username: str, task_id: str) -> None:
    with session_scope() as session:
        task = session.get(SyncTaskModel, task_id)
        if task is None:
            return
        if task.owner_username != owner_username:
            raise RuntimeError("不能执行其他用户的同步任务。")
        task.status = "running"
        task.message = sync_task_running_message(task)
        task.error_detail = None
        task.started_at = datetime.now()
        task.finished_at = None
        store_id = task.store_id
        task_type = task.task_type or "store_sync"
        try:
            payload = json.loads(task.payload_json or "{}")
        except ValueError:
            payload = {}

    try:
        if store_id is None:
            raise RuntimeError("同步任务没有关联店铺。")
        if task_type == "listing_status":
            listing_status = normalize_text(payload.get("listingStatus"))
            result = perform_store_listing_status_sync(owner_username, store_id, listing_status, task_id=task_id)
            payload["result"] = {
                "successIds": list(result.get("successIds") or []),
                "failedIds": list(result.get("failedIds") or []),
            }
            total_count = int(result.get("totalCount") or 0)
            success_count = int(result.get("successCount") or 0)
            failed_count = int(result.get("failedCount") or 0)
            action_label = "上架" if listing_status == "listed" else "下架"
            status = "success" if failed_count == 0 else "partial"
            message = f"完成，{action_label} {success_count} 条，异常 {failed_count} 条"
            error_detail = summarize_task_errors(list(result.get("errors") or []), limit=50)
        elif task_type == "product_listing_status":
            listing_status = normalize_text(payload.get("listingStatus"))
            product_ids = normalize_product_ids(list(payload.get("productIds") or []))
            result = perform_product_listing_status_sync(owner_username, store_id, product_ids, listing_status, task_id=task_id)
            payload["result"] = {
                "successIds": list(result.get("successIds") or []),
                "failedIds": list(result.get("failedIds") or []),
            }
            total_count = int(result.get("totalCount") or 0)
            success_count = int(result.get("successCount") or 0)
            failed_count = int(result.get("failedCount") or 0)
            action_label = "上架" if listing_status == "listed" else "下架"
            status = "success" if failed_count == 0 else "partial"
            message = f"完成，{action_label} {success_count} 条，异常 {failed_count} 条"
            error_detail = summarize_task_errors(list(result.get("errors") or []), limit=50)
        elif task_type == "product_delete":
            product_ids = normalize_product_ids(list(payload.get("productIds") or []))
            result = perform_product_delete_sync(owner_username, store_id, product_ids, task_id=task_id)
            payload["result"] = {
                "successIds": list(result.get("successIds") or []),
                "failedIds": list(result.get("failedIds") or []),
            }
            total_count = int(result.get("totalCount") or 0)
            success_count = int(result.get("successCount") or 0)
            failed_count = int(result.get("failedCount") or 0)
            cabinet_deleted_count = int(result.get("cabinetDeletedCount") or 0)
            status = "success" if failed_count == 0 else "partial"
            message = f"完成，删除 {success_count} 条，异常 {failed_count} 条"
            if cabinet_deleted_count:
                message = f"{message}，同步删除图片 {cabinet_deleted_count} 张"
            error_detail = summarize_task_errors(
                [*list(result.get("errors") or []), *list(result.get("warnings") or [])],
                limit=50,
            )
        else:
            result = perform_store_sync(owner_username, store_id, task_id=task_id)
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
        task.payload_json = json.dumps(payload, ensure_ascii=False)
        task.finished_at = datetime.now()


def sync_task_running_message(task: SyncTaskModel) -> str:
    task_type = task.task_type or "store_sync"
    if task_type in {"listing_status", "product_listing_status"}:
        try:
            payload = json.loads(task.payload_json or "{}")
        except ValueError:
            payload = {}
        action_label = "上架" if normalize_text(payload.get("listingStatus")) == "listed" else "下架"
        return f"正在执行{'全部' if task_type == 'listing_status' else '批量'}{action_label}"
    if task_type == "product_delete":
        return "正在执行批量删除"
    return "正在同步店铺商品"


def perform_store_listing_status_sync(
    owner_username: str,
    store_id: int,
    listing_status: str,
    *,
    task_id: str | None = None,
) -> dict[str, Any]:
    if listing_status not in {"listed", "unlisted"}:
        raise RuntimeError("上架状态不合法。")
    with session_scope() as session:
        store = session.get(StoreModel, store_id)
        if store is None:
            raise RuntimeError("店铺不存在。")
        if not store.enabled:
            raise RuntimeError("店铺已停用，不能更新上架状态。")
        products = session.scalars(
            select(ProductModel).where(
                ProductModel.owner_username == owner_username,
                ProductModel.store_id == store_id,
                ProductModel.review_status == "listed",
            )
        ).all()
        if not products:
            raise RuntimeError("当前店铺没有可操作的店铺商品。")
        result = apply_products_listing_status(
            session,
            products,
            listing_status,
            progress_callback=sync_task_progress_callback(task_id, len(products), "上下架同步") if task_id else None,
        )
        session.flush()
        summary = listing_status_result_summary(result, len(products))
        return {
            "store": store_to_public(store),
            "totalCount": summary["total"],
            "successCount": summary["successCount"],
            "failedCount": summary["failedCount"],
            "successIds": summary["successIds"],
            "failedIds": summary["failedIds"],
            "errors": summary["errors"],
        }


def perform_product_listing_status_sync(
    owner_username: str,
    store_id: int,
    product_ids: list[int],
    listing_status: str,
    *,
    task_id: str | None = None,
) -> dict[str, Any]:
    if listing_status not in {"listed", "unlisted"}:
        raise RuntimeError("上架状态不合法。")
    if not product_ids:
        raise RuntimeError("同步任务缺少商品。")
    with session_scope() as session:
        store = session.get(StoreModel, store_id)
        if store is None:
            raise RuntimeError("店铺不存在。")
        if not store.enabled:
            raise RuntimeError("店铺已停用，不能更新上架状态。")
        products = session.scalars(
            select(ProductModel).where(
                ProductModel.owner_username == owner_username,
                ProductModel.store_id == store_id,
                ProductModel.id.in_(product_ids),
                ProductModel.review_status == "listed",
            )
        ).all()
        found_ids = {product.id for product in products}
        missing_ids = [product_id for product_id in product_ids if product_id not in found_ids]
        if task_id:
            update_task_progress(
                SyncTaskModel,
                task_id,
                total_count=len(product_ids),
                success_count=0,
                failed_count=len(missing_ids),
                message=f"上下架同步中，已处理 0 / {len(product_ids)} 条",
            )
        result = (
            apply_products_listing_status(
                session,
                products,
                listing_status,
                progress_callback=sync_task_progress_callback(task_id, len(product_ids), "上下架同步", initial_failed=len(missing_ids)) if task_id else None,
            )
            if products
            else {"successIds": [], "errors": []}
        )
        errors = list(result.get("errors") or [])
        errors.extend(f"{product_id}: 商品不存在或不是店铺商品" for product_id in missing_ids)
        session.flush()
        success_ids = list(result.get("successIds") or [])
        failed_ids = [*list(result.get("failedIds") or []), *missing_ids]
        failed_ids = list(dict.fromkeys(failed_ids))
        success_count = len(success_ids)
        failed_count = max(0, len(product_ids) - success_count)
        return {
            "store": store_to_public(store),
            "totalCount": len(product_ids),
            "successCount": success_count,
            "failedCount": failed_count,
            "successIds": success_ids,
            "failedIds": failed_ids,
            "errors": errors,
        }


def perform_product_delete_sync(
    owner_username: str,
    store_id: int,
    product_ids: list[int],
    *,
    task_id: str | None = None,
) -> dict[str, Any]:
    if not product_ids:
        raise RuntimeError("同步任务缺少商品。")
    with session_scope() as session:
        store = session.get(StoreModel, store_id)
        if store is None:
            raise RuntimeError("店铺不存在。")
        if not store.enabled:
            raise RuntimeError("店铺已停用，不能删除商品。")
        rows = session.scalars(
            select(ProductModel).where(
                ProductModel.owner_username == owner_username,
                ProductModel.store_id == store_id,
                ProductModel.id.in_(product_ids),
                ProductModel.review_status == "listed",
            )
        ).all()
        found_ids = {row.id for row in rows}
        missing_ids = [product_id for product_id in product_ids if product_id not in found_ids]
        success_ids: list[int] = []
        failed_ids: list[int] = list(missing_ids)
        success_count = 0
        failed_count = len(missing_ids)
        cabinet_deleted_count = 0
        errors = [f"{product_id}: 商品不存在或不是店铺商品" for product_id in missing_ids]
        warnings: list[str] = []
        credential_cache: dict[int, tuple[StoreModel, str, str]] = {}
        if task_id:
            update_task_progress(
                SyncTaskModel,
                task_id,
                total_count=len(product_ids),
                success_count=0,
                failed_count=failed_count,
                message=f"删除中，已处理 0 / {len(product_ids)} 条",
            )
        for index, row in enumerate(rows, start=1):
            try:
                delete_store_product_from_rakuten(session, row, credential_cache)
                cabinet_deleted_count += int(getattr(row, "_deleted_cabinet_count", 0) or 0)
            except Exception as exc:
                failed_count += 1
                failed_ids.append(row.id)
                error_text = str(exc)
                row.last_error = error_text
                errors.append(f"{productCodeForError(row)}: {error_text}")
                if task_id:
                    update_task_progress(
                        SyncTaskModel,
                        task_id,
                        total_count=len(product_ids),
                        success_count=success_count,
                        failed_count=failed_count,
                        message=f"删除中，已处理 {index + len(missing_ids)} / {len(product_ids)} 条",
                    )
                continue
            warnings.extend(getattr(row, "_delete_warnings", []) or [])
            remove_listed_store_mark_for_store_product(session, row)
            success_ids.append(row.id)
            clear_product_temp_image_files(row.id)
            session.delete(row)
            success_count += 1
            if task_id:
                update_task_progress(
                    SyncTaskModel,
                    task_id,
                    total_count=len(product_ids),
                    success_count=success_count,
                    failed_count=failed_count,
                    message=f"删除中，已处理 {index + len(missing_ids)} / {len(product_ids)} 条",
                )
        session.flush()
        return {
            "store": store_to_public(store),
            "totalCount": len(product_ids),
            "successCount": success_count,
            "failedCount": failed_count,
            "successIds": success_ids,
            "failedIds": failed_ids,
            "cabinetDeletedCount": cabinet_deleted_count,
            "errors": errors,
            "warnings": warnings,
        }


def retry_sync_task(owner_username: str, task_id: str) -> dict[str, Any]:
    with session_scope() as session:
        task = session.get(SyncTaskModel, task_id)
        if task is None:
            raise RuntimeError("同步任务不存在。")
        if task.owner_username != owner_username:
            raise RuntimeError("不能重试其他用户的同步任务。")
        ensure_user_task_capacity(
            session,
            SyncTaskModel,
            owner_username,
            limit=settings.max_running_sync_tasks_per_user,
            label="同步",
            exclude_task_id=task_id,
        )
        ensure_store_task_capacity(session, task.store_id, exclude_sync_task_id=task_id)
        task.status = "running"
        task.message = "重新同步中"
        task.error_detail = None
        task.started_at = datetime.now()
        task.finished_at = None
    dispatch_sync_task(owner_username, task_id)
    with session_scope() as session:
        task = session.get(SyncTaskModel, task_id)
        return sync_task_to_public(task) if task else {"id": task_id}


def verify_all_stores(owner_username: str) -> dict[str, Any]:
    with session_scope() as session:
        rows = session.scalars(
            select(StoreModel)
            .where(StoreModel.owner_username == owner_username)
            .order_by(StoreModel.created_at.desc())
        ).all()
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
        parsed_target, existing_limit, _ = parse_ranking_target(strip_shop_ranking_prefix(raw_target))
        normalized_target = normalize_rakuten_shop_target(parsed_target)
        if not normalized_target:
            raise RuntimeError(RAKUTEN_SHOP_TARGET_ERROR)
        schedule_time = normalize_schedule_time(getattr(payload, "scheduleTime", "09:00"))
        period_label = ranking_period_label(getattr(payload, "rankingPeriod", "daily"))
        limit_label = crawl_limit_label(
            getattr(payload, "crawlLimit", None),
            default="全部" if existing_limit is None else f"前 {existing_limit}",
        )

        row.source_id = None
        row.source_type = "shop"
        row.target = f"店铺:{normalized_target} {period_label} {limit_label}"
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
    run_scheduled_crawl_job(owner_username, schedule_id)
    with session_scope() as session:
        row = session.get(ScheduledCrawlModel, schedule_id)
        if row is None:
            raise RuntimeError("定时任务不存在。")
        return scheduled_crawl_to_public(row)


def run_scheduled_crawl_job(owner_username: str, schedule_id: int) -> None:
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
                dispatch_scheduled_crawl(owner_username, schedule_id)
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


def run_periodic_maintenance_once() -> None:
    cleanup_expired_product_image_drafts_if_due()


def cleanup_expired_product_image_drafts_if_due() -> int:
    global DRAFT_IMAGE_CLEANUP_LAST_RUN_AT
    now = time.time()
    if DRAFT_IMAGE_CLEANUP_LAST_RUN_AT and now - DRAFT_IMAGE_CLEANUP_LAST_RUN_AT < 24 * 60 * 60:
        return 0
    DRAFT_IMAGE_CLEANUP_LAST_RUN_AT = now
    return cleanup_expired_product_image_drafts()


def start_schedule_runner(interval_seconds: int = 60) -> None:
    global SCHEDULE_RUNNER_STARTED
    if SCHEDULE_RUNNER_STARTED:
        return
    SCHEDULE_RUNNER_STARTED = True

    def loop() -> None:
        while True:
            try:
                run_due_scheduled_crawls_once()
                run_periodic_maintenance_once()
            except Exception:
                pass
            time.sleep(max(10, interval_seconds))

    threading.Thread(target=loop, name="lt-schedule-runner", daemon=True).start()


def update_product_status(owner_username: str, product_ids: list[int], status: str, *, message: str = "") -> list[dict[str, Any]]:
    if status not in {"pending", "approved", "error", "listed", "listed_master", "rejected"}:
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
        if any(row.review_status == "listed" for row in rows):
            if any(row.review_status != "listed" for row in rows):
                raise RuntimeError("店铺商品删除任务不能和其他状态商品混选。")
            if len({row.store_id for row in rows if row.store_id}) != 1:
                raise RuntimeError("请选择同一个店铺下的店铺商品。")
            return create_product_delete_sync_task(owner_username, normalized_ids)

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
                remove_listed_store_mark_for_store_product(session, row)
            if row.review_status == "listed_master":
                child_rows = session.scalars(
                    select(ProductModel).where(ProductModel.parent_product_id == row.id)
                ).all()
                for child in child_rows:
                    child.parent_product_id = None
            deleted_ids.append(row.id)
            clear_product_temp_image_files(row.id)
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
    folder_path = normalize_text(target.get("folderPath"))
    cabinet_path = normalize_text(target.get("cabinetPath"))
    if file_name:
        records = search_rakuten_cabinet_files(service_secret, license_key, file_name=file_name)
        if file_path or folder_path or cabinet_path:
            exact_ids = [
                int(record["fileId"])
                for record in records
                if cabinet_record_matches_target(record, file_path, folder_path=folder_path, cabinet_path=cabinet_path)
            ]
            if exact_ids:
                return exact_ids
        if len(records) == 1:
            return [int(records[0]["fileId"])]
    if file_path:
        try:
            records = search_rakuten_cabinet_files(service_secret, license_key, file_path=file_path)
        except RuntimeError:
            records = []
        exact_ids = [
            int(record["fileId"])
            for record in records
            if cabinet_record_matches_target(record, file_path, folder_path=folder_path, cabinet_path=cabinet_path)
        ]
        if exact_ids:
            return exact_ids
    return []


def cabinet_record_matches_target(
    record: dict[str, Any],
    file_path: str,
    *,
    folder_path: str = "",
    cabinet_path: str = "",
) -> bool:
    expected = normalize_cabinet_path(cabinet_path or file_path)
    expected_file_path = normalize_text(file_path).strip("/").lower()
    expected_folder_path = normalize_text(folder_path).strip("/").lower()
    record_file_path = normalize_text(record.get("filePath")).strip("/").lower()
    record_folder_path = normalize_text(record.get("folderPath")).strip("/").lower()
    if expected_file_path and record_file_path != expected_file_path:
        return False
    if expected_folder_path and record_folder_path != expected_folder_path:
        return False
    if expected_file_path or expected_folder_path:
        return True
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
    file_path = normalize_text(record.get("filePath"))
    if file_path and folder_path and not file_path.lower().startswith(folder_path.strip("/").lower() + "/"):
        return f"/cabinet/{folder_path.strip('/')}/{file_path.strip('/')}"
    return file_path


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


def repair_store_product_images(owner_username: str, product_id: int) -> dict[str, Any]:
    with session_scope() as session:
        product = session.get(ProductModel, product_id)
        if product is None or product.owner_username != owner_username:
            raise RuntimeError("商品不存在。")
        if product.review_status != "listed":
            raise RuntimeError("只有店铺商品可以修复同步图片。")
        if not product.store_id:
            raise RuntimeError("商品未关联店铺，不能修复同步图片。")
        manage_number = normalize_text(product.rakuten_manage_number or product.item_number)
        if not manage_number:
            raise RuntimeError("商品缺少商品管理编号，不能修复同步图片。")
        store = session.get(StoreModel, product.store_id)
        if store is None:
            raise RuntimeError("商品关联店铺不存在。")
        if not store.enabled:
            raise RuntimeError("商品关联店铺已停用，不能修复同步图片。")

        raw_payload = product_raw_payload(product)
        source_images = listing_image_sources_for_repair(raw_payload, product_shop_code(product, raw_payload))
        if not source_images:
            raise RuntimeError("没有找到可重新同步的原始商品图片。")
        service_secret = decrypt_text(store.rakuten_service_secret_encrypted)
        license_key = decrypt_text(store.rakuten_license_key_encrypted)
        cabinet_context: dict[str, Any] = {}
        uploaded_images: list[dict[str, str]] = []
        try:
            uploaded_images = upload_product_images_to_rakuten(
                service_secret,
                license_key,
                store,
                product,
                manage_number,
                cabinet_context=cabinet_context,
                source_images=source_images,
            )
            patch_rakuten_item_images(service_secret, license_key, manage_number, uploaded_images, product.title)
        except Exception as exc:
            rollback_message = rollback_uploaded_listing_images(service_secret, license_key, uploaded_images)
            product.last_error = f"{exc}；已回滚本次上传图片：{rollback_message}" if rollback_message else str(exc)
            raise RuntimeError(product.last_error) from exc

        edited_images = [
            build_rakuten_cabinet_image_url(store.store_code, image["location"])
            for image in uploaded_images
            if image.get("location")
        ]
        updated_payload = dict(raw_payload)
        updated_payload["images"] = uploaded_images
        updated_payload["ltEditedImages"] = edited_images
        updated_payload["updated"] = datetime.now().isoformat(timespec="seconds")
        product.raw_payload_json = json.dumps(updated_payload, ensure_ascii=False)
        product.image_url = edited_images[0] if edited_images else product.image_url
        product.store_last_seen_at = datetime.now()
        product.last_error = None
        session.flush()
        return product_detail_to_public(product)


def listing_image_sources_for_repair(raw_payload: dict[str, Any], shop_code: str) -> list[str]:
    sources: list[str] = []
    for image in raw_payload.get("images") if isinstance(raw_payload.get("images"), list) else []:
        source_url = image.get("sourceUrl") if isinstance(image, dict) else None
        url = normalize_product_image_url(source_url, shop_code=shop_code)
        if url and url not in sources:
            sources.append(url)
    if sources:
        return sources[:RAKUTEN_LISTING_IMAGE_LIMIT]
    fallback_payload = dict(raw_payload)
    fallback_payload.pop("ltEditedImages", None)
    fallback_payload.pop("images", None)
    return product_image_urls(fallback_payload, shop_code=shop_code)[:RAKUTEN_LISTING_IMAGE_LIMIT]


def request_rakuten_write(
    method: str,
    url: str,
    *,
    headers: dict[str, str],
    operation: str,
    **kwargs: Any,
) -> requests.Response:
    timeout = max(int(settings.rakuten_write_timeout_seconds), int(settings.crawler_timeout_seconds))
    last_error: Exception | None = None
    for attempt in range(RAKUTEN_WRITE_MAX_RETRIES):
        try:
            response = requests.request(method, url, timeout=timeout, headers=headers, **kwargs)
        except (requests.Timeout, requests.ConnectionError) as exc:
            last_error = exc
            if attempt < RAKUTEN_WRITE_MAX_RETRIES - 1:
                time.sleep(1.5 * (attempt + 1))
                continue
            raise RuntimeError(f"{operation}超时或连接失败，已重试 {RAKUTEN_WRITE_MAX_RETRIES} 次：{exc}") from exc

        if response.status_code in RAKUTEN_WRITE_RETRY_STATUS_CODES and attempt < RAKUTEN_WRITE_MAX_RETRIES - 1:
            response.close()
            time.sleep(1.5 * (attempt + 1))
            continue
        return response
    if last_error is not None:
        raise RuntimeError(f"{operation}失败：{last_error}") from last_error
    raise RuntimeError(f"{operation}失败。")


def put_rakuten_item(
    service_secret: str,
    license_key: str,
    manage_number: str,
    payload: dict[str, Any],
) -> None:
    if not service_secret or not license_key:
        raise RuntimeError("乐天 Secret 或乐天 Key 未配置。")
    normalized_manage_number = normalize_text(manage_number)
    if not normalized_manage_number:
        raise RuntimeError("商品管理编号为空，不能创建乐天商品。")
    response = request_rakuten_write(
        "PUT",
        RAKUTEN_ITEM_PATCH_URL.format(manageNumber=quote(normalized_manage_number, safe="")),
        operation=f"乐天商品 {normalized_manage_number} 创建",
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
        message = f"乐天商品 {normalized_manage_number} 创建失败"
        if detail:
            message = f"{message}：{detail[:800]}"
        raise RuntimeError(message) from exc


def create_store_product_on_rakuten(
    service_secret: str,
    license_key: str,
    store: StoreModel,
    product: ProductModel,
    cabinet_context: dict[str, Any] | None = None,
) -> dict[str, Any]:
    raw_payload = product_raw_payload(product)
    product._listing_store_id = store.id
    manage_number = generate_listing_manage_number(product, raw_payload)
    uploaded_product_images: list[dict[str, str]] = []
    uploaded_description_images: list[dict[str, str]] = []
    item_write_started = False
    try:
        uploaded_product_images = upload_product_images_to_rakuten(
            service_secret,
            license_key,
            store,
            product,
            manage_number,
            cabinet_context=cabinet_context,
        )
        description_result = upload_product_description_images_to_rakuten(
            service_secret,
            license_key,
            store,
            product,
            manage_number,
            raw_payload,
            cabinet_context=cabinet_context,
        )
        raw_payload = description_result["rawPayload"]
        uploaded_description_images = description_result["uploadedImages"]
        payload = build_rakuten_item_upsert_payload(product, raw_payload, uploaded_product_images, manage_number=manage_number)
        item_write_started = True
        put_rakuten_item(service_secret, license_key, manage_number, payload)
        inventory_payloads = build_rakuten_inventory_upsert_payloads(
            manage_number,
            payload.get("variants") if isinstance(payload.get("variants"), dict) else {},
        )
        bulk_upsert_rakuten_inventories(service_secret, license_key, inventory_payloads)
    except Exception as exc:
        if item_write_started:
            try:
                delete_rakuten_item(service_secret, license_key, manage_number)
            except Exception:
                pass
        uploaded_images_for_rollback = [*uploaded_product_images, *uploaded_description_images]
        rollback_message = rollback_uploaded_listing_images(service_secret, license_key, uploaded_images_for_rollback)
        if rollback_message:
            raise RuntimeError(f"{exc}；已回滚本次上传图片：{rollback_message}") from exc
        raise
    now = datetime.now()
    updated_payload = dict(raw_payload)
    updated_payload.update(payload)
    updated_payload["manageNumber"] = manage_number
    updated_payload["itemNumber"] = payload.get("itemNumber") or manage_number
    updated_payload["images"] = uploaded_product_images
    updated_payload["descriptionImages"] = uploaded_description_images
    updated_payload["ltEditedImages"] = [
        build_rakuten_cabinet_image_url(store.store_code, image["location"])
        for image in uploaded_product_images
        if image.get("location")
    ]
    updated_payload["listingStore"] = {
        "storeId": store.id,
        "storeCode": store.store_code,
        "storeName": store.store_name,
        "aliasName": store.alias_name,
    }
    updated_payload["created"] = updated_payload.get("created") or now.isoformat(timespec="seconds")
    updated_payload["updated"] = now.isoformat(timespec="seconds")
    return {
        "manageNumber": manage_number,
        "itemNumber": payload.get("itemNumber") or manage_number,
        "payload": updated_payload,
        "price": price_from_rakuten_item(updated_payload),
        "imageUrl": build_rakuten_cabinet_image_url(store.store_code, uploaded_product_images[0]["location"]) if uploaded_product_images else product.image_url,
    }


def upsert_listed_store_product_from_listing_result(
    session: Any,
    owner_username: str,
    source_product: ProductModel,
    store: StoreModel,
    listing_result: dict[str, Any],
) -> ProductModel:
    manage_number = normalize_text(listing_result.get("manageNumber"))
    if not manage_number:
        raise RuntimeError("上架结果缺少商品管理编号。")
    row = session.scalar(
        select(ProductModel).where(
            ProductModel.store_id == store.id,
            ProductModel.rakuten_manage_number == manage_number,
        )
    )
    payload = listing_result.get("payload") if isinstance(listing_result.get("payload"), dict) else {}
    source_url = build_public_item_page_url(store.store_code, listing_result.get("itemNumber") or manage_number)
    source_hash_url = f"{source_url}#store={store.id}&manage={quote(manage_number, safe='')}"
    if row is None:
        row = ProductModel(owner_username=owner_username, source_url=source_url, source_url_hash=make_source_url_hash(source_hash_url))
        session.add(row)
    row.owner_username = owner_username
    row.parent_product_id = source_product.id
    row.listing_task_id = None
    row.task_id = source_product.task_id
    row.store_id = store.id
    row.rakuten_manage_number = manage_number
    row.item_number = normalize_text(listing_result.get("itemNumber")) or manage_number
    row.title = source_product.title
    row.source_url = source_url
    row.source_url_hash = make_source_url_hash(source_hash_url)
    row.shop_name = store.store_name or source_product.shop_name
    row.image_url = normalize_text(listing_result.get("imageUrl")) or source_product.image_url
    row.price = Decimal(str(listing_result["price"])) if listing_result.get("price") is not None else source_product.price
    row.currency = source_product.currency or "JPY"
    row.genre_id = source_product.genre_id
    row.review_status = "listed"
    row.store_product_status = "active"
    row.rakuten_listing_status = "listed"
    row.raw_payload_json = json.dumps(payload, ensure_ascii=False)
    row.listed_at = datetime.now()
    row.store_last_seen_at = datetime.now()
    row.last_error = None
    return row


def record_product_listed_store(
    product: ProductModel,
    listed_product: ProductModel,
    store: StoreModel,
    listing_result: dict[str, Any],
) -> None:
    upsert_product_listed_store_record(product, {
        "storeId": store.id,
        "storeCode": store.store_code,
        "storeName": store.store_name,
        "aliasName": store.alias_name,
        "manageNumber": normalize_text(listing_result.get("manageNumber")) or listed_product.rakuten_manage_number,
        "itemNumber": normalize_text(listing_result.get("itemNumber")) or listed_product.item_number,
        "productId": listed_product.id,
        "listedAt": datetime.now().isoformat(sep=" ", timespec="seconds"),
    })


def ensure_product_listed_store_mark_from_store_product(session: Any, store_product: ProductModel, store: StoreModel) -> None:
    if not store_product.parent_product_id:
        return
    parent = session.get(ProductModel, store_product.parent_product_id)
    if parent is None or parent.owner_username != store_product.owner_username:
        return
    upsert_product_listed_store_record(parent, {
        "storeId": store.id,
        "storeCode": store.store_code,
        "storeName": store.store_name,
        "aliasName": store.alias_name,
        "manageNumber": normalize_text(store_product.rakuten_manage_number),
        "itemNumber": normalize_text(store_product.item_number),
        "productId": store_product.id,
        "listedAt": (store_product.listed_at or datetime.now()).isoformat(sep=" ", timespec="seconds"),
    })


def upsert_product_listed_store_record(product: ProductModel, record: dict[str, Any]) -> None:
    raw_payload = product_raw_payload(product)
    listed_stores = raw_payload.get("listedStores") if isinstance(raw_payload.get("listedStores"), list) else []
    try:
        next_store_id = int(record.get("storeId") or 0)
    except (TypeError, ValueError):
        next_store_id = 0
    if not next_store_id:
        return
    next_stores: list[dict[str, Any]] = []
    replaced = False
    for item in listed_stores:
        if not isinstance(item, dict):
            continue
        try:
            store_id = int(item.get("storeId") or 0)
        except (TypeError, ValueError):
            store_id = 0
        if store_id == next_store_id:
            next_stores.append(record)
            replaced = True
        else:
            next_stores.append(item)
    if not replaced:
        next_stores.append(record)
    raw_payload["listedStores"] = next_stores
    product.raw_payload_json = json.dumps(raw_payload, ensure_ascii=False)
    product.review_status = "listed_master"
    product.listing_task_id = None
    product.last_error = None
    product.listed_at = product.listed_at or datetime.now()


def remove_product_listed_store_mark(product: ProductModel, store_id: int) -> bool:
    raw_payload = product_raw_payload(product)
    listed_stores = raw_payload.get("listedStores") if isinstance(raw_payload.get("listedStores"), list) else []
    next_stores: list[dict[str, Any]] = []
    removed = False
    for item in listed_stores:
        if not isinstance(item, dict):
            continue
        try:
            item_store_id = int(item.get("storeId") or 0)
        except (TypeError, ValueError):
            item_store_id = 0
        if item_store_id == int(store_id):
            removed = True
            continue
        next_stores.append(item)
    if not removed:
        return False
    raw_payload["listedStores"] = next_stores
    product.raw_payload_json = json.dumps(raw_payload, ensure_ascii=False)
    if next_stores:
        product.review_status = "listed_master"
    elif product.review_status == "listed_master":
        product.review_status = "approved"
    product.listing_task_id = None
    return True


def remove_listed_store_mark_for_store_product(session: Any, store_product: ProductModel) -> None:
    if not store_product.store_id:
        return
    parent = session.get(ProductModel, store_product.parent_product_id) if store_product.parent_product_id else None
    if parent is not None and parent.owner_username == store_product.owner_username:
        remove_product_listed_store_mark(parent, int(store_product.store_id))


def reconcile_listed_master_store_marks_after_store_sync(
    session: Any,
    owner_username: str,
    store: StoreModel,
    seen_identifiers: set[str],
) -> None:
    normalized_seen = {normalize_text(value) for value in seen_identifiers if normalize_text(value)}
    products = session.scalars(
        select(ProductModel).where(
            ProductModel.owner_username == owner_username,
            ProductModel.review_status == "listed_master",
        )
    ).all()
    for product in products:
        store_records = [
            record for record in product_listed_stores(product_raw_payload(product))
            if int(record.get("storeId") or 0) == int(store.id)
        ]
        if not store_records:
            continue
        for record in store_records:
            if listed_store_record_is_seen_after_sync(session, product, record, normalized_seen):
                continue
            remove_product_listed_store_mark(product, int(store.id))
            break


def listed_store_record_is_seen_after_sync(
    session: Any,
    product: ProductModel,
    record: dict[str, Any],
    seen_identifiers: set[str],
) -> bool:
    if not seen_identifiers:
        return False
    record_identifiers = {
        normalize_text(record.get("manageNumber")),
        normalize_text(record.get("itemNumber")),
    }
    if any(identifier and identifier in seen_identifiers for identifier in record_identifiers):
        return True
    store_product = linked_store_product_from_listed_store_record(session, product, record)
    if store_product is None:
        return False
    product_identifiers = {
        normalize_text(store_product.rakuten_manage_number),
        normalize_text(store_product.item_number),
    }
    return any(identifier and identifier in seen_identifiers for identifier in product_identifiers)


def linked_store_product_from_listed_store_record(
    session: Any,
    product: ProductModel,
    record: dict[str, Any],
) -> ProductModel | None:
    product_id = record.get("productId")
    try:
        normalized_product_id = int(product_id or 0)
    except (TypeError, ValueError):
        normalized_product_id = 0
    if normalized_product_id:
        row = session.get(ProductModel, normalized_product_id)
        if (
            row is not None
            and row.owner_username == product.owner_username
            and row.parent_product_id == product.id
            and int(row.store_id or 0) == int(record.get("storeId") or 0)
        ):
            return row
    return session.scalar(
        select(ProductModel).where(
            ProductModel.owner_username == product.owner_username,
            ProductModel.parent_product_id == product.id,
            ProductModel.store_id == int(record.get("storeId") or 0),
            ProductModel.review_status == "listed",
        )
    )


def generate_listing_manage_number(product: ProductModel, raw_payload: dict[str, Any]) -> str:
    existing = normalize_text(product.rakuten_manage_number) if product.review_status == "listed" else ""
    if existing:
        return existing[:32]
    base = (
        first_text_from_keys(raw_payload, ("itemNumber", "manageNumber"))
        or normalize_text(product.item_number)
        or f"lt{product.id}"
    )
    normalized = re.sub(r"[^A-Za-z0-9_-]+", "-", base).strip("-_").lower()
    if not normalized:
        normalized = f"lt{product.id}"
    store_id = int(getattr(product, "_listing_store_id", 0) or 0)
    suffix = f"-{store_id}-{product.id}" if store_id else f"-{product.id}"
    max_base_len = max(1, 32 - len(suffix))
    if normalized.endswith(suffix):
        return normalized[:32]
    return f"{normalized[:max_base_len]}{suffix}"[:32]


def upload_product_images_to_rakuten(
    service_secret: str,
    license_key: str,
    store: StoreModel,
    product: ProductModel,
    manage_number: str,
    cabinet_context: dict[str, Any] | None = None,
    source_images: list[str] | None = None,
) -> list[dict[str, str]]:
    images = (source_images or product_images_for_edit(product))[:RAKUTEN_LISTING_IMAGE_LIMIT]
    if not images:
        raise RuntimeError("商品缺少图片，不能上架到乐天。")
    uploaded_images: list[dict[str, str]] = []
    cabinet_folder = ensure_listing_cabinet_folder_for_upload(
        service_secret,
        license_key,
        store,
        len(images),
        cabinet_context=cabinet_context,
    )
    folder_id = int(cabinet_folder.get("folderId") or 0)
    folder_path = normalize_text(cabinet_folder.get("directoryName") or cabinet_folder.get("folderPath") or cabinet_folder.get("folderName"))
    if not folder_id or not folder_path:
        raise RuntimeError("R-Cabinet 上架文件夹不可用。")
    try:
        for index, image_url in enumerate(images, start=1):
            image_data = prepare_rakuten_cabinet_image(
                load_product_image_bytes(
                    image_url,
                    max_bytes=MAX_PRODUCT_IMAGE_DOWNLOAD_BYTES,
                    size_error_message="图片下载大小不能超过 20MB。",
                )
            )
            suffix = image_data["suffix"]
            file_path = listing_cabinet_upload_file_path(manage_number, index, suffix, kind="p")
            file_name = normalize_cabinet_file_name(f"{product.title[:24]}-{index}")
            result = insert_rakuten_cabinet_file(
                service_secret,
                license_key,
                file_name=file_name,
                file_path=file_path,
                content=image_data["content"],
                content_type=image_data["contentType"],
                folder_id=folder_id,
                overwrite=True,
            )
            location = cabinet_image_location(folder_path, result.get("filePath") or file_path)
            uploaded_images.append(
                {
                    "type": "CABINET",
                    "location": location,
                    "alt": product.title[:255],
                    "fileId": str(result.get("fileId") or ""),
                    "folderId": str(folder_id),
                    "folderPath": folder_path,
                    "sourceUrl": image_url,
                    "fileUrl": result.get("fileUrl") or build_rakuten_cabinet_image_url(store.store_code, location),
                }
            )
    except Exception as exc:
        rollback_message = rollback_uploaded_listing_images(service_secret, license_key, uploaded_images)
        if rollback_message:
            raise RuntimeError(f"{exc}；已回滚本次已上传图片：{rollback_message}") from exc
        raise
    reserve_listing_cabinet_folder_slots(cabinet_context, cabinet_folder, len(uploaded_images))
    return uploaded_images


def upload_product_description_images_to_rakuten(
    service_secret: str,
    license_key: str,
    store: StoreModel,
    product: ProductModel,
    manage_number: str,
    raw_payload: dict[str, Any],
    cabinet_context: dict[str, Any] | None = None,
) -> dict[str, Any]:
    description_items = product_descriptions(raw_payload)
    image_urls = unique_texts(
        [
            url
            for description in description_items
            for url in description_image_urls(description.get("value"))
            if should_transfer_description_image(url, store.store_code)
        ]
    )
    if not image_urls:
        return {"rawPayload": raw_payload, "uploadedImages": []}

    uploaded_images: list[dict[str, str]] = []
    replacement_map: dict[str, str] = {}
    cabinet_folder = ensure_listing_cabinet_folder_for_upload(
        service_secret,
        license_key,
        store,
        len(image_urls),
        cabinet_context=cabinet_context,
    )
    folder_id = int(cabinet_folder.get("folderId") or 0)
    folder_path = normalize_text(cabinet_folder.get("directoryName") or cabinet_folder.get("folderPath") or cabinet_folder.get("folderName"))
    if not folder_id or not folder_path:
        raise RuntimeError("R-Cabinet 上架说明图文件夹不可用。")
    try:
        for index, image_url in enumerate(image_urls, start=1):
            image_data = prepare_rakuten_cabinet_image(
                load_product_image_bytes(
                    image_url,
                    max_bytes=MAX_PRODUCT_IMAGE_DOWNLOAD_BYTES,
                    size_error_message="图片下载大小不能超过 20MB。",
                )
            )
            suffix = image_data["suffix"]
            file_path = listing_cabinet_upload_file_path(manage_number, index, suffix, kind="d")
            file_name = normalize_cabinet_file_name(f"{product.title[:20]}-说明-{index}")
            result = insert_rakuten_cabinet_file(
                service_secret,
                license_key,
                file_name=file_name,
                file_path=file_path,
                content=image_data["content"],
                content_type=image_data["contentType"],
                folder_id=folder_id,
                overwrite=True,
            )
            location = cabinet_image_location(folder_path, result.get("filePath") or file_path)
            file_url = result.get("fileUrl") or build_rakuten_cabinet_image_url(store.store_code, location)
            uploaded_images.append(
                {
                    "type": "CABINET_DESCRIPTION",
                    "location": location,
                    "alt": product.title[:255],
                    "fileId": str(result.get("fileId") or ""),
                    "folderId": str(folder_id),
                    "folderPath": folder_path,
                    "sourceUrl": image_url,
                    "fileUrl": file_url,
                }
            )
            replacement_map[image_url] = file_url
    except Exception as exc:
        rollback_message = rollback_uploaded_listing_images(service_secret, license_key, uploaded_images)
        if rollback_message:
            raise RuntimeError(f"{exc}；已回滚本次已上传说明图：{rollback_message}") from exc
        raise

    updated_payload = replace_product_description_image_urls(raw_payload, replacement_map)
    reserve_listing_cabinet_folder_slots(cabinet_context, cabinet_folder, len(uploaded_images))
    return {"rawPayload": updated_payload, "uploadedImages": uploaded_images}


def description_image_urls(html: Any) -> list[str]:
    soup = BeautifulSoup(str(html or ""), "lxml")
    urls: list[str] = []
    for image in soup.select("img, source"):
        for attr in ("src", "data-src", "data-original", "data-lazy-src"):
            url = normalize_description_image_url(image.get(attr))
            if url:
                urls.append(url)
        srcset = image.get("srcset")
        if srcset:
            for candidate_url, _descriptor in parse_srcset_candidates(srcset):
                url = normalize_description_image_url(candidate_url)
                if url:
                    urls.append(url)
    return unique_texts(urls)


def normalize_description_image_url(value: Any) -> str:
    text = normalize_text(value)
    if not text or text.lower().startswith(("data:", "blob:", "javascript:")):
        return ""
    if text.startswith(LOCAL_PRODUCT_IMAGE_URL_PREFIX):
        return text.split("?", 1)[0].split("#", 1)[0]
    if text.startswith("//"):
        text = f"https:{text}"
    if not text.startswith(("http://", "https://")):
        return ""
    return text


def should_transfer_description_image(url: str, target_shop_code: str) -> bool:
    text = normalize_description_image_url(url)
    if not text:
        return False
    parsed = urlsplit(text)
    host = parsed.netloc.lower()
    path = unquote(parsed.path or "").lower()
    normalized_shop_code = normalize_shop_code(target_shop_code).lower()
    if normalized_shop_code and host == "image.rakuten.co.jp":
        parts = [part for part in path.split("/") if part]
        if parts[:2] and parts[0] == normalized_shop_code and parts[1] == "cabinet":
            return False
    if normalized_shop_code and host in {"shop.r10s.jp", "tshop.r10s.jp"}:
        parts = [part for part in path.split("/") if part]
        if len(parts) >= 2 and parts[0] == normalized_shop_code and parts[1] == "cabinet":
            return False
    return True


def replace_product_description_image_urls(raw_payload: dict[str, Any], replacement_map: dict[str, str]) -> dict[str, Any]:
    if not replacement_map:
        return raw_payload
    updated_payload = json.loads(json.dumps(raw_payload, ensure_ascii=False))
    product_description = updated_payload.get("productDescription")
    if isinstance(product_description, dict):
        for key in ("pc", "sp", "smartphone", "value"):
            if key in product_description:
                product_description[key] = replace_description_html_image_urls(product_description.get(key), replacement_map)
    elif "productDescription" in updated_payload:
        updated_payload["productDescription"] = replace_description_html_image_urls(product_description, replacement_map)

    for key in ("description", "pcDescription", "spDescription", "smartphoneDescription", "salesDescription"):
        if key in updated_payload:
            updated_payload[key] = replace_description_html_image_urls(updated_payload.get(key), replacement_map)

    replace_embedded_item_description_image_urls(updated_payload, replacement_map)

    raw_descriptions = updated_payload.get("descriptions")
    if isinstance(raw_descriptions, list):
        for item in raw_descriptions:
            if isinstance(item, dict) and "value" in item:
                item["value"] = replace_description_html_image_urls(item.get("value"), replacement_map)
    return updated_payload


def replace_embedded_item_description_image_urls(raw_payload: dict[str, Any], replacement_map: dict[str, str]) -> None:
    embedded_item = raw_payload.get("embeddedItem")
    if not isinstance(embedded_item, dict):
        return
    pc_fields = embedded_item.get("pcFields")
    if isinstance(pc_fields, dict) and "productDescription" in pc_fields:
        pc_fields["productDescription"] = replace_description_html_image_urls(pc_fields.get("productDescription"), replacement_map)
    for key in ("newProductDescription", "salesDescription"):
        if key in embedded_item:
            embedded_item[key] = replace_description_html_image_urls(embedded_item.get(key), replacement_map)


def replace_description_html_image_urls(html: Any, replacement_map: dict[str, str]) -> str:
    if not html or not replacement_map:
        return str(html or "")
    soup = BeautifulSoup(str(html), "lxml")
    for image in soup.select("img, source"):
        for attr in ("src", "data-src", "data-original", "data-lazy-src"):
            value = normalize_description_image_url(image.get(attr))
            if value and value in replacement_map:
                image[attr] = replacement_map[value]
        srcset = image.get("srcset")
        if srcset:
            image["srcset"] = replace_srcset_image_urls(srcset, replacement_map)
    body = soup.body
    return body.decode_contents().strip() if body is not None else str(soup).strip()


def remove_product_description_image_urls(raw_payload: dict[str, Any], image_urls: list[str]) -> dict[str, Any]:
    normalized_urls = {normalize_description_image_url(url) for url in image_urls}
    normalized_urls.discard("")
    if not normalized_urls:
        return raw_payload
    updated_payload = json.loads(json.dumps(raw_payload, ensure_ascii=False))
    product_description = updated_payload.get("productDescription")
    if isinstance(product_description, dict):
        for key in ("pc", "sp", "smartphone", "value"):
            if key in product_description:
                product_description[key] = remove_description_html_image_urls(product_description.get(key), normalized_urls)
    elif "productDescription" in updated_payload:
        updated_payload["productDescription"] = remove_description_html_image_urls(product_description, normalized_urls)

    for key in ("description", "pcDescription", "spDescription", "smartphoneDescription", "salesDescription"):
        if key in updated_payload:
            updated_payload[key] = remove_description_html_image_urls(updated_payload.get(key), normalized_urls)

    remove_embedded_item_description_image_urls(updated_payload, normalized_urls)

    raw_descriptions = updated_payload.get("descriptions")
    if isinstance(raw_descriptions, list):
        for item in raw_descriptions:
            if isinstance(item, dict) and "value" in item:
                item["value"] = remove_description_html_image_urls(item.get("value"), normalized_urls)
    return updated_payload


def remove_embedded_item_description_image_urls(raw_payload: dict[str, Any], image_urls: set[str]) -> None:
    embedded_item = raw_payload.get("embeddedItem")
    if not isinstance(embedded_item, dict):
        return
    pc_fields = embedded_item.get("pcFields")
    if isinstance(pc_fields, dict) and "productDescription" in pc_fields:
        pc_fields["productDescription"] = remove_description_html_image_urls(pc_fields.get("productDescription"), image_urls)
    for key in ("newProductDescription", "salesDescription"):
        if key in embedded_item:
            embedded_item[key] = remove_description_html_image_urls(embedded_item.get(key), image_urls)


def remove_description_html_image_urls(html: Any, image_urls: set[str]) -> str:
    if not html or not image_urls:
        return str(html or "")
    soup = BeautifulSoup(str(html), "lxml")
    for image in soup.select("img, source"):
        should_remove = False
        for attr in ("src", "data-src", "data-original", "data-lazy-src"):
            value = normalize_description_image_url(image.get(attr))
            if value and value in image_urls:
                should_remove = True
                break
        srcset = image.get("srcset")
        if srcset:
            srcset_urls = {normalize_description_image_url(url) for url, _descriptor in parse_srcset_candidates(srcset)}
            srcset_urls.discard("")
            if srcset_urls and srcset_urls.issubset(image_urls):
                should_remove = True
        if should_remove:
            image.decompose()
    body = soup.body
    return body.decode_contents().strip() if body is not None else str(soup).strip()


def parse_srcset_candidates(value: str) -> list[tuple[str, str]]:
    candidates: list[tuple[str, str]] = []
    for raw_candidate in str(value or "").split(","):
        candidate = raw_candidate.strip()
        if not candidate:
            continue
        parts = candidate.split()
        url = parts[0] if parts else ""
        descriptor = " ".join(parts[1:]) if len(parts) > 1 else ""
        if url:
            candidates.append((url, descriptor))
    return candidates


def replace_srcset_image_urls(value: str, replacement_map: dict[str, str]) -> str:
    candidates = []
    for url, descriptor in parse_srcset_candidates(value):
        normalized_url = normalize_description_image_url(url)
        next_url = replacement_map.get(normalized_url, url)
        candidates.append(f"{next_url} {descriptor}".strip())
    return ", ".join(candidates)


def ensure_listing_cabinet_folder_for_upload(
    service_secret: str,
    license_key: str,
    store: StoreModel,
    required_slots: int,
    *,
    cabinet_context: dict[str, Any] | None = None,
) -> dict[str, Any]:
    context = cabinet_context if isinstance(cabinet_context, dict) else {}
    cached_folder = context.get("currentFolder") if isinstance(context.get("currentFolder"), dict) else None
    if cached_folder and cabinet_folder_remaining_slots(cached_folder) >= required_slots:
        return cached_folder
    folder = ensure_listing_cabinet_folder(
        service_secret,
        license_key,
        store,
        required_slots,
        usage=context.get("usage") if isinstance(context.get("usage"), dict) else None,
    )
    if cabinet_context is not None:
        cabinet_context["currentFolder"] = folder
    return folder


def reserve_listing_cabinet_folder_slots(
    cabinet_context: dict[str, Any] | None,
    folder: dict[str, Any],
    used_slots: int,
) -> None:
    if cabinet_context is None or not isinstance(folder, dict):
        return
    folder["fileCount"] = int(folder.get("fileCount") or 0) + max(0, used_slots)
    cabinet_context["currentFolder"] = folder


def cabinet_image_location(folder_path: str, file_path: str) -> str:
    normalized_folder = normalize_text(folder_path).strip("/")
    normalized_file = normalize_text(file_path).strip("/")
    if not normalized_folder:
        return normalized_file
    if normalized_file.lower().startswith(f"{normalized_folder.lower()}/"):
        return normalized_file
    return f"{normalized_folder}/{normalized_file}"


def listing_cabinet_upload_file_path(manage_number: str, index: int, suffix: str, *, kind: str) -> str:
    normalized_kind = re.sub(r"[^a-z0-9]+", "", normalize_text(kind).lower())[:1] or "p"
    normalized_suffix = normalize_text(suffix).lower()
    if normalized_suffix == ".jpeg":
        normalized_suffix = ".jpg"
    if normalized_suffix not in {".jpg", ".png", ".gif"}:
        normalized_suffix = ".jpg"
    digest = hashlib.sha1(normalize_text(manage_number).encode("utf-8")).hexdigest()[:8]
    stem = f"{normalized_kind}{digest}{max(1, index):03d}"
    return normalize_cabinet_file_path(f"{stem}{normalized_suffix}")


def rollback_uploaded_listing_images(
    service_secret: str,
    license_key: str,
    uploaded_images: list[dict[str, str]],
) -> str:
    if not uploaded_images:
        return ""
    deleted_count = 0
    warnings: list[str] = []
    attempted_ids: set[int] = set()
    for image in uploaded_images:
        file_ids = cabinet_file_ids_from_uploaded_image(service_secret, license_key, image)
        if not file_ids:
            warnings.append(f"{image.get('location') or image.get('filePath') or image.get('fileUrl') or '-'} 未找到文件ID")
            continue
        for file_id in file_ids:
            if file_id in attempted_ids:
                continue
            attempted_ids.add(file_id)
            try:
                delete_rakuten_cabinet_file(service_secret, license_key, file_id)
                deleted_count += 1
            except Exception as exc:
                warnings.append(f"图片 {file_id} 删除失败：{exc}")
    message = f"删除 {deleted_count} 张"
    if warnings:
        message = f"{message}，警告：{'；'.join(warnings[:5])}"
    return message


def cabinet_file_ids_from_uploaded_image(
    service_secret: str,
    license_key: str,
    image: dict[str, str],
) -> list[int]:
    raw_file_id = normalize_text(image.get("fileId"))
    if raw_file_id:
        try:
            return [int(float(raw_file_id))]
        except ValueError:
            pass
    target = {
        "filePath": image.get("location") or image.get("filePath") or "",
        "fileName": Path(normalize_text(image.get("location") or image.get("filePath") or "")).name,
    }
    if target["filePath"] or target["fileName"]:
        try:
            return resolve_cabinet_file_ids(service_secret, license_key, target)
        except Exception:
            return []
    return []


def load_product_image_bytes(
    image_url: str,
    *,
    max_bytes: int = MAX_PRODUCT_IMAGE_BYTES,
    size_error_message: str = "图片大小不能超过 2MB。",
) -> dict[str, Any]:
    normalized_max_bytes = max(1, int(max_bytes or MAX_PRODUCT_IMAGE_BYTES))
    local_path = local_product_image_path_from_url(image_url)
    if local_path:
        if not local_path.exists():
            raise RuntimeError("本地图片文件不存在。")
        content = local_path.read_bytes()
        suffix = local_path.suffix.lower()
        content_type = mimetypes.guess_type(str(local_path))[0] or "application/octet-stream"
    else:
        response = requests.get(
            image_url,
            timeout=settings.crawler_timeout_seconds,
            headers={"User-Agent": settings.crawler_user_agent},
            proxies=crawler_request_proxies(),
            stream=True,
        )
        try:
            response.raise_for_status()
            chunks: list[bytes] = []
            size = 0
            for chunk in response.iter_content(chunk_size=1024 * 256):
                if not chunk:
                    continue
                size += len(chunk)
                if size > normalized_max_bytes:
                    raise RuntimeError(size_error_message)
                chunks.append(chunk)
            content = b"".join(chunks)
            suffix = Path(urlsplit(image_url).path).suffix.lower()
            content_type = normalize_text(response.headers.get("Content-Type")).split(";", 1)[0].lower()
            if not suffix:
                suffix = product_image_suffix_from_content_type(content_type)
        except requests.RequestException as exc:
            raise RuntimeError("读取商品图片失败。") from exc
        finally:
            response.close()
    if suffix == ".jpeg":
        suffix = ".jpg"
    if suffix not in ALLOWED_PRODUCT_IMAGE_EXTENSIONS:
        raise RuntimeError("图片格式只支持 jpg、jpeg、png、gif。")
    if suffix == ".jpg":
        content_type = "image/jpeg"
    elif suffix == ".png":
        content_type = "image/png"
    elif suffix == ".gif":
        content_type = "image/gif"
    if content_type not in ALLOWED_PRODUCT_IMAGE_MIME_TYPES:
        raise RuntimeError("图片文件类型不正确。")
    if not content:
        raise RuntimeError("图片内容为空。")
    if len(content) > normalized_max_bytes:
        raise RuntimeError(size_error_message)
    return {"content": content, "suffix": suffix, "contentType": content_type}


def prepare_rakuten_cabinet_image(image_data: dict[str, Any]) -> dict[str, Any]:
    content = image_data.get("content") or b""
    suffix = normalize_text(image_data.get("suffix")).lower()
    content_type = normalize_text(image_data.get("contentType")).lower()
    if not content:
        raise RuntimeError("图片内容为空。")
    if suffix == ".jpeg":
        suffix = ".jpg"
    if suffix == ".gif":
        validate_rakuten_cabinet_gif(content)
        return {"content": content, "suffix": ".gif", "contentType": "image/gif"}
    if suffix not in {".jpg", ".png"}:
        raise RuntimeError("R-Cabinet 图片格式只支持 jpg、png、gif。")
    try:
        from PIL import Image, ImageOps, UnidentifiedImageError
    except ImportError as exc:
        raise RuntimeError("服务器缺少 Pillow，不能自动处理 R-Cabinet 图片尺寸，请先执行 pip install -r requirements.txt。") from exc

    try:
        with Image.open(BytesIO(content)) as source:
            image = ImageOps.exif_transpose(source)
            image.load()
    except UnidentifiedImageError as exc:
        raise RuntimeError("图片文件无法识别。") from exc
    except OSError as exc:
        raise RuntimeError("图片文件无法读取。") from exc

    width, height = image.size
    if (
        suffix == ".jpg"
        and content_type == "image/jpeg"
        and len(content) <= RAKUTEN_CABINET_MAX_IMAGE_BYTES
        and width <= RAKUTEN_CABINET_MAX_IMAGE_DIMENSION
        and height <= RAKUTEN_CABINET_MAX_IMAGE_DIMENSION
    ):
        return {"content": content, "suffix": ".jpg", "contentType": "image/jpeg"}

    image = resize_image_to_max_dimension(image, RAKUTEN_CABINET_MAX_IMAGE_DIMENSION)
    if image.mode in {"RGBA", "LA"} or (image.mode == "P" and "transparency" in image.info):
        background = Image.new("RGB", image.size, (255, 255, 255))
        transparent_image = image.convert("RGBA")
        background.paste(transparent_image, mask=transparent_image.getchannel("A"))
        image = background
    elif image.mode not in {"RGB", "L"}:
        image = image.convert("RGB")
    elif image.mode == "L":
        image = image.convert("RGB")

    quality_values = (88, 82, 76, 70, 64, 58)
    current = image
    for scale_attempt in range(5):
        if scale_attempt:
            next_width = max(1, int(current.width * 0.9))
            next_height = max(1, int(current.height * 0.9))
            current = current.resize((next_width, next_height), Image.Resampling.LANCZOS)
        for quality in quality_values:
            output = BytesIO()
            current.save(output, format="JPEG", quality=quality, optimize=True, progressive=True)
            normalized_content = output.getvalue()
            if len(normalized_content) <= RAKUTEN_CABINET_MAX_IMAGE_BYTES:
                return {"content": normalized_content, "suffix": ".jpg", "contentType": "image/jpeg"}
    raise RuntimeError("图片压缩后仍超过 R-Cabinet 2MB 限制，请替换为更小的图片。")


def validate_rakuten_cabinet_gif(content: bytes) -> None:
    if len(content) > RAKUTEN_CABINET_MAX_IMAGE_BYTES:
        raise RuntimeError("GIF 图片超过 R-Cabinet 2MB 限制，请替换为 jpg/png。")
    try:
        from PIL import Image, UnidentifiedImageError
    except ImportError as exc:
        raise RuntimeError("服务器缺少 Pillow，不能自动检查 R-Cabinet 图片尺寸，请先执行 pip install -r requirements.txt。") from exc
    try:
        with Image.open(BytesIO(content)) as image:
            width, height = image.size
    except UnidentifiedImageError as exc:
        raise RuntimeError("GIF 图片文件无法识别。") from exc
    except OSError as exc:
        raise RuntimeError("GIF 图片文件无法读取。") from exc
    if width > RAKUTEN_CABINET_MAX_IMAGE_DIMENSION or height > RAKUTEN_CABINET_MAX_IMAGE_DIMENSION:
        raise RuntimeError("GIF 图片尺寸超过 R-Cabinet 限制，请替换为 jpg/png。")


def resize_image_to_max_dimension(image: Any, max_dimension: int) -> Any:
    width, height = image.size
    if width <= max_dimension and height <= max_dimension:
        return image.copy()
    scale = min(max_dimension / width, max_dimension / height)
    next_size = (max(1, int(width * scale)), max(1, int(height * scale)))
    try:
        from PIL import Image
    except ImportError as exc:
        raise RuntimeError("服务器缺少 Pillow，不能自动处理 R-Cabinet 图片尺寸，请先执行 pip install -r requirements.txt。") from exc
    return image.resize(next_size, Image.Resampling.LANCZOS)


def product_image_suffix_from_content_type(content_type: str) -> str:
    normalized = normalize_text(content_type).lower()
    if normalized == "image/jpeg":
        return ".jpg"
    if normalized == "image/png":
        return ".png"
    if normalized == "image/gif":
        return ".gif"
    return ""


def build_rakuten_item_upsert_payload(
    product: ProductModel,
    raw_payload: dict[str, Any],
    uploaded_images: list[dict[str, str]],
    *,
    manage_number: str | None = None,
) -> dict[str, Any]:
    title = first_text_from_keys(raw_payload, ("itemName", "title", "name")) or product.title
    title = normalize_text(title)
    if not title:
        raise RuntimeError("商品标题为空，不能上架到乐天。")
    genre_id = first_text_from_keys(raw_payload, ("genreId", "genre_id", "genre")) or product.genre_id
    if not re.fullmatch(r"\d{6}", normalize_text(genre_id)):
        raise RuntimeError("商品缺少 6 位乐天ジャンルID，不能上架到乐天。")
    variants = build_rakuten_listing_variants(raw_payload, product)
    if not variants:
        raise RuntimeError("商品缺少 SKU 价格信息，不能上架到乐天。")
    item_number = normalize_text(manage_number) or normalize_text(product.rakuten_manage_number) or generate_listing_manage_number(product, raw_payload)
    payload: dict[str, Any] = {
        "itemNumber": item_number[:32],
        "title": title[:255],
        "tagline": product_tagline(raw_payload)[:174],
        "itemType": "NORMAL",
        "genreId": normalize_text(genre_id),
        "hideItem": False,
        "unlimitedInventoryFlag": False,
        "images": build_rakuten_listing_images(uploaded_images, title),
        "productDescription": build_rakuten_product_description(raw_payload),
        "salesDescription": build_rakuten_sales_description(raw_payload),
        "variantSelectors": build_rakuten_variant_selectors(raw_payload, variants),
        "variants": variants,
    }
    return {key: value for key, value in payload.items() if value not in (None, "", [], {})}


def build_rakuten_listing_images(uploaded_images: list[dict[str, str]], title: str) -> list[dict[str, str]]:
    images: list[dict[str, str]] = []
    for image in uploaded_images:
        location = normalize_rakuten_item_image_location(image.get("location"))
        if not location:
            continue
        images.append({"type": "CABINET", "location": location, "alt": image.get("alt") or title[:255]})
        if len(images) >= RAKUTEN_LISTING_IMAGE_LIMIT:
            break
    return images


def normalize_rakuten_item_image_location(value: Any) -> str:
    location = normalize_text(value)
    if not location:
        return ""
    if location.startswith(("http://", "https://")):
        parsed = urlsplit(location)
        path = unquote(parsed.path)
        cabinet_index = path.lower().find("/cabinet/")
        if cabinet_index >= 0:
            location = path[cabinet_index + len("/cabinet/") :]
        else:
            return ""
    location = location.strip()
    if location.lower().startswith("cabinet/"):
        location = location[len("cabinet/") :]
    location = location.lstrip("/")
    if not location or "/" not in location:
        return ""
    return f"/{location}"


def build_rakuten_listing_variants(raw_payload: dict[str, Any], product: ProductModel) -> dict[str, dict[str, Any]]:
    raw_variants = raw_payload.get("variants")
    variant_items: list[tuple[str, dict[str, Any]]] = []
    if isinstance(raw_variants, dict):
        variant_items = [(normalize_text(key), value) for key, value in raw_variants.items() if isinstance(value, dict)]
    elif isinstance(raw_variants, list):
        for index, value in enumerate(raw_variants, start=1):
            if isinstance(value, dict):
                variant_id = first_text_from_keys(value, ("variantId", "skuId", "merchantDefinedSkuId")) or f"sku-{index}"
                variant_items.append((variant_id, value))
    if not variant_items:
        price = price_from_rakuten_item(raw_payload)
        if price is None and product.price is not None:
            price = float(product.price)
        if price is None:
            return {}
        variant_items = [("default", {"standardPrice": str(int(price)), "selectorValues": {}})]

    result: dict[str, dict[str, Any]] = {}
    for index, (variant_id, variant) in enumerate(variant_items, start=1):
        normalized_variant_id = re.sub(r"[^A-Za-z0-9_-]+", "-", normalize_text(variant_id)).strip("-_") or f"sku-{index}"
        price_text = first_text_from_keys(variant, ("standardPrice", "price", "displayPrice"))
        if not price_text and product.price is not None:
            price_text = str(product.price)
        normalized_price = normalize_rakuten_price(price_text)
        if not normalized_price:
            continue
        selector_values = variant.get("selectorValues") if isinstance(variant.get("selectorValues"), dict) else {}
        next_variant: dict[str, Any] = {
            "standardPrice": normalized_price,
            "hidden": bool(variant.get("hidden", False)),
            "articleNumber": normalize_article_number(variant.get("articleNumber")),
        }
        merchant_sku = first_text_from_keys(variant, ("merchantDefinedSkuId",))
        if merchant_sku:
            next_variant["merchantDefinedSkuId"] = merchant_sku[:96]
        if selector_values:
            next_variant["selectorValues"] = {
                normalize_text(key): normalize_text(value)[:32]
                for key, value in selector_values.items()
                if normalize_text(key) and normalize_text(value)
            }
        attributes = normalize_rakuten_variant_attributes(variant.get("attributes"))
        if attributes:
            next_variant["attributes"] = attributes
        result[normalized_variant_id[:32]] = {key: value for key, value in next_variant.items() if value not in (None, "", {}, [])}
    return result


def build_rakuten_inventory_upsert_payloads(
    manage_number: str,
    variants: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    normalized_manage_number = normalize_text(manage_number)
    if not normalized_manage_number or not variants:
        return []
    quantity = max(0, int(settings.rakuten_default_inventory_quantity))
    normal_delivery_time_id = int(settings.rakuten_default_normal_delivery_time_id)
    back_order_delivery_time_id = int(settings.rakuten_default_back_order_delivery_time_id)
    inventories: list[dict[str, Any]] = []
    for variant_id, variant in variants.items():
        normalized_variant_id = normalize_text(variant_id)
        if not normalized_variant_id:
            continue
        inventory: dict[str, Any] = {
            "manageNumber": normalized_manage_number,
            "variantId": normalized_variant_id,
            "mode": "ABSOLUTE",
            "quantity": quantity,
        }
        operation_lead_time: dict[str, int] = {}
        if normal_delivery_time_id > 0:
            operation_lead_time["normalDeliveryTimeId"] = normal_delivery_time_id
        if back_order_delivery_time_id > 0:
            operation_lead_time["backOrderDeliveryTimeId"] = back_order_delivery_time_id
        if operation_lead_time:
            inventory["operationLeadTime"] = operation_lead_time
        ship_from_ids = variant.get("shipFromIds") if isinstance(variant, dict) else None
        if isinstance(ship_from_ids, list):
            normalized_ship_from_ids: list[int] = []
            for value in ship_from_ids:
                try:
                    ship_from_id = int(value)
                except (TypeError, ValueError):
                    continue
                if ship_from_id > 0 and ship_from_id not in normalized_ship_from_ids:
                    normalized_ship_from_ids.append(ship_from_id)
            if normalized_ship_from_ids:
                inventory["shipFromIds"] = normalized_ship_from_ids
        inventories.append(inventory)
    return inventories


def normalize_rakuten_variant_attributes(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    attributes: list[dict[str, Any]] = []
    seen_names: set[str] = set()
    for item in value:
        if not isinstance(item, dict):
            continue
        name = first_text_from_keys(item, ("name", "attributeName", "label"))
        attribute_value = first_text_from_keys(item, ("value", "attributeValue", "text"))
        if not name or not attribute_value or name in seen_names:
            continue
        seen_names.add(name)
        attribute: dict[str, Any] = {"name": name, "values": [attribute_value]}
        unit = first_text_from_keys(item, ("unit",))
        if unit:
            attribute["unit"] = unit
        attributes.append(attribute)
    return attributes


def normalize_rakuten_price(value: Any) -> str:
    text = first_text_value(value)
    normalized = re.sub(r"[^0-9.]", "", text)
    if not normalized:
        return ""
    try:
        price = Decimal(normalized)
    except Exception:
        return ""
    if price <= 0 or price != price.to_integral_value():
        return ""
    return str(int(price))


def normalize_article_number(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        article_value = first_text_from_keys(value, ("value", "articleNumber"))
        exemption_reason = value.get("exemptionReason")
        if article_value:
            return {"value": article_value}
        try:
            reason = int(exemption_reason)
        except (TypeError, ValueError):
            reason = 5
        return {"exemptionReason": reason}
    text = first_text_value(value)
    if text:
        return {"value": text}
    return {"exemptionReason": 5}


def build_rakuten_variant_selectors(raw_payload: dict[str, Any], variants: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    selectors = product_variant_selectors(raw_payload)
    selector_names = {normalize_text(selector.get("key")): normalize_text(selector.get("name")) for selector in selectors}
    normalized_selectors: list[dict[str, Any]] = []
    selector_keys_used = {
        key
        for variant in variants.values()
        for key in (variant.get("selectorValues") if isinstance(variant.get("selectorValues"), dict) else {}).keys()
    }
    for index, selector in enumerate(selectors, start=1):
        key = normalize_text(selector.get("key")) or f"choice-{index}"
        if key not in selector_keys_used:
            continue
        values = []
        seen: set[str] = set()
        for variant in variants.values():
            selector_values = variant.get("selectorValues") if isinstance(variant.get("selectorValues"), dict) else {}
            value = normalize_text(selector_values.get(key))
            if value and value not in seen:
                seen.add(value)
                values.append({"displayValue": value[:32]})
        if values:
            normalized_selectors.append(
                {
                    "key": key[:32],
                    "displayName": (normalize_text(selector.get("name")) or key)[:32],
                    "values": values[:40],
                }
            )
    existing_keys = {selector["key"] for selector in normalized_selectors}
    for key in selector_keys_used:
        if key in existing_keys:
            continue
        values = []
        seen: set[str] = set()
        for variant in variants.values():
            selector_values = variant.get("selectorValues") if isinstance(variant.get("selectorValues"), dict) else {}
            value = normalize_text(selector_values.get(key))
            if value and value not in seen:
                seen.add(value)
                values.append({"displayValue": value[:32]})
        if values:
            normalized_selectors.append(
                {
                    "key": key[:32],
                    "displayName": (selector_names.get(key) or key)[:32],
                    "values": values[:40],
                }
            )
    return normalized_selectors


def build_rakuten_product_description(raw_payload: dict[str, Any]) -> dict[str, str]:
    descriptions = product_descriptions(raw_payload)
    pc_description = first_description_by_label(descriptions, ("PC用 商品説明文",))
    sp_description = first_description_by_label(descriptions, ("スマートフォン用 商品説明文",))
    pc_html = sanitize_rakuten_listing_description_html(pc_description, max_length=10240)
    sp_html = sanitize_rakuten_sp_description_html(sp_description, max_length=10240)
    result: dict[str, str] = {}
    if pc_html:
        result["pc"] = pc_html
    if sp_html:
        result["sp"] = sp_html
    return result


def build_rakuten_sales_description(raw_payload: dict[str, Any]) -> str:
    descriptions = product_descriptions(raw_payload)
    return sanitize_rakuten_listing_description_html(
        first_description_by_label(descriptions, ("PC用 販売説明文",)),
        max_length=10240,
    )


def sanitize_rakuten_listing_description_html(value: Any, *, max_length: int) -> str:
    text = str(value or "")
    if not text:
        return ""
    text = re.sub(r"<\s*thcolspan(\s|>)", r'<th colspan="2"\1', text, flags=re.I)
    text = re.sub(r"</\s*thcolspan\s*>", "</th>", text, flags=re.I)
    text = sanitize_rakuten_pc_description_html(text)
    text = re.sub(r"<\s*thcolspan(\s|>)", r'<th colspan="2"\1', text, flags=re.I)
    text = re.sub(r"</\s*thcolspan\s*>", "</th>", text, flags=re.I)
    return truncate_text(text, max_length)


def sanitize_rakuten_pc_description_html(value: Any) -> str:
    soup = BeautifulSoup(str(value or ""), "lxml")
    for comment in soup.find_all(string=lambda item: isinstance(item, Comment)):
        comment.extract()
    for element in soup.select("script, object, embed, link, meta, svg, canvas, video, audio, form, input, select, textarea, button"):
        element.decompose()
    for element in soup.select("*"):
        for attribute in list(element.attrs):
            name = normalize_text(attribute).lower()
            attr_values = element.get_attribute_list(attribute)
            value_text = " ".join(str(value) for value in attr_values).strip()
            if is_unsafe_html_attribute_value(name, value_text):
                del element.attrs[attribute]
    body = soup.body
    return body.decode_contents().strip() if body is not None else str(soup).strip()


def sanitize_rakuten_sp_description_html(value: Any, *, max_length: int) -> str:
    text = sanitize_rakuten_listing_description_html(value, max_length=max_length)
    if not text:
        return ""
    soup = BeautifulSoup(text, "lxml")
    sanitize_rakuten_sp_description_soup(soup)
    image_count = 0
    for image in soup.select("img"):
        image_count += 1
        if image_count > RAKUTEN_SP_DESCRIPTION_IMAGE_LIMIT:
            image.decompose()
    body = soup.body
    cleaned = body.decode_contents().strip() if body is not None else str(soup).strip()
    return truncate_text(cleaned, max_length)


def sanitize_rakuten_sp_description_soup(soup: BeautifulSoup) -> None:
    for comment in soup.find_all(string=lambda value: isinstance(value, Comment)):
        comment.extract()
    for element in list(soup.find_all(True)):
        tag_name = normalize_text(element.name).lower()
        if tag_name in RAKUTEN_SP_DESCRIPTION_DROP_TAGS:
            element.decompose()
            continue
        if tag_name in {"html", "body"}:
            continue
        if tag_name not in RAKUTEN_SP_DESCRIPTION_ALLOWED_TAGS:
            element.unwrap()
            continue
        allowed_attributes = set(RAKUTEN_SP_DESCRIPTION_ALLOWED_ATTRIBUTES.get("*", set()))
        allowed_attributes.update(RAKUTEN_SP_DESCRIPTION_ALLOWED_ATTRIBUTES.get(tag_name, set()))
        for attribute in list(element.attrs):
            attr_name = normalize_text(attribute).lower()
            attr_values = element.get_attribute_list(attribute)
            attr_value = " ".join(str(value) for value in attr_values).strip()
            if attr_name not in allowed_attributes or is_unsafe_html_attribute_value(attr_name, attr_value):
                del element.attrs[attribute]


def is_unsafe_html_attribute_value(name: str, value: str) -> bool:
    normalized_name = normalize_text(name).lower()
    normalized_value = normalize_text(value).lower()
    if normalized_name.startswith("on"):
        return True
    if normalized_value.startswith(("javascript:", "data:", "vbscript:")):
        return True
    if normalized_name in {"src", "href"} and normalized_value and not normalized_value.startswith(("http://", "https://", "/", "#", "mailto:", "tel:")):
        return True
    return False


def truncate_text(value: Any, max_length: int) -> str:
    text = str(value or "")
    return text[:max_length]


def update_product_local_detail(owner_username: str, product_id: int, payload: Any) -> dict[str, Any]:
    with session_scope() as session:
        product = session.get(ProductModel, product_id)
        if product is None or product.owner_username != owner_username:
            raise RuntimeError("商品不存在。")
        if product.review_status == "listed":
            raise RuntimeError("店铺商品请使用同步修改。")
        if product.review_status != "pending" and getattr(payload, "imageChanges", None):
            raise RuntimeError("只有待审核商品可以修改图片。")

        updated_payload = patch_local_item_detail(
            product_raw_payload(product),
            title=getattr(payload, "title", ""),
            tagline=getattr(payload, "tagline", ""),
            variants=list(getattr(payload, "variants", []) or []),
        )
        image_changes = getattr(payload, "imageChanges", None)
        if product.review_status == "pending":
            updated_payload = apply_product_image_changes(product, updated_payload, image_changes)
        product.title = first_text_from_keys(updated_payload, ("itemName", "title", "name")) or product.title
        product.price = price_from_rakuten_item(updated_payload)
        if image_changes:
            images = product_editable_image_urls(updated_payload, shop_code=product_shop_code(product, updated_payload))
            product.image_url = images[0] if images else ""
        product.raw_payload_json = json.dumps(updated_payload, ensure_ascii=False)
        product.last_error = None
        session.flush()
        return product_detail_to_public(product)


def apply_product_image_changes(product: ProductModel, raw_payload: dict[str, Any], image_changes: Any) -> dict[str, Any]:
    if not image_changes:
        return raw_payload
    images = unique_texts([
        normalize_product_image_url(image, shop_code=product_shop_code(product, raw_payload))
        for image in list(getattr(image_changes, "images", []) or [])
    ])
    old_images = product_images_for_edit(product)
    replacements: dict[str, str] = {}
    for item in list(getattr(image_changes, "replacements", []) or []):
        old_url = normalize_product_image_url(getattr(item, "from_", ""), shop_code=product_shop_code(product, raw_payload))
        new_url = normalize_product_image_url(getattr(item, "to", ""), shop_code=product_shop_code(product, raw_payload))
        if old_url and new_url:
            replacements[old_url] = new_url
    remove_urls = unique_texts([
        normalize_product_image_url(image, shop_code=product_shop_code(product, raw_payload))
        for image in list(getattr(image_changes, "removeUrls", []) or [])
    ])
    finalized_urls: dict[str, str] = {}

    def finalize_once(image_url: str) -> str:
        if image_url not in finalized_urls:
            finalized_urls[image_url] = finalize_product_image_url(product.id, image_url)
        return finalized_urls[image_url]

    normalized_images = [finalize_once(image) for image in images]
    finalized_replacements = {
        old_url: finalize_once(new_url)
        for old_url, new_url in replacements.items()
    }
    updated_payload = set_product_image_urls_with_description_updates(
        raw_payload,
        normalized_images,
        replace_map=finalized_replacements,
        remove_urls=remove_urls,
    )
    current_images = set(normalized_images)
    for old_url in old_images:
        if old_url not in current_images:
            remove_local_product_image_if_unused(old_url, normalized_images)
    return updated_payload


def product_images_for_edit(product: ProductModel) -> list[str]:
    raw_payload = product_raw_payload(product)
    images = product_editable_image_urls(raw_payload, shop_code=product_shop_code(product, raw_payload))
    if product.image_url and product.image_url not in images:
        images.insert(0, product.image_url)
    return images


def product_image_download_info(owner_username: str, product_id: int, image_index: int) -> dict[str, Any]:
    with session_scope() as session:
        product = session.get(ProductModel, product_id)
        if product is None or product.owner_username != owner_username:
            raise RuntimeError("商品不存在。")
        images = product_images_for_edit(product)
        image_url = image_url_at_index(images, image_index)
    local_path = local_product_image_path_from_url(image_url)
    filename = product_image_download_name(product_id, image_index, image_url)
    if local_path and local_path.exists():
        return {
            "type": "local",
            "path": local_path,
            "filename": filename,
            "mediaType": mimetypes.guess_type(str(local_path))[0] or "application/octet-stream",
        }
    return {
        "type": "remote",
        "url": image_url,
        "filename": filename,
        "mediaType": mimetypes.guess_type(filename)[0] or "application/octet-stream",
    }


def replace_product_image(owner_username: str, product_id: int, image_index: int, upload_file: Any) -> dict[str, Any]:
    raise RuntimeError("图片替换请在待审核商品详情中操作，并点击保存后生效。")


def delete_product_image(owner_username: str, product_id: int, image_index: int) -> dict[str, Any]:
    raise RuntimeError("图片删除请在待审核商品详情中操作，并点击保存后生效。")


def image_url_at_index(images: list[str], image_index: int) -> str:
    if image_index < 0 or image_index >= len(images):
        raise RuntimeError("图片不存在。")
    return images[image_index]


def save_product_image_draft(owner_username: str, product_id: int, upload_file: Any) -> str:
    with session_scope() as session:
        product = session.get(ProductModel, product_id)
        if product is None or product.owner_username != owner_username:
            raise RuntimeError("商品不存在。")
        if product.review_status != "pending":
            raise RuntimeError("只有待审核商品可以修改图片。")
    return save_uploaded_product_image_file(
        upload_file,
        LOCAL_PRODUCT_IMAGE_DRAFT_DIR / str(product_id),
        lambda filename: local_product_image_draft_url(product_id, filename),
        name_prefix="draft",
    )


def save_uploaded_product_image(product_id: int, image_index: int, upload_file: Any) -> str:
    return save_uploaded_product_image_file(
        upload_file,
        LOCAL_PRODUCT_IMAGE_DIR / str(product_id),
        lambda filename: local_product_image_url(product_id, filename),
        name_prefix=str(image_index + 1),
    )


def save_uploaded_product_image_file(upload_file: Any, image_dir: Path, url_builder: Callable[[str], str], *, name_prefix: str) -> str:
    filename = normalize_text(getattr(upload_file, "filename", ""))
    suffix = Path(filename).suffix.lower()
    if suffix == ".jpeg":
        suffix = ".jpg"
    content_type = normalize_text(getattr(upload_file, "content_type", ""))
    if suffix not in ALLOWED_PRODUCT_IMAGE_EXTENSIONS:
        raise RuntimeError("图片格式只支持 jpg、jpeg、png、gif。")
    if content_type and content_type not in ALLOWED_PRODUCT_IMAGE_MIME_TYPES:
        raise RuntimeError("图片文件类型不正确。")
    image_dir.mkdir(parents=True, exist_ok=True)
    safe_name = f"{name_prefix}-{uuid.uuid4().hex[:12]}{suffix}"
    target_path = image_dir / safe_name
    size = 0
    try:
        with target_path.open("wb") as target:
            while True:
                chunk = upload_file.file.read(1024 * 1024)
                if not chunk:
                    break
                size += len(chunk)
                if size > MAX_PRODUCT_IMAGE_BYTES:
                    target.close()
                    target_path.unlink(missing_ok=True)
                    raise RuntimeError("图片大小不能超过 2MB。")
                target.write(chunk)
    finally:
        try:
            upload_file.file.seek(0)
        except Exception:
            pass
    if size <= 0:
        target_path.unlink(missing_ok=True)
        raise RuntimeError("上传的图片为空。")
    return url_builder(safe_name)


def local_product_image_url(product_id: int, filename: str) -> str:
    return f"{LOCAL_PRODUCT_IMAGE_URL_PREFIX}/{int(product_id)}/{quote(filename, safe='')}"


def local_product_image_draft_url(product_id: int, filename: str) -> str:
    return f"{LOCAL_PRODUCT_IMAGE_DRAFT_URL_PREFIX}/{int(product_id)}/{quote(filename, safe='')}"


def finalize_product_image_url(product_id: int, image_url: str) -> str:
    draft_path = local_product_image_path_from_url(image_url)
    if not draft_path or not is_product_image_draft_url(image_url):
        return image_url
    target_dir = LOCAL_PRODUCT_IMAGE_DIR / str(product_id)
    target_dir.mkdir(parents=True, exist_ok=True)
    target_name = f"saved-{uuid.uuid4().hex[:12]}{draft_path.suffix.lower()}"
    target_path = target_dir / target_name
    shutil.move(str(draft_path), str(target_path))
    return local_product_image_url(product_id, target_name)


def is_product_image_draft_url(image_url: str) -> bool:
    return normalize_text(image_url).startswith(LOCAL_PRODUCT_IMAGE_DRAFT_URL_PREFIX)


def local_product_image_path_from_url(image_url: str) -> Path | None:
    text = normalize_text(image_url)
    if text.startswith(LOCAL_PRODUCT_IMAGE_DRAFT_URL_PREFIX):
        root_dir = LOCAL_PRODUCT_IMAGE_DRAFT_DIR
        prefix = LOCAL_PRODUCT_IMAGE_DRAFT_URL_PREFIX
    elif text.startswith(LOCAL_PRODUCT_IMAGE_URL_PREFIX):
        root_dir = LOCAL_PRODUCT_IMAGE_DIR
        prefix = LOCAL_PRODUCT_IMAGE_URL_PREFIX
    else:
        return None
    relative = text.removeprefix(prefix).lstrip("/")
    parts = [unquote(part) for part in relative.split("/") if part]
    if len(parts) != 2:
        return None
    product_id, filename = parts
    if not re.fullmatch(r"\d+", product_id):
        return None
    if "/" in filename or "\\" in filename:
        return None
    candidate = (root_dir / product_id / filename).resolve()
    root = root_dir.resolve()
    try:
        candidate.relative_to(root)
    except ValueError:
        return None
    return candidate


def remove_local_product_image_if_unused(image_url: str, current_images: list[str]) -> None:
    if image_url in current_images:
        return
    path = local_product_image_path_from_url(image_url)
    if path and path.exists():
        path.unlink(missing_ok=True)


def clear_product_temp_image_files(product_id: int) -> None:
    for root_dir in (LOCAL_PRODUCT_IMAGE_DIR, LOCAL_PRODUCT_IMAGE_DRAFT_DIR):
        image_dir = (root_dir / str(int(product_id))).resolve()
        root = root_dir.resolve()
        try:
            image_dir.relative_to(root)
        except ValueError:
            continue
        if image_dir.exists() and image_dir.is_dir():
            shutil.rmtree(image_dir, ignore_errors=True)


def cleanup_expired_product_image_drafts() -> int:
    root = LOCAL_PRODUCT_IMAGE_DRAFT_DIR
    if not root.exists():
        return 0
    cutoff = time.time() - settings.product_image_draft_retention_days * 24 * 60 * 60
    deleted_count = 0
    for path in root.rglob("*"):
        if not path.is_file():
            continue
        try:
            if path.stat().st_mtime >= cutoff:
                continue
            path.unlink(missing_ok=True)
            deleted_count += 1
        except OSError:
            continue
    for directory in sorted((item for item in root.rglob("*") if item.is_dir()), key=lambda item: len(item.parts), reverse=True):
        try:
            directory.rmdir()
        except OSError:
            continue
    return deleted_count


def product_image_download_name(product_id: int, image_index: int, image_url: str) -> str:
    try:
        suffix = Path(urlsplit(image_url).path).suffix.lower()
    except Exception:
        suffix = ""
    if suffix not in ALLOWED_PRODUCT_IMAGE_EXTENSIONS:
        suffix = ".jpg"
    return f"product-{product_id}-{image_index + 1}{suffix}"


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
    return create_product_listing_status_sync_task(owner_username, normalized_ids, listing_status)


def update_store_all_products_listing_status(
    owner_username: str,
    store_id: int,
    listing_status: str,
) -> dict[str, Any]:
    return create_listing_status_sync_task(owner_username, store_id, listing_status)


def apply_products_listing_status(
    session: Any,
    products: list[ProductModel],
    listing_status: str,
    *,
    progress_callback: Callable[[int, int, int], None] | None = None,
) -> dict[str, Any]:
    success_ids: list[int] = []
    failed_ids: list[int] = []
    errors: list[str] = []
    credential_cache: dict[int, tuple[str, str]] = {}
    failed_count = 0
    for index, product in enumerate(products, start=1):
        manage_number = normalize_text(product.rakuten_manage_number or product.item_number)
        if not product.store_id:
            errors.append(f"{product.title} 未关联店铺")
            product.last_error = "未关联店铺，不能更新乐天上架状态。"
            failed_ids.append(product.id)
            failed_count += 1
            if progress_callback:
                progress_callback(index, len(success_ids), failed_count)
            continue
        if not manage_number:
            errors.append(f"{product.title} 缺少商品管理编号")
            product.last_error = "缺少商品管理编号，不能更新乐天上架状态。"
            failed_ids.append(product.id)
            failed_count += 1
            if progress_callback:
                progress_callback(index, len(success_ids), failed_count)
            continue

        credentials = credential_cache.get(product.store_id)
        if credentials is None:
            store = session.get(StoreModel, product.store_id)
            if store is None:
                errors.append(f"{product.title} 关联店铺不存在")
                product.last_error = "关联店铺不存在，不能更新乐天上架状态。"
                failed_ids.append(product.id)
                failed_count += 1
                if progress_callback:
                    progress_callback(index, len(success_ids), failed_count)
                continue
            if not store.enabled:
                errors.append(f"{store.alias_name or store.store_name} 已停用")
                product.last_error = "关联店铺已停用，不能更新乐天上架状态。"
                failed_ids.append(product.id)
                failed_count += 1
                if progress_callback:
                    progress_callback(index, len(success_ids), failed_count)
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
            failed_ids.append(product.id)
            failed_count += 1
            if progress_callback:
                progress_callback(index, len(success_ids), failed_count)
            continue

        product.rakuten_listing_status = listing_status
        product.last_error = None
        product.store_product_status = "active"
        product.store_last_seen_at = datetime.now()
        success_ids.append(product.id)
        if progress_callback:
            progress_callback(index, len(success_ids), failed_count)
    return {"successIds": success_ids, "failedIds": failed_ids, "errors": errors}


def sync_task_progress_callback(
    task_id: str | None,
    total_count: int,
    action_label: str,
    *,
    initial_failed: int = 0,
) -> Callable[[int, int, int], None] | None:
    if not task_id:
        return None

    def update(processed_count: int, success_count: int, failed_count: int) -> None:
        update_task_progress(
            SyncTaskModel,
            task_id,
            total_count=total_count,
            success_count=success_count,
            failed_count=initial_failed + failed_count,
            message=f"{action_label}中，已处理 {min(total_count, processed_count + initial_failed)} / {total_count} 条",
        )

    return update


def listing_status_result_summary(result: dict[str, Any], total_count: int) -> dict[str, Any]:
    success_ids = list(result.get("successIds") or [])
    failed_ids = list(result.get("failedIds") or [])
    success_count = len(success_ids)
    failed_count = max(0, int(total_count) - success_count)
    return {
        "total": int(total_count),
        "successCount": success_count,
        "failedCount": failed_count,
        "successIds": success_ids,
        "failedIds": failed_ids,
        "message": f"完成，成功 {success_count} 个，失败 {failed_count} 个",
        "errors": list(result.get("errors") or [])[:20],
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
    if not store_id:
        raise RuntimeError("请选择上架店铺。")
    with session_scope() as session:
        ensure_user_task_capacity(
            session,
            ListingTaskModel,
            owner_username,
            limit=settings.max_running_listing_tasks_per_user,
            label="上架",
        )
        ensure_store_task_capacity(session, int(store_id))
        store = session.get(StoreModel, store_id)
        if store is None:
            raise RuntimeError("上架店铺不存在。")
        if store.owner_username != owner_username:
            raise RuntimeError("不能使用其他用户的店铺上架。")
        if not store.enabled:
            raise RuntimeError("上架店铺已停用。")
        products = session.scalars(
            select(ProductModel).where(
                ProductModel.owner_username == owner_username,
                ProductModel.id.in_(product_ids),
            )
        ).all()
        if not products:
            raise RuntimeError("没有找到可上架的商品。")
        found_ids = {product.id for product in products}
        missing_ids = [product_id for product_id in product_ids if product_id not in found_ids]
        if missing_ids:
            raise RuntimeError("部分商品不存在，不能创建上架任务。")
        invalid_products = [product for product in products if product.review_status not in {"approved", "listed_master"} or product.listing_task_id]
        if invalid_products:
            names = "、".join(productCodeForError(product) for product in invalid_products[:5])
            raise RuntimeError(f"只有已审核或已上架管理商品可以创建上架任务，且商品不能正在上架中。异常商品：{names}")
        duplicated_products = [
            product for product in products
            if any(int(item.get("storeId") or 0) == int(store.id) for item in product_listed_stores(product_raw_payload(product)))
        ]
        if duplicated_products:
            names = "、".join(productCodeForError(product) for product in duplicated_products[:5])
            raise RuntimeError(f"以下商品已上架过该店铺，请选择其他店铺：{names}")
        task_id = uuid.uuid4().hex
        for product in products:
            product.listing_task_id = task_id
            product.last_error = None
        task = ListingTaskModel(
            id=task_id,
            owner_username=owner_username,
            store_id=store.id,
            task_name=task_name or f"上架任务 {datetime.now():%Y-%m-%d %H:%M}",
            status="running",
            total_count=len(products),
            success_count=0,
            failed_count=0,
            product_ids_json=json.dumps([product.id for product in products], ensure_ascii=False),
            message="正在同步到乐天",
            started_at=datetime.now(),
        )
        session.add(task)
        session.flush()

    dispatch_listing_task(owner_username, task_id)
    with session_scope() as session:
        task = session.get(ListingTaskModel, task_id)
        return listing_task_to_public(task) if task else {"id": task_id}


def run_listing_task(owner_username: str, task_id: str) -> None:
    try:
        _run_listing_task(owner_username, task_id)
    except Exception as exc:
        fail_listing_task_unexpectedly(owner_username, task_id, exc)


def _run_listing_task(owner_username: str, task_id: str) -> None:
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
        store = session.get(StoreModel, task.store_id) if task.store_id else None
        products = session.scalars(
            select(ProductModel).where(
                ProductModel.owner_username == owner_username,
                ProductModel.id.in_(product_ids or [-1]),
            )
        ).all()
        success_count = 0
        failed_count = 0
        success_ids: list[int] = []
        failed_ids: list[int] = []
        errors: list[str] = []
        if store is None:
            task.status = "failed"
            task.failed_count = len(products)
            task.total_count = len(products)
            task.message = "上架店铺不存在"
            task.error_detail = task.message
            task.finished_at = datetime.now()
            task.product_ids_json = json.dumps({"productIds": product_ids, "successIds": [], "failedIds": product_ids}, ensure_ascii=False)
            for product in products:
                product.last_error = task.message
                clear_listing_product_lock(product, task_id)
            return
        if not store.enabled:
            task.status = "failed"
            task.failed_count = len(products)
            task.total_count = len(products)
            task.message = "上架店铺已停用"
            task.error_detail = task.message
            task.finished_at = datetime.now()
            task.product_ids_json = json.dumps({"productIds": product_ids, "successIds": [], "failedIds": product_ids}, ensure_ascii=False)
            for product in products:
                product.last_error = task.message
                clear_listing_product_lock(product, task_id)
            return
        service_secret = decrypt_text(store.rakuten_service_secret_encrypted)
        license_key = decrypt_text(store.rakuten_license_key_encrypted)
        if not service_secret or not license_key:
            task.status = "failed"
            task.failed_count = len(products)
            task.total_count = len(products)
            task.message = "上架店铺缺少乐天 Secret 或乐天 Key"
            task.error_detail = task.message
            task.finished_at = datetime.now()
            task.product_ids_json = json.dumps({"productIds": product_ids, "successIds": [], "failedIds": product_ids}, ensure_ascii=False)
            for product in products:
                product.last_error = task.message
                clear_listing_product_lock(product, task_id)
            return
        cabinet_context: dict[str, Any] = {}
        try:
            cabinet_usage = fetch_rakuten_cabinet_usage(service_secret, license_key)
            cabinet_context["usage"] = cabinet_usage
            apply_store_cabinet_usage(store, cabinet_usage)
        except Exception as exc:
            errors.append(f"R-Cabinet 使用量检测失败: {exc}")
        task.total_count = len(products)
        task.success_count = 0
        task.failed_count = 0
        task.message = f"上架中，已处理 0 / {len(products)} 条"
        session.flush()
        for index, product in enumerate(products, start=1):
            if product.review_status not in {"approved", "listed_master"} or product.listing_task_id != task_id:
                product.last_error = "商品状态已变化或不属于当前上架任务，不能上架。"
                clear_listing_product_lock(product, task_id)
                failed_count += 1
                failed_ids.append(product.id)
                errors.append(f"{productCodeForError(product)}: {product.last_error}")
                task.total_count = len(products)
                task.success_count = success_count
                task.failed_count = failed_count
                task.message = f"上架中，已处理 {index} / {len(products)} 条"
                session.flush()
                continue
            try:
                listing_result = create_store_product_on_rakuten(
                    service_secret,
                    license_key,
                    store,
                    product,
                    cabinet_context=cabinet_context,
                )
                listed_product = upsert_listed_store_product_from_listing_result(session, owner_username, product, store, listing_result)
                session.flush()
                record_product_listed_store(product, listed_product, store, listing_result)
                success_count += 1
                success_ids.append(product.id)
            except Exception as exc:
                error_text = str(exc)
                clear_listing_product_lock(product, task_id)
                product.last_error = error_text
                failed_count += 1
                failed_ids.append(product.id)
                errors.append(f"{productCodeForError(product)}: {error_text}")
            task.total_count = len(products)
            task.success_count = success_count
            task.failed_count = failed_count
            task.message = f"上架中，已处理 {index} / {len(products)} 条"
            session.flush()
        sync_store_cabinet_usage_fields(store, service_secret, license_key)
        task.total_count = len(products)
        task.success_count = success_count
        task.failed_count = failed_count
        if success_count and failed_count:
            task.status = "partial"
        elif success_count:
            task.status = "success"
        else:
            task.status = "failed"
        task.message = f"完成，上架 {success_count} 条，异常 {failed_count} 条"
        task.error_detail = "\n".join(errors[:50]) if errors else None
        task.product_ids_json = json.dumps({"productIds": product_ids, "successIds": success_ids, "failedIds": failed_ids}, ensure_ascii=False)
        task.finished_at = datetime.now()


def clear_listing_product_lock(product: ProductModel, task_id: str | None = None) -> None:
    if task_id is None or product.listing_task_id == task_id:
        product.listing_task_id = None


def fail_listing_task_unexpectedly(owner_username: str, task_id: str, exc: Exception) -> None:
    message = str(exc) or "上架任务执行失败。"
    with session_scope() as session:
        task = session.get(ListingTaskModel, task_id)
        if task is not None and task.owner_username == owner_username:
            try:
                product_ids = json.loads(task.product_ids_json or "[]")
            except ValueError:
                product_ids = []
            if isinstance(product_ids, dict):
                product_ids = product_ids.get("productIds") if isinstance(product_ids.get("productIds"), list) else []
            products = session.scalars(
                select(ProductModel).where(
                    ProductModel.owner_username == owner_username,
                    ProductModel.id.in_(product_ids or [-1]),
                )
            ).all()
            for product in products:
                clear_listing_product_lock(product, task_id)
                product.last_error = message
            task.status = "failed"
            task.failed_count = len(products)
            task.total_count = len(products)
            task.message = "上架失败"
            task.error_detail = message
            task.product_ids_json = json.dumps({"productIds": product_ids, "successIds": [], "failedIds": product_ids}, ensure_ascii=False)
            task.finished_at = datetime.now()
    log_event(owner_username, task_id, "error", message)


def retry_listing_task(owner_username: str, task_id: str) -> dict[str, Any]:
    with session_scope() as session:
        task = session.get(ListingTaskModel, task_id)
        if task is None:
            raise RuntimeError("上架任务不存在。")
        if task.owner_username != owner_username:
            raise RuntimeError("不能重试其他用户的上架任务。")
        ensure_user_task_capacity(
            session,
            ListingTaskModel,
            owner_username,
            limit=settings.max_running_listing_tasks_per_user,
            label="上架",
            exclude_task_id=task_id,
        )
        ensure_store_task_capacity(session, task.store_id, exclude_listing_task_id=task_id)
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
        for product in products:
            if product.review_status in {"approved", "listed_master"} and not product.listing_task_id:
                product.listing_task_id = task_id
                product.last_error = None
        task.status = "running"
        task.message = "重新执行中"
        task.error_detail = None
        task.started_at = datetime.now()
        task.finished_at = None
    dispatch_listing_task(owner_username, task_id)
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
        ensure_user_task_capacity(
            session,
            CrawlTaskModel,
            owner_username,
            limit=settings.max_running_crawl_tasks_per_user,
            label="采集",
        )
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
        elif source_type == "shop":
            parsed_target, existing_limit, existing_period = parse_ranking_target(strip_shop_ranking_prefix(target))
            raw_target = normalize_rakuten_shop_target(parsed_target)
            if not raw_target:
                raise RuntimeError(RAKUTEN_SHOP_TARGET_ERROR)
            period_label = ranking_period_label(getattr(payload, "rankingPeriod", None) or existing_period)
            limit_label = crawl_limit_label(
                getattr(payload, "crawlLimit", None),
                default="全部" if existing_limit is None else f"前 {existing_limit}",
            )
            target = f"店铺:{raw_target} {period_label} {limit_label}"
        task = CrawlTaskModel(
            id=uuid.uuid4().hex,
            owner_username=owner_username,
            source_id=source.id if source else None,
            source_type=source_type,
            target=target,
            mode=str(getattr(payload, "mode", "") or "manual"),
            status="queued",
            total_count=initial_crawl_task_total_count(source_type, target),
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
        ensure_user_task_capacity(
            session,
            CrawlTaskModel,
            owner_username,
            limit=settings.max_running_crawl_tasks_per_user,
            label="采集",
            exclude_task_id=task_id,
        )
        task.status = "queued"
        task.total_count = initial_crawl_task_total_count(task.source_type, task.target)
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
        if task.total_count <= 0:
            task.total_count = initial_crawl_task_total_count(task.source_type, task.target)
        owner_username = task.owner_username
        source_type = task.source_type
        target = task.target

    try:
        items = collect_items(source_type, target, task_id=task_id)
        update_task_progress(
            CrawlTaskModel,
            task_id,
            total_count=len(items),
            success_count=0,
            failed_count=0,
            message=f"采集中，已处理 0 / {len(items)} 条",
        )
        success_count = 0
        failed_count = 0
        saved_count = 0
        errors: list[str] = []
        if not items:
            errors.append("未采集到商品，请检查采集内容、榜单时间或乐天页面结构。")
        batch_size = max(1, int(settings.crawler_batch_size))
        processed_count = 0
        batches = list(chunk_items(items, batch_size))
        for batch_index, batch_items in enumerate(batches, start=1):
            for item in batch_items:
                processed_count += 1
                item_error = collected_item_error(item)
                save_result = save_collected_item(owner_username, task_id, item)
                saved = bool(save_result.get("saved"))
                save_error = normalize_text(save_result.get("error"))
                if saved:
                    saved_count += 1
                if saved and item_error is None and not save_error:
                    success_count += 1
                else:
                    failed_count += 1
                    if item_error:
                        errors.append(item_error)
                    elif save_error:
                        errors.append(save_error)
                    elif not saved:
                        name = str(item.get("title") or item.get("source_url") or "商品").strip()
                        errors.append(f"{name}: 商品未保存，可能缺少商品标题、商品链接，或已存在于店铺商品中。")
                update_task_progress(
                    CrawlTaskModel,
                    task_id,
                    total_count=len(items),
                    success_count=success_count,
                    failed_count=failed_count,
                    message=f"采集中，批次 {batch_index} / {len(batches)}，已处理 {processed_count} / {len(items)} 条",
                )
            if batch_index < len(batches) and settings.crawler_batch_pause_seconds > 0:
                time.sleep(settings.crawler_batch_pause_seconds)
        with session_scope() as session:
            task = session.get(CrawlTaskModel, task_id)
            if task is None:
                return
            task.success_count = success_count
            task.failed_count = failed_count
            task.status = resolve_crawl_task_status("failed" if not items else "success", len(items), success_count, failed_count)
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


def save_collected_item(owner_username: str, task_id: str, item: dict[str, Any]) -> dict[str, Any]:
    product_id: int | None = None
    with session_scope() as session:
        saved = upsert_product(session, owner_username, task_id, item)
        if saved:
            session.flush()
            source_url = str(item.get("source_url") or "").strip()
            if source_url:
                product = session.scalar(
                    select(ProductModel).where(
                        ProductModel.owner_username == owner_username,
                        ProductModel.source_url_hash == make_source_url_hash(source_url),
                    )
                )
                product_id = product.id if product is not None else None
        else:
            return {"saved": False, "error": ""}
    image_error = ""
    if product_id is not None:
        try:
            image_error = localize_collected_product_images(owner_username, product_id)
        except Exception as exc:
            image_error = f"图片本地化失败：{exc}"
            mark_product_local_image_error(owner_username, product_id, image_error)
    return {"saved": True, "error": image_error}


def chunk_items(items: list[dict[str, Any]], batch_size: int) -> list[list[dict[str, Any]]]:
    normalized_size = max(1, int(batch_size or 1))
    return [items[index:index + normalized_size] for index in range(0, len(items), normalized_size)]


def initial_crawl_task_total_count(source_type: str, target: str) -> int:
    normalized_source_type = normalize_text(source_type)
    normalized_target = normalize_text(target)
    if not normalized_source_type or not normalized_target:
        return 0
    if normalized_source_type == "product_url":
        return 1
    if normalized_source_type in {"shop", "ranking"}:
        _, limit, _ = parse_ranking_target(
            strip_shop_ranking_prefix(normalized_target) if normalized_source_type == "shop" else normalized_target
        )
        return int(limit or 0)
    return 0


def collect_items(source_type: str, target: str, *, task_id: str | None = None) -> list[dict[str, Any]]:
    if source_type == "product_url":
        return [collect_product_detail(normalize_rakuten_product_target(target))]
    limit: int | None = 30
    shop_code_filter = ""
    if source_type == "shop":
        target, limit, period = parse_ranking_target(strip_shop_ranking_prefix(target))
        normalized_shop_target = normalize_rakuten_shop_target(target)
        if looks_like_rakuten_shop_code(normalized_shop_target):
            shop_code_filter = normalize_shop_code(normalized_shop_target)
        target = resolve_rakuten_shop_search_keyword(target)
    elif source_type == "ranking":
        target, limit, period = parse_ranking_target(target)
    else:
        period = "daily"
    if source_type == "ranking":
        url = build_ranking_source_url(target, period)
    elif source_type == "shop" and period == "realtime":
        url = build_ranking_source_url(target, period)
    else:
        url = build_source_url(source_type, target)
        if source_type == "shop":
            url = build_ranking_source_url(target, period)
    listing_limit = None if shop_code_filter else limit
    items = collect_listing_items(url, listing_limit)
    if source_type in {"ranking", "shop"} and period == "realtime":
        keyword = normalize_text(target).lower()
        items = [item for item in items if keyword in normalize_text(item.get("title")).lower()]
    if source_type == "shop" and shop_code_filter:
        items = [item for item in items if product_url_shop_code(item.get("source_url")) == shop_code_filter]
    limited_items = items if limit is None else items[:limit]
    if task_id:
        update_task_progress(
            CrawlTaskModel,
            task_id,
            total_count=len(limited_items),
            message=f"已发现 {len(limited_items)} 个商品，开始采集详情",
        )
    return enrich_collected_items_with_detail(limited_items)


def collect_listing_items(url: str, requested_limit: int | None) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    seen: set[str] = set()
    ranking_total: int | None = None
    page_number = 1
    while page_number <= settings.crawler_max_ranking_pages:
        page_url = ranking_page_url(url, page_number)
        html = fetch_listing_html(page_url)
        assert_listing_page_available(html, page_url)
        if ranking_total is None:
            ranking_total = parse_ranking_total_count(html)
        page_items = parse_search_items(html, page_url)
        new_count = 0
        for item in page_items:
            source_url = normalize_text(item.get("source_url"))
            if not source_url or source_url in seen:
                continue
            seen.add(source_url)
            items.append(item)
            new_count += 1
            if requested_limit is not None and len(items) >= requested_limit:
                return items
            if ranking_total is not None and len(items) >= ranking_total:
                return items
        if not should_fetch_next_ranking_page(
            page_items=page_items,
            new_count=new_count,
            collected_count=len(items),
            requested_limit=requested_limit,
            ranking_total=ranking_total,
        ):
            break
        page_number += 1
    return items


def should_fetch_next_ranking_page(
    *,
    page_items: list[dict[str, Any]],
    new_count: int,
    collected_count: int,
    requested_limit: int | None,
    ranking_total: int | None,
) -> bool:
    if not page_items or new_count <= 0:
        return False
    if requested_limit is not None and collected_count >= requested_limit:
        return False
    if ranking_total is not None:
        return collected_count < ranking_total
    return requested_limit is None or collected_count < requested_limit


def parse_ranking_total_count(html: str) -> int | None:
    text = normalize_text(BeautifulSoup(html or "", "lxml").get_text(" ", strip=True))
    patterns = (
        r"(?:共|全)\s*([0-9,，]+)\s*(?:个|件)",
        r"\(\s*(?:共|全)\s*([0-9,，]+)\s*(?:个|件)\s*\)",
        r"([0-9,，]+)\s*(?:個|件)\s*(?:中|$)",
    )
    for pattern in patterns:
        match = re.search(pattern, text)
        if not match:
            continue
        number = int(match.group(1).replace(",", "").replace("，", ""))
        if number > 0:
            return number
    return None


def ranking_page_url(url: str, page_number: int) -> str:
    if page_number <= 1:
        return url
    parsed = urlsplit(url)
    query = parse_qs(parsed.query, keep_blank_values=True)
    query["p"] = [str(page_number)]
    return urlunsplit((parsed.scheme, parsed.netloc, parsed.path, urlencode(query, doseq=True), parsed.fragment))


def product_url_shop_code(source_url: Any) -> str:
    parsed = parse_rakuten_product_target(normalize_text(source_url))
    return normalize_shop_code(parsed[0]) if parsed else ""


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
    match = re.search(r"(?:^|\s)前\s*([0-9]{1,5})\s*$", normalized)
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
    return normalized, None if limit is None else max(1, limit), period


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


def crawler_browser_headers(url: str = "") -> dict[str, str]:
    parsed = urlsplit(url) if url else None
    origin = f"{parsed.scheme}://{parsed.netloc}" if parsed and parsed.scheme and parsed.netloc else "https://www.rakuten.co.jp"
    return {
        "User-Agent": settings.crawler_user_agent,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
        "Accept-Language": "ja-JP,ja;q=0.9,zh-CN;q=0.8,zh;q=0.7,en-US;q=0.6,en;q=0.5",
        "Accept-Encoding": "gzip, deflate",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
        "Connection": "keep-alive",
        "DNT": "1",
        "Referer": origin + "/",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "same-origin",
        "Sec-Fetch-User": "?1",
        "Upgrade-Insecure-Requests": "1",
    }


def crawler_request_proxies() -> dict[str, str] | None:
    proxy_url = normalize_text(settings.crawler_proxy_url)
    if not proxy_url:
        return None
    return {"http": proxy_url, "https": proxy_url}


def crawler_delay_seconds() -> float:
    min_ms = max(0, int(settings.crawler_min_delay_ms or 0))
    max_ms = max(min_ms, int(settings.crawler_max_delay_ms or min_ms))
    if max_ms <= 0:
        return 0.0
    return random.uniform(min_ms / 1000, max_ms / 1000)


def throttle_crawler_request() -> None:
    global CRAWLER_LAST_REQUEST_AT
    delay = crawler_delay_seconds()
    if delay <= 0:
        return
    with CRAWLER_REQUEST_LOCK:
        elapsed = time.monotonic() - CRAWLER_LAST_REQUEST_AT
        wait_seconds = max(0.0, delay - elapsed)
        if wait_seconds:
            time.sleep(wait_seconds)
        CRAWLER_LAST_REQUEST_AT = time.monotonic()


def crawler_backoff_seconds(attempt: int) -> float:
    return min(12.0, (1.5 ** max(0, attempt - 1)) + random.uniform(0.2, 1.2))


def get_crawler_session() -> requests.Session:
    session = getattr(CRAWLER_SESSION_LOCAL, "session", None)
    if isinstance(session, requests.Session):
        return session
    session = requests.Session()
    session.headers.update(crawler_browser_headers())
    CRAWLER_SESSION_LOCAL.session = session
    CRAWLER_SESSION_LOCAL.warmed = False
    return session


def warmup_crawler_session(session: requests.Session) -> None:
    if getattr(CRAWLER_SESSION_LOCAL, "warmed", False):
        return
    CRAWLER_SESSION_LOCAL.warmed = True
    warmup_url = normalize_text(settings.crawler_warmup_url)
    if not warmup_url:
        return
    try:
        throttle_crawler_request()
        session.get(
            warmup_url,
            timeout=settings.crawler_timeout_seconds,
            headers=crawler_browser_headers(warmup_url),
            proxies=crawler_request_proxies(),
        )
    except requests.RequestException:
        return


def fetch_html(url: str) -> str:
    session = get_crawler_session()
    warmup_crawler_session(session)
    max_attempts = max(1, int(settings.crawler_max_retries or 0) + 1)
    last_exc: Exception | None = None
    for attempt in range(1, max_attempts + 1):
        try:
            throttle_crawler_request()
            response = session.get(
                url,
                timeout=settings.crawler_timeout_seconds,
                headers=crawler_browser_headers(url),
                proxies=crawler_request_proxies(),
            )
            if response.status_code in CRAWLER_HTTP_RETRY_STATUS_CODES and attempt < max_attempts:
                time.sleep(crawler_backoff_seconds(attempt))
                continue
            response.raise_for_status()
            response.encoding = response.encoding or response.apparent_encoding
            html = response.text
            if is_rakuten_access_limited_page(html) and attempt < max_attempts:
                time.sleep(crawler_backoff_seconds(attempt))
                continue
            return html
        except requests.RequestException as exc:
            last_exc = exc
            if attempt >= max_attempts:
                raise
            time.sleep(crawler_backoff_seconds(attempt))
    if last_exc:
        raise last_exc
    raise RuntimeError("乐天页面采集失败。")


def fetch_listing_html(url: str) -> str:
    try:
        html = fetch_html(url)
    except requests.RequestException as exc:
        if not settings.crawler_browser_fallback_enabled:
            raise
        try:
            return fetch_html_with_browser(url)
        except Exception as browser_exc:
            raise RuntimeError(f"乐天列表页采集失败：{exc}；浏览器兜底采集失败：{browser_exc}") from browser_exc
    if should_retry_listing_with_browser(html):
        try:
            return fetch_html_with_browser(url)
        except Exception as browser_exc:
            if is_rakuten_access_limited_page(html):
                raise RuntimeError(f"乐天列表页返回访问集中/拦截页，浏览器兜底采集失败：{browser_exc}") from browser_exc
            return html
    return html


def should_retry_listing_with_browser(html: str) -> bool:
    if not settings.crawler_browser_fallback_enabled:
        return False
    if is_blocked_or_empty_rakuten_html(html):
        return True
    if "item.rakuten.co.jp" not in (html or "") and "brandavenue.rakuten.co.jp/item/" not in (html or ""):
        return True
    if len(html or "") < 2000:
        text = normalize_text(BeautifulSoup(html or "", "lxml").get_text(" ", strip=True))
        return "Reference #" in text or "楽天" not in text
    return False


def assert_listing_page_available(html: str, url: str) -> None:
    text = normalize_text(BeautifulSoup(html or "", "lxml").get_text(" ", strip=True))
    if is_rakuten_access_limited_page(html):
        raise RuntimeError(f"乐天排行页当前返回访问集中/拦截页，无法采集：{url}")
    if not text:
        raise RuntimeError(f"乐天列表页为空，无法采集：{url}")


def is_rakuten_access_limited_page(html: str) -> bool:
    text = normalize_text(BeautifulSoup(html or "", "lxml").get_text(" ", strip=True))
    return "アクセスが集中しております" in text or re.search(r"Reference\s+#", text) is not None


def parse_search_items(html: str, page_url: str) -> list[dict[str, Any]]:
    soup = BeautifulSoup(html, "lxml")
    items: list[dict[str, Any]] = []
    seen: set[str] = set()
    for item in parse_search_items_from_json_ld(soup, page_url):
        source_url = normalize_text(item.get("source_url"))
        if not source_url or source_url in seen:
            continue
        seen.add(source_url)
        items.append(item)
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


def parse_search_items_from_json_ld(soup: BeautifulSoup, page_url: str) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    for entry in extract_json_ld_objects(soup):
        entry_type = entry.get("@type")
        if entry_type == "ItemList" or (isinstance(entry_type, list) and "ItemList" in entry_type):
            for product in json_ld_item_list_products(entry):
                item = search_item_from_json_ld_product(product, page_url)
                if item:
                    items.append(item)
            continue
        if entry_type == "Product" or (isinstance(entry_type, list) and "Product" in entry_type):
            item = search_item_from_json_ld_product(entry, page_url)
            if item:
                items.append(item)
    return items


def json_ld_item_list_products(item_list: dict[str, Any]) -> list[dict[str, Any]]:
    values = item_list.get("itemListElement")
    if not isinstance(values, list):
        return []
    products: list[dict[str, Any]] = []
    for value in values:
        if not isinstance(value, dict):
            continue
        product = value.get("item") if isinstance(value.get("item"), dict) else value
        if isinstance(product, dict):
            products.append(product)
    return products


def search_item_from_json_ld_product(product: dict[str, Any], page_url: str) -> dict[str, Any] | None:
    href = normalize_product_href(
        first_text_from_keys(product, ("url", "@id", "itemUrl", "itemPageUrl")),
        page_url,
    )
    if not href:
        return None
    title = first_text_from_keys(product, ("name", "itemName", "title")) or href
    offers = product.get("offers") if isinstance(product.get("offers"), dict) else {}
    image_url = normalize_product_image_url(first_url_from_keys(product, ("image", "imageUrl", "thumbnailUrl")))
    return {
        "title": title[:500],
        "source_url": href,
        "image_url": image_url,
        "price": price_from_rakuten_item(offers) if isinstance(offers, dict) else price_from_rakuten_item(product),
        "shop_name": "",
        "item_number": extract_item_number(href),
        "genre_id": "",
        "raw": {"pageUrl": page_url, "listSource": "json_ld"},
    }


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
    try:
        return collect_product_detail_from_html(normalized_url, fetch_html(normalized_url), source="http")
    except Exception as exc:
        if not should_retry_product_detail_with_browser(exc):
            raise
        try:
            html = fetch_html_with_browser(normalized_url)
        except Exception as browser_exc:
            raise RuntimeError(f"{exc}；浏览器兜底采集失败：{browser_exc}") from browser_exc
        try:
            return collect_product_detail_from_html(normalized_url, html, source="browser")
        except Exception as browser_parse_exc:
            raise RuntimeError(f"{exc}；浏览器兜底采集后仍无法解析：{browser_parse_exc}") from browser_parse_exc


def collect_product_detail_from_html(normalized_url: str, html: str, *, source: str = "http") -> dict[str, Any]:
    soup = BeautifulSoup(html, "lxml")
    if parse_rakuten_fashion_product_code(normalized_url):
        result = collect_rakuten_fashion_product_detail(normalized_url, html, soup)
    else:
        result = collect_rakuten_market_product_detail(normalized_url, html, soup)
    raw = result.get("raw")
    if isinstance(raw, dict):
        raw["detailFetchSource"] = source
    return result


def should_retry_product_detail_with_browser(exc: Exception) -> bool:
    if not settings.crawler_browser_fallback_enabled:
        return False
    text = str(exc)
    retry_markers = (
        "拦截页",
        "后端 HTTP 直接采集",
        "页面被拦截",
        "页面模板不支持",
        "未能从乐天商品详情页解析到",
        "403",
        "429",
    )
    return any(marker in text for marker in retry_markers)


def fetch_html_with_browser(url: str) -> str:
    try:
        from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
        from playwright.sync_api import sync_playwright
    except ImportError as exc:
        raise RuntimeError("服务器未安装 Playwright，请执行 pip install -r requirements.txt，并运行 python -m playwright install chromium。") from exc

    timeout_ms = max(5000, int(settings.crawler_browser_timeout_seconds) * 1000)
    try:
        with sync_playwright() as playwright:
            browser = playwright.chromium.launch(headless=True)
            try:
                context = browser.new_context(
                    user_agent=settings.crawler_user_agent,
                    locale="ja-JP",
                    timezone_id="Asia/Tokyo",
                    viewport={"width": 1366, "height": 900},
                    extra_http_headers={
                        "Accept-Language": "ja-JP,ja;q=0.9,en-US;q=0.8,en;q=0.7",
                    },
                )
                page = context.new_page()
                page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)
                try:
                    page.wait_for_load_state("networkidle", timeout=min(timeout_ms, 12000))
                except PlaywrightTimeoutError:
                    pass
                html = page.content()
                context.close()
                return html
            finally:
                browser.close()
    except PlaywrightTimeoutError as exc:
        raise RuntimeError("浏览器加载乐天页面超时。") from exc
    except Exception as exc:
        message = normalize_text(str(exc))
        if "Executable doesn't exist" in message or "playwright install" in message:
            raise RuntimeError("Playwright 浏览器内核未安装，请运行 python -m playwright install chromium。") from exc
        raise


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
    tagline = first_text_from_keys(embedded_item, RAKUTEN_TAGLINE_KEYS)
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
        "tagline": tagline,
        "catchCopyTrans": first_text_from_keys(embedded_item, ("catchCopyTrans",)) or tagline,
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
        "tagline": first_text_from_keys(product, RAKUTEN_TAGLINE_KEYS),
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
        if '"itemInfoSku"' not in text:
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
                return merge_rakuten_market_embedded_payload(value, payload)
    return {}


def merge_rakuten_market_embedded_payload(item_info: dict[str, Any], payload: dict[str, Any]) -> dict[str, Any]:
    result = dict(item_info)
    result["embeddedPayload"] = payload
    for key in (
        "title",
        "itemName",
        "name",
        "tagline",
        "catchcopy",
        "catchCopy",
        "catchCopyTrans",
        "subTitle",
        "subtitle",
        "newProductDescription",
        "salesDescription",
        "rCategoryId",
        "genreId",
        "manageNumber",
    ):
        if not has_description_source(result.get(key)):
            value = first_value_by_key(payload, key)
            if has_description_source(value):
                result[key] = value
    pc_fields = result.get("pcFields") if isinstance(result.get("pcFields"), dict) else {}
    pc_description = pc_fields.get("productDescription") if isinstance(pc_fields, dict) else None
    if not has_description_source(pc_description):
        value = first_value_by_key(payload, "productDescription")
        if has_description_source(value):
            next_pc_fields = dict(pc_fields)
            next_pc_fields["productDescription"] = value
            result["pcFields"] = next_pc_fields
    return result


def first_value_by_key(source: Any, target_key: str) -> Any:
    if isinstance(source, dict):
        if target_key in source and has_description_source(source.get(target_key)):
            return source.get(target_key)
        for child in source.values():
            value = first_value_by_key(child, target_key)
            if has_description_source(value):
                return value
    elif isinstance(source, list):
        for child in source:
            value = first_value_by_key(child, target_key)
            if has_description_source(value):
                return value
    return None


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
            ("スマートフォン用 商品説明文", embedded_item.get("newProductDescription")),
            ("PC用 商品説明文", (embedded_item.get("pcFields") or {}).get("productDescription") if isinstance(embedded_item.get("pcFields"), dict) else None),
            ("PC用 販売説明文", embedded_item.get("salesDescription")),
        )
        for label, value in embedded_descriptions:
            html_value = normalize_detail_html(value)
            if html_value and all(description_content_key(item["value"]) != description_content_key(html_value) for item in descriptions):
                descriptions.append({"label": label, "value": html_value})
    for selector, label in (
        ("#itemCaption", "商品说明"),
        ("#itemDescription", "商品详情"),
        ("[class*='description']", "商品说明"),
    ):
        node = soup.select_one(selector)
        if node:
            text_value = strip_low_quality_description_lines(str(node))
            if text_value and all(description_content_key(item["value"]) != description_content_key(text_value) for item in descriptions):
                descriptions.append({"label": label, "value": text_value})
    return clean_market_product_descriptions(descriptions, keep_empty_labels=RAKUTEN_DESCRIPTION_FIELD_LABELS)


RAKUTEN_DESCRIPTION_FIELD_LABELS = (
    "PC用 商品説明文",
    "スマートフォン用 商品説明文",
    "PC用 販売説明文",
    "PC用商品说明文",
    "智能手机用商品说明文",
    "PC用销售说明文",
)


RAKUTEN_STANDARD_DESCRIPTION_LABELS = (
    "PC用 商品説明文",
    "スマートフォン用 商品説明文",
    "PC用 販売説明文",
)


LOW_QUALITY_DESCRIPTION_KEYWORDS = (
    "キャンセルポリシー",
    "メーカー希望小売価格",
    "メーカーカタログ",
    "メーカーサイトに基づいて掲載",
    "メーカーサイトTOP",
    "メーカーサイト会社概要",
    "特定商取引法表示",
    "会社概要",
    "有効期間",
    "年間ランキング",
    "ランキング",
    "受賞",
    "買い物かご",
    "商品レビュー",
    "ショップレビュー",
)
PRODUCT_DESCRIPTION_KEYWORDS = (
    "商品ポイント",
    "デザイン",
    "シルエット",
    "コーディネート",
    "サイズ",
    "カラー",
    "素材",
    "原産国",
    "生産国",
    "商品名",
    "商品コード",
    "洗濯",
    "着用",
    "アイテム",
    "セットアップ",
    "パンツ",
    "トップス",
    "ウエスト",
    "伸縮",
    "仕様",
    "重量",
    "長さ",
)


def clean_market_product_descriptions(
    descriptions: list[dict[str, str]],
    *,
    keep_empty_labels: tuple[str, ...] = (),
) -> list[dict[str, str]]:
    if not descriptions:
        return []
    normalized: list[dict[str, str]] = []
    seen_by_content: dict[str, int] = {}
    seen_empty_official: set[str] = set()
    keep_empty_label_set = {normalize_text(label) for label in keep_empty_labels}
    for item in descriptions:
        label = normalize_text(item.get("label")) or "商品说明"
        is_official_rakuten_field = label in keep_empty_label_set
        value = normalize_listing_detail_html(item.get("value")) if is_official_rakuten_field else normalize_detail_html(item.get("value"))
        if not is_official_rakuten_field:
            value = strip_low_quality_description_lines(value)
        value_key = description_content_key(value)
        if (not value or not value_key) and not is_official_rakuten_field:
            continue
        if not is_official_rakuten_field and is_low_quality_product_description(value):
            continue
        if not value_key:
            if label in seen_empty_official:
                continue
            seen_empty_official.add(label)
            normalized.append({"label": label, "value": value})
            continue
        content_key = f"{label}\0{value_key}" if is_official_rakuten_field else value_key
        previous_index = seen_by_content.get(content_key)
        current = {"label": label, "value": value}
        if previous_index is not None:
            previous = normalized[previous_index]
            if description_label_priority(label, keep_empty_label_set) > description_label_priority(previous["label"], keep_empty_label_set):
                normalized[previous_index] = current
            continue
        seen_by_content[content_key] = len(normalized)
        normalized.append(current)
    if not normalized:
        return []
    return normalized


def normalize_rakuten_description_fields(descriptions: list[dict[str, str]]) -> list[dict[str, str]]:
    fields: dict[str, str] = {label: "" for label in RAKUTEN_STANDARD_DESCRIPTION_LABELS}
    extras: list[dict[str, str]] = []
    for item in descriptions:
        label = normalize_text(item.get("label")) or "商品说明"
        target_label = standard_rakuten_description_label(label)
        value = normalize_listing_detail_html(item.get("value")) if target_label else normalize_detail_html(item.get("value"))
        if target_label:
            if value and not fields[target_label]:
                fields[target_label] = value
            continue
        if value:
            extras.append({"label": label, "value": value})

    for item in extras:
        label = item["label"]
        value = item["value"]
        target_label = fallback_rakuten_description_label(label, value)
        if value and not fields[target_label]:
            fields[target_label] = value

    return [{"label": label, "value": fields[label]} for label in RAKUTEN_STANDARD_DESCRIPTION_LABELS]


def standard_rakuten_description_label(label: str) -> str:
    normalized = normalize_text(label).replace(" ", "")
    if normalized in {"PC用商品説明文", "PC用商品说明文", "PC商品说明", "PC用商品説明"}:
        return "PC用 商品説明文"
    if normalized in {"スマートフォン用商品説明文", "スマートフォン用商品说明文", "智能手机商品说明", "智能手机用商品说明文", "移动端商品说明"}:
        return "スマートフォン用 商品説明文"
    if normalized in {"PC用販売説明文", "PC用销售说明文", "销售说明", "販売説明文"}:
        return "PC用 販売説明文"
    return ""


def fallback_rakuten_description_label(label: str, value: str) -> str:
    normalized = normalize_text(label)
    if "販売" in normalized or "销售" in normalized or "sale" in normalized.lower():
        return "PC用 販売説明文"
    if "スマートフォン" in normalized or "智能手机" in normalized or "移动" in normalized:
        return "スマートフォン用 商品説明文"
    return "PC用 商品説明文"


def description_label_priority(label: str, official_labels: set[str]) -> int:
    normalized_label = normalize_text(label)
    if normalized_label in official_labels:
        return 20
    if normalized_label in {"结构化商品说明", "商品详情", "商品说明"}:
        return 10
    return 0


def first_description_by_label(descriptions: list[dict[str, str]], labels: tuple[str, ...]) -> str:
    normalized_labels = {normalize_text(label) for label in labels}
    for description in descriptions:
        if normalize_text(description.get("label")) in normalized_labels:
            return normalize_listing_detail_html(description.get("value"))
    return ""


def best_product_description(descriptions: list[dict[str, str]]) -> str:
    if not descriptions:
        return ""
    return max(descriptions, key=product_description_quality_score).get("value") or ""


def strip_low_quality_description_lines(value: str) -> str:
    text = detail_html_plain_text(normalize_detail_html(value))
    if not text:
        return ""
    lines: list[str] = []
    for raw_line in re.split(r"[\r\n]+", text):
        line = normalize_text(raw_line)
        if not line:
            continue
        if is_low_quality_description_line(line):
            continue
        lines.append(line)
    return "\n".join(lines).strip()


def is_low_quality_description_line(line: str) -> bool:
    if not line or line in {"?", "？", "-", "ー"}:
        return True
    keyword_hits = sum(1 for keyword in LOW_QUALITY_DESCRIPTION_KEYWORDS if keyword in line)
    product_hits = sum(1 for keyword in PRODUCT_DESCRIPTION_KEYWORDS if keyword in line)
    if keyword_hits and product_hits:
        return False
    if keyword_hits >= 1 and len(line) <= 120:
        return True
    if keyword_hits >= 2 and len(line) <= 240:
        return True
    return False


def is_low_quality_product_description(value: str) -> bool:
    plain_text = detail_html_plain_text(value)
    if not plain_text:
        return True
    if plain_text in {"?", "？", "-", "ー"}:
        return True
    keyword_hits = sum(1 for keyword in LOW_QUALITY_DESCRIPTION_KEYWORDS if keyword in plain_text)
    product_hits = sum(1 for keyword in PRODUCT_DESCRIPTION_KEYWORDS if keyword in plain_text)
    if len(plain_text) < 40 and product_hits == 0:
        return True
    if keyword_hits >= 2 and product_hits == 0:
        return True
    if len(plain_text) < 120 and keyword_hits >= 1 and product_hits == 0:
        return True
    return False


def is_near_duplicate_description(left: str, right: str) -> bool:
    left_text = normalize_text(left)
    right_text = normalize_text(right)
    if not left_text or not right_text:
        return False
    if left_text == right_text:
        return True
    shorter, longer = sorted((left_text, right_text), key=len)
    return len(shorter) >= 80 and shorter in longer and (len(longer) - len(shorter) <= 80)


def product_description_quality_score(item: dict[str, str]) -> int:
    label = normalize_text(item.get("label"))
    value = normalize_detail_html(item.get("value"))
    plain_text = detail_html_plain_text(value)
    score = min(len(plain_text), 2000)
    score += sum(250 for keyword in PRODUCT_DESCRIPTION_KEYWORDS if keyword in plain_text)
    score -= sum(350 for keyword in LOW_QUALITY_DESCRIPTION_KEYWORDS if keyword in plain_text)
    if "销售" in label or "sales" in label.lower():
        score += 300
    if "PC" in label or "商品说明" in label:
        score += 80
    return score


def detail_html_plain_text(value: Any) -> str:
    return normalize_text(BeautifulSoup(str(value or ""), "lxml").get_text(" ", strip=True))


def description_content_key(value: Any) -> str:
    html = str(value or "")
    plain_text = detail_html_plain_text(html)
    if plain_text:
        return plain_text
    soup = BeautifulSoup(html, "lxml")
    image_sources: list[str] = []
    for image in soup.select("img, source"):
        src = image.get("src") or image.get("data-src") or image.get("data-original") or image.get("srcset")
        src_text = normalize_text(src)
        if src_text:
            image_sources.append(src_text)
    if image_sources:
        return "|".join(image_sources)
    return normalize_text(html)


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
    if "/cabinet/" in path and normalized_shop and (normalized_shop in path or normalized_shop in host):
        return True
    item_tokens = item_number_image_tokens(item_number)
    if item_tokens:
        return any(token in path for token in item_tokens)
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


def extract_image_urls_from_soup(
    soup: BeautifulSoup,
    *,
    shop_code: str = "",
    item_number: str = "",
) -> list[str]:
    urls: list[str] = []
    for node in soup.select("img"):
        for attr in ("src", "data-src", "data-original", "data-lazy-src"):
            value = node.get(attr)
            if value:
                normalized = normalize_product_image_url(value)
                if normalized and (
                    not shop_code
                    or is_relevant_market_item_image(normalized, shop_code=shop_code, item_number=item_number)
                ):
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
    return sanitize_description_html(text).strip()


def normalize_listing_detail_html(value: Any) -> str:
    text = str(value or "").replace("\\/", "/")
    text = unescape(text)
    if not text.strip():
        return ""
    return sanitize_rakuten_pc_description_html(text).strip()


def sanitize_description_html(value: str) -> str:
    soup = BeautifulSoup(value or "", "lxml")
    for element in soup.select("script, style, iframe, object, embed, link, meta, svg, canvas, video, audio"):
        element.decompose()
    for element in soup.select("*"):
        for attribute in list(element.attrs):
            name = attribute.lower()
            value_text = " ".join(element.get_attribute_list(attribute)).strip()
            if name.startswith("on") or value_text.lower().startswith("javascript:"):
                del element.attrs[attribute]
    body = soup.body
    if body is not None:
        return body.decode_contents().strip()
    return str(soup).strip()


def has_description_source(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, str):
        return bool(value.strip())
    if isinstance(value, (list, tuple, set, dict)):
        return bool(value)
    return True


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
    saved = upsert_product(session, owner_username, None, normalized, review_status="listed", store_id=store.id)
    if saved and manage_number:
        row = session.scalar(
            select(ProductModel).where(
                ProductModel.store_id == store.id,
                ProductModel.rakuten_manage_number == manage_number,
                ProductModel.review_status == "listed",
            )
        )
        if row is not None:
            ensure_product_listed_store_mark_from_store_product(session, row, store)
    return saved


def mark_missing_store_products_removed(session: Any, store: StoreModel, seen_manage_numbers: set[str]) -> None:
    query = select(ProductModel).where(
        ProductModel.store_id == store.id,
        ProductModel.review_status == "listed",
        ProductModel.rakuten_manage_number.is_not(None),
    )
    if seen_manage_numbers:
        query = query.where(ProductModel.rakuten_manage_number.not_in(seen_manage_numbers))
    rows = session.scalars(query).all()
    for row in rows:
        row.store_product_status = "removed"
        row.last_error = "本次更新未从乐天店铺后台返回，可能已在乐天下架或删除。"
        remove_listed_store_mark_for_store_product(session, row)


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
