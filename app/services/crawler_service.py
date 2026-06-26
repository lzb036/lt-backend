from __future__ import annotations

import json
import re
import uuid
import base64
import xml.etree.ElementTree as ET
import time
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Any
from urllib.parse import quote, urlsplit

import requests
from bs4 import BeautifulSoup
from sqlalchemy import select

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
    make_source_url_hash,
)

RAKUTEN_SEARCH_BASE = "https://search.rakuten.co.jp/search/mall/"
RAKUTEN_RANKING_BASE = "https://ranking.rakuten.co.jp/search"
RAKUTEN_SHOP_MASTER_URL = "https://api.rms.rakuten.co.jp/es/1.0/shop/shopMaster"
RAKUTEN_CABINET_USAGE_URL = "https://api.rms.rakuten.co.jp/es/1.0/cabinet/usage/get"
RAKUTEN_ITEM_SEARCH_URL = "https://api.rms.rakuten.co.jp/es/2.0/items/search"
RAKUTEN_ITEM_SEARCH_HITS = 100
RAKUTEN_ITEM_SEARCH_MAX_RETRIES = 4


def log_event(owner_username: str, task_id: str | None, level: str, message: str) -> None:
    with session_scope() as session:
        session.add(CrawlLogModel(owner_username=owner_username, task_id=task_id, level=level, message=message))


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
    return {
        "id": row.id,
        "ownerUsername": row.owner_username,
        "taskId": row.task_id,
        "storeId": row.store_id,
        "rakutenManageNumber": row.rakuten_manage_number,
        "storeProductStatus": row.store_product_status,
        "storeLastSeenAt": row.store_last_seen_at.isoformat(sep=" ") if row.store_last_seen_at else None,
        "title": row.title,
        "sourceUrl": row.source_url,
        "itemNumber": row.item_number,
        "shopName": row.shop_name,
        "imageUrl": row.image_url,
        "price": float(row.price) if row.price is not None else None,
        "currency": row.currency,
        "genreId": row.genre_id,
        "reviewStatus": row.review_status,
        "lastError": row.last_error,
        "createdAt": row.created_at.isoformat(sep=" ") if row.created_at else None,
        "updatedAt": row.updated_at.isoformat(sep=" ") if row.updated_at else None,
    }


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
    value = first_text_from_keys(item, ("itemPrice", "price", "standardPrice", "displayPrice"))
    if not value:
        value = first_variant_price(item)
    if not value:
        return None
    normalized = re.sub(r"[^0-9.]", "", value)
    if not normalized:
        return None
    try:
        return float(normalized)
    except ValueError:
        return None


def first_variant_price(item: dict[str, Any]) -> str:
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
    if not prices:
        return ""
    return str(min(prices))


def list_sources(owner_username: str) -> list[dict[str, Any]]:
    with session_scope() as session:
        rows = session.scalars(
            select(CrawlSourceModel)
            .where(CrawlSourceModel.owner_username == owner_username)
            .order_by(CrawlSourceModel.created_at.desc())
        ).all()
        return [source_to_public(row) for row in rows]


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


def list_tasks(owner_username: str) -> list[dict[str, Any]]:
    with session_scope() as session:
        rows = session.scalars(
            select(CrawlTaskModel)
            .where(CrawlTaskModel.owner_username == owner_username)
            .order_by(CrawlTaskModel.created_at.desc())
            .limit(100)
        ).all()
        return [task_to_public(row) for row in rows]


def list_products(
    owner_username: str,
    *,
    status: str | None = None,
    keyword: str | None = None,
    store_id: int | None = None,
) -> list[dict[str, Any]]:
    with session_scope() as session:
        query = select(ProductModel).where(ProductModel.owner_username == owner_username)
        product_status = _product_status_filter(status)
        if product_status:
            query = query.where(ProductModel.review_status == product_status)
        if store_id is not None:
            query = query.where(ProductModel.store_id == store_id)
        if keyword:
            query = query.where(
                ProductModel.title.like(f"%{keyword}%")
                | ProductModel.item_number.like(f"%{keyword}%")
                | ProductModel.rakuten_manage_number.like(f"%{keyword}%")
            )
        rows = session.scalars(query.order_by(ProductModel.created_at.desc())).all()
        return [product_to_public(row) for row in rows]


def list_stores() -> list[dict[str, Any]]:
    with session_scope() as session:
        rows = session.scalars(
            select(StoreModel)
            .order_by(StoreModel.created_at.desc())
        ).all()
        return [store_to_public(row) for row in rows]


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
    with session_scope() as session:
        row = session.get(StoreModel, store_id)
        if row is None:
            raise RuntimeError("店铺不存在。")
        if not row.enabled:
            raise RuntimeError("店铺已停用，不能更新商品。")
        synced_count = 0
        try:
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
            mark_missing_store_products_removed(session, row, seen_manage_numbers)
        except Exception as exc:
            row.last_synced_at = datetime.now()
            row.last_error = str(exc)
        session.flush()
        return {
            "store": store_to_public(row),
            "syncedCount": synced_count,
        }


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


def list_scheduled_crawls(owner_username: str) -> list[dict[str, Any]]:
    with session_scope() as session:
        rows = session.scalars(
            select(ScheduledCrawlModel)
            .where(ScheduledCrawlModel.owner_username == owner_username)
            .order_by(ScheduledCrawlModel.created_at.desc())
        ).all()
        return [scheduled_crawl_to_public(row) for row in rows]


def save_scheduled_crawl(owner_username: str, payload: Any, schedule_id: int | None = None) -> dict[str, Any]:
    source_id = getattr(payload, "sourceId", None)
    with session_scope() as session:
        row = session.get(ScheduledCrawlModel, schedule_id) if schedule_id else None
        if row is None:
            row = ScheduledCrawlModel(owner_username=owner_username)
            session.add(row)
        if row.owner_username != owner_username:
            raise RuntimeError("不能修改其他用户的定时任务。")
        source = session.get(CrawlSourceModel, source_id) if source_id else None
        if source is not None:
            if source.owner_username != owner_username:
                raise RuntimeError("不能使用其他用户的采集源。")
            row.source_id = source.id
            row.source_type = source.source_type
            row.target = source.target
        else:
            row.source_id = None
            row.source_type = str(getattr(payload, "sourceType", "") or "keyword").strip()
            row.target = str(getattr(payload, "target", "") or "").strip()

        row.name = str(getattr(payload, "name", "") or "").strip()
        row.crawl_content = str(getattr(payload, "crawlContent", "") or row.target).strip()
        row.crawl_condition = str(getattr(payload, "crawlCondition", "") or row.source_type).strip()
        row.enabled = bool(getattr(payload, "enabled", True))
        row.interval_minutes = int(getattr(payload, "intervalMinutes", 60) or 60)
        row.notes = str(getattr(payload, "notes", "") or "").strip()
        row.status = "idle" if row.enabled else "disabled"
        row.next_run_at = datetime.now() + timedelta(minutes=row.interval_minutes) if row.enabled else None
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
        row.next_run_at = datetime.now() + timedelta(minutes=row.interval_minutes) if row.enabled else None
        session.flush()
        return scheduled_crawl_to_public(row)


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
        for row in rows:
            row.review_status = status
            if message:
                row.last_error = message if status in {"error", "rejected"} else None
        session.flush()
        return [product_to_public(row) for row in rows]


def delete_products(owner_username: str, product_ids: list[int]) -> None:
    with session_scope() as session:
        rows = session.scalars(
            select(ProductModel).where(
                ProductModel.owner_username == owner_username,
                ProductModel.id.in_(product_ids or [-1]),
            )
        ).all()
        for row in rows:
            session.delete(row)


def list_listing_tasks(owner_username: str) -> list[dict[str, Any]]:
    with session_scope() as session:
        rows = session.scalars(
            select(ListingTaskModel)
            .where(ListingTaskModel.owner_username == owner_username)
            .order_by(ListingTaskModel.created_at.desc())
            .limit(100)
        ).all()
        return [listing_task_to_public(row) for row in rows]


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


def list_roles() -> list[dict[str, Any]]:
    ensure_default_roles()
    with session_scope() as session:
        rows = session.scalars(select(RoleModel).order_by(RoleModel.id.asc())).all()
        return [role_to_public(row) for row in rows]


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

    run_task(task_public["id"])
    with session_scope() as session:
        task = session.get(CrawlTaskModel, task_public["id"])
        return task_to_public(task) if task else task_public


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
    run_task(task_id)
    with session_scope() as session:
        task = session.get(CrawlTaskModel, task_id)
        return task_to_public(task) if task else {"id": task_id}


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
            for item in items:
                if upsert_product(session, owner_username, task_id, item):
                    success_count += 1
            task.success_count = success_count
            task.failed_count = max(0, len(items) - success_count)
            task.status = "success" if task.failed_count == 0 else "partial"
            task.finished_at = datetime.now()
            task.message = f"完成，采集 {len(items)} 条，保存 {success_count} 条"
        log_event(owner_username, task_id, "info", f"任务完成，保存 {success_count} 条商品")
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
        return [collect_product_detail(target)]
    url = build_source_url(source_type, target)
    html = fetch_html(url)
    return parse_search_items(html, url)[:30]


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
    response.encoding = response.apparent_encoding or response.encoding
    return response.text


def parse_search_items(html: str, page_url: str) -> list[dict[str, Any]]:
    soup = BeautifulSoup(html, "lxml")
    items: list[dict[str, Any]] = []
    seen: set[str] = set()
    for link in soup.select("a[href*='item.rakuten.co.jp']"):
        href = str(link.get("href") or "").split("?")[0]
        title = " ".join(link.get_text(" ", strip=True).split())
        if not href or href in seen or len(title) < 4:
            continue
        seen.add(href)
        container = link.find_parent(["div", "li", "article"]) or link
        image = ""
        image_node = container.select_one("img")
        if image_node:
            image = str(image_node.get("src") or image_node.get("data-src") or "")
        price = extract_price(container.get_text(" ", strip=True))
        items.append(
            {
                "title": title[:500],
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


def collect_product_detail(url: str) -> dict[str, Any]:
    html = fetch_html(url)
    soup = BeautifulSoup(html, "lxml")
    title = ""
    title_node = soup.select_one("h1") or soup.select_one("title")
    if title_node:
        title = " ".join(title_node.get_text(" ", strip=True).split())
    image = ""
    image_node = soup.select_one("img[src*='image.rakuten.co.jp'], img[src*='thumbnail.image.rakuten.co.jp']")
    if image_node:
        image = str(image_node.get("src") or "")
    text = soup.get_text(" ", strip=True)
    return {
        "title": title[:500] or url,
        "source_url": url,
        "image_url": image,
        "price": extract_price(text),
        "shop_name": "",
        "item_number": extract_item_number(url),
        "genre_id": "",
        "raw": {"url": url},
    }


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
    row.raw_payload_json = json.dumps(item.get("raw") or item, ensure_ascii=False)
    row.last_error = None
    return True
