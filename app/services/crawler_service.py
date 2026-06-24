from __future__ import annotations

import json
import re
import uuid
from datetime import datetime
from decimal import Decimal
from typing import Any
from urllib.parse import quote

import requests
from bs4 import BeautifulSoup
from sqlalchemy import select

from app.core.config import settings
from app.db.database import session_scope
from app.db.models import CrawlLogModel, CrawlSourceModel, CrawlTaskModel, ProductModel, make_source_url_hash

RAKUTEN_SEARCH_BASE = "https://search.rakuten.co.jp/search/mall/"
RAKUTEN_RANKING_BASE = "https://ranking.rakuten.co.jp/search"


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


def list_products(owner_username: str, *, status: str | None = None, keyword: str | None = None) -> list[dict[str, Any]]:
    with session_scope() as session:
        query = select(ProductModel).where(ProductModel.owner_username == owner_username)
        if status:
            query = query.where(ProductModel.review_status == status)
        if keyword:
            query = query.where(ProductModel.title.like(f"%{keyword}%"))
        rows = session.scalars(query.order_by(ProductModel.created_at.desc()).limit(200)).all()
        return [product_to_public(row) for row in rows]


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


def upsert_product(session: Any, owner_username: str, task_id: str, item: dict[str, Any]) -> bool:
    source_url = str(item.get("source_url") or "").strip()
    title = str(item.get("title") or "").strip()
    if not source_url or not title:
        return False
    source_url_hash = make_source_url_hash(source_url)
    row = session.scalar(
        select(ProductModel).where(
            ProductModel.owner_username == owner_username,
            ProductModel.source_url_hash == source_url_hash,
        )
    )
    if row is None:
        row = ProductModel(owner_username=owner_username, source_url=source_url, source_url_hash=source_url_hash)
        session.add(row)
    row.source_url = source_url
    row.source_url_hash = source_url_hash
    row.task_id = task_id
    row.title = title[:500]
    row.image_url = str(item.get("image_url") or "")
    row.item_number = str(item.get("item_number") or "")
    row.shop_name = str(item.get("shop_name") or "")
    row.genre_id = str(item.get("genre_id") or "")
    price = item.get("price")
    row.price = Decimal(str(price)) if price is not None else None
    row.currency = "JPY"
    row.review_status = "pending"
    row.raw_payload_json = json.dumps(item.get("raw") or item, ensure_ascii=False)
    row.last_error = None
    return True
