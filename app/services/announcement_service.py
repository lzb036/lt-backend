from __future__ import annotations

import json
import logging
import mimetypes
import uuid
from io import BytesIO
from pathlib import Path
from typing import Any
from urllib.parse import quote, unquote, urlsplit

from PIL import Image
from sqlalchemy import delete, select

from app.core.config import settings
from app.db.database import SessionLocal
from app.db.models import SystemAnnouncementModel, SystemAnnouncementReadModel
from app.services.product_image_storage import product_image_storage


logger = logging.getLogger(__name__)

ANNOUNCEMENT_IMAGE_URL_PREFIX = "/api/static/announcement-images"
ANNOUNCEMENT_IMAGE_OBJECT_PREFIX = "announcement-images"
LOCAL_ANNOUNCEMENT_IMAGE_DIR = (
    settings.backend_dir / "data" / ANNOUNCEMENT_IMAGE_OBJECT_PREFIX
)
ALLOWED_ANNOUNCEMENT_IMAGE_EXTENSIONS = {
    ".jpg",
    ".jpeg",
    ".png",
    ".gif",
    ".webp",
}
ALLOWED_ANNOUNCEMENT_IMAGE_MIME_TYPES = {
    "image/jpeg",
    "image/png",
    "image/gif",
    "image/webp",
}
MAX_ANNOUNCEMENT_IMAGE_BYTES = 5 * 1024 * 1024
MAX_ANNOUNCEMENT_IMAGES = 12
DEFAULT_MANUAL_ANNOUNCEMENT_CREATED_BY = "system:operator-manual"
DEFAULT_MANUAL_ANNOUNCEMENT_TITLE = "商品采集系统使用手册"
DEFAULT_MANUAL_ANNOUNCEMENT_CONTENT = (
    "商品采集系统使用手册已发布，可通过下方链接查看完整操作说明。"
)
DEFAULT_MANUAL_ANNOUNCEMENT_LINK_LABEL = "查看使用手册"
DEFAULT_MANUAL_ANNOUNCEMENT_LINK_URL = "/help/operator-manual"


def list_announcements(
    *,
    include_unpublished: bool = False,
    username: str | None = None,
) -> list[dict[str, Any]]:
    with SessionLocal() as session:
        query = select(SystemAnnouncementModel)
        if not include_unpublished:
            query = query.where(SystemAnnouncementModel.published.is_(True))
        rows = session.scalars(
            query.order_by(
                SystemAnnouncementModel.updated_at.desc(),
                SystemAnnouncementModel.id.desc(),
            )
        ).all()
        read_ids = announcement_read_ids(
            session,
            username=username,
            announcement_ids=[int(row.id) for row in rows],
        )
        return [
            announcement_to_public(row, is_read=int(row.id) in read_ids)
            for row in rows
        ]


def has_unread_announcements(username: str) -> bool:
    normalized_username = str(username or "").strip()
    if not normalized_username:
        return False
    with SessionLocal() as session:
        unread_id = session.execute(
            select(SystemAnnouncementModel.id)
            .outerjoin(
                SystemAnnouncementReadModel,
                (
                    SystemAnnouncementReadModel.announcement_id
                    == SystemAnnouncementModel.id
                )
                & (SystemAnnouncementReadModel.username == normalized_username),
            )
            .where(
                SystemAnnouncementModel.published.is_(True),
                SystemAnnouncementReadModel.announcement_id.is_(None),
            )
            .limit(1)
        ).scalar_one_or_none()
        return unread_id is not None


def mark_announcements_read(
    announcement_ids: list[int],
    *,
    username: str,
) -> list[int]:
    normalized_username = str(username or "").strip()
    normalized_ids = list(
        dict.fromkeys(
            int(announcement_id)
            for announcement_id in announcement_ids
            if int(announcement_id) > 0
        )
    )
    if not normalized_username or not normalized_ids:
        return []
    with SessionLocal() as session:
        published_ids = list(
            session.scalars(
                select(SystemAnnouncementModel.id).where(
                    SystemAnnouncementModel.id.in_(normalized_ids),
                    SystemAnnouncementModel.published.is_(True),
                )
            ).all()
        )
        existing_ids = announcement_read_ids(
            session,
            username=normalized_username,
            announcement_ids=published_ids,
        )
        for announcement_id in published_ids:
            if int(announcement_id) not in existing_ids:
                session.add(
                    SystemAnnouncementReadModel(
                        announcement_id=int(announcement_id),
                        username=normalized_username,
                    )
                )
        session.commit()
        return [int(announcement_id) for announcement_id in published_ids]


def create_announcement(
    *,
    title: str,
    content: str,
    image_urls: list[str],
    link_label: str = "",
    link_url: str = "",
    published: bool,
    operated_by: str,
) -> dict[str, Any]:
    (
        normalized_title,
        normalized_content,
        normalized_images,
        normalized_link_label,
        normalized_link_url,
    ) = normalize_payload(
        title,
        content,
        image_urls,
        link_label,
        link_url,
    )
    with SessionLocal() as session:
        row = SystemAnnouncementModel(
            title=normalized_title,
            content=normalized_content,
            images_json=json.dumps(normalized_images, ensure_ascii=False),
            link_label=normalized_link_label,
            link_url=normalized_link_url,
            published=bool(published),
            created_by=str(operated_by or "").strip(),
            updated_by=str(operated_by or "").strip(),
        )
        session.add(row)
        session.commit()
        session.refresh(row)
        return announcement_to_public(row)


def update_announcement(
    announcement_id: int,
    *,
    title: str,
    content: str,
    image_urls: list[str],
    link_label: str = "",
    link_url: str = "",
    published: bool,
    operated_by: str,
) -> dict[str, Any]:
    (
        normalized_title,
        normalized_content,
        normalized_images,
        normalized_link_label,
        normalized_link_url,
    ) = normalize_payload(
        title,
        content,
        image_urls,
        link_label,
        link_url,
    )
    removed_images: list[str] = []
    with SessionLocal() as session:
        row = session.get(SystemAnnouncementModel, int(announcement_id))
        if row is None:
            raise LookupError("公告不存在。")
        previous_images = announcement_images(row)
        removed_images = [
            image_url
            for image_url in previous_images
            if image_url not in normalized_images
        ]
        row.title = normalized_title
        row.content = normalized_content
        row.images_json = json.dumps(normalized_images, ensure_ascii=False)
        row.link_label = normalized_link_label
        row.link_url = normalized_link_url
        row.published = bool(published)
        row.updated_by = str(operated_by or "").strip()
        session.execute(
            delete(SystemAnnouncementReadModel).where(
                SystemAnnouncementReadModel.announcement_id == row.id
            )
        )
        session.commit()
        session.refresh(row)
        result = announcement_to_public(row)
    delete_images_if_unreferenced_safely(removed_images)
    return result


def delete_announcement(announcement_id: int) -> None:
    image_urls: list[str] = []
    with SessionLocal() as session:
        row = session.get(SystemAnnouncementModel, int(announcement_id))
        if row is None:
            return
        image_urls = announcement_images(row)
        session.delete(row)
        session.commit()
    delete_images_if_unreferenced_safely(image_urls)


def save_announcement_image(upload_file: Any) -> str:
    filename = str(getattr(upload_file, "filename", "") or "").strip()
    suffix = Path(filename).suffix.lower()
    content_type = str(getattr(upload_file, "content_type", "") or "").strip().lower()
    if suffix not in ALLOWED_ANNOUNCEMENT_IMAGE_EXTENSIONS:
        raise RuntimeError("公告图片仅支持 JPG、PNG、GIF 或 WEBP 格式。")
    if content_type and content_type not in ALLOWED_ANNOUNCEMENT_IMAGE_MIME_TYPES:
        raise RuntimeError("公告图片格式不受支持。")

    chunks: list[bytes] = []
    size = 0
    try:
        while True:
            chunk = upload_file.file.read(1024 * 1024)
            if not chunk:
                break
            size += len(chunk)
            if size > MAX_ANNOUNCEMENT_IMAGE_BYTES:
                raise RuntimeError("单张公告图片不能超过 5MB。")
            chunks.append(chunk)
    finally:
        try:
            upload_file.file.seek(0)
        except Exception:
            pass
    content = b"".join(chunks)
    if not content:
        raise RuntimeError("公告图片内容为空。")
    try:
        with Image.open(BytesIO(content)) as image:
            image.verify()
    except Exception as exc:
        raise RuntimeError("公告图片文件无效或已损坏。") from exc

    stored_filename = f"{uuid.uuid4().hex}{suffix}"
    object_key = f"{ANNOUNCEMENT_IMAGE_OBJECT_PREFIX}/{stored_filename}"
    if product_image_storage.enabled:
        product_image_storage.put_bytes(
            object_key,
            content,
            content_type or image_media_type(stored_filename),
        )
    else:
        LOCAL_ANNOUNCEMENT_IMAGE_DIR.mkdir(parents=True, exist_ok=True)
        (LOCAL_ANNOUNCEMENT_IMAGE_DIR / stored_filename).write_bytes(content)
    return announcement_image_url(stored_filename)


def delete_unreferenced_announcement_image(image_url: str) -> None:
    normalized_url = normalize_announcement_image_url(image_url)
    with SessionLocal() as session:
        rows = session.scalars(select(SystemAnnouncementModel.images_json)).all()
    if any(
        normalized_url in parse_image_urls(images_json)
        for images_json in rows
    ):
        raise RuntimeError("该图片正在被公告使用，不能单独删除。")
    delete_announcement_image(normalized_url)


def announcement_image_http_info(
    filename: str,
    *,
    include_body: bool,
) -> dict[str, Any]:
    normalized_filename = normalize_announcement_image_filename(filename)
    media_type = image_media_type(normalized_filename)
    object_key = f"{ANNOUNCEMENT_IMAGE_OBJECT_PREFIX}/{normalized_filename}"
    if product_image_storage.enabled:
        if include_body:
            stream = product_image_storage.open_stream(
                object_key,
                max_bytes=MAX_ANNOUNCEMENT_IMAGE_BYTES,
            )
            if stream is None:
                raise RuntimeError("公告图片不存在。")
            return {
                "type": "oss",
                "body": stream,
                "size": stream.size,
                "mediaType": media_type,
            }
        size = product_image_storage.object_size(object_key)
        if size is None:
            raise RuntimeError("公告图片不存在。")
        return {
            "type": "oss",
            "body": None,
            "size": size,
            "mediaType": media_type,
        }

    path = (LOCAL_ANNOUNCEMENT_IMAGE_DIR / normalized_filename).resolve()
    if path.parent != LOCAL_ANNOUNCEMENT_IMAGE_DIR.resolve() or not path.is_file():
        raise RuntimeError("公告图片不存在。")
    return {
        "type": "local",
        "path": path,
        "size": path.stat().st_size,
        "mediaType": media_type,
    }


def announcement_to_public(
    row: SystemAnnouncementModel,
    *,
    is_read: bool | None = None,
) -> dict[str, Any]:
    return {
        "id": int(row.id),
        "title": row.title,
        "content": row.content,
        "imageUrls": announcement_images(row),
        "linkLabel": row.link_label,
        "linkUrl": row.link_url,
        "published": bool(row.published),
        "createdBy": row.created_by,
        "updatedBy": row.updated_by,
        "createdAt": datetime_to_public(row.created_at),
        "updatedAt": datetime_to_public(row.updated_at),
        "isRead": is_read,
    }


def announcement_read_ids(
    session: Any,
    *,
    username: str | None,
    announcement_ids: list[int],
) -> set[int]:
    normalized_username = str(username or "").strip()
    if not normalized_username or not announcement_ids:
        return set()
    return {
        int(announcement_id)
        for announcement_id in session.scalars(
            select(SystemAnnouncementReadModel.announcement_id).where(
                SystemAnnouncementReadModel.username == normalized_username,
                SystemAnnouncementReadModel.announcement_id.in_(announcement_ids),
            )
        ).all()
    }


def normalize_payload(
    title: str,
    content: str,
    image_urls: list[str],
    link_label: str = "",
    link_url: str = "",
) -> tuple[str, str, list[str], str, str]:
    normalized_title = str(title or "").strip()
    normalized_content = str(content or "").strip()
    if not normalized_title:
        raise RuntimeError("请输入公告标题。")
    if len(normalized_title) > 255:
        raise RuntimeError("公告标题不能超过 255 个字符。")
    if len(normalized_content) > 20_000:
        raise RuntimeError("公告内容不能超过 20000 个字符。")
    normalized_link_label = str(link_label or "").strip()
    normalized_link_url = str(link_url or "").strip()
    if len(normalized_link_label) > 255:
        raise RuntimeError("公告链接文字不能超过 255 个字符。")
    if len(normalized_link_url) > 1000:
        raise RuntimeError("公告链接地址不能超过 1000 个字符。")
    if normalized_link_label and not normalized_link_url:
        raise RuntimeError("填写公告链接文字后必须填写链接地址。")
    if normalized_link_url and not (
        normalized_link_url.startswith("/")
        or normalized_link_url.startswith("https://")
        or normalized_link_url.startswith("http://")
    ):
        raise RuntimeError("公告链接仅支持站内路径或 HTTP/HTTPS 地址。")
    if normalized_link_url and not normalized_link_label:
        normalized_link_label = "查看详情"
    normalized_images = list(
        dict.fromkeys(
            normalize_announcement_image_url(image_url)
            for image_url in image_urls
            if str(image_url or "").strip()
        )
    )
    if len(normalized_images) > MAX_ANNOUNCEMENT_IMAGES:
        raise RuntimeError(f"每条公告最多上传 {MAX_ANNOUNCEMENT_IMAGES} 张图片。")
    if not normalized_content and not normalized_images and not normalized_link_url:
        raise RuntimeError("公告内容、图片和链接不能同时为空。")
    return (
        normalized_title,
        normalized_content,
        normalized_images,
        normalized_link_label,
        normalized_link_url,
    )


def ensure_default_manual_announcement(session: Any) -> bool:
    existing = session.execute(
        select(SystemAnnouncementModel.id).where(
            SystemAnnouncementModel.created_by
            == DEFAULT_MANUAL_ANNOUNCEMENT_CREATED_BY
        )
    ).scalar_one_or_none()
    if existing is not None:
        return False
    session.add(
        SystemAnnouncementModel(
            title=DEFAULT_MANUAL_ANNOUNCEMENT_TITLE,
            content=DEFAULT_MANUAL_ANNOUNCEMENT_CONTENT,
            images_json="[]",
            link_label=DEFAULT_MANUAL_ANNOUNCEMENT_LINK_LABEL,
            link_url=DEFAULT_MANUAL_ANNOUNCEMENT_LINK_URL,
            published=True,
            created_by=DEFAULT_MANUAL_ANNOUNCEMENT_CREATED_BY,
            updated_by=DEFAULT_MANUAL_ANNOUNCEMENT_CREATED_BY,
        )
    )
    return True


def announcement_images(row: SystemAnnouncementModel) -> list[str]:
    return parse_image_urls(row.images_json)


def parse_image_urls(value: str) -> list[str]:
    try:
        parsed = json.loads(value or "[]")
    except ValueError:
        return []
    if not isinstance(parsed, list):
        return []
    result: list[str] = []
    for item in parsed:
        try:
            result.append(normalize_announcement_image_url(item))
        except RuntimeError:
            continue
    return list(dict.fromkeys(result))


def normalize_announcement_image_url(image_url: str) -> str:
    path = urlsplit(str(image_url or "").strip()).path
    if not path.startswith(f"{ANNOUNCEMENT_IMAGE_URL_PREFIX}/"):
        raise RuntimeError("公告图片地址无效。")
    relative = path.removeprefix(ANNOUNCEMENT_IMAGE_URL_PREFIX).lstrip("/")
    filename = normalize_announcement_image_filename(unquote(relative))
    return announcement_image_url(filename)


def normalize_announcement_image_filename(filename: str) -> str:
    normalized = str(filename or "").strip()
    if (
        not normalized
        or normalized in {".", ".."}
        or "/" in normalized
        or "\\" in normalized
        or Path(normalized).suffix.lower()
        not in ALLOWED_ANNOUNCEMENT_IMAGE_EXTENSIONS
    ):
        raise RuntimeError("公告图片地址无效。")
    return normalized


def announcement_image_url(filename: str) -> str:
    return f"{ANNOUNCEMENT_IMAGE_URL_PREFIX}/{quote(filename, safe='')}"


def delete_announcement_image(image_url: str) -> None:
    normalized_url = normalize_announcement_image_url(image_url)
    filename = unquote(
        urlsplit(normalized_url).path
        .removeprefix(ANNOUNCEMENT_IMAGE_URL_PREFIX)
        .lstrip("/")
    )
    object_key = f"{ANNOUNCEMENT_IMAGE_OBJECT_PREFIX}/{filename}"
    if product_image_storage.enabled:
        product_image_storage.delete(object_key)
        return
    (LOCAL_ANNOUNCEMENT_IMAGE_DIR / filename).unlink(missing_ok=True)


def delete_images_safely(image_urls: list[str]) -> None:
    for image_url in image_urls:
        try:
            delete_announcement_image(image_url)
        except Exception:
            logger.warning(
                "删除公告图片失败 image_url=%s",
                image_url,
                exc_info=True,
            )


def delete_images_if_unreferenced_safely(image_urls: list[str]) -> None:
    for image_url in image_urls:
        try:
            delete_unreferenced_announcement_image(image_url)
        except RuntimeError as exc:
            if "正在被公告使用" in str(exc):
                continue
            logger.warning(
                "检查公告图片引用失败 image_url=%s",
                image_url,
                exc_info=True,
            )
        except Exception:
            logger.warning(
                "删除无引用公告图片失败 image_url=%s",
                image_url,
                exc_info=True,
            )


def image_media_type(filename: str) -> str:
    return mimetypes.guess_type(filename)[0] or "application/octet-stream"


def datetime_to_public(value: Any) -> str | None:
    return value.isoformat(sep=" ", timespec="seconds") if value is not None else None
