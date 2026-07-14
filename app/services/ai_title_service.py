from __future__ import annotations

import base64
import json
import re
from collections.abc import Iterator
from typing import Any

import requests
from sqlalchemy import delete, select, update

from app.core.secure_storage import decrypt_text, encrypt_text, mask_secret
from app.db.database import session_scope
from app.db.models import AiTitleSettingsModel, ProductModel, ProductTitleVersionModel
from app.services import sensitive_word_service


DEFAULT_API_BASE_URL = "https://dashscope.aliyuncs.com/compatible-mode/v1"
DEFAULT_TITLE_PROMPT = """你是日本乐天市场的商品标题优化专家。根据商品现有文本、属性、规格和主图，生成准确、自然、便于检索的日语商品标题。
不得虚构品牌、材质、尺寸、认证、排名、销量、赠品、折扣、库存或发货速度；不得堆砌关键词；保留影响购买决策的真实属性。标题不超过127个字符。"""
DEFAULT_SUBTITLE_PROMPT = """你是日本乐天市场的商品副标题优化专家。根据商品信息和主图生成简洁自然的日语副标题，补充标题未表达的真实卖点。
不得虚构信息，不得使用无法证明的绝对化、排名、促销或时效表述。副标题不超过255个字符。"""


def extract_stream_text(line: bytes | str) -> str:
    text = line.decode("utf-8", errors="ignore") if isinstance(line, bytes) else str(line)
    text = text.strip()
    if not text.startswith("data:"):
        return ""
    payload_text = text[5:].strip()
    if not payload_text or payload_text == "[DONE]":
        return ""
    try:
        payload = json.loads(payload_text)
    except ValueError:
        return ""
    choices = payload.get("choices")
    if not isinstance(choices, list) or not choices:
        return ""
    delta = choices[0].get("delta") if isinstance(choices[0], dict) else None
    content = delta.get("content") if isinstance(delta, dict) else ""
    return content if isinstance(content, str) else ""


def parse_generated_result(text: str) -> dict[str, str]:
    normalized = str(text or "").strip()
    start = normalized.find("{")
    end = normalized.rfind("}")
    if start < 0 or end <= start:
        raise RuntimeError("千问未返回有效的标题结果。")
    try:
        payload = json.loads(normalized[start : end + 1])
    except ValueError as exc:
        raise RuntimeError("千问返回格式不正确，请重试。") from exc
    title = str(payload.get("title") or "").strip()
    subtitle = str(payload.get("subtitle") or "").strip()
    if not title:
        raise RuntimeError("千问生成的商品标题为空。")
    if len(title) > 500:
        raise RuntimeError("千问生成的商品标题超过系统长度限制。")
    return {"title": title, "subtitle": subtitle[:1000]}


def settings_to_public(row: AiTitleSettingsModel) -> dict[str, Any]:
    key = decrypt_text(row.api_key_encrypted)
    return {
        "enabled": bool(row.enabled),
        "apiBaseUrl": row.api_base_url or DEFAULT_API_BASE_URL,
        "apiKeyConfigured": bool(key),
        "apiKeyMasked": mask_secret(key),
        "modelName": row.model_name or "qwen-vl-max",
        "titlePrompt": row.title_prompt or DEFAULT_TITLE_PROMPT,
        "subtitlePrompt": row.subtitle_prompt or DEFAULT_SUBTITLE_PROMPT,
        "temperature": float(row.temperature or "0.3"),
        "maxTokens": int(row.max_tokens or 1000),
    }


def ensure_settings(session: Any) -> AiTitleSettingsModel:
    row = session.get(AiTitleSettingsModel, 1)
    if row is None:
        row = AiTitleSettingsModel(
            id=1,
            api_base_url=DEFAULT_API_BASE_URL,
            model_name="qwen-vl-max",
            title_prompt=DEFAULT_TITLE_PROMPT,
            subtitle_prompt=DEFAULT_SUBTITLE_PROMPT,
        )
        session.add(row)
        session.flush()
    return row


def get_settings() -> dict[str, Any]:
    with session_scope() as session:
        return settings_to_public(ensure_settings(session))


def update_settings(payload: Any) -> dict[str, Any]:
    with session_scope() as session:
        row = ensure_settings(session)
        row.enabled = bool(payload.enabled)
        row.api_base_url = str(payload.apiBaseUrl or DEFAULT_API_BASE_URL).strip().rstrip("/")
        row.model_name = str(payload.modelName or "qwen-vl-max").strip()
        row.title_prompt = str(payload.titlePrompt or DEFAULT_TITLE_PROMPT).strip()
        row.subtitle_prompt = str(payload.subtitlePrompt or DEFAULT_SUBTITLE_PROMPT).strip()
        row.temperature = str(float(payload.temperature))
        row.max_tokens = int(payload.maxTokens)
        api_key = str(payload.apiKey or "").strip()
        if api_key:
            row.api_key_encrypted = encrypt_text(api_key)
        session.flush()
        return settings_to_public(row)


def openai_headers(api_key: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}


def test_settings_connection() -> dict[str, Any]:
    with session_scope() as session:
        row = ensure_settings(session)
        api_key = decrypt_text(row.api_key_encrypted)
        if not api_key:
            raise RuntimeError("请先配置 API Key。")
        url = f"{row.api_base_url.rstrip('/')}/chat/completions"
        response = requests.post(
            url,
            headers=openai_headers(api_key),
            json={
                "model": row.model_name,
                "messages": [{"role": "user", "content": "只回复 OK"}],
                "max_tokens": 8,
                "stream": False,
            },
            timeout=30,
        )
        if response.status_code >= 400:
            raise RuntimeError(ai_error_message(response))
        return {"success": True, "message": "连接成功。"}


def ai_error_message(response: requests.Response) -> str:
    try:
        payload = response.json()
    except ValueError:
        payload = {}
    error = payload.get("error") if isinstance(payload, dict) else None
    message = error.get("message") if isinstance(error, dict) else ""
    return str(message or f"千问接口请求失败（HTTP {response.status_code}）。")


def version_to_public(row: ProductTitleVersionModel) -> dict[str, Any]:
    return {
        "id": row.id,
        "productId": row.product_id,
        "title": row.title,
        "subtitle": row.subtitle,
        "source": row.source,
        "modelName": row.model_name,
        "isSelected": bool(row.is_selected),
        "createdBy": row.created_by,
        "createdAt": row.created_at.isoformat(sep=" ", timespec="seconds") if row.created_at else None,
    }


def ensure_current_title_version_in_session(
    session: Any,
    product: ProductModel,
) -> ProductTitleVersionModel:
    from app.services.crawler_service import product_raw_payload, product_tagline

    subtitle = product_tagline(product_raw_payload(product))
    rows = session.scalars(
        select(ProductTitleVersionModel)
        .where(ProductTitleVersionModel.product_id == product.id)
        .order_by(ProductTitleVersionModel.created_at.desc(), ProductTitleVersionModel.id.desc())
    ).all()
    current = next(
        (row for row in rows if row.title == product.title and row.subtitle == subtitle),
        None,
    )
    if current is None:
        current = ProductTitleVersionModel(
            product_id=product.id,
            owner_username=product.owner_username,
            title=product.title,
            subtitle=subtitle,
            source="original",
            model_name="",
            input_snapshot_json="{}",
            is_selected=True,
            created_by=product.owner_username,
        )
        session.add(current)
    for row in rows:
        row.is_selected = row is current
    current.is_selected = True
    session.flush()
    return current


def list_versions(owner_username: str, product_id: int) -> dict[str, Any]:
    with session_scope() as session:
        product = session.get(ProductModel, product_id)
        if product is None or product.owner_username != owner_username:
            raise RuntimeError("商品不存在。")
        ensure_current_title_version_in_session(session, product)
        rows = session.scalars(
            select(ProductTitleVersionModel)
            .where(ProductTitleVersionModel.product_id == product_id)
            .order_by(ProductTitleVersionModel.created_at.desc(), ProductTitleVersionModel.id.desc())
        ).all()
        return {"versions": [version_to_public(row) for row in rows]}


def save_title_version_in_session(
    session: Any,
    *,
    owner_username: str,
    product_id: int,
    version_id: int,
) -> ProductTitleVersionModel:
    from app.services.crawler_service import patch_local_item_detail, product_raw_payload

    product = session.get(ProductModel, product_id)
    version = session.get(ProductTitleVersionModel, version_id)
    if product is None or product.owner_username != owner_username:
        raise RuntimeError("商品不存在。")
    if product.review_status != "pending":
        raise RuntimeError("只有待审核商品可以保存优化标题。")
    if version is None or version.product_id != product_id or version.owner_username != owner_username:
        raise RuntimeError("标题优化版本不存在。")
    payload = patch_local_item_detail(
        product_raw_payload(product),
        title=version.title,
        tagline=version.subtitle,
        variants=[],
    )
    product.title = version.title
    product.raw_payload_json = json.dumps(payload, ensure_ascii=False)
    session.execute(
        update(ProductTitleVersionModel)
        .where(ProductTitleVersionModel.product_id == product_id)
        .values(is_selected=False)
    )
    version.is_selected = True
    session.flush()
    return version


def save_title_version(owner_username: str, product_id: int, version_id: int) -> dict[str, Any]:
    with session_scope() as session:
        version = save_title_version_in_session(
            session,
            owner_username=owner_username,
            product_id=product_id,
            version_id=version_id,
        )
        return version_to_public(version)


def cleanup_title_versions_for_approved_product(session: Any, product: ProductModel) -> None:
    from app.services.crawler_service import product_raw_payload, product_tagline

    rows = session.scalars(
        select(ProductTitleVersionModel).where(ProductTitleVersionModel.product_id == product.id)
    ).all()
    if not rows:
        return
    subtitle = product_tagline(product_raw_payload(product))
    keep = next(
        (row for row in rows if row.title == product.title and row.subtitle == subtitle),
        None,
    )
    if keep is None:
        keep = ProductTitleVersionModel(
            product_id=product.id,
            owner_username=product.owner_username,
            title=product.title,
            subtitle=subtitle,
            source="final",
            is_selected=True,
            created_by=product.owner_username,
        )
        session.add(keep)
    for row in rows:
        if row is keep:
            row.is_selected = True
        else:
            session.delete(row)


def _compact_product_context(product: ProductModel, raw_payload: dict[str, Any]) -> dict[str, Any]:
    from app.services.crawler_service import (
        product_descriptions,
        product_tagline,
        product_variant_selectors,
        product_variants,
    )

    return {
        "currentTitle": product.title,
        "currentSubtitle": product_tagline(raw_payload),
        "shopName": product.shop_name,
        "genreId": product.genre_id,
        "price": str(product.price or ""),
        "descriptions": product_descriptions(raw_payload)[:6],
        "variantSelectors": product_variant_selectors(raw_payload),
        "variants": product_variants(raw_payload)[:30],
    }


def _image_data_url(product: ProductModel, raw_payload: dict[str, Any]) -> str:
    from app.services.crawler_service import (
        load_product_image_bytes,
        product_editable_image_urls,
        product_shop_code,
    )

    images = product_editable_image_urls(raw_payload, shop_code=product_shop_code(product, raw_payload))
    image_url = images[0] if images else product.image_url
    if not image_url:
        return ""
    image = load_product_image_bytes(
        image_url,
        max_bytes=4 * 1024 * 1024,
        size_error_message="商品主图不能超过 4MB。",
    )
    encoded = base64.b64encode(image["content"]).decode("ascii")
    return f"data:{image['contentType']};base64,{encoded}"


def stream_generate_version(owner_username: str, product_id: int, created_by: str) -> Iterator[dict[str, Any]]:
    from app.services.crawler_service import product_raw_payload

    with session_scope() as session:
        product = session.get(ProductModel, product_id)
        if product is None or product.owner_username != owner_username:
            raise RuntimeError("商品不存在。")
        if product.review_status != "pending":
            raise RuntimeError("只有待审核商品可以优化标题。")
        settings = ensure_settings(session)
        if not settings.enabled:
            raise RuntimeError("标题优化功能未启用。")
        api_key = decrypt_text(settings.api_key_encrypted)
        if not api_key:
            raise RuntimeError("标题优化尚未配置 API Key。")
        raw_payload = product_raw_payload(product)
        snapshot = _compact_product_context(product, raw_payload)
        image_data_url = _image_data_url(product, raw_payload)
        request_payload = {
            "model": settings.model_name,
            "stream": True,
            "temperature": float(settings.temperature or "0.3"),
            "max_tokens": int(settings.max_tokens or 1000),
            "messages": [
                {
                    "role": "system",
                    "content": (
                        f"{settings.title_prompt or DEFAULT_TITLE_PROMPT}\n\n"
                        f"{settings.subtitle_prompt or DEFAULT_SUBTITLE_PROMPT}\n\n"
                        "仅返回一个 JSON 对象，格式为："
                        '{"title":"日语标题","subtitle":"日语副标题"}。不要输出 Markdown。'
                    ),
                },
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "text",
                            "text": "请结合以下商品文本信息和商品主图生成标题与副标题：\n"
                            + json.dumps(snapshot, ensure_ascii=False),
                        },
                        *(
                            [{"type": "image_url", "image_url": {"url": image_data_url}}]
                            if image_data_url
                            else []
                        ),
                    ],
                },
            ],
        }
        response = requests.post(
            f"{settings.api_base_url.rstrip('/')}/chat/completions",
            headers=openai_headers(api_key),
            json=request_payload,
            stream=True,
            timeout=(20, 120),
        )
        if response.status_code >= 400:
            raise RuntimeError(ai_error_message(response))
        chunks: list[str] = []
        for line in response.iter_lines():
            delta = extract_stream_text(line)
            if delta:
                chunks.append(delta)
                yield {"type": "delta", "content": delta}
        generated = parse_generated_result("".join(chunks))
        active_words = sensitive_word_service.active_sensitive_words(session)
        cleaned, _ = sensitive_word_service.sanitize_product_payload(
            {"title": generated["title"], "subtitle": generated["subtitle"]},
            active_words,
        )
        title = str(cleaned.get("title") or "").strip()
        subtitle = str(cleaned.get("subtitle") or "").strip()
        if not title:
            raise RuntimeError("生成标题经过敏感词处理后为空，请调整提示词后重试。")
        version = ProductTitleVersionModel(
            product_id=product.id,
            owner_username=product.owner_username,
            title=title,
            subtitle=subtitle,
            source="ai",
            model_name=settings.model_name,
            input_snapshot_json=json.dumps(snapshot, ensure_ascii=False),
            is_selected=False,
            created_by=created_by,
        )
        session.add(version)
        session.flush()
        yield {"type": "completed", "version": version_to_public(version)}
