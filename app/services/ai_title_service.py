from __future__ import annotations

import base64
import json
import re
from collections.abc import Iterator
from datetime import datetime
from typing import Any

from sqlalchemy import select, update

from app.core.secure_storage import decrypt_text, encrypt_text, mask_secret
from app.core.text_limits import (
    RAKUTEN_TAGLINE_MAX_BYTES,
    RAKUTEN_TITLE_MAX_BYTES,
    truncate_utf8_bytes,
)
from app.db.database import session_scope
from app.db.models import AiTitleSettingsModel, ProductModel, ProductTitleVersionModel, UserAiTitleSettingsModel
from app.services import sensitive_word_service


DEFAULT_API_BASE_URL = "https://dashscope.aliyuncs.com/compatible-mode/v1"
DEFAULT_TITLE_PROMPT = """你是日本乐天市场的商品标题优化专家。根据商品现有文本、属性、规格和主图，生成准确、自然、便于检索的日语商品标题。
不得虚构品牌、材质、尺寸、认证、排名、销量、赠品、折扣、库存或发货速度；不得堆砌关键词；保留影响购买决策的真实属性。主标题的 UTF-8 编码长度不得超过255字节。"""
DEFAULT_SUBTITLE_PROMPT = """你是日本乐天市场的商品副标题优化专家。根据商品信息和主图生成简洁自然的日语副标题，补充标题未表达的真实卖点。
不得虚构信息，不得使用无法证明的绝对化、排名、促销或时效表述。副标题的 UTF-8 编码长度不得超过174字节。"""
PROVIDER_CATALOG = [
    {
        "value": "aliyun",
        "label": "阿里云百炼",
        "apiBaseUrl": "https://dashscope.aliyuncs.com/compatible-mode/v1",
        "models": ["openai/qwen3.7-plus", "openai/qwen3.6-flash", "openai/qwen3-vl-plus", "openai/qwen3-vl-flash"],
    },
    {
        "value": "siliconflow",
        "label": "硅基流动",
        "apiBaseUrl": "https://api.siliconflow.cn/v1",
        "models": ["openai/Qwen/Qwen3-VL-235B-A22B-Instruct", "openai/Pro/Qwen/Qwen2.5-VL-7B-Instruct"],
    },
    {
        "value": "openai",
        "label": "OpenAI",
        "apiBaseUrl": "https://api.openai.com/v1",
        "models": ["openai/gpt-5.2", "openai/gpt-5-mini", "openai/gpt-4.1"],
    },
    {
        "value": "anthropic",
        "label": "Anthropic Claude",
        "apiBaseUrl": "",
        "models": ["anthropic/claude-opus-4-6", "anthropic/claude-sonnet-4-6"],
    },
    {
        "value": "gemini",
        "label": "Google Gemini",
        "apiBaseUrl": "",
        "models": ["gemini/gemini-3.1-pro-preview", "gemini/gemini-3-flash-preview"],
    },
    {
        "value": "openrouter",
        "label": "OpenRouter",
        "apiBaseUrl": "https://openrouter.ai/api/v1",
        "models": ["openrouter/openai/gpt-5.2", "openrouter/anthropic/claude-sonnet-4-6", "openrouter/google/gemini-3.1-pro-preview"],
    },
    {
        "value": "deepseek",
        "label": "DeepSeek",
        "apiBaseUrl": "https://api.deepseek.com/v1",
        "models": [],
    },
    {
        "value": "moonshot",
        "label": "Moonshot / Kimi",
        "apiBaseUrl": "https://api.moonshot.cn/v1",
        "models": [],
    },
    {
        "value": "ollama",
        "label": "Ollama",
        "apiBaseUrl": "http://127.0.0.1:11434",
        "models": ["ollama/llava", "ollama/qwen3-vl"],
    },
    {
        "value": "custom_openai",
        "label": "自定义 OpenAI 兼容接口",
        "apiBaseUrl": "",
        "models": [],
    },
]
PROVIDER_VALUES = {item["value"] for item in PROVIDER_CATALOG}


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
    return {
        "title": truncate_utf8_bytes(title, RAKUTEN_TITLE_MAX_BYTES),
        "subtitle": truncate_utf8_bytes(subtitle, RAKUTEN_TAGLINE_MAX_BYTES),
    }


def provider_catalog() -> list[dict[str, Any]]:
    return [dict(item, models=list(item["models"])) for item in PROVIDER_CATALOG]


def settings_to_public(row: UserAiTitleSettingsModel) -> dict[str, Any]:
    key = decrypt_text(row.api_key_encrypted)
    return {
        "provider": row.provider or "custom_openai",
        "apiBaseUrl": row.api_base_url,
        "apiKeyConfigured": bool(key),
        "apiKeyMasked": mask_secret(key),
        "modelName": row.model_name,
        "titlePrompt": row.title_prompt or DEFAULT_TITLE_PROMPT,
        "subtitlePrompt": row.subtitle_prompt or DEFAULT_SUBTITLE_PROMPT,
        "verified": bool(row.verified_at and not row.last_error),
        "verifiedAt": row.verified_at.isoformat(sep=" ", timespec="seconds") if row.verified_at else None,
        "lastError": row.last_error,
    }


def ensure_user_settings(session: Any, owner_username: str) -> UserAiTitleSettingsModel:
    row = session.get(UserAiTitleSettingsModel, owner_username)
    if row is None and owner_username == "superadmin":
        legacy = session.get(AiTitleSettingsModel, 1)
        if legacy is not None:
            row = UserAiTitleSettingsModel(
                owner_username=owner_username,
                provider="aliyun",
                api_base_url=legacy.api_base_url,
                api_key_encrypted=legacy.api_key_encrypted,
                model_name=(
                    legacy.model_name
                    if "/" in legacy.model_name
                    else f"openai/{legacy.model_name}"
                ),
                title_prompt=legacy.title_prompt,
                subtitle_prompt=legacy.subtitle_prompt,
            )
            session.add(row)
    if row is None:
        row = UserAiTitleSettingsModel(
            owner_username=owner_username,
            provider="custom_openai",
            api_base_url="",
            model_name="",
            title_prompt=DEFAULT_TITLE_PROMPT,
            subtitle_prompt=DEFAULT_SUBTITLE_PROMPT,
        )
        session.add(row)
        session.flush()
    return row


def get_settings(owner_username: str) -> dict[str, Any]:
    with session_scope() as session:
        return settings_to_public(ensure_user_settings(session, owner_username))


def update_settings(owner_username: str, payload: Any) -> dict[str, Any]:
    with session_scope() as session:
        row = ensure_user_settings(session, owner_username)
        provider = str(payload.provider or "custom_openai").strip()
        if provider not in PROVIDER_VALUES:
            raise RuntimeError("不支持的运营商类型。")
        row.provider = provider
        row.api_base_url = str(payload.apiBaseUrl or "").strip().rstrip("/")
        row.model_name = str(payload.modelName or "").strip()
        if not row.model_name:
            raise RuntimeError("请填写模型名称。")
        row.title_prompt = str(payload.titlePrompt or DEFAULT_TITLE_PROMPT).strip()
        row.subtitle_prompt = str(payload.subtitlePrompt or DEFAULT_SUBTITLE_PROMPT).strip()
        api_key = str(payload.apiKey or "").strip()
        if api_key:
            row.api_key_encrypted = encrypt_text(api_key)
        row.verified_at = None
        row.last_error = None
        session.flush()
        return settings_to_public(row)


def resolved_model_name(row: UserAiTitleSettingsModel) -> str:
    model = str(row.model_name or "").strip()
    if not model:
        return ""
    if "/" in model:
        return model
    if row.provider in {"aliyun", "siliconflow", "custom_openai", "deepseek", "moonshot"}:
        return f"openai/{model}"
    return model


def litellm_completion(**kwargs: Any) -> Any:
    from litellm import completion

    return completion(**kwargs)


def test_settings_connection(owner_username: str) -> dict[str, Any]:
    with session_scope() as session:
        row = ensure_user_settings(session, owner_username)
        api_key = decrypt_text(row.api_key_encrypted)
        if not api_key:
            raise RuntimeError("请先配置 API Key。")
        model = resolved_model_name(row)
        if not model:
            raise RuntimeError("请先配置模型名称。")
        try:
            litellm_completion(
                model=model,
                messages=[
                    {
                        "role": "user",
                        "content": [
                            {"type": "text", "text": "只回复 OK"},
                            {
                                "type": "image_url",
                                "image_url": {
                                    "url": (
                                        "data:image/png;base64,"
                                        "iVBORw0KGgoAAAANSUhEUgAAABAAAAAQCAIAAACQkWg2"
                                        "AAAAFElEQVR42mP8z8AARMAgYKSAAQYAAT0AARpN6DkAAAAASUVORK5CYII="
                                    )
                                },
                            },
                        ],
                    }
                ],
                api_key=api_key,
                api_base=row.api_base_url or None,
                max_tokens=8,
                stream=False,
                timeout=60,
                drop_params=True,
            )
        except Exception as exc:
            row.verified_at = None
            row.last_error = str(exc)[:2000]
            raise RuntimeError(f"模型连接测试失败：{exc}") from exc
        row.verified_at = datetime.now()
        row.last_error = None
        session.flush()
        return {"success": True, "message": "连接成功，图片和文本输入可用。"}


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
    if product.review_status not in {"pending", "listed"}:
        raise RuntimeError("只有待审核商品或店铺商品可以保存优化标题。")
    if version is None or version.product_id != product_id or version.owner_username != owner_username:
        raise RuntimeError("标题优化版本不存在。")
    title = truncate_utf8_bytes(version.title, RAKUTEN_TITLE_MAX_BYTES)
    subtitle = truncate_utf8_bytes(version.subtitle, RAKUTEN_TAGLINE_MAX_BYTES)
    payload = patch_local_item_detail(
        product_raw_payload(product),
        title=title,
        tagline=subtitle,
        variants=[],
    )
    product.title = title
    product.raw_payload_json = json.dumps(payload, ensure_ascii=False)
    version.title = title
    version.subtitle = subtitle
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


def delete_title_version_in_session(
    session: Any,
    *,
    owner_username: str,
    product_id: int,
    version_id: int,
) -> None:
    product = session.get(ProductModel, product_id)
    version = session.get(ProductTitleVersionModel, version_id)
    if product is None or product.owner_username != owner_username:
        raise RuntimeError("商品不存在。")
    if product.review_status not in {"pending", "listed"}:
        raise RuntimeError("只有待审核商品或店铺商品可以删除标题历史版本。")
    if version is None or version.product_id != product_id or version.owner_username != owner_username:
        raise RuntimeError("标题优化版本不存在。")
    if version.is_selected:
        raise RuntimeError("当前使用的标题版本不能删除。")
    session.delete(version)
    session.flush()


def delete_title_version(owner_username: str, product_id: int, version_id: int) -> None:
    with session_scope() as session:
        delete_title_version_in_session(
            session,
            owner_username=owner_username,
            product_id=product_id,
            version_id=version_id,
        )


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
        if product.review_status not in {"pending", "listed"}:
            raise RuntimeError("只有待审核商品或店铺商品可以优化标题。")
        settings = ensure_user_settings(session, created_by)
        api_key = decrypt_text(settings.api_key_encrypted)
        if not api_key:
            raise RuntimeError("请先在 AI 管理中配置你自己的 API Key。")
        if not settings.verified_at or settings.last_error:
            raise RuntimeError("请先在 AI 管理中保存配置并通过连接测试。")
        model_name = resolved_model_name(settings)
        if not model_name:
            raise RuntimeError("请先在 AI 管理中配置模型名称。")
        raw_payload = product_raw_payload(product)
        snapshot = _compact_product_context(product, raw_payload)
        image_data_url = _image_data_url(product, raw_payload)
        messages = [
                {
                    "role": "system",
                    "content": (
                        f"{settings.title_prompt or DEFAULT_TITLE_PROMPT}\n\n"
                        f"{settings.subtitle_prompt or DEFAULT_SUBTITLE_PROMPT}\n\n"
                        "强制长度要求：主标题 UTF-8 编码最多255字节；副标题 UTF-8 编码最多174字节。"
                        "日文汉字、假名通常占3字节，必须按字节计算，不是按字符数计算。\n\n"
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
            ]
        try:
            response = litellm_completion(
                model=model_name,
                messages=messages,
                api_key=api_key,
                api_base=settings.api_base_url or None,
                stream=True,
                temperature=0.3,
                max_tokens=1000,
                timeout=120,
                drop_params=True,
            )
        except Exception as exc:
            raise RuntimeError(f"模型调用失败：{exc}") from exc
        chunks: list[str] = []
        for chunk in response:
            try:
                delta = chunk.choices[0].delta.content or ""
            except (AttributeError, IndexError, TypeError):
                delta = ""
            if isinstance(delta, str) and delta:
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
            model_name=model_name,
            input_snapshot_json=json.dumps(snapshot, ensure_ascii=False),
            is_selected=False,
            created_by=created_by,
        )
        session.add(version)
        session.flush()
        yield {"type": "completed", "version": version_to_public(version)}


def generate_version(owner_username: str, product_id: int, created_by: str) -> dict[str, Any]:
    completed_version: dict[str, Any] | None = None
    for event in stream_generate_version(owner_username, product_id, created_by):
        if event.get("type") == "completed" and isinstance(event.get("version"), dict):
            completed_version = event["version"]
    if completed_version is None:
        raise RuntimeError("标题优化未生成有效结果。")
    return completed_version
