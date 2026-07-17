from __future__ import annotations

from datetime import datetime
from typing import Any

from app.core.secure_storage import decrypt_text, encrypt_text, mask_secret
from app.db.database import session_scope
from app.db.models import (
    UserAiTitleSettingsModel,
    UserSalesAnalysisModelSettingsModel,
)
from app.services.ai_title_service import (
    PROVIDER_VALUES,
    litellm_completion,
    provider_catalog,
)


def settings_to_public(
    row: UserSalesAnalysisModelSettingsModel,
) -> dict[str, Any]:
    key = decrypt_text(row.api_key_encrypted)
    return {
        "provider": row.provider or "custom_openai",
        "apiBaseUrl": row.api_base_url,
        "apiKeyConfigured": bool(key),
        "apiKeyMasked": mask_secret(key),
        "modelName": row.model_name,
        "verified": bool(row.verified_at and not row.last_error),
        "verifiedAt": (
            row.verified_at.isoformat(sep=" ", timespec="seconds")
            if row.verified_at
            else None
        ),
        "lastError": row.last_error,
    }


def ensure_user_settings(
    session: Any,
    owner_username: str,
) -> UserSalesAnalysisModelSettingsModel:
    row = session.get(
        UserSalesAnalysisModelSettingsModel,
        owner_username,
    )
    if row is not None:
        return row

    title_settings = session.get(
        UserAiTitleSettingsModel,
        owner_username,
    )
    row = UserSalesAnalysisModelSettingsModel(
        owner_username=owner_username,
        provider=(
            title_settings.provider
            if title_settings is not None
            else "custom_openai"
        ),
        api_base_url=(
            title_settings.api_base_url
            if title_settings is not None
            else ""
        ),
        api_key_encrypted=(
            title_settings.api_key_encrypted
            if title_settings is not None
            else ""
        ),
        model_name=(
            title_settings.model_name
            if title_settings is not None
            else ""
        ),
        verified_at=(
            title_settings.verified_at
            if title_settings is not None
            else None
        ),
        last_error=(
            title_settings.last_error
            if title_settings is not None
            else None
        ),
    )
    session.add(row)
    session.flush()
    return row


def get_settings(owner_username: str) -> dict[str, Any]:
    with session_scope() as session:
        return settings_to_public(
            ensure_user_settings(session, owner_username)
        )


def update_settings(
    owner_username: str,
    payload: Any,
) -> dict[str, Any]:
    with session_scope() as session:
        row = ensure_user_settings(session, owner_username)
        provider = str(payload.provider or "custom_openai").strip()
        if provider not in PROVIDER_VALUES:
            raise RuntimeError("不支持的运营商类型。")
        model_name = str(payload.modelName or "").strip()
        if not model_name:
            raise RuntimeError("请填写模型名称。")

        row.provider = provider
        row.api_base_url = str(
            payload.apiBaseUrl or ""
        ).strip().rstrip("/")
        row.model_name = model_name
        api_key = str(payload.apiKey or "").strip()
        if api_key:
            row.api_key_encrypted = encrypt_text(api_key)
        row.verified_at = None
        row.last_error = None
        session.flush()
        return settings_to_public(row)


def resolved_model_name(
    row: UserSalesAnalysisModelSettingsModel,
) -> str:
    model = str(row.model_name or "").strip()
    if not model:
        return ""
    if "/" in model:
        return model
    if row.provider in {
        "aliyun",
        "siliconflow",
        "custom_openai",
        "deepseek",
        "moonshot",
    }:
        return f"openai/{model}"
    return model


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
                        "content": "只回复 OK",
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
            raise RuntimeError(
                f"模型连接测试失败：{exc}"
            ) from exc
        row.verified_at = datetime.now()
        row.last_error = None
        session.flush()
        return {
            "success": True,
            "message": "连接成功，商品分析模型可用。",
        }


__all__ = [
    "ensure_user_settings",
    "get_settings",
    "provider_catalog",
    "resolved_model_name",
    "test_settings_connection",
    "update_settings",
]
