from __future__ import annotations

from datetime import datetime
from typing import Any

from app.core.secure_storage import decrypt_text, encrypt_text, mask_secret
from app.db.database import session_scope
from app.db.models import UserSecretProfileModel

ENCRYPTED_FIELDS = {
    "rakuten_service_secret_encrypted": "rakutenServiceSecret",
    "rakuten_license_key_encrypted": "rakutenLicenseKey",
    "alibaba_app_key_encrypted": "alibabaAppKey",
    "alibaba_app_secret_encrypted": "alibabaAppSecret",
    "alibaba_access_token_encrypted": "alibabaAccessToken",
    "logistics_username_encrypted": "logisticsUsername",
    "logistics_password_encrypted": "logisticsPassword",
    "proxy_url_encrypted": "proxyUrl",
    "oss_access_key_id_encrypted": "ossAccessKeyId",
    "oss_access_key_secret_encrypted": "ossAccessKeySecret",
}


def _decrypted(row: UserSecretProfileModel, field: str) -> str:
    return decrypt_text(str(getattr(row, field) or ""))


def _profile_to_public(row: UserSecretProfileModel, *, reveal: bool = False) -> dict[str, Any]:
    secrets = {payload_field: _decrypted(row, model_field) for model_field, payload_field in ENCRYPTED_FIELDS.items()}
    return {
        "ownerUsername": row.owner_username,
        "rakutenShopUrl": row.rakuten_shop_url,
        "rakutenShopName": row.rakuten_shop_name,
        "rakutenServiceSecret": secrets["rakutenServiceSecret"] if reveal else "",
        "rakutenLicenseKey": secrets["rakutenLicenseKey"] if reveal else "",
        "alibabaAppKey": secrets["alibabaAppKey"] if reveal else "",
        "alibabaAppSecret": secrets["alibabaAppSecret"] if reveal else "",
        "alibabaAccessToken": secrets["alibabaAccessToken"] if reveal else "",
        "logisticsBaseUrl": row.logistics_base_url,
        "logisticsUsername": secrets["logisticsUsername"] if reveal else "",
        "logisticsPassword": secrets["logisticsPassword"] if reveal else "",
        "proxyUrl": secrets["proxyUrl"] if reveal else "",
        "ossAccessKeyId": secrets["ossAccessKeyId"] if reveal else "",
        "ossAccessKeySecret": secrets["ossAccessKeySecret"] if reveal else "",
        "masked": {key: mask_secret(value) for key, value in secrets.items()},
        "ossBucket": row.oss_bucket,
        "ossEndpoint": row.oss_endpoint,
        "defaultPriceMultiplier": row.default_price_multiplier,
        "autoCrawlEnabled": bool(row.auto_crawl_enabled),
        "autoCrawlIntervalMinutes": row.auto_crawl_interval_minutes,
        "lastVerifiedAt": row.last_verified_at.isoformat(sep=" ") if row.last_verified_at else None,
        "lastError": row.last_error,
        "updatedAt": row.updated_at.isoformat(sep=" ") if row.updated_at else None,
    }


def get_profile(username: str, *, reveal: bool = False) -> dict[str, Any]:
    with session_scope() as session:
        row = session.get(UserSecretProfileModel, username)
        if row is None:
            row = UserSecretProfileModel(owner_username=username)
            session.add(row)
            session.flush()
        return _profile_to_public(row, reveal=reveal)


def update_profile(username: str, payload: Any) -> dict[str, Any]:
    with session_scope() as session:
        row = session.get(UserSecretProfileModel, username)
        if row is None:
            row = UserSecretProfileModel(owner_username=username)
            session.add(row)

        normal_fields = {
            "rakuten_shop_url": "rakutenShopUrl",
            "rakuten_shop_name": "rakutenShopName",
            "logistics_base_url": "logisticsBaseUrl",
            "oss_bucket": "ossBucket",
            "oss_endpoint": "ossEndpoint",
            "default_price_multiplier": "defaultPriceMultiplier",
            "auto_crawl_enabled": "autoCrawlEnabled",
            "auto_crawl_interval_minutes": "autoCrawlIntervalMinutes",
        }
        for model_field, payload_field in normal_fields.items():
            if hasattr(payload, payload_field):
                value = getattr(payload, payload_field)
                if value is not None:
                    setattr(row, model_field, value)

        for model_field, payload_field in ENCRYPTED_FIELDS.items():
            if hasattr(payload, payload_field):
                value = getattr(payload, payload_field)
                if value is not None and str(value).strip():
                    setattr(row, model_field, encrypt_text(str(value)))

        row.last_error = None
        session.flush()
        return _profile_to_public(row)


def mark_profile_verified(username: str) -> dict[str, Any]:
    with session_scope() as session:
        row = session.get(UserSecretProfileModel, username)
        if row is None:
            row = UserSecretProfileModel(owner_username=username)
            session.add(row)
        row.last_verified_at = datetime.now()
        row.last_error = None
        session.flush()
        return _profile_to_public(row)
