from __future__ import annotations

import base64
import hashlib
import hmac
import json
import re
import secrets
from typing import Any

from sqlalchemy import delete, func, select, update

from app.core.config import settings
from app.db.database import session_scope
from app.db.models import (
    CrawlLogModel,
    ProductModel,
    SystemMaintenanceSettingModel,
    SystemSettingModel,
    SystemTaskControlModel,
    UserAccountModel,
    UserSecretProfileModel,
)

PASSWORD_ITERATIONS = 240000
MAX_PAGE_SIZE = 500
DEFAULT_USER_PERMISSIONS = [
    "crawler.manage",
    "products.manage",
    "stores.manage",
    "ai.manage",
]
SUPERADMIN_PERMISSIONS = [
    "users.manage",
    "crawler.manage",
    "products.manage",
    "stores.manage",
    "settings.manage",
    "ai.manage",
]
KNOWN_PERMISSIONS = set(SUPERADMIN_PERMISSIONS)
CRAWL_PRICE_OPERATORS = {"all", "gt", "gte", "lt", "lte", "range"}
CRAWL_PRICE_MAX_VALUE = 10_000_000
PAGINATION_PREFERENCE_KEY_PATTERN = re.compile(r"^[a-z0-9][a-z0-9:._-]{0,127}$")
MAX_PAGINATION_PREFERENCES = 100
DELETED_PRODUCT_IMAGE_CLEANUP_USER_SETTING_PREFIX = "deletedProductImageCleanup:"


def normalize_username(value: Any) -> str:
    return str(value or "").strip()


def normalize_page_params(page: int | None, page_size: int | None) -> tuple[int, int | None]:
    normalized_page = max(1, int(page or 1))
    normalized_page_size = min(MAX_PAGE_SIZE, max(1, int(page_size or 0))) if page_size else None
    return normalized_page, normalized_page_size


def paginate_query(session: Any, query: Any, *, page: int | None, page_size: int | None) -> list[dict[str, Any]] | dict[str, Any]:
    normalized_page, normalized_page_size = normalize_page_params(page, page_size)
    if not normalized_page_size:
        rows = session.scalars(query.order_by(UserAccountModel.created_at.asc())).all()
        return [account_to_public(row) for row in rows]

    total = int(session.scalar(select(func.count()).select_from(query.order_by(None).subquery())) or 0)
    if total:
        max_page = max(1, (total + normalized_page_size - 1) // normalized_page_size)
        normalized_page = min(normalized_page, max_page)
    rows = session.scalars(
        query.order_by(UserAccountModel.created_at.asc())
        .offset((normalized_page - 1) * normalized_page_size)
        .limit(normalized_page_size)
    ).all()
    return {
        "users": [account_to_public(row) for row in rows],
        "total": total,
        "page": normalized_page,
        "pageSize": normalized_page_size,
    }


def hash_password(password: str, *, salt: bytes | None = None, iterations: int = PASSWORD_ITERATIONS) -> dict[str, Any]:
    password_salt = salt or secrets.token_bytes(16)
    password_hash = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), password_salt, iterations)
    return {
        "salt_b64": base64.b64encode(password_salt).decode("ascii"),
        "hash_b64": base64.b64encode(password_hash).decode("ascii"),
        "iterations": iterations,
    }


def verify_password(password: str, *, salt_b64: str, hash_b64: str, iterations: int) -> bool:
    try:
        salt = base64.b64decode(salt_b64)
        expected = base64.b64decode(hash_b64)
    except ValueError:
        return False
    actual = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, iterations)
    return hmac.compare_digest(actual, expected)


def normalize_permissions(values: Any, *, role: str = "operator") -> list[str]:
    if role == "superadmin":
        return list(SUPERADMIN_PERMISSIONS)
    if values is None:
        values = DEFAULT_USER_PERMISSIONS
    normalized: list[str] = []
    for value in values or []:
        permission = str(value or "").strip()
        if permission in KNOWN_PERMISSIONS and permission not in normalized and permission != "users.manage":
            normalized.append(permission)
    return normalized


def fixed_permissions_for_role(role: str) -> list[str]:
    if role == "superadmin":
        return list(SUPERADMIN_PERMISSIONS)
    return list(DEFAULT_USER_PERMISSIONS)


def parse_permissions_json(value: str | None, *, role: str = "operator") -> list[str]:
    if role == "superadmin":
        return list(SUPERADMIN_PERMISSIONS)
    if value is None or not str(value).strip():
        return list(DEFAULT_USER_PERMISSIONS)
    try:
        raw_value = json.loads(value)
    except ValueError:
        raw_value = DEFAULT_USER_PERMISSIONS
    return normalize_permissions(raw_value, role=role)


def normalize_pagination_preferences(value: Any) -> dict[str, int]:
    if isinstance(value, str):
        try:
            value = json.loads(value or "{}")
        except ValueError:
            value = {}
    if not isinstance(value, dict):
        return {}
    normalized: dict[str, int] = {}
    for raw_key, raw_page_size in value.items():
        key = str(raw_key or "").strip()
        if not PAGINATION_PREFERENCE_KEY_PATTERN.fullmatch(key):
            continue
        try:
            page_size = int(raw_page_size)
        except (TypeError, ValueError):
            continue
        if 1 <= page_size <= MAX_PAGE_SIZE:
            normalized[key] = page_size
        if len(normalized) >= MAX_PAGINATION_PREFERENCES:
            break
    return normalized


def normalize_pagination_preference_key(value: Any) -> str:
    key = str(value or "").strip()
    if not PAGINATION_PREFERENCE_KEY_PATTERN.fullmatch(key):
        raise RuntimeError("分页列表标识无效。")
    return key


def permissions_to_flags(permissions: list[str], *, role: str) -> dict[str, bool]:
    permission_set = set(permissions)
    return {
        "manageUsers": role == "superadmin" or "users.manage" in permission_set,
        "manageOwnSecrets": True,
        "manageCrawler": "crawler.manage" in permission_set or role == "superadmin",
        "manageProducts": "products.manage" in permission_set or role == "superadmin",
        "manageStores": "stores.manage" in permission_set or role == "superadmin",
        "manageSettings": "settings.manage" in permission_set or role == "superadmin",
        "manageAi": "ai.manage" in permission_set or role == "superadmin",
    }


def normalize_crawl_min_price(value: Any) -> int:
    try:
        normalized = int(value or 0)
    except (TypeError, ValueError) as exc:
        raise RuntimeError("采集价格设置无效。") from exc
    if normalized not in {0, 2500, 3800}:
        raise RuntimeError("采集价格只能选择：全部、2500 日元以上、3800 日元以上。")
    return normalized


def normalize_crawl_price_value(value: Any, field_label: str) -> int:
    try:
        normalized = int(value)
    except (TypeError, ValueError) as exc:
        raise RuntimeError(f"{field_label}必须是整数。") from exc
    if normalized < 1 or normalized > CRAWL_PRICE_MAX_VALUE:
        raise RuntimeError(f"{field_label}必须在 1 至 {CRAWL_PRICE_MAX_VALUE:,} 日元之间。")
    return normalized


def legacy_crawl_price_rule(crawl_min_price: Any) -> dict[str, Any]:
    normalized = normalize_crawl_min_price(crawl_min_price)
    if normalized in {2500, 3800}:
        return {"operator": "gte", "value": normalized}
    return {"operator": "all"}


def normalize_crawl_price_rule(value: Any, *, legacy_min_price: Any = 0) -> dict[str, Any]:
    if value is None:
        return legacy_crawl_price_rule(legacy_min_price)
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except ValueError:
            return legacy_crawl_price_rule(legacy_min_price)
    if not isinstance(value, dict) or not value:
        return legacy_crawl_price_rule(legacy_min_price)
    operator = str(value.get("operator") or "").strip().lower()
    if operator not in CRAWL_PRICE_OPERATORS:
        raise RuntimeError("采集价格条件无效。")
    if operator == "all":
        return {"operator": "all"}
    if operator == "range":
        min_price = normalize_crawl_price_value(value.get("minPrice"), "价格下限")
        max_price = normalize_crawl_price_value(value.get("maxPrice"), "价格上限")
        if min_price >= max_price:
            raise RuntimeError("价格区间下限必须小于上限。")
        return {"operator": "range", "minPrice": min_price, "maxPrice": max_price}
    return {
        "operator": operator,
        "value": normalize_crawl_price_value(value.get("value"), "价格"),
    }


def account_crawl_price_rule(row: UserAccountModel) -> dict[str, Any]:
    return normalize_crawl_price_rule(
        row.crawl_price_rule_json,
        legacy_min_price=row.crawl_min_price,
    )


def crawl_min_price_from_rule(rule: dict[str, Any]) -> int:
    if rule.get("operator") == "gte" and rule.get("value") in {2500, 3800}:
        return int(rule["value"])
    return 0


def account_to_public(
    row: UserAccountModel,
    *,
    include_pagination_preferences: bool = False,
) -> dict[str, Any]:
    permissions = fixed_permissions_for_role(row.role)
    crawl_price_rule = account_crawl_price_rule(row)
    payload = {
        "username": row.username,
        "displayName": row.display_name or row.username,
        "role": row.role,
        "enabled": bool(row.enabled),
        "crawlMinPrice": crawl_min_price_from_rule(crawl_price_rule),
        "crawlPriceRule": crawl_price_rule,
        "permissionCodes": permissions,
        "permissions": permissions_to_flags(permissions, role=row.role),
        "createdAt": row.created_at.isoformat(sep=" ") if row.created_at else None,
        "updatedAt": row.updated_at.isoformat(sep=" ") if row.updated_at else None,
    }
    if include_pagination_preferences:
        payload["paginationPreferences"] = normalize_pagination_preferences(
            row.pagination_preferences_json
        )
    return payload


def ensure_initial_superadmin() -> None:
    username = normalize_username(settings.initial_superadmin_username) or "superadmin"
    with session_scope() as session:
        row = session.get(UserAccountModel, username)
        if row is None:
            password_record = hash_password(settings.initial_superadmin_password)
            row = UserAccountModel(
                username=username,
                display_name="超级管理员",
                role="superadmin",
                enabled=True,
                permissions_json=json.dumps(SUPERADMIN_PERMISSIONS, ensure_ascii=False),
                password_salt_b64=password_record["salt_b64"],
                password_hash_b64=password_record["hash_b64"],
                password_iterations=password_record["iterations"],
            )
            session.add(row)
        row.role = "superadmin"
        row.enabled = True
        row.permissions_json = json.dumps(SUPERADMIN_PERMISSIONS, ensure_ascii=False)
        session.flush()


def verify_account_login(username: str, password: str) -> dict[str, Any] | None:
    ensure_initial_superadmin()
    normalized_username = normalize_username(username)
    if not normalized_username:
        return None
    with session_scope() as session:
        row = session.get(UserAccountModel, normalized_username)
        if row is None or not row.enabled:
            return None
        if not verify_password(
            password,
            salt_b64=row.password_salt_b64,
            hash_b64=row.password_hash_b64,
            iterations=row.password_iterations,
        ):
            return None
        return account_to_public(row, include_pagination_preferences=True)


def require_existing_account(username: str) -> dict[str, Any]:
    with session_scope() as session:
        row = session.get(UserAccountModel, normalize_username(username))
        if row is None or not row.enabled:
            raise RuntimeError("用户不存在或已停用。")
        return account_to_public(row, include_pagination_preferences=True)


def get_crawl_settings(username: str) -> dict[str, Any]:
    with session_scope() as session:
        row = session.get(UserAccountModel, normalize_username(username))
        if row is None or not row.enabled:
            raise RuntimeError("用户不存在或已停用。")
        rule = account_crawl_price_rule(row)
        return {
            "crawlMinPrice": crawl_min_price_from_rule(rule),
            "crawlPriceRule": rule,
        }


def update_crawl_settings(
    username: str,
    *,
    crawl_price_rule: Any = None,
    crawl_min_price: Any = None,
) -> dict[str, Any]:
    if crawl_price_rule is None and crawl_min_price is None:
        raise RuntimeError("请设置采集价格条件。")
    with session_scope() as session:
        row = session.get(UserAccountModel, normalize_username(username))
        if row is None or not row.enabled:
            raise RuntimeError("用户不存在或已停用。")
        rule = normalize_crawl_price_rule(
            crawl_price_rule,
            legacy_min_price=crawl_min_price,
        )
        row.crawl_price_rule_json = json.dumps(rule, ensure_ascii=False)
        row.crawl_min_price = crawl_min_price_from_rule(rule)
        session.flush()
        return {
            "crawlMinPrice": row.crawl_min_price,
            "crawlPriceRule": rule,
        }


def update_pagination_preference(username: str, list_key: Any, page_size: Any) -> dict[str, int]:
    normalized_key = normalize_pagination_preference_key(list_key)
    try:
        normalized_page_size = int(page_size)
    except (TypeError, ValueError) as exc:
        raise RuntimeError("分页数量必须是整数。") from exc
    if normalized_page_size < 1 or normalized_page_size > MAX_PAGE_SIZE:
        raise RuntimeError(f"分页数量必须在 1 至 {MAX_PAGE_SIZE} 之间。")

    with session_scope() as session:
        row = session.get(UserAccountModel, normalize_username(username))
        if row is None or not row.enabled:
            raise RuntimeError("用户不存在或已停用。")
        preferences = normalize_pagination_preferences(row.pagination_preferences_json)
        preferences[normalized_key] = normalized_page_size
        row.pagination_preferences_json = json.dumps(preferences, ensure_ascii=False, sort_keys=True)
        session.flush()
        return preferences


def list_users(
    *,
    page: int | None = None,
    page_size: int | None = None,
    username: str | None = None,
    display_name: str | None = None,
) -> list[dict[str, Any]] | dict[str, Any]:
    ensure_initial_superadmin()
    with session_scope() as session:
        query = select(UserAccountModel)
        normalized_username = normalize_username(username)
        normalized_display_name = normalize_username(display_name)
        if normalized_username:
            query = query.where(UserAccountModel.username.like(f"%{normalized_username}%"))
        if normalized_display_name:
            query = query.where(UserAccountModel.display_name.like(f"%{normalized_display_name}%"))
        return paginate_query(session, query, page=page, page_size=page_size)


def create_user(username: str, password: str, display_name: str = "") -> dict[str, Any]:
    username = normalize_username(username)
    if not username:
        raise RuntimeError("用户名不能为空。")
    if len(password) < 6:
        raise RuntimeError("密码至少需要 6 位。")
    password_record = hash_password(password)
    with session_scope() as session:
        if session.get(UserAccountModel, username) is not None:
            raise RuntimeError("用户名已存在。")
        row = UserAccountModel(
            username=username,
            display_name=(display_name or username).strip(),
            role="operator",
            enabled=True,
            permissions_json=json.dumps(DEFAULT_USER_PERMISSIONS, ensure_ascii=False),
            password_salt_b64=password_record["salt_b64"],
            password_hash_b64=password_record["hash_b64"],
            password_iterations=password_record["iterations"],
        )
        session.add(row)
        session.add(UserSecretProfileModel(owner_username=username))
        session.flush()
        from app.services.sensitive_word_service import seed_default_sensitive_words_for_user

        seed_default_sensitive_words_for_user(session, username)
        return account_to_public(row)


def update_user(
    username: str,
    *,
    display_name: str | None = None,
    enabled: bool | None = None,
) -> dict[str, Any]:
    with session_scope() as session:
        row = session.get(UserAccountModel, normalize_username(username))
        if row is None:
            raise RuntimeError("用户不存在。")
        if row.role == "superadmin" and enabled is False:
            raise RuntimeError("不能停用超级管理员。")
        if display_name is not None:
            row.display_name = display_name.strip() or row.username
        if enabled is not None:
            row.enabled = bool(enabled)
        if row.role != "superadmin":
            row.permissions_json = json.dumps(DEFAULT_USER_PERMISSIONS, ensure_ascii=False)
        session.flush()
        return account_to_public(row)


def reset_password(username: str, password: str) -> dict[str, Any]:
    if len(password) < 6:
        raise RuntimeError("密码至少需要 6 位。")
    with session_scope() as session:
        row = session.get(UserAccountModel, normalize_username(username))
        if row is None:
            raise RuntimeError("用户不存在。")
        password_record = hash_password(password)
        row.password_salt_b64 = password_record["salt_b64"]
        row.password_hash_b64 = password_record["hash_b64"]
        row.password_iterations = password_record["iterations"]
        session.flush()
        return account_to_public(row)


def delete_user(username: str, *, confirmation_text: str) -> dict[str, Any]:
    normalized_username = normalize_username(username)
    expected_confirmation = f"删除用户 {normalized_username}"
    if confirmation_text.strip() != expected_confirmation:
        raise RuntimeError(f"请输入“{expected_confirmation}”确认删除。")

    product_ids: list[int] = []
    with session_scope() as session:
        row = session.get(UserAccountModel, normalized_username)
        if row is None:
            raise RuntimeError("用户不存在。")
        if row.role == "superadmin":
            raise RuntimeError("不能删除超级管理员。")

        product_ids = [
            int(product_id)
            for product_id in session.scalars(
                select(ProductModel.id).where(
                    ProductModel.owner_username == normalized_username,
                )
            ).all()
        ]
        session.execute(
            delete(CrawlLogModel).where(
                CrawlLogModel.owner_username == normalized_username,
            )
        )
        session.execute(
            delete(SystemSettingModel).where(
                SystemSettingModel.key
                == f"{DELETED_PRODUCT_IMAGE_CLEANUP_USER_SETTING_PREFIX}{normalized_username}",
            )
        )
        session.execute(
            update(SystemMaintenanceSettingModel)
            .where(SystemMaintenanceSettingModel.updated_by == normalized_username)
            .values(updated_by="")
        )
        session.execute(
            update(SystemTaskControlModel)
            .where(SystemTaskControlModel.stopped_by == normalized_username)
            .values(stopped_by="")
        )
        session.execute(
            update(SystemTaskControlModel)
            .where(SystemTaskControlModel.resumed_by == normalized_username)
            .values(resumed_by="")
        )
        session.delete(row)

    if product_ids:
        from app.services.crawler_service import cleanup_product_image_ids

        cleanup_product_image_ids(product_ids)
    return {
        "deleted": True,
        "username": normalized_username,
        "deletedProductCount": len(product_ids),
    }
