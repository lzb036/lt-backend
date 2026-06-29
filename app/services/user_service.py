from __future__ import annotations

import base64
import hashlib
import hmac
import json
import secrets
from typing import Any

from sqlalchemy import func, select

from app.core.config import settings
from app.db.database import session_scope
from app.db.models import UserAccountModel, UserSecretProfileModel

PASSWORD_ITERATIONS = 240000
MAX_PAGE_SIZE = 500
DEFAULT_USER_PERMISSIONS = ["crawler.manage", "products.manage", "stores.manage"]
SUPERADMIN_PERMISSIONS = [
    "users.manage",
    "crawler.manage",
    "products.manage",
    "stores.manage",
    "settings.manage",
]
KNOWN_PERMISSIONS = set(SUPERADMIN_PERMISSIONS)


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


def permissions_to_flags(permissions: list[str], *, role: str) -> dict[str, bool]:
    permission_set = set(permissions)
    return {
        "manageUsers": role == "superadmin" or "users.manage" in permission_set,
        "manageOwnSecrets": True,
        "manageCrawler": "crawler.manage" in permission_set or role == "superadmin",
        "manageProducts": "products.manage" in permission_set or role == "superadmin",
        "manageStores": "stores.manage" in permission_set or role == "superadmin",
        "manageSettings": "settings.manage" in permission_set or role == "superadmin",
    }


def account_to_public(row: UserAccountModel) -> dict[str, Any]:
    permissions = parse_permissions_json(row.permissions_json, role=row.role)
    return {
        "username": row.username,
        "displayName": row.display_name or row.username,
        "role": row.role,
        "enabled": bool(row.enabled),
        "permissionCodes": permissions,
        "permissions": permissions_to_flags(permissions, role=row.role),
        "createdAt": row.created_at.isoformat(sep=" ") if row.created_at else None,
        "updatedAt": row.updated_at.isoformat(sep=" ") if row.updated_at else None,
    }


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
        return account_to_public(row)


def require_existing_account(username: str) -> dict[str, Any]:
    with session_scope() as session:
        row = session.get(UserAccountModel, normalize_username(username))
        if row is None or not row.enabled:
            raise RuntimeError("用户不存在或已停用。")
        return account_to_public(row)


def list_users(*, page: int | None = None, page_size: int | None = None) -> list[dict[str, Any]] | dict[str, Any]:
    ensure_initial_superadmin()
    with session_scope() as session:
        return paginate_query(session, select(UserAccountModel), page=page, page_size=page_size)


def create_user(username: str, password: str, display_name: str = "", permissions: Any = None) -> dict[str, Any]:
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
            permissions_json=json.dumps(normalize_permissions(permissions), ensure_ascii=False),
            password_salt_b64=password_record["salt_b64"],
            password_hash_b64=password_record["hash_b64"],
            password_iterations=password_record["iterations"],
        )
        session.add(row)
        session.add(UserSecretProfileModel(owner_username=username))
        session.flush()
        return account_to_public(row)


def update_user(
    username: str,
    *,
    display_name: str | None = None,
    enabled: bool | None = None,
    permissions: Any = None,
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
        if permissions is not None and row.role != "superadmin":
            row.permissions_json = json.dumps(normalize_permissions(permissions, role=row.role), ensure_ascii=False)
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
