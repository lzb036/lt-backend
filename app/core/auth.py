from __future__ import annotations

import base64
import hashlib
import hmac
import json
import time
from typing import Any

from fastapi import Cookie, Depends, HTTPException

from app.core.config import settings
from app.services.user_service import require_existing_account, verify_account_login

SESSION_SECRET = settings.session_secret.encode("utf-8")


def _b64encode(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).decode("ascii").rstrip("=")


def _b64decode(value: str) -> bytes:
    padding = "=" * (-len(value) % 4)
    return base64.urlsafe_b64decode(value + padding)


def create_session_token(username: str) -> str:
    payload = {"sub": username, "exp": int(time.time()) + settings.session_duration_seconds}
    payload_bytes = json.dumps(payload, separators=(",", ":"), ensure_ascii=True).encode("utf-8")
    payload_part = _b64encode(payload_bytes)
    signature = hmac.new(SESSION_SECRET, payload_part.encode("utf-8"), hashlib.sha256).digest()
    return f"{payload_part}.{_b64encode(signature)}"


def read_session_token(token: str) -> dict[str, Any]:
    try:
        payload_part, signature_part = token.split(".", 1)
    except ValueError as exc:
        raise HTTPException(status_code=401, detail="登录状态无效") from exc

    expected = hmac.new(SESSION_SECRET, payload_part.encode("utf-8"), hashlib.sha256).digest()
    actual = _b64decode(signature_part)
    if not hmac.compare_digest(actual, expected):
        raise HTTPException(status_code=401, detail="登录状态无效")

    try:
        payload = json.loads(_b64decode(payload_part).decode("utf-8"))
    except (ValueError, UnicodeDecodeError) as exc:
        raise HTTPException(status_code=401, detail="登录状态无效") from exc

    if int(payload.get("exp") or 0) <= int(time.time()):
        raise HTTPException(status_code=401, detail="登录已过期")
    return payload


def authenticate_login_credentials(username: str, password: str) -> dict[str, Any] | None:
    return verify_account_login(username, password)


def require_authenticated_account(
    session_token: str | None = Cookie(default=None, alias=settings.session_cookie_name),
) -> dict[str, Any]:
    if not session_token:
        raise HTTPException(status_code=401, detail="请先登录")
    payload = read_session_token(session_token)
    username = str(payload.get("sub") or "")
    if not username:
        raise HTTPException(status_code=401, detail="登录状态无效")
    try:
        return require_existing_account(username)
    except RuntimeError as exc:
        raise HTTPException(status_code=401, detail=str(exc)) from exc


def require_superadmin(user: dict[str, Any] = Depends(require_authenticated_account)) -> dict[str, Any]:
    if user.get("role") != "superadmin":
        raise HTTPException(status_code=403, detail="需要超级管理员权限")
    return user
