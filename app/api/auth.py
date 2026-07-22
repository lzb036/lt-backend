from __future__ import annotations

import hashlib
import ipaddress
import time

from fastapi import APIRouter, Depends, HTTPException, Request, Response
from pydantic import BaseModel, Field

from app.core.auth import authenticate_login_credentials, create_session_token, require_authenticated_account
from app.core.config import settings
from app.core.task_queue import redis_connection

router = APIRouter(prefix="/auth", tags=["auth"])
LOGIN_FAILURES: dict[str, dict[str, int | float]] = {}
MAX_MEMORY_LOGIN_FAILURE_KEYS = 10_000


class LoginRequest(BaseModel):
    username: str = Field(min_length=1)
    password: str = Field(min_length=1)


@router.get("/session")
def session(user: dict = Depends(require_authenticated_account)) -> dict:
    return user


@router.post("/login")
def login(payload: LoginRequest, request: Request, response: Response) -> dict:
    pair_key, ip_key = login_failure_keys(request, payload.username)
    failure_keys = (
        (pair_key, settings.login_max_failed_attempts),
        (ip_key, settings.login_max_failed_attempts * 10),
    )
    assert_login_not_locked(failure_keys)
    user = authenticate_login_credentials(payload.username, payload.password)
    if user is None:
        record_login_failure(failure_keys)
        raise HTTPException(status_code=401, detail="用户名或密码错误")
    clear_login_failure(pair_key)
    token = create_session_token(user["username"])
    response.set_cookie(
        key=settings.session_cookie_name,
        value=token,
        httponly=True,
        secure=settings.session_cookie_secure,
        samesite="lax",
        max_age=settings.session_duration_seconds,
        path="/",
    )
    return user


@router.post("/logout")
def logout(response: Response) -> dict[str, str]:
    response.delete_cookie(
        key=settings.session_cookie_name,
        path="/",
        secure=settings.session_cookie_secure,
        httponly=True,
        samesite="lax",
    )
    return {"status": "ok"}


def login_failure_keys(request: Request, username: str) -> tuple[str, str]:
    client_ip = login_client_ip(request)
    normalized_username = username.strip().lower()
    return (
        login_failure_key("pair", f"{client_ip}:{normalized_username}"),
        login_failure_key("ip", client_ip),
    )


def login_client_ip(request: Request) -> str:
    candidates = [
        request.headers.get("x-real-ip", "").strip(),
        request.client.host if request.client is not None else "",
    ]
    for candidate in candidates:
        try:
            return str(ipaddress.ip_address(candidate))
        except ValueError:
            continue
    return "unknown"


def login_failure_key(kind: str, value: str) -> str:
    digest = hashlib.sha256(value.encode("utf-8")).hexdigest()
    return f"lt:auth:failures:{kind}:{digest}"


def assert_login_not_locked(keys: tuple[tuple[str, int], ...]) -> None:
    try:
        connection = redis_connection()
        for key, limit in keys:
            count = int(connection.get(key) or 0)
            if count < limit:
                continue
            remaining = max(1, int(connection.ttl(key) or settings.login_lockout_seconds))
            raise HTTPException(status_code=429, detail=f"登录失败次数过多，请 {remaining} 秒后再试")
        return
    except HTTPException:
        raise
    except Exception:
        pass

    for key, limit in keys:
        assert_memory_login_not_locked(key, limit)


def assert_memory_login_not_locked(key: str, limit: int) -> None:
    state = LOGIN_FAILURES.get(key)
    now = time.time()
    locked_until = float(state.get("locked_until") or 0) if state else 0
    count = int(state.get("count") or 0) if state else 0
    if locked_until and locked_until <= now:
        LOGIN_FAILURES.pop(key, None)
        return
    if locked_until > now or count >= limit:
        remaining = max(1, int(locked_until - now))
        raise HTTPException(status_code=429, detail=f"登录失败次数过多，请 {remaining} 秒后再试")


def record_login_failure(keys: tuple[tuple[str, int], ...]) -> None:
    try:
        connection = redis_connection()
        pipeline = connection.pipeline()
        for key, _limit in keys:
            pipeline.incr(key)
            pipeline.expire(key, settings.login_lockout_seconds)
        pipeline.execute()
        return
    except Exception:
        pass

    cleanup_memory_login_failures()
    for key, limit in keys:
        record_memory_login_failure(key, limit)


def record_memory_login_failure(key: str, limit: int) -> None:
    now = time.time()
    state = LOGIN_FAILURES.get(key) or {"count": 0, "locked_until": 0}
    locked_until = float(state.get("locked_until") or 0)
    if locked_until and locked_until <= now:
        state = {"count": 0, "locked_until": 0}
    count = int(state.get("count") or 0) + 1
    state["count"] = count
    if count >= limit:
        state["locked_until"] = now + settings.login_lockout_seconds
    LOGIN_FAILURES[key] = state


def clear_login_failure(key: str) -> None:
    try:
        redis_connection().delete(key)
    except Exception:
        pass
    LOGIN_FAILURES.pop(key, None)


def cleanup_memory_login_failures() -> None:
    if len(LOGIN_FAILURES) < MAX_MEMORY_LOGIN_FAILURE_KEYS:
        return
    now = time.time()
    expired_keys = [
        key
        for key, state in LOGIN_FAILURES.items()
        if float(state.get("locked_until") or 0) <= now
    ]
    for key in expired_keys:
        LOGIN_FAILURES.pop(key, None)
    while len(LOGIN_FAILURES) >= MAX_MEMORY_LOGIN_FAILURE_KEYS:
        LOGIN_FAILURES.pop(next(iter(LOGIN_FAILURES)))
