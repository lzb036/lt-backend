from __future__ import annotations

import time

from fastapi import APIRouter, Depends, HTTPException, Request, Response
from pydantic import BaseModel, Field

from app.core.auth import authenticate_login_credentials, create_session_token, require_authenticated_account
from app.core.config import settings

router = APIRouter(prefix="/auth", tags=["auth"])
LOGIN_FAILURES: dict[str, dict[str, int | float]] = {}


class LoginRequest(BaseModel):
    username: str = Field(min_length=1)
    password: str = Field(min_length=1)


@router.get("/session")
def session(user: dict = Depends(require_authenticated_account)) -> dict:
    return user


@router.post("/login")
def login(payload: LoginRequest, request: Request, response: Response) -> dict:
    failure_key = login_failure_key(request, payload.username)
    assert_login_not_locked(failure_key)
    user = authenticate_login_credentials(payload.username, payload.password)
    if user is None:
        record_login_failure(failure_key)
        raise HTTPException(status_code=401, detail="用户名或密码错误")
    clear_login_failure(failure_key)
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
    response.delete_cookie(key=settings.session_cookie_name, path="/")
    return {"status": "ok"}


def login_failure_key(request: Request, username: str) -> str:
    forwarded_for = request.headers.get("x-forwarded-for", "")
    client_ip = forwarded_for.split(",", 1)[0].strip() if forwarded_for else ""
    if not client_ip and request.client is not None:
        client_ip = request.client.host
    return f"{client_ip or 'unknown'}:{username.strip().lower()}"


def assert_login_not_locked(key: str) -> None:
    state = LOGIN_FAILURES.get(key)
    now = time.time()
    locked_until = float(state.get("locked_until") or 0) if state else 0
    if locked_until > now:
        remaining = max(1, int(locked_until - now))
        raise HTTPException(status_code=429, detail=f"登录失败次数过多，请 {remaining} 秒后再试")


def record_login_failure(key: str) -> None:
    now = time.time()
    state = LOGIN_FAILURES.get(key) or {"count": 0, "locked_until": 0}
    locked_until = float(state.get("locked_until") or 0)
    if locked_until and locked_until <= now:
        state = {"count": 0, "locked_until": 0}
    count = int(state.get("count") or 0) + 1
    state["count"] = count
    if count >= settings.login_max_failed_attempts:
        state["locked_until"] = now + settings.login_lockout_seconds
        state["count"] = 0
    LOGIN_FAILURES[key] = state


def clear_login_failure(key: str) -> None:
    LOGIN_FAILURES.pop(key, None)
