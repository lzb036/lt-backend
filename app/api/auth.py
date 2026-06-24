from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Response
from pydantic import BaseModel, Field

from app.core.auth import authenticate_login_credentials, create_session_token, require_authenticated_account
from app.core.config import settings

router = APIRouter(prefix="/auth", tags=["auth"])


class LoginRequest(BaseModel):
    username: str = Field(min_length=1)
    password: str = Field(min_length=1)


@router.get("/session")
def session(user: dict = Depends(require_authenticated_account)) -> dict:
    return user


@router.post("/login")
def login(payload: LoginRequest, response: Response) -> dict:
    user = authenticate_login_credentials(payload.username, payload.password)
    if user is None:
        raise HTTPException(status_code=401, detail="用户名或密码错误")
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
