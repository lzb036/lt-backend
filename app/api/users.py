from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field

from app.core.auth import require_superadmin
from app.services import user_service

router = APIRouter(prefix="/users", tags=["users"])


class CreateUserRequest(BaseModel):
    username: str = Field(min_length=1)
    password: str = Field(min_length=6)
    displayName: str = ""


class UpdateUserRequest(BaseModel):
    displayName: str | None = None
    enabled: bool | None = None


class ResetPasswordRequest(BaseModel):
    password: str = Field(min_length=6)


@router.get("")
def list_user_accounts(
    page: int | None = Query(default=None, ge=1),
    pageSize: int | None = Query(default=None, ge=1, le=500),
    _: dict = Depends(require_superadmin),
) -> dict:
    result = user_service.list_users(page=page, page_size=pageSize)
    if isinstance(result, dict):
        return result
    return {"users": result, "total": len(result), "page": 1, "pageSize": len(result) or 30}


@router.post("")
def create_user(payload: CreateUserRequest, _: dict = Depends(require_superadmin)) -> dict:
    try:
        user = user_service.create_user(payload.username, payload.password, payload.displayName)
        return {"user": user}
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.put("/{username}")
def update_user(username: str, payload: UpdateUserRequest, _: dict = Depends(require_superadmin)) -> dict:
    try:
        user = user_service.update_user(
            username,
            display_name=payload.displayName,
            enabled=payload.enabled,
        )
        return {"user": user}
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.put("/{username}/password")
def reset_password(username: str, payload: ResetPasswordRequest, _: dict = Depends(require_superadmin)) -> dict:
    try:
        user = user_service.reset_password(username, payload.password)
        return {"user": user}
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
