from __future__ import annotations

from typing import Literal

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field

from app.core.auth import require_authenticated_account
from app.services import profile_service, user_service

router = APIRouter(prefix="/profile", tags=["profile"])


class SecretProfileUpdate(BaseModel):
    rakutenShopUrl: str | None = None
    rakutenShopName: str | None = None
    rakutenServiceSecret: str | None = None
    rakutenLicenseKey: str | None = None
    alibabaAppKey: str | None = None
    alibabaAppSecret: str | None = None
    alibabaAccessToken: str | None = None
    logisticsBaseUrl: str | None = None
    logisticsUsername: str | None = None
    logisticsPassword: str | None = None
    proxyUrl: str | None = None
    ossAccessKeyId: str | None = None
    ossAccessKeySecret: str | None = None
    ossBucket: str | None = None
    ossEndpoint: str | None = None
    autoCrawlEnabled: bool | None = None
    autoCrawlIntervalMinutes: int | None = Field(default=None, ge=5, le=1440)


class CrawlPriceRuleUpdate(BaseModel):
    operator: Literal["all", "gt", "gte", "lt", "lte", "range"]
    value: int | None = None
    minPrice: int | None = None
    maxPrice: int | None = None


class CrawlSettingsUpdate(BaseModel):
    crawlMinPrice: int | None = None
    crawlPriceRule: CrawlPriceRuleUpdate | None = None


@router.get("/secrets")
def get_profile(user: dict = Depends(require_authenticated_account)) -> dict:
    return {"profile": profile_service.get_profile(user["username"])}


@router.put("/secrets")
def update_profile(payload: SecretProfileUpdate, user: dict = Depends(require_authenticated_account)) -> dict:
    try:
        return {"profile": profile_service.update_profile(user["username"], payload)}
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/secrets/verify")
def verify_profile(user: dict = Depends(require_authenticated_account)) -> dict:
    return {"profile": profile_service.mark_profile_verified(user["username"])}


@router.get("/crawl-settings")
def get_crawl_settings(user: dict = Depends(require_authenticated_account)) -> dict:
    return {"settings": user_service.get_crawl_settings(user["username"])}


@router.put("/crawl-settings")
def update_crawl_settings(
    payload: CrawlSettingsUpdate,
    user: dict = Depends(require_authenticated_account),
) -> dict:
    try:
        return {
            "settings": user_service.update_crawl_settings(
                user["username"],
                crawl_price_rule=payload.crawlPriceRule.model_dump() if payload.crawlPriceRule else None,
                crawl_min_price=payload.crawlMinPrice,
            )
        }
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
