from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field

from app.core.auth import require_authenticated_account
from app.services import crawler_service

router = APIRouter(prefix="/crawler", tags=["crawler"])


class CrawlSourcePayload(BaseModel):
    name: str = Field(min_length=1)
    sourceType: str = Field(pattern="^(keyword|shop|ranking|product_url)$")
    target: str = Field(min_length=1)
    enabled: bool = True
    scheduleEnabled: bool = False
    intervalMinutes: int = Field(default=60, ge=5, le=1440)
    notes: str = ""


class CreateTaskPayload(BaseModel):
    sourceId: int | None = None
    sourceType: str | None = None
    target: str | None = None
    mode: str = "manual"


@router.get("/sources")
def list_sources(user: dict = Depends(require_authenticated_account)) -> dict:
    return {"sources": crawler_service.list_sources(user["username"])}


@router.post("/sources")
def create_source(payload: CrawlSourcePayload, user: dict = Depends(require_authenticated_account)) -> dict:
    try:
        source = crawler_service.save_source(user["username"], payload)
        return {"source": source, "sources": crawler_service.list_sources(user["username"])}
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.put("/sources/{source_id}")
def update_source(source_id: int, payload: CrawlSourcePayload, user: dict = Depends(require_authenticated_account)) -> dict:
    try:
        source = crawler_service.save_source(user["username"], payload, source_id)
        return {"source": source, "sources": crawler_service.list_sources(user["username"])}
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.delete("/sources/{source_id}")
def delete_source(source_id: int, user: dict = Depends(require_authenticated_account)) -> dict:
    try:
        crawler_service.delete_source(user["username"], source_id)
        return {"sources": crawler_service.list_sources(user["username"])}
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/tasks")
def list_tasks(user: dict = Depends(require_authenticated_account)) -> dict:
    return {"tasks": crawler_service.list_tasks(user["username"])}


@router.post("/tasks")
def create_task(payload: CreateTaskPayload, user: dict = Depends(require_authenticated_account)) -> dict:
    try:
        task = crawler_service.create_task(user["username"], payload)
        return {"task": task, "tasks": crawler_service.list_tasks(user["username"])}
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/products")
def list_products(
    status: str | None = Query(default=None),
    keyword: str | None = Query(default=None),
    user: dict = Depends(require_authenticated_account),
) -> dict:
    return {"products": crawler_service.list_products(user["username"], status=status, keyword=keyword)}
