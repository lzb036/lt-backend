from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field

from app.core.auth import require_authenticated_account, require_superadmin
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


class StorePayload(BaseModel):
    storeCode: str = Field(min_length=1)
    storeName: str = Field(min_length=1)
    aliasName: str = ""
    platform: str = "rakuten"
    storeUrl: str = ""
    enabled: bool = True
    contactName: str = ""
    contactPhone: str = ""
    description: str = ""
    rakutenServiceSecret: str = ""
    rakutenLicenseKey: str = ""
    priceMultiplier: str = "1.00"


class ScheduledCrawlPayload(BaseModel):
    sourceId: int | None = None
    name: str = Field(min_length=1)
    crawlContent: str = ""
    crawlCondition: str = ""
    sourceType: str = Field(default="keyword", pattern="^(keyword|shop|ranking|product_url)$")
    target: str = ""
    enabled: bool = True
    intervalMinutes: int = Field(default=60, ge=5, le=1440)
    notes: str = ""


class ProductStatusPayload(BaseModel):
    productIds: list[int] = Field(default_factory=list)
    status: str = Field(pattern="^(pending|approved|error|listed|rejected)$")
    message: str = ""


class ProductDeletePayload(BaseModel):
    productIds: list[int] = Field(default_factory=list)


class ListingTaskPayload(BaseModel):
    productIds: list[int] = Field(default_factory=list)
    storeId: int | None = None
    taskName: str = ""


class RolePayload(BaseModel):
    name: str = Field(min_length=1)
    code: str = Field(min_length=1)
    scope: str = "own"
    enabled: bool = True
    permissions: list[str] = Field(default_factory=list)
    notes: str = ""


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


@router.post("/tasks/{task_id}/restart")
def restart_task(task_id: str, user: dict = Depends(require_authenticated_account)) -> dict:
    try:
        task = crawler_service.run_existing_task(user["username"], task_id)
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


@router.put("/products/status")
def update_product_status(payload: ProductStatusPayload, user: dict = Depends(require_authenticated_account)) -> dict:
    try:
        products = crawler_service.update_product_status(
            user["username"],
            payload.productIds,
            payload.status,
            message=payload.message,
        )
        return {"products": products}
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.api_route("/products", methods=["DELETE"])
def delete_products(payload: ProductDeletePayload, user: dict = Depends(require_authenticated_account)) -> dict:
    crawler_service.delete_products(user["username"], payload.productIds)
    return {"products": crawler_service.list_products(user["username"])}


@router.get("/stores")
def list_stores(user: dict = Depends(require_authenticated_account)) -> dict:
    return {"stores": crawler_service.list_stores(user["username"])}


@router.post("/stores")
def create_store(payload: StorePayload, user: dict = Depends(require_authenticated_account)) -> dict:
    try:
        store = crawler_service.save_store(user["username"], payload)
        return {"store": store, "stores": crawler_service.list_stores(user["username"])}
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.put("/stores/{store_id}")
def update_store(store_id: int, payload: StorePayload, user: dict = Depends(require_authenticated_account)) -> dict:
    try:
        store = crawler_service.save_store(user["username"], payload, store_id)
        return {"store": store, "stores": crawler_service.list_stores(user["username"])}
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.delete("/stores/{store_id}")
def delete_store(store_id: int, user: dict = Depends(require_authenticated_account)) -> dict:
    try:
        crawler_service.delete_store(user["username"], store_id)
        return {"stores": crawler_service.list_stores(user["username"])}
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/stores/{store_id}/sync")
def sync_store(store_id: int, user: dict = Depends(require_authenticated_account)) -> dict:
    try:
        store = crawler_service.sync_store(user["username"], store_id)
        return {"store": store, "stores": crawler_service.list_stores(user["username"])}
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/schedules")
def list_schedules(user: dict = Depends(require_authenticated_account)) -> dict:
    return {"schedules": crawler_service.list_scheduled_crawls(user["username"])}


@router.post("/schedules")
def create_schedule(payload: ScheduledCrawlPayload, user: dict = Depends(require_authenticated_account)) -> dict:
    try:
        schedule = crawler_service.save_scheduled_crawl(user["username"], payload)
        return {"schedule": schedule, "schedules": crawler_service.list_scheduled_crawls(user["username"])}
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.put("/schedules/{schedule_id}")
def update_schedule(
    schedule_id: int,
    payload: ScheduledCrawlPayload,
    user: dict = Depends(require_authenticated_account),
) -> dict:
    try:
        schedule = crawler_service.save_scheduled_crawl(user["username"], payload, schedule_id)
        return {"schedule": schedule, "schedules": crawler_service.list_scheduled_crawls(user["username"])}
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.delete("/schedules/{schedule_id}")
def delete_schedule(schedule_id: int, user: dict = Depends(require_authenticated_account)) -> dict:
    try:
        crawler_service.delete_scheduled_crawl(user["username"], schedule_id)
        return {"schedules": crawler_service.list_scheduled_crawls(user["username"])}
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/schedules/{schedule_id}/run")
def run_schedule(schedule_id: int, user: dict = Depends(require_authenticated_account)) -> dict:
    try:
        schedule = crawler_service.run_scheduled_crawl(user["username"], schedule_id)
        return {"schedule": schedule, "schedules": crawler_service.list_scheduled_crawls(user["username"])}
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/listing-tasks")
def list_listing_tasks(user: dict = Depends(require_authenticated_account)) -> dict:
    return {"listingTasks": crawler_service.list_listing_tasks(user["username"])}


@router.post("/listing-tasks")
def create_listing_task(payload: ListingTaskPayload, user: dict = Depends(require_authenticated_account)) -> dict:
    try:
        task = crawler_service.create_listing_task(user["username"], payload)
        return {"listingTask": task, "listingTasks": crawler_service.list_listing_tasks(user["username"])}
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/listing-tasks/{task_id}/retry")
def retry_listing_task(task_id: str, user: dict = Depends(require_authenticated_account)) -> dict:
    try:
        task = crawler_service.retry_listing_task(user["username"], task_id)
        return {"listingTask": task, "listingTasks": crawler_service.list_listing_tasks(user["username"])}
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/roles")
def list_roles(_: dict = Depends(require_superadmin)) -> dict:
    return {"roles": crawler_service.list_roles()}


@router.post("/roles")
def create_role(payload: RolePayload, _: dict = Depends(require_superadmin)) -> dict:
    try:
        role = crawler_service.save_role(payload)
        return {"role": role, "roles": crawler_service.list_roles()}
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.put("/roles/{role_id}")
def update_role(role_id: int, payload: RolePayload, _: dict = Depends(require_superadmin)) -> dict:
    try:
        role = crawler_service.save_role(payload, role_id)
        return {"role": role, "roles": crawler_service.list_roles()}
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.delete("/roles/{role_id}")
def delete_role(role_id: int, _: dict = Depends(require_superadmin)) -> dict:
    try:
        crawler_service.delete_role(role_id)
        return {"roles": crawler_service.list_roles()}
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
