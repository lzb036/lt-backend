from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field
from decimal import Decimal

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
    aliasName: str = ""
    platform: str = "rakuten"
    enabled: bool = True
    description: str = ""
    rakutenServiceSecret: str = ""
    rakutenLicenseKey: str = ""


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


class ProductListingStatusPayload(BaseModel):
    productIds: list[int] = Field(default_factory=list)
    listingStatus: str = Field(pattern="^(listed|unlisted)$")


class ProductPricePayload(BaseModel):
    price: Decimal = Field(gt=0, max_digits=12, decimal_places=2)


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
    storeId: int | None = Query(default=None),
    listingStatus: str | None = Query(default=None),
    listedAtFrom: str | None = Query(default=None),
    listedAtTo: str | None = Query(default=None),
    user: dict = Depends(require_authenticated_account),
) -> dict:
    return {
        "products": crawler_service.list_products(
            user["username"],
            status=status,
            keyword=keyword,
            store_id=storeId,
            listing_status=listingStatus,
            listed_at_from=listedAtFrom,
            listed_at_to=listedAtTo,
        )
    }


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
    try:
        return crawler_service.delete_products(user["username"], payload.productIds)
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.put("/products/listing-status")
def update_products_listing_status(
    payload: ProductListingStatusPayload,
    user: dict = Depends(require_authenticated_account),
) -> dict:
    try:
        return crawler_service.update_store_products_listing_status(
            user["username"],
            payload.productIds,
            payload.listingStatus,
        )
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/products/{product_id}")
def get_product_detail(product_id: int, user: dict = Depends(require_authenticated_account)) -> dict:
    try:
        return {"product": crawler_service.get_product_detail(user["username"], product_id)}
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.put("/products/{product_id}/price")
def update_product_price(
    product_id: int,
    payload: ProductPricePayload,
    user: dict = Depends(require_authenticated_account),
) -> dict:
    try:
        return {"product": crawler_service.update_store_product_price(user["username"], product_id, payload.price)}
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/stores")
def list_stores(user: dict = Depends(require_authenticated_account)) -> dict:
    return {"stores": crawler_service.list_stores()}


@router.post("/stores")
def create_store(payload: StorePayload, user: dict = Depends(require_authenticated_account)) -> dict:
    try:
        store = crawler_service.save_store(user["username"], payload)
        return {"store": store, "stores": crawler_service.list_stores()}
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.put("/stores/{store_id}")
def update_store(store_id: int, payload: StorePayload, user: dict = Depends(require_authenticated_account)) -> dict:
    try:
        store = crawler_service.save_store(user["username"], payload, store_id)
        return {"store": store, "stores": crawler_service.list_stores()}
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.delete("/stores/{store_id}")
def delete_store(store_id: int, user: dict = Depends(require_authenticated_account)) -> dict:
    try:
        crawler_service.delete_store(store_id)
        return {"stores": crawler_service.list_stores()}
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/stores/verify")
def verify_stores(user: dict = Depends(require_authenticated_account)) -> dict:
    return crawler_service.verify_all_stores()


@router.post("/stores/{store_id}/sync")
def sync_store(store_id: int, user: dict = Depends(require_authenticated_account)) -> dict:
    try:
        result = crawler_service.sync_store(user["username"], store_id)
        return {
            **result,
            "stores": crawler_service.list_stores(),
            "syncTasks": crawler_service.list_sync_tasks(user["username"]),
        }
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/sync-tasks")
def list_sync_tasks(user: dict = Depends(require_authenticated_account)) -> dict:
    return {"syncTasks": crawler_service.list_sync_tasks(user["username"])}


@router.post("/sync-tasks/{task_id}/retry")
def retry_sync_task(task_id: str, user: dict = Depends(require_authenticated_account)) -> dict:
    try:
        task = crawler_service.retry_sync_task(user["username"], task_id)
        return {"syncTask": task, "syncTasks": crawler_service.list_sync_tasks(user["username"])}
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
