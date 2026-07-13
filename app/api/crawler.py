from __future__ import annotations

from io import BytesIO
from urllib.parse import quote

import requests
from fastapi import APIRouter, Depends, HTTPException, Query, UploadFile, File
from fastapi.responses import FileResponse, StreamingResponse
from pydantic import BaseModel, Field
from decimal import Decimal

from app.core.auth import has_permission, require_any_permission, require_permission, require_superadmin
from app.services import crawler_service, sensitive_word_service
from app.services.user_service import require_existing_account

router = APIRouter(prefix="/crawler", tags=["crawler"])

require_crawler_permission = require_permission("crawler.manage")
require_products_permission = require_permission("products.manage")
require_stores_permission = require_permission("stores.manage")
require_settings_permission = require_permission("settings.manage")
require_products_or_stores_permission = require_any_permission("products.manage", "stores.manage")
SENSITIVE_WORD_TEMPLATE_FILENAME = "敏感词导入模板.xlsx"
SENSITIVE_WORD_TEMPLATE_FALLBACK_FILENAME = "sensitive-word-template.xlsx"


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
    rankingPeriod: str | None = Field(default=None, pattern="^(daily|weekly|monthly)$")
    crawlLimit: int | str | None = None
    mode: str = "manual"


class TaskDeletePayload(BaseModel):
    taskIds: list[str] = Field(default_factory=list)


class ScheduleDeletePayload(BaseModel):
    scheduleIds: list[int] = Field(default_factory=list)


class ScheduleStatusBatchPayload(BaseModel):
    scheduleIds: list[int] = Field(default_factory=list)
    enabled: bool


class ScheduleRunAllPayload(BaseModel):
    keyword: str | None = None
    enabledStatus: str | None = None
    status: str | None = None
    scheduleTime: str | None = None
    createdAtFrom: str | None = None
    createdAtTo: str | None = None


class StorePayload(BaseModel):
    ownerUsername: str | None = None
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
    rankingPeriod: str = Field(default="daily", pattern="^(daily|weekly|monthly)$")
    crawlLimit: int | str | None = None
    enabled: bool = True
    intervalMinutes: int = Field(default=60, ge=5, le=1440)
    scheduleTime: str = Field(default="09:00", pattern=r"^\d{2}:\d{2}$")
    notes: str = ""


class SensitiveWordPayload(BaseModel):
    word: str = Field(min_length=1, max_length=500)
    enabled: bool = True


class ProductStatusPayload(BaseModel):
    productIds: list[int] = Field(default_factory=list)
    status: str = Field(pattern="^(pending|approved|error|listed|listed_master|rejected)$")
    message: str = ""


class ProductDeletePayload(BaseModel):
    productIds: list[int] = Field(default_factory=list)


class ProductListingStatusPayload(BaseModel):
    productIds: list[int] = Field(default_factory=list)
    listingStatus: str = Field(pattern="^(listed|unlisted)$")


class StoreListingStatusPayload(BaseModel):
    storeId: int
    listingStatus: str = Field(pattern="^(listed|unlisted)$")


class ProductPricePayload(BaseModel):
    price: Decimal = Field(gt=0, max_digits=12, decimal_places=2)


class ProductVariantEditPayload(BaseModel):
    variantId: str = Field(min_length=1)
    standardPrice: Decimal = Field(gt=0, max_digits=12, decimal_places=0)
    hidden: bool = False


class ProductImageReplacementPayload(BaseModel):
    from_: str = Field(alias="from")
    to: str


class ProductImageChangesPayload(BaseModel):
    images: list[str] = Field(default_factory=list)
    replacements: list[ProductImageReplacementPayload] = Field(default_factory=list)
    removeUrls: list[str] = Field(default_factory=list)


class ProductImageBase64DraftPayload(BaseModel):
    imageBase64: str = Field(min_length=1)
    ext: str = ""


class ProductDetailEditPayload(BaseModel):
    title: str = Field(min_length=1, max_length=500)
    tagline: str = ""
    variants: list[ProductVariantEditPayload] = Field(default_factory=list)
    imageChanges: ProductImageChangesPayload | None = None


class ListingTaskPayload(BaseModel):
    productIds: list[int] = Field(default_factory=list)
    storeId: int | None = None
    storeIds: list[int] = Field(default_factory=list)
    taskName: str = ""


class RolePayload(BaseModel):
    name: str = Field(min_length=1)
    code: str = Field(min_length=1)
    scope: str = "own"
    enabled: bool = True
    permissions: list[str] = Field(default_factory=list)
    notes: str = ""


class TimeSettingsPayload(BaseModel):
    cleanupWeekday: int = Field(ge=0, le=6)
    cleanupTime: str = Field(pattern=r"^\d{2}:\d{2}$")


def visible_time_settings(user: dict, settings_payload: dict) -> dict:
    result = dict(settings_payload)
    if user.get("role") != "superadmin":
        result.pop("queueHealth", None)
    return result


def resolve_target_username(user: dict, owner_username: str | None = None, *, require_child_owner: bool = False) -> str:
    if owner_username and not has_permission(user, "stores.manage"):
        raise HTTPException(status_code=403, detail="没有管理店铺所属用户的权限")
    if user.get("role") == "superadmin" and owner_username:
        target_user = require_existing_account(owner_username)
        if require_child_owner and target_user.get("role") == "superadmin":
            raise HTTPException(status_code=400, detail="请选择子公司用户作为店铺所属用户")
        return owner_username
    if user.get("role") == "superadmin" and require_child_owner:
        raise HTTPException(status_code=400, detail="请选择店铺所属用户")
    return user["username"]


@router.get("/dashboard/summary")
def get_dashboard_summary(user: dict = Depends(require_any_permission("crawler.manage", "products.manage", "stores.manage"))) -> dict:
    return {
        "summary": crawler_service.dashboard_summary(
            user["username"],
            include_stores=has_permission(user, "stores.manage"),
            include_crawler=has_permission(user, "crawler.manage"),
            include_products=has_permission(user, "products.manage"),
            include_sync_tasks=has_permission(user, "products.manage") or has_permission(user, "stores.manage"),
        )
    }


@router.get("/settings/time")
def get_time_settings(user: dict = Depends(require_any_permission("crawler.manage", "settings.manage"))) -> dict:
    include_queue_health = user.get("role") == "superadmin"
    return {
        "settings": visible_time_settings(
            user,
            crawler_service.get_time_settings(include_queue_health=include_queue_health),
        )
    }


@router.put("/settings/time")
def update_time_settings(payload: TimeSettingsPayload, user: dict = Depends(require_settings_permission)) -> dict:
    try:
        include_queue_health = user.get("role") == "superadmin"
        return {
            "settings": visible_time_settings(
                user,
                crawler_service.save_time_settings(
                    payload,
                    include_queue_health=include_queue_health,
                ),
            )
        }
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/settings/time/scheduled-task-cleanup/run")
def run_scheduled_task_cleanup(user: dict = Depends(require_settings_permission)) -> dict:
    try:
        include_queue_health = user.get("role") == "superadmin"
        return {
            "settings": visible_time_settings(
                user,
                crawler_service.run_completed_scheduled_crawl_tasks_cleanup_now(
                    include_queue_health=include_queue_health,
                ),
            )
        }
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/settings/time/unlisted-products/run")
def run_unlisted_product_cleanup(user: dict = Depends(require_settings_permission)) -> dict:
    try:
        include_queue_health = user.get("role") == "superadmin"
        result = crawler_service.run_store_unlisted_product_cleanup_now(
            include_queue_health=include_queue_health,
        )
        return {
            **result,
            "settings": visible_time_settings(user, result["settings"]),
        }
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/settings/resources/proxy-usage")
def get_proxy_resource_usage(
    refresh: bool = Query(default=False),
    user: dict = Depends(require_superadmin),
) -> dict:
    try:
        return {"proxyUsage": crawler_service.get_proxy_resource_usage(force=refresh)}
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/settings/sensitive-words")
def list_sensitive_words(
    page: int | None = Query(default=None, ge=1),
    pageSize: int | None = Query(default=None, ge=1, le=500),
    keyword: str | None = Query(default=None),
    _: dict = Depends(require_superadmin),
) -> dict:
    return sensitive_word_service.list_sensitive_words(page=page, page_size=pageSize, keyword=keyword or "")


@router.post("/settings/sensitive-words")
def create_sensitive_word(payload: SensitiveWordPayload, _: dict = Depends(require_superadmin)) -> dict:
    try:
        return {"sensitiveWord": sensitive_word_service.create_sensitive_word(payload.word, payload.enabled)}
    except RuntimeError as exc:
        raise _sensitive_word_http_exception(exc) from exc


@router.put("/settings/sensitive-words/{word_id}")
def update_sensitive_word(word_id: int, payload: SensitiveWordPayload, _: dict = Depends(require_superadmin)) -> dict:
    try:
        return {"sensitiveWord": sensitive_word_service.update_sensitive_word(word_id, payload.word, payload.enabled)}
    except RuntimeError as exc:
        raise _sensitive_word_http_exception(exc) from exc


@router.delete("/settings/sensitive-words/{word_id}")
def delete_sensitive_word(word_id: int, _: dict = Depends(require_superadmin)) -> dict:
    if sensitive_word_service.delete_sensitive_word(word_id):
        return {"deleted": True}
    raise HTTPException(status_code=404, detail="敏感词不存在。")


@router.get("/settings/sensitive-words/template")
def download_sensitive_word_template(_: dict = Depends(require_superadmin)) -> StreamingResponse:
    try:
        content = sensitive_word_service.build_sensitive_word_template()
    except RuntimeError as exc:
        raise _sensitive_word_http_exception(exc) from exc
    encoded_filename = quote(SENSITIVE_WORD_TEMPLATE_FILENAME)
    return StreamingResponse(
        BytesIO(content),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={
            "Content-Disposition": (
                "attachment; "
                f"filename={SENSITIVE_WORD_TEMPLATE_FALLBACK_FILENAME}; "
                f"filename*=UTF-8''{encoded_filename}"
            ),
        },
    )


@router.post("/settings/sensitive-words/import")
async def import_sensitive_words(file: UploadFile = File(...), _: dict = Depends(require_superadmin)) -> dict:
    try:
        content = await file.read()
        return sensitive_word_service.import_sensitive_words(content, file.filename or "")
    except RuntimeError as exc:
        raise _sensitive_word_http_exception(exc) from exc


@router.get("/sources")
def list_sources(
    page: int | None = Query(default=None, ge=1),
    pageSize: int | None = Query(default=None, ge=1, le=500),
    user: dict = Depends(require_crawler_permission),
) -> dict:
    result = crawler_service.list_sources(user["username"], page=page, page_size=pageSize)
    if isinstance(result, dict):
        return result
    return {"sources": result, "total": len(result), "page": 1, "pageSize": len(result) or 30}


@router.post("/sources")
def create_source(payload: CrawlSourcePayload, user: dict = Depends(require_crawler_permission)) -> dict:
    try:
        source = crawler_service.save_source(user["username"], payload)
        return {"source": source}
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.put("/sources/{source_id}")
def update_source(source_id: int, payload: CrawlSourcePayload, user: dict = Depends(require_crawler_permission)) -> dict:
    try:
        source = crawler_service.save_source(user["username"], payload, source_id)
        return {"source": source}
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.delete("/sources/{source_id}")
def delete_source(source_id: int, user: dict = Depends(require_crawler_permission)) -> dict:
    try:
        crawler_service.delete_source(user["username"], source_id)
        return {"deleted": True}
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/tasks")
def list_tasks(
    page: int | None = Query(default=None, ge=1),
    pageSize: int | None = Query(default=None, ge=1, le=500),
    target: str | None = Query(default=None),
    status: str | None = Query(default=None),
    sourceType: str | None = Query(default=None),
    mode: str | None = Query(default=None),
    createdAtFrom: str | None = Query(default=None),
    createdAtTo: str | None = Query(default=None),
    user: dict = Depends(require_crawler_permission),
) -> dict:
    result = crawler_service.list_tasks(
        user["username"],
        page=page,
        page_size=pageSize,
        target=target,
        status=status,
        source_type=sourceType,
        mode=mode,
        created_at_from=createdAtFrom,
        created_at_to=createdAtTo,
    )
    if isinstance(result, dict):
        return result
    return {"tasks": result, "total": len(result), "page": 1, "pageSize": len(result) or 30}


@router.post("/tasks")
def create_task(payload: CreateTaskPayload, user: dict = Depends(require_crawler_permission)) -> dict:
    try:
        task = crawler_service.create_task(user["username"], payload)
        return {"task": task}
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/tasks/import-template")
def download_manual_task_import_template(user: dict = Depends(require_crawler_permission)) -> StreamingResponse:
    try:
        content = crawler_service.manual_crawl_import_template_bytes()
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    filename = crawler_service.MANUAL_CRAWL_IMPORT_TEMPLATE_FILENAME
    encoded_filename = quote(filename)
    return StreamingResponse(
        BytesIO(content),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={
            "Content-Disposition": f"attachment; filename={filename}; filename*=UTF-8''{encoded_filename}",
        },
    )


@router.post("/tasks/import")
async def import_manual_tasks(file: UploadFile = File(...), user: dict = Depends(require_crawler_permission)) -> dict:
    try:
        content = await file.read()
        return crawler_service.import_manual_crawl_tasks(user["username"], file.filename or "", content)
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/tasks/{task_id}/restart")
def restart_task(task_id: str, user: dict = Depends(require_crawler_permission)) -> dict:
    try:
        task = crawler_service.run_existing_task(user["username"], task_id)
        return {"task": task}
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/tasks/{task_id}/cancel")
def cancel_task(task_id: str, user: dict = Depends(require_crawler_permission)) -> dict:
    try:
        task = crawler_service.cancel_crawl_task(user["username"], task_id)
        return {"task": task}
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.api_route("/tasks", methods=["DELETE"])
def delete_tasks(payload: TaskDeletePayload, user: dict = Depends(require_crawler_permission)) -> dict:
    try:
        return crawler_service.delete_tasks(user["username"], payload.taskIds)
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/products")
def list_products(
    status: str | None = Query(default=None),
    keyword: str | None = Query(default=None),
    taskId: str | None = Query(default=None),
    storeId: int | None = Query(default=None),
    listedStoreId: str | None = Query(default=None),
    listingStatus: str | None = Query(default=None),
    listedAtFrom: str | None = Query(default=None),
    listedAtTo: str | None = Query(default=None),
    priceMin: Decimal | None = Query(default=None, ge=0),
    priceMax: Decimal | None = Query(default=None, ge=0),
    collectedAtFrom: str | None = Query(default=None),
    collectedAtTo: str | None = Query(default=None),
    page: int | None = Query(default=None, ge=1),
    pageSize: int | None = Query(default=None, ge=1, le=500),
    user: dict = Depends(require_products_or_stores_permission),
) -> dict:
    if status != "listed" and not has_permission(user, "products.manage"):
        raise HTTPException(status_code=403, detail="没有管理商品的权限")
    result = crawler_service.list_products(
        user["username"],
        status=status,
        keyword=keyword,
        task_id=taskId,
        store_id=storeId,
        listed_store_id=listedStoreId,
        listing_status=listingStatus,
        listed_at_from=listedAtFrom,
        listed_at_to=listedAtTo,
        price_min=priceMin,
        price_max=priceMax,
        collected_at_from=collectedAtFrom,
        collected_at_to=collectedAtTo,
        page=page,
        page_size=pageSize,
    )
    if isinstance(result, dict):
        return result
    return {
        "products": result,
        "total": len(result),
        "page": 1,
        "pageSize": len(result) or 30,
    }


@router.put("/products/status")
def update_product_status(payload: ProductStatusPayload, user: dict = Depends(require_products_permission)) -> dict:
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
def delete_products(payload: ProductDeletePayload, user: dict = Depends(require_products_or_stores_permission)) -> dict:
    try:
        review_statuses = crawler_service.product_review_statuses(user["username"], payload.productIds)
        if review_statuses and review_statuses <= {"listed"}:
            if not has_permission(user, "stores.manage"):
                raise HTTPException(status_code=403, detail="没有管理店铺商品的权限")
        elif not has_permission(user, "products.manage"):
            raise HTTPException(status_code=403, detail="没有管理商品的权限")
        return crawler_service.delete_products(user["username"], payload.productIds)
    except HTTPException:
        raise
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.put("/products/listing-status")
def update_products_listing_status(
    payload: ProductListingStatusPayload,
    user: dict = Depends(require_stores_permission),
) -> dict:
    try:
        return crawler_service.update_store_products_listing_status(
            user["username"],
            payload.productIds,
            payload.listingStatus,
        )
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.put("/stores/listing-status")
def update_store_listing_status(
    payload: StoreListingStatusPayload,
    user: dict = Depends(require_stores_permission),
) -> dict:
    try:
        return crawler_service.update_store_all_products_listing_status(
            user["username"],
            payload.storeId,
            payload.listingStatus,
        )
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/products/{product_id}")
def get_product_detail(product_id: int, user: dict = Depends(require_products_or_stores_permission)) -> dict:
    try:
        product = crawler_service.get_product_detail(user["username"], product_id)
        if product.get("reviewStatus") == "listed":
            if not has_permission(user, "stores.manage") and not has_permission(user, "products.manage"):
                raise HTTPException(status_code=403, detail="没有查看店铺商品的权限")
        elif not has_permission(user, "products.manage"):
            raise HTTPException(status_code=403, detail="没有查看商品的权限")
        return {"product": product}
    except HTTPException:
        raise
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.put("/products/{product_id}/price")
def update_product_price(
    product_id: int,
    payload: ProductPricePayload,
    user: dict = Depends(require_stores_permission),
) -> dict:
    try:
        return {"product": crawler_service.update_store_product_price(user["username"], product_id, payload.price)}
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.put("/products/{product_id}/detail")
def update_product_detail(
    product_id: int,
    payload: ProductDetailEditPayload,
    user: dict = Depends(require_stores_permission),
) -> dict:
    try:
        return {"product": crawler_service.update_store_product_detail(user["username"], product_id, payload)}
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.put("/products/{product_id}/local-detail")
def update_product_local_detail(
    product_id: int,
    payload: ProductDetailEditPayload,
    user: dict = Depends(require_products_permission),
) -> dict:
    try:
        return {"product": crawler_service.update_product_local_detail(user["username"], product_id, payload)}
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/products/{product_id}/images/{image_index}/download")
def download_product_image(
    product_id: int,
    image_index: int,
    user: dict = Depends(require_products_or_stores_permission),
):
    try:
        review_statuses = crawler_service.product_review_statuses(user["username"], [product_id])
        if review_statuses != {"listed"} and not has_permission(user, "products.manage"):
            raise HTTPException(status_code=403, detail="没有下载该商品图片的权限")
        info = crawler_service.product_image_download_info(user["username"], product_id, image_index)
        headers = {"Content-Disposition": f'attachment; filename="{info["filename"]}"'}
        if info["type"] == "stream":
            return StreamingResponse(
                info["body"],
                media_type=info["mediaType"],
                headers=headers,
            )
        if info["type"] == "local":
            return FileResponse(info["path"], media_type=info["mediaType"], filename=info["filename"])
        response = requests.get(
            info["url"],
            timeout=crawler_service.settings.crawler_timeout_seconds,
            headers={"User-Agent": crawler_service.settings.crawler_user_agent},
            stream=True,
        )
        response.raise_for_status()

        def stream_content():
            size = 0
            for chunk in response.iter_content(chunk_size=1024 * 256):
                if not chunk:
                    continue
                size += len(chunk)
                if size > crawler_service.MAX_PRODUCT_IMAGE_DOWNLOAD_BYTES:
                    response.close()
                    raise RuntimeError("图片文件过大，已停止下载。")
                yield chunk

        return StreamingResponse(stream_content(), media_type=info["mediaType"], headers=headers)
    except HTTPException:
        raise
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except requests.RequestException as exc:
        raise HTTPException(status_code=400, detail="图片下载失败，请稍后重试。") from exc


@router.post("/products/{product_id}/images/draft")
def upload_product_image_draft(
    product_id: int,
    file: UploadFile = File(...),
    user: dict = Depends(require_products_permission),
) -> dict:
    try:
        return {"url": crawler_service.save_product_image_draft(user["username"], product_id, file)}
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/products/{product_id}/images/draft-base64")
def upload_product_image_draft_base64(
    product_id: int,
    payload: ProductImageBase64DraftPayload,
    user: dict = Depends(require_products_permission),
) -> dict:
    try:
        return {
            "url": crawler_service.save_product_image_draft_base64(
                user["username"],
                product_id,
                payload.imageBase64,
                payload.ext,
            )
        }
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/products/{product_id}/images/{image_index}/replace")
def replace_product_image(
    product_id: int,
    image_index: int,
    file: UploadFile = File(...),
    user: dict = Depends(require_products_permission),
) -> dict:
    try:
        return {"product": crawler_service.replace_product_image(user["username"], product_id, image_index, file)}
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.delete("/products/{product_id}/images/{image_index}")
def delete_product_image(
    product_id: int,
    image_index: int,
    user: dict = Depends(require_products_permission),
) -> dict:
    try:
        return {"product": crawler_service.delete_product_image(user["username"], product_id, image_index)}
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/stores")
def list_stores(
    page: int | None = Query(default=None, ge=1),
    pageSize: int | None = Query(default=None, ge=1, le=500),
    ownerUsername: str | None = Query(default=None),
    user: dict = Depends(require_products_or_stores_permission),
) -> dict:
    target_username = resolve_target_username(user, ownerUsername)
    result = crawler_service.list_stores(target_username, page=page, page_size=pageSize)
    if isinstance(result, dict):
        return result
    return {"stores": result, "total": len(result), "page": 1, "pageSize": len(result) or 30}


@router.post("/stores")
def create_store(payload: StorePayload, user: dict = Depends(require_superadmin)) -> dict:
    try:
        target_username = resolve_target_username(user, payload.ownerUsername, require_child_owner=bool(payload.ownerUsername))
        store = crawler_service.save_store(target_username, payload)
        return {"store": store}
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.put("/stores/{store_id}")
def update_store(store_id: int, payload: StorePayload, user: dict = Depends(require_superadmin)) -> dict:
    try:
        target_username = resolve_target_username(user, payload.ownerUsername, require_child_owner=bool(payload.ownerUsername))
        store = crawler_service.save_store(target_username, payload, store_id)
        return {"store": store}
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.delete("/stores/{store_id}")
def delete_store(
    store_id: int,
    ownerUsername: str | None = Query(default=None),
    user: dict = Depends(require_superadmin),
) -> dict:
    try:
        target_username = resolve_target_username(user, ownerUsername, require_child_owner=bool(ownerUsername))
        crawler_service.delete_store(target_username, store_id)
        return {"deleted": True}
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/stores/verify")
def verify_stores(
    ownerUsername: str | None = Query(default=None),
    user: dict = Depends(require_stores_permission),
) -> dict:
    target_username = resolve_target_username(user, ownerUsername)
    return crawler_service.verify_all_stores(target_username)


@router.post("/stores/product-counts")
def refresh_store_product_counts(
    ownerUsername: str | None = Query(default=None),
    user: dict = Depends(require_stores_permission),
) -> dict:
    target_username = resolve_target_username(user, ownerUsername)
    return crawler_service.refresh_all_store_product_counts(target_username)


@router.post("/stores/{store_id}/verify")
def verify_store(
    store_id: int,
    ownerUsername: str | None = Query(default=None),
    user: dict = Depends(require_stores_permission),
) -> dict:
    try:
        target_username = resolve_target_username(user, ownerUsername)
        return {"store": crawler_service.verify_store(target_username, store_id)}
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/stores/{store_id}/product-counts")
def refresh_single_store_product_counts(
    store_id: int,
    ownerUsername: str | None = Query(default=None),
    user: dict = Depends(require_stores_permission),
) -> dict:
    try:
        target_username = resolve_target_username(user, ownerUsername)
        return {"store": crawler_service.refresh_store_product_counts(target_username, store_id)}
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/stores/{store_id}/sync")
def sync_store(
    store_id: int,
    ownerUsername: str | None = Query(default=None),
    user: dict = Depends(require_stores_permission),
) -> dict:
    try:
        target_username = resolve_target_username(user, ownerUsername)
        result = crawler_service.sync_store(target_username, store_id)
        return result
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/stores/{store_id}/cabinet/empty-folders")
def list_store_empty_cabinet_folders(
    store_id: int,
    ownerUsername: str | None = Query(default=None),
    user: dict = Depends(require_stores_permission),
) -> dict:
    try:
        target_username = resolve_target_username(user, ownerUsername)
        return crawler_service.list_store_empty_cabinet_folders(target_username, store_id)
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/sync-tasks")
def list_sync_tasks(
    page: int | None = Query(default=None, ge=1),
    pageSize: int | None = Query(default=None, ge=1, le=500),
    user: dict = Depends(require_products_or_stores_permission),
) -> dict:
    result = crawler_service.list_sync_tasks(user["username"], page=page, page_size=pageSize)
    if isinstance(result, dict):
        return result
    return {"syncTasks": result, "total": len(result), "page": 1, "pageSize": len(result) or 30}


@router.post("/sync-tasks/{task_id}/retry")
def retry_sync_task(task_id: str, user: dict = Depends(require_products_or_stores_permission)) -> dict:
    try:
        task = crawler_service.retry_sync_task(user["username"], task_id)
        return {"syncTask": task}
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/sync-tasks/{task_id}/cancel")
def cancel_sync_task(task_id: str, user: dict = Depends(require_products_or_stores_permission)) -> dict:
    try:
        task = crawler_service.cancel_sync_task(user["username"], task_id)
        return {"syncTask": task}
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.api_route("/sync-tasks", methods=["DELETE"])
def delete_sync_tasks(payload: TaskDeletePayload, user: dict = Depends(require_products_or_stores_permission)) -> dict:
    try:
        return crawler_service.delete_sync_tasks(user["username"], payload.taskIds)
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/schedules")
def list_schedules(
    page: int | None = Query(default=None, ge=1),
    pageSize: int | None = Query(default=None, ge=1, le=500),
    keyword: str | None = Query(default=None),
    enabledStatus: str | None = Query(default=None),
    status: str | None = Query(default=None),
    scheduleTime: str | None = Query(default=None),
    createdAtFrom: str | None = Query(default=None),
    createdAtTo: str | None = Query(default=None),
    user: dict = Depends(require_crawler_permission),
) -> dict:
    result = crawler_service.list_scheduled_crawls(
        user["username"],
        page=page,
        page_size=pageSize,
        keyword=keyword,
        enabled_status=enabledStatus,
        status=status,
        schedule_time=scheduleTime,
        created_at_from=createdAtFrom,
        created_at_to=createdAtTo,
    )
    if isinstance(result, dict):
        return result
    return {"schedules": result, "total": len(result), "page": 1, "pageSize": len(result) or 30}


@router.get("/schedules/import-template")
def download_schedule_import_template(user: dict = Depends(require_crawler_permission)) -> StreamingResponse:
    try:
        content = crawler_service.scheduled_crawl_import_template_bytes()
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    filename = crawler_service.SCHEDULE_IMPORT_TEMPLATE_FILENAME
    encoded_filename = quote(filename)
    return StreamingResponse(
        BytesIO(content),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={
            "Content-Disposition": f"attachment; filename={filename}; filename*=UTF-8''{encoded_filename}",
        },
    )


@router.get("/schedules/export")
def export_schedules(
    keyword: str | None = Query(default=None),
    enabledStatus: str | None = Query(default=None),
    status: str | None = Query(default=None),
    scheduleTime: str | None = Query(default=None),
    createdAtFrom: str | None = Query(default=None),
    createdAtTo: str | None = Query(default=None),
    user: dict = Depends(require_crawler_permission),
) -> StreamingResponse:
    try:
        content = crawler_service.scheduled_crawl_export_bytes(
            user["username"],
            keyword=keyword,
            enabled_status=enabledStatus,
            status=status,
            schedule_time=scheduleTime,
            created_at_from=createdAtFrom,
            created_at_to=createdAtTo,
        )
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    filename = crawler_service.SCHEDULE_EXPORT_FILENAME
    encoded_filename = quote(filename)
    return StreamingResponse(
        BytesIO(content),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={
            "Content-Disposition": f"attachment; filename={filename}; filename*=UTF-8''{encoded_filename}",
        },
    )


@router.post("/schedules/import")
async def import_schedules(file: UploadFile = File(...), user: dict = Depends(require_crawler_permission)) -> dict:
    try:
        content = await file.read()
        return crawler_service.import_scheduled_crawls(user["username"], file.filename or "", content)
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/schedules")
def create_schedule(payload: ScheduledCrawlPayload, user: dict = Depends(require_crawler_permission)) -> dict:
    try:
        schedule = crawler_service.save_scheduled_crawl(user["username"], payload)
        return {"schedule": schedule}
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.api_route("/schedules", methods=["DELETE"])
def delete_schedules(payload: ScheduleDeletePayload, user: dict = Depends(require_crawler_permission)) -> dict:
    try:
        return crawler_service.delete_scheduled_crawls(user["username"], payload.scheduleIds)
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.put("/schedules/status")
def update_schedule_statuses(
    payload: ScheduleStatusBatchPayload,
    user: dict = Depends(require_crawler_permission),
) -> dict:
    try:
        return crawler_service.update_scheduled_crawl_statuses(
            user["username"],
            payload.scheduleIds,
            payload.enabled,
        )
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.put("/schedules/{schedule_id}")
def update_schedule(
    schedule_id: int,
    payload: ScheduledCrawlPayload,
    user: dict = Depends(require_crawler_permission),
) -> dict:
    try:
        schedule = crawler_service.save_scheduled_crawl(user["username"], payload, schedule_id)
        return {"schedule": schedule}
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.delete("/schedules/{schedule_id}")
def delete_schedule(schedule_id: int, user: dict = Depends(require_crawler_permission)) -> dict:
    try:
        crawler_service.delete_scheduled_crawl(user["username"], schedule_id)
        return {"deleted": True}
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/schedules/run-all")
def run_all_schedules(payload: ScheduleRunAllPayload, user: dict = Depends(require_crawler_permission)) -> dict:
    try:
        return crawler_service.run_scheduled_crawls_now(
            user["username"],
            keyword=payload.keyword,
            enabled_status=payload.enabledStatus,
            status=payload.status,
            schedule_time=payload.scheduleTime,
            created_at_from=payload.createdAtFrom,
            created_at_to=payload.createdAtTo,
        )
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/schedules/{schedule_id}/run")
def run_schedule(schedule_id: int, user: dict = Depends(require_crawler_permission)) -> dict:
    try:
        schedule = crawler_service.run_scheduled_crawl(user["username"], schedule_id)
        return {"schedule": schedule}
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/listing-tasks")
def list_listing_tasks(
    page: int | None = Query(default=None, ge=1),
    pageSize: int | None = Query(default=None, ge=1, le=500),
    user: dict = Depends(require_products_permission),
) -> dict:
    result = crawler_service.list_listing_tasks(user["username"], page=page, page_size=pageSize)
    if isinstance(result, dict):
        return result
    return {"listingTasks": result, "total": len(result), "page": 1, "pageSize": len(result) or 30}


@router.post("/listing-tasks/preflight")
def preflight_listing_task(payload: ListingTaskPayload, user: dict = Depends(require_products_permission)) -> dict:
    try:
        return crawler_service.preflight_listing_task(user["username"], payload)
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/listing-tasks")
def create_listing_task(payload: ListingTaskPayload, user: dict = Depends(require_products_permission)) -> dict:
    try:
        result = crawler_service.create_listing_task(user["username"], payload)
        if isinstance(result, dict) and "listingTask" in result:
            return result
        return {"listingTask": result}
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/listing-tasks/{task_id}/retry")
def retry_listing_task(task_id: str, user: dict = Depends(require_products_permission)) -> dict:
    try:
        task = crawler_service.retry_listing_task(user["username"], task_id)
        return {"listingTask": task}
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/listing-tasks/{task_id}/cancel")
def cancel_listing_task(task_id: str, user: dict = Depends(require_products_permission)) -> dict:
    try:
        task = crawler_service.cancel_listing_task(user["username"], task_id)
        return {"listingTask": task}
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.api_route("/listing-tasks", methods=["DELETE"])
def delete_listing_tasks(payload: TaskDeletePayload, user: dict = Depends(require_products_permission)) -> dict:
    try:
        return crawler_service.delete_listing_tasks(user["username"], payload.taskIds)
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/roles")
def list_roles(
    page: int | None = Query(default=None, ge=1),
    pageSize: int | None = Query(default=None, ge=1, le=500),
    _: dict = Depends(require_superadmin),
) -> dict:
    result = crawler_service.list_roles(page=page, page_size=pageSize)
    if isinstance(result, dict):
        return result
    return {"roles": result, "total": len(result), "page": 1, "pageSize": len(result) or 30}


@router.post("/roles")
def create_role(payload: RolePayload, _: dict = Depends(require_superadmin)) -> dict:
    try:
        role = crawler_service.save_role(payload)
        return {"role": role}
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.put("/roles/{role_id}")
def update_role(role_id: int, payload: RolePayload, _: dict = Depends(require_superadmin)) -> dict:
    try:
        role = crawler_service.save_role(payload, role_id)
        return {"role": role}
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.delete("/roles/{role_id}")
def delete_role(role_id: int, _: dict = Depends(require_superadmin)) -> dict:
    try:
        crawler_service.delete_role(role_id)
        return {"deleted": True}
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


def _sensitive_word_http_exception(exc: RuntimeError) -> HTTPException:
    detail = str(exc)
    if "已存在" in detail:
        return HTTPException(status_code=409, detail=detail)
    if "不存在" in detail:
        return HTTPException(status_code=404, detail=detail)
    return HTTPException(status_code=400, detail=detail)
