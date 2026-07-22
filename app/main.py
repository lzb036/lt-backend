from __future__ import annotations

from contextlib import asynccontextmanager
from urllib.parse import urlsplit

from fastapi import Depends, FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse, Response, StreamingResponse
from sqlalchemy import text

from app.api.auth import router as auth_router
from app.api.crawler import router as crawler_router
from app.api.profile import router as profile_router
from app.api.users import router as users_router
from app.core.config import settings
from app.core.auth import require_superadmin
from app.db.database import SessionLocal, init_database
from app.services import crawler_service
from app.services.crawler_service import LOCAL_PRODUCT_IMAGE_DIR, LOCAL_PRODUCT_IMAGE_DRAFT_DIR, start_schedule_runner
from app.services.product_image_storage import product_image_storage

DESIGNKIT_IMAGE_CORS_ORIGINS = {
    "https://designkit.cn",
    "https://www.designkit.cn",
    "https://pre.designkit.cn",
    "https://beta.designkit.cn",
}


@asynccontextmanager
async def lifespan(_app: FastAPI):
    LOCAL_PRODUCT_IMAGE_DIR.mkdir(parents=True, exist_ok=True)
    LOCAL_PRODUCT_IMAGE_DRAFT_DIR.mkdir(parents=True, exist_ok=True)
    init_database()
    start_schedule_runner()
    yield


app = FastAPI(
    title=settings.app_name,
    version=settings.app_version,
    docs_url="/docs" if settings.api_docs_enabled else None,
    redoc_url="/redoc" if settings.api_docs_enabled else None,
    openapi_url="/openapi.json" if settings.api_docs_enabled else None,
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5174", "http://127.0.0.1:5174"],
    allow_origin_regex=r"^http://(localhost|127\\.0\\.0\\.1):\\d+$",
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(auth_router, prefix="/api")
app.include_router(users_router, prefix="/api")
app.include_router(profile_router, prefix="/api")
app.include_router(crawler_router, prefix="/api")


@app.middleware("http")
async def reject_cross_site_authenticated_writes(request: Request, call_next):
    if (
        request.method.upper() not in {"GET", "HEAD", "OPTIONS", "TRACE"}
        and request.cookies.get(settings.session_cookie_name)
        and not is_same_origin_browser_request(request)
    ):
        return JSONResponse(status_code=403, content={"detail": "跨站请求已拒绝"})
    return await call_next(request)


@app.middleware("http")
async def add_product_image_cors_headers(request, call_next):
    response = await call_next(request)
    if request.url.path.startswith(("/api/static/product-images", "/api/static/product-image-drafts")):
        origin = request.headers.get("origin")
        if origin in DESIGNKIT_IMAGE_CORS_ORIGINS:
            response.headers["Access-Control-Allow-Origin"] = origin
            response.headers["Access-Control-Allow-Methods"] = "GET, HEAD, OPTIONS"
            response.headers["Access-Control-Allow-Headers"] = "*"
            response.headers["Cross-Origin-Resource-Policy"] = "cross-origin"
            response.headers["Vary"] = "Origin"
    return response


def is_same_origin_browser_request(request: Request) -> bool:
    fetch_site = request.headers.get("sec-fetch-site", "").strip().lower()
    if fetch_site == "cross-site":
        return False
    expected_host = request.headers.get("host", "").strip().lower()
    for header_name in ("origin", "referer"):
        raw_value = request.headers.get(header_name, "").strip()
        if not raw_value:
            continue
        try:
            parsed = urlsplit(raw_value)
        except ValueError:
            return False
        return parsed.scheme in {"http", "https"} and parsed.netloc.lower() == expected_host
    return True


@app.api_route(
    "/api/static/product-images/{product_id}/{filename}",
    methods=["GET", "HEAD", "OPTIONS"],
    include_in_schema=False,
)
def product_image_file(product_id: int, filename: str, request: Request):
    if request.method == "OPTIONS":
        return Response(status_code=204)
    return build_product_image_response(
        crawler_service.local_product_image_url(product_id, filename),
        method=request.method,
    )


@app.api_route(
    "/api/static/product-image-drafts/{product_id}/{filename}",
    methods=["GET", "HEAD", "OPTIONS"],
    include_in_schema=False,
)
def product_image_draft_file(product_id: int, filename: str, request: Request):
    if request.method == "OPTIONS":
        return Response(status_code=204)
    try:
        expires_at = int(request.query_params.get("expires") or 0)
    except ValueError:
        return Response(status_code=403)
    if not crawler_service.verify_product_image_draft_access(
        product_id,
        filename,
        expires_at,
        request.query_params.get("signature") or "",
    ):
        return Response(status_code=403)
    return build_product_image_response(
        crawler_service.local_product_image_draft_url(product_id, filename),
        method=request.method,
        cache_control="private, max-age=300",
    )


def build_product_image_response(
    image_url: str,
    *,
    method: str,
    cache_control: str = "public, max-age=3600",
) -> Response:
    try:
        info = crawler_service.product_image_http_info(
            image_url,
            include_body=method.upper() != "HEAD",
        )
    except RuntimeError as exc:
        status_code = 404 if "不存在" in str(exc) else 503
        return Response(status_code=status_code)
    headers = {
        "Cache-Control": cache_control,
        "Content-Length": str(info["size"]),
    }
    if method.upper() == "HEAD":
        return Response(media_type=info["mediaType"], headers=headers)
    if info["type"] == "local":
        return FileResponse(
            info["path"],
            media_type=info["mediaType"],
            headers=headers,
        )
    return StreamingResponse(
        info["body"],
        media_type=info["mediaType"],
        headers=headers,
    )


@app.get("/")
def root() -> dict[str, str]:
    return {"name": settings.app_name, "version": settings.app_version}


@app.get("/api/health")
def health() -> dict[str, object]:
    checks = health_checks()
    return {
        "status": "ok" if all(checks.values()) else "degraded",
        "checks": {
            "database": checks["database"],
            "productImageStorage": checks["productImageStorage"],
        },
    }


@app.get("/api/health/details")
def health_details(_: dict = Depends(require_superadmin)) -> dict[str, object]:
    checks = health_checks()
    return {
        "status": "ok" if all(checks.values()) else "degraded",
        "name": settings.app_name,
        "version": settings.app_version,
        "checks": checks,
        "scheduler": crawler_service.schedule_runner_health(),
        "settings": {
            "productImageDraftRetentionDays": settings.product_image_draft_retention_days,
            "productImageOrphanRetentionDays": settings.product_image_orphan_retention_days,
            "productImageStorage": settings.product_image_storage,
            "taskQueueMode": settings.task_queue_mode,
            "crawlerBatchSize": settings.crawler_batch_size,
            "crawlerBatchPauseSeconds": settings.crawler_batch_pause_seconds,
        },
    }


def health_checks() -> dict[str, bool]:
    product_image_storage_ok = product_image_storage.health_check()
    return {
        "database": check_database(),
        "productImagesWritable": product_image_storage_ok if product_image_storage.enabled else check_directory_writable(LOCAL_PRODUCT_IMAGE_DIR),
        "productImageDraftsWritable": product_image_storage_ok if product_image_storage.enabled else check_directory_writable(LOCAL_PRODUCT_IMAGE_DRAFT_DIR),
        "productImageStorage": product_image_storage_ok,
    }


def check_database() -> bool:
    session = SessionLocal()
    try:
        session.execute(text("SELECT 1"))
        return True
    except Exception:
        return False
    finally:
        session.close()


def check_directory_writable(directory) -> bool:
    try:
        directory.mkdir(parents=True, exist_ok=True)
        probe = directory / ".healthcheck"
        probe.write_text("ok", encoding="utf-8")
        probe.unlink(missing_ok=True)
        return True
    except Exception:
        return False
