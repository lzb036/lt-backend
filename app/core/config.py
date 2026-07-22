from __future__ import annotations

import os
import secrets
from pathlib import Path

from pydantic import BaseModel

BACKEND_DIR = Path(__file__).resolve().parents[2]
PROJECT_DIR = BACKEND_DIR.parent
TASK_QUEUE_JOB_TIMEOUT_DEFAULT_SECONDS = 3 * 60 * 60


def _load_dotenv(path: Path) -> None:
    if not path.exists():
        return
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError:
        return
    for raw_line in lines:
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        if key and key not in os.environ:
            os.environ[key] = value.strip().strip('"').strip("'")


_load_dotenv(BACKEND_DIR / ".env")
_load_dotenv(PROJECT_DIR / ".env")


def _env_text(name: str, default: str = "") -> str:
    return (os.getenv(name) or default).strip()


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _env_int(name: str, default: int) -> int:
    raw = _env_text(name)
    if not raw:
        return default
    return int(raw)


class Settings(BaseModel):
    app_name: str = "LT Product Collector API"
    app_version: str = "0.1.0"
    api_docs_enabled: bool = False
    backend_dir: Path = BACKEND_DIR
    database_url: str
    database_echo: bool = False
    database_auto_create: bool = True
    database_pool_size: int = 10
    database_max_overflow: int = 20
    database_pool_timeout: int = 30
    session_cookie_name: str = "lt_session"
    session_cookie_secure: bool = True
    session_duration_seconds: int = 60 * 60 * 24 * 7
    login_max_failed_attempts: int = 5
    login_lockout_seconds: int = 15 * 60
    session_secret: str
    credential_encryption_secret: str
    initial_superadmin_username: str = "superadmin"
    initial_superadmin_password: str = "123456"
    crawler_timeout_seconds: int = 20
    rakuten_write_timeout_seconds: int = 60
    max_running_crawl_tasks_per_user: int = 2
    max_running_sync_tasks_per_user: int = 2
    max_running_listing_tasks_per_user: int = 1
    crawler_browser_fallback_enabled: bool = False
    crawler_browser_timeout_seconds: int = 35
    crawler_max_ranking_pages: int = 200
    crawler_min_delay_ms: int = 600
    crawler_max_delay_ms: int = 1600
    crawler_max_retries: int = 3
    crawler_batch_size: int = 10
    crawler_batch_pause_seconds: float = 3.0
    crawler_warmup_url: str = "https://www.rakuten.co.jp/"
    crawler_proxy_url: str = ""
    proxy_subscription_url: str = ""
    proxy_traffic_total_gib: float = 168.0
    proxy_traffic_reset_day: int = 2
    proxy_usage_cache_seconds: int = 300
    mihomo_config_path: str = "/etc/mihomo/config.yaml"
    product_image_draft_retention_days: int = 7
    product_image_draft_url_ttl_seconds: int = 24 * 60 * 60
    product_image_orphan_retention_days: int = 7
    product_image_storage: str = "local"
    oss_bucket: str = ""
    oss_endpoint: str = ""
    oss_region: str = ""
    oss_ecs_role_name: str = ""
    oss_connect_timeout_seconds: int = 10
    task_queue_mode: str = "thread"
    redis_url: str = "redis://127.0.0.1:6379/0"
    task_queue_name: str = "lt-tasks"
    task_queue_crawl_name: str = "lt-tasks-crawl"
    task_queue_sync_name: str = "lt-tasks-sync"
    task_queue_listing_name: str = "lt-tasks-listing"
    task_queue_schedule_name: str = "lt-tasks-schedule"
    task_queue_job_timeout_seconds: int = TASK_QUEUE_JOB_TIMEOUT_DEFAULT_SECONDS
    task_queue_result_ttl_seconds: int = 60 * 60
    task_queue_failure_ttl_seconds: int = 24 * 60 * 60
    task_queue_queued_requeue_limit: int = 25
    scheduled_crawl_dispatch_batch_size: int = 50
    scheduled_crawl_dispatch_pause_seconds: float = 0.1
    rakuten_default_inventory_quantity: int = 1000
    rakuten_default_normal_delivery_time_id: int = 0
    rakuten_default_back_order_delivery_time_id: int = 0
    crawler_user_agent: str = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/149.0.0.0 Safari/537.36"
    )


def _load_secret(name: str, file_name: str) -> str:
    env_value = _env_text(name)
    if env_value:
        return env_value
    data_dir = BACKEND_DIR / "data"
    secret_file = data_dir / file_name
    if secret_file.exists():
        value = secret_file.read_text(encoding="utf-8").strip()
        if value:
            return value
    data_dir.mkdir(parents=True, exist_ok=True)
    value = secrets.token_urlsafe(48)
    secret_file.write_text(value, encoding="utf-8")
    return value


def build_settings() -> Settings:
    database_url = _env_text("LT_DATABASE_URL")
    if not database_url:
        raise RuntimeError(
            "缺少 MySQL 配置 LT_DATABASE_URL，例如 "
            "mysql+pymysql://user:password@127.0.0.1:3306/lt_collector?charset=utf8mb4"
        )
    product_image_storage = _env_text("LT_PRODUCT_IMAGE_STORAGE", "local").lower() or "local"
    if product_image_storage not in {"local", "oss"}:
        raise RuntimeError("LT_PRODUCT_IMAGE_STORAGE 只能配置为 local 或 oss。")
    oss_bucket = _env_text("LT_OSS_BUCKET")
    oss_endpoint = _env_text("LT_OSS_ENDPOINT")
    if oss_endpoint and "://" not in oss_endpoint:
        oss_endpoint = f"https://{oss_endpoint}"
    oss_region = _env_text("LT_OSS_REGION")
    oss_ecs_role_name = _env_text("LT_OSS_ECS_ROLE_NAME")
    if product_image_storage == "oss":
        required_oss_settings = {
            "LT_OSS_BUCKET": oss_bucket,
            "LT_OSS_ENDPOINT": oss_endpoint,
            "LT_OSS_REGION": oss_region,
            "LT_OSS_ECS_ROLE_NAME": oss_ecs_role_name,
        }
        missing = [name for name, value in required_oss_settings.items() if not value]
        if missing:
            raise RuntimeError(f"OSS 图片存储缺少配置：{', '.join(missing)}。")
        if not oss_endpoint.lower().startswith("https://"):
            raise RuntimeError("LT_OSS_ENDPOINT 在 OSS 模式下必须使用 HTTPS。")
    return Settings(
        database_url=database_url,
        api_docs_enabled=_env_bool("LT_API_DOCS_ENABLED", False),
        database_echo=_env_bool("LT_DATABASE_ECHO", False),
        database_auto_create=_env_bool("LT_DATABASE_AUTO_CREATE", True),
        database_pool_size=max(1, _env_int("LT_DATABASE_POOL_SIZE", 10)),
        database_max_overflow=max(0, _env_int("LT_DATABASE_MAX_OVERFLOW", 20)),
        database_pool_timeout=max(1, _env_int("LT_DATABASE_POOL_TIMEOUT", 30)),
        session_cookie_name=_env_text("LT_SESSION_COOKIE_NAME", "lt_session"),
        session_cookie_secure=_env_bool("LT_SESSION_COOKIE_SECURE", True),
        session_duration_seconds=_env_int("LT_SESSION_DURATION_SECONDS", 60 * 60 * 24 * 7),
        login_max_failed_attempts=max(1, _env_int("LT_LOGIN_MAX_FAILED_ATTEMPTS", 5)),
        login_lockout_seconds=max(60, _env_int("LT_LOGIN_LOCKOUT_SECONDS", 15 * 60)),
        session_secret=_load_secret("LT_SESSION_SECRET", "session_secret.txt"),
        credential_encryption_secret=_load_secret("LT_CREDENTIAL_SECRET", "credential_secret.txt"),
        initial_superadmin_username=_env_text("LT_INITIAL_SUPERADMIN_USERNAME", "superadmin"),
        initial_superadmin_password=_env_text("LT_INITIAL_SUPERADMIN_PASSWORD", "123456"),
        crawler_timeout_seconds=_env_int("LT_CRAWLER_TIMEOUT_SECONDS", 20),
        rakuten_write_timeout_seconds=_env_int("LT_RAKUTEN_WRITE_TIMEOUT_SECONDS", 60),
        max_running_crawl_tasks_per_user=max(1, _env_int("LT_MAX_RUNNING_CRAWL_TASKS_PER_USER", 2)),
        max_running_sync_tasks_per_user=max(1, _env_int("LT_MAX_RUNNING_SYNC_TASKS_PER_USER", 2)),
        max_running_listing_tasks_per_user=max(1, _env_int("LT_MAX_RUNNING_LISTING_TASKS_PER_USER", 1)),
        crawler_browser_fallback_enabled=_env_bool("LT_CRAWLER_BROWSER_FALLBACK_ENABLED", False),
        crawler_browser_timeout_seconds=_env_int("LT_CRAWLER_BROWSER_TIMEOUT_SECONDS", 35),
        crawler_max_ranking_pages=max(1, _env_int("LT_CRAWLER_MAX_RANKING_PAGES", 200)),
        crawler_min_delay_ms=max(0, _env_int("LT_CRAWLER_MIN_DELAY_MS", 600)),
        crawler_max_delay_ms=max(0, _env_int("LT_CRAWLER_MAX_DELAY_MS", 1600)),
        crawler_max_retries=max(0, _env_int("LT_CRAWLER_MAX_RETRIES", 3)),
        crawler_batch_size=max(1, _env_int("LT_CRAWLER_BATCH_SIZE", 10)),
        crawler_batch_pause_seconds=max(0, float(_env_text("LT_CRAWLER_BATCH_PAUSE_SECONDS", "3") or "0")),
        crawler_warmup_url=_env_text("LT_CRAWLER_WARMUP_URL", "https://www.rakuten.co.jp/"),
        crawler_proxy_url=_env_text("LT_CRAWLER_PROXY_URL", ""),
        proxy_subscription_url=_env_text("LT_PROXY_SUBSCRIPTION_URL", ""),
        proxy_traffic_total_gib=max(0.0, float(_env_text("LT_PROXY_TRAFFIC_TOTAL_GIB", "168") or "0")),
        proxy_traffic_reset_day=min(28, max(1, _env_int("LT_PROXY_TRAFFIC_RESET_DAY", 2))),
        proxy_usage_cache_seconds=max(60, _env_int("LT_PROXY_USAGE_CACHE_SECONDS", 300)),
        mihomo_config_path=_env_text("LT_MIHOMO_CONFIG_PATH", "/etc/mihomo/config.yaml"),
        product_image_draft_retention_days=max(1, _env_int("LT_PRODUCT_IMAGE_DRAFT_RETENTION_DAYS", 7)),
        product_image_draft_url_ttl_seconds=max(
            60,
            _env_int("LT_PRODUCT_IMAGE_DRAFT_URL_TTL_SECONDS", 24 * 60 * 60),
        ),
        product_image_orphan_retention_days=max(1, _env_int("LT_PRODUCT_IMAGE_ORPHAN_RETENTION_DAYS", 7)),
        product_image_storage=product_image_storage,
        oss_bucket=oss_bucket,
        oss_endpoint=oss_endpoint,
        oss_region=oss_region,
        oss_ecs_role_name=oss_ecs_role_name,
        oss_connect_timeout_seconds=max(1, _env_int("LT_OSS_CONNECT_TIMEOUT_SECONDS", 10)),
        task_queue_mode=_env_text("LT_TASK_QUEUE_MODE", "thread").lower() or "thread",
        redis_url=_env_text("LT_REDIS_URL", "redis://127.0.0.1:6379/0"),
        task_queue_name=(base_task_queue_name := _env_text("LT_TASK_QUEUE_NAME", "lt-tasks")),
        task_queue_crawl_name=_env_text("LT_TASK_QUEUE_CRAWL_NAME", f"{base_task_queue_name}-crawl"),
        task_queue_sync_name=_env_text("LT_TASK_QUEUE_SYNC_NAME", f"{base_task_queue_name}-sync"),
        task_queue_listing_name=_env_text("LT_TASK_QUEUE_LISTING_NAME", f"{base_task_queue_name}-listing"),
        task_queue_schedule_name=_env_text("LT_TASK_QUEUE_SCHEDULE_NAME", f"{base_task_queue_name}-schedule"),
        task_queue_job_timeout_seconds=max(
            TASK_QUEUE_JOB_TIMEOUT_DEFAULT_SECONDS,
            _env_int("LT_TASK_QUEUE_JOB_TIMEOUT_SECONDS", TASK_QUEUE_JOB_TIMEOUT_DEFAULT_SECONDS),
        ),
        task_queue_result_ttl_seconds=max(0, _env_int("LT_TASK_QUEUE_RESULT_TTL_SECONDS", 60 * 60)),
        task_queue_failure_ttl_seconds=max(60, _env_int("LT_TASK_QUEUE_FAILURE_TTL_SECONDS", 24 * 60 * 60)),
        task_queue_queued_requeue_limit=max(1, _env_int("LT_TASK_QUEUE_QUEUED_REQUEUE_LIMIT", 25)),
        scheduled_crawl_dispatch_batch_size=max(1, _env_int("LT_SCHEDULED_CRAWL_DISPATCH_BATCH_SIZE", 50)),
        scheduled_crawl_dispatch_pause_seconds=max(0, float(_env_text("LT_SCHEDULED_CRAWL_DISPATCH_PAUSE_SECONDS", "0.1") or "0")),
        rakuten_default_inventory_quantity=max(0, _env_int("LT_RAKUTEN_DEFAULT_INVENTORY_QUANTITY", 1000)),
        rakuten_default_normal_delivery_time_id=max(0, _env_int("LT_RAKUTEN_DEFAULT_NORMAL_DELIVERY_TIME_ID", 0)),
        rakuten_default_back_order_delivery_time_id=max(0, _env_int("LT_RAKUTEN_DEFAULT_BACK_ORDER_DELIVERY_TIME_ID", 0)),
        crawler_user_agent=_env_text("LT_CRAWLER_USER_AGENT") or Settings.model_fields["crawler_user_agent"].default,
    )


settings = build_settings()
