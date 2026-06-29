from __future__ import annotations

import os
import secrets
from pathlib import Path

from pydantic import BaseModel

BACKEND_DIR = Path(__file__).resolve().parents[2]
PROJECT_DIR = BACKEND_DIR.parent


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
    backend_dir: Path = BACKEND_DIR
    database_url: str
    database_echo: bool = False
    database_auto_create: bool = True
    session_cookie_name: str = "lt_session"
    session_cookie_secure: bool = False
    session_duration_seconds: int = 60 * 60 * 24 * 7
    session_secret: str
    credential_encryption_secret: str
    initial_superadmin_username: str = "superadmin"
    initial_superadmin_password: str = "123456"
    crawler_timeout_seconds: int = 20
    crawler_browser_fallback_enabled: bool = True
    crawler_browser_timeout_seconds: int = 35
    rakuten_default_inventory_quantity: int = 1000
    rakuten_default_normal_delivery_time_id: int = 0
    rakuten_default_back_order_delivery_time_id: int = 0
    crawler_user_agent: str = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126 Safari/537.36"
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
    return Settings(
        database_url=database_url,
        database_echo=_env_bool("LT_DATABASE_ECHO", False),
        database_auto_create=_env_bool("LT_DATABASE_AUTO_CREATE", True),
        session_cookie_name=_env_text("LT_SESSION_COOKIE_NAME", "lt_session"),
        session_cookie_secure=_env_bool("LT_SESSION_COOKIE_SECURE", False),
        session_duration_seconds=_env_int("LT_SESSION_DURATION_SECONDS", 60 * 60 * 24 * 7),
        session_secret=_load_secret("LT_SESSION_SECRET", "session_secret.txt"),
        credential_encryption_secret=_load_secret("LT_CREDENTIAL_SECRET", "credential_secret.txt"),
        initial_superadmin_username=_env_text("LT_INITIAL_SUPERADMIN_USERNAME", "superadmin"),
        initial_superadmin_password=_env_text("LT_INITIAL_SUPERADMIN_PASSWORD", "123456"),
        crawler_timeout_seconds=_env_int("LT_CRAWLER_TIMEOUT_SECONDS", 20),
        crawler_browser_fallback_enabled=_env_bool("LT_CRAWLER_BROWSER_FALLBACK_ENABLED", True),
        crawler_browser_timeout_seconds=_env_int("LT_CRAWLER_BROWSER_TIMEOUT_SECONDS", 35),
        rakuten_default_inventory_quantity=max(0, _env_int("LT_RAKUTEN_DEFAULT_INVENTORY_QUANTITY", 1000)),
        rakuten_default_normal_delivery_time_id=max(0, _env_int("LT_RAKUTEN_DEFAULT_NORMAL_DELIVERY_TIME_ID", 0)),
        rakuten_default_back_order_delivery_time_id=max(0, _env_int("LT_RAKUTEN_DEFAULT_BACK_ORDER_DELIVERY_TIME_ID", 0)),
        crawler_user_agent=_env_text("LT_CRAWLER_USER_AGENT") or Settings.model_fields["crawler_user_agent"].default,
    )


settings = build_settings()
