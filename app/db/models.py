from __future__ import annotations

import hashlib
from datetime import datetime

from sqlalchemy import Boolean, DateTime, ForeignKey, Index, Integer, Numeric, String, Text, UniqueConstraint, func
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.database import Base


class TimestampMixin:
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=False), nullable=False, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=False),
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    )


class UserAccountModel(TimestampMixin, Base):
    __tablename__ = "lt_user_accounts"

    username: Mapped[str] = mapped_column(String(255), primary_key=True)
    display_name: Mapped[str] = mapped_column(String(255), nullable=False, default="")
    role: Mapped[str] = mapped_column(String(32), nullable=False, default="operator", server_default="operator")
    enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True, server_default="1")
    password_salt_b64: Mapped[str] = mapped_column(String(255), nullable=False)
    password_hash_b64: Mapped[str] = mapped_column(String(255), nullable=False)
    password_iterations: Mapped[int] = mapped_column(Integer, nullable=False, default=240000, server_default="240000")


class UserSecretProfileModel(TimestampMixin, Base):
    __tablename__ = "lt_user_secret_profiles"

    owner_username: Mapped[str] = mapped_column(
        String(255),
        ForeignKey("lt_user_accounts.username", ondelete="CASCADE"),
        primary_key=True,
    )
    rakuten_service_secret_encrypted: Mapped[str] = mapped_column(Text, nullable=False, default="")
    rakuten_license_key_encrypted: Mapped[str] = mapped_column(Text, nullable=False, default="")
    rakuten_shop_url: Mapped[str] = mapped_column(String(255), nullable=False, default="")
    rakuten_shop_name: Mapped[str] = mapped_column(String(255), nullable=False, default="")
    alibaba_app_key_encrypted: Mapped[str] = mapped_column(Text, nullable=False, default="")
    alibaba_app_secret_encrypted: Mapped[str] = mapped_column(Text, nullable=False, default="")
    alibaba_access_token_encrypted: Mapped[str] = mapped_column(Text, nullable=False, default="")
    logistics_base_url: Mapped[str] = mapped_column(String(255), nullable=False, default="")
    logistics_username_encrypted: Mapped[str] = mapped_column(Text, nullable=False, default="")
    logistics_password_encrypted: Mapped[str] = mapped_column(Text, nullable=False, default="")
    proxy_url_encrypted: Mapped[str] = mapped_column(Text, nullable=False, default="")
    oss_access_key_id_encrypted: Mapped[str] = mapped_column(Text, nullable=False, default="")
    oss_access_key_secret_encrypted: Mapped[str] = mapped_column(Text, nullable=False, default="")
    oss_bucket: Mapped[str] = mapped_column(String(255), nullable=False, default="")
    oss_endpoint: Mapped[str] = mapped_column(String(255), nullable=False, default="")
    default_price_multiplier: Mapped[str] = mapped_column(String(32), nullable=False, default="1.00")
    auto_crawl_enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False, server_default="0")
    auto_crawl_interval_minutes: Mapped[int] = mapped_column(Integer, nullable=False, default=60, server_default="60")
    last_verified_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=False))
    last_error: Mapped[str | None] = mapped_column(Text)

    owner: Mapped[UserAccountModel] = relationship()


class CrawlSourceModel(TimestampMixin, Base):
    __tablename__ = "lt_crawl_sources"
    __table_args__ = (
        UniqueConstraint("owner_username", "name", name="uq_lt_crawl_source_owner_name"),
        Index("ix_lt_crawl_source_owner_enabled", "owner_username", "enabled"),
    )

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    owner_username: Mapped[str] = mapped_column(
        String(255),
        ForeignKey("lt_user_accounts.username", ondelete="CASCADE"),
        nullable=False,
    )
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    source_type: Mapped[str] = mapped_column(String(32), nullable=False, default="keyword")
    target: Mapped[str] = mapped_column(Text, nullable=False, default="")
    enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True, server_default="1")
    schedule_enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False, server_default="0")
    interval_minutes: Mapped[int] = mapped_column(Integer, nullable=False, default=60, server_default="60")
    last_run_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=False))
    notes: Mapped[str] = mapped_column(Text, nullable=False, default="")


class CrawlTaskModel(TimestampMixin, Base):
    __tablename__ = "lt_crawl_tasks"
    __table_args__ = (
        Index("ix_lt_crawl_task_owner_status", "owner_username", "status"),
        Index("ix_lt_crawl_task_owner_created", "owner_username", "created_at"),
    )

    id: Mapped[str] = mapped_column(String(64), primary_key=True)
    owner_username: Mapped[str] = mapped_column(
        String(255),
        ForeignKey("lt_user_accounts.username", ondelete="CASCADE"),
        nullable=False,
    )
    source_id: Mapped[int | None] = mapped_column(ForeignKey("lt_crawl_sources.id", ondelete="SET NULL"))
    source_type: Mapped[str] = mapped_column(String(32), nullable=False)
    target: Mapped[str] = mapped_column(Text, nullable=False)
    mode: Mapped[str] = mapped_column(String(32), nullable=False, default="manual")
    status: Mapped[str] = mapped_column(String(32), nullable=False, default="queued")
    total_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    success_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    failed_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    message: Mapped[str] = mapped_column(Text, nullable=False, default="")
    error_detail: Mapped[str | None] = mapped_column(Text)
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=False))
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=False))


class ProductModel(TimestampMixin, Base):
    __tablename__ = "lt_products"
    __table_args__ = (
        UniqueConstraint("owner_username", "source_url_hash", name="uq_lt_product_owner_source_url_hash"),
        Index("ix_lt_product_owner_status", "owner_username", "review_status"),
        Index("ix_lt_product_owner_title", "owner_username", "title"),
    )

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    owner_username: Mapped[str] = mapped_column(
        String(255),
        ForeignKey("lt_user_accounts.username", ondelete="CASCADE"),
        nullable=False,
    )
    task_id: Mapped[str | None] = mapped_column(String(64), ForeignKey("lt_crawl_tasks.id", ondelete="SET NULL"))
    title: Mapped[str] = mapped_column(String(500), nullable=False)
    source_url: Mapped[str] = mapped_column(Text, nullable=False)
    source_url_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    item_number: Mapped[str] = mapped_column(String(255), nullable=False, default="")
    shop_name: Mapped[str] = mapped_column(String(255), nullable=False, default="")
    image_url: Mapped[str] = mapped_column(Text, nullable=False, default="")
    price: Mapped[float | None] = mapped_column(Numeric(12, 2))
    currency: Mapped[str] = mapped_column(String(16), nullable=False, default="JPY")
    genre_id: Mapped[str] = mapped_column(String(64), nullable=False, default="")
    review_status: Mapped[str] = mapped_column(String(32), nullable=False, default="pending")
    raw_payload_json: Mapped[str] = mapped_column(Text, nullable=False, default="{}")
    last_error: Mapped[str | None] = mapped_column(Text)


class CrawlLogModel(TimestampMixin, Base):
    __tablename__ = "lt_crawl_logs"
    __table_args__ = (Index("ix_lt_crawl_log_owner_task", "owner_username", "task_id"),)

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    owner_username: Mapped[str] = mapped_column(String(255), nullable=False)
    task_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    level: Mapped[str] = mapped_column(String(16), nullable=False, default="info")
    message: Mapped[str] = mapped_column(Text, nullable=False)


def make_source_url_hash(source_url: str) -> str:
    return hashlib.sha256(source_url.strip().encode("utf-8")).hexdigest()
