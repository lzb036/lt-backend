from __future__ import annotations

import hashlib
import uuid
from datetime import date, datetime
from decimal import Decimal

from sqlalchemy import Boolean, DateTime, ForeignKey, ForeignKeyConstraint, Index, Integer, Numeric, String, Text, UniqueConstraint, func
from sqlalchemy.dialects.mysql import LONGTEXT
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
    crawl_min_price: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    permissions_json: Mapped[str] = mapped_column(Text, nullable=False, default="[]")
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


class RoleModel(TimestampMixin, Base):
    __tablename__ = "lt_roles"
    __table_args__ = (UniqueConstraint("code", name="uq_lt_role_code"),)

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(120), nullable=False)
    code: Mapped[str] = mapped_column(String(64), nullable=False)
    scope: Mapped[str] = mapped_column(String(32), nullable=False, default="own", server_default="own")
    enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True, server_default="1")
    permissions_json: Mapped[str] = mapped_column(Text, nullable=False, default="[]")
    notes: Mapped[str] = mapped_column(Text, nullable=False, default="")


class SystemSettingModel(TimestampMixin, Base):
    __tablename__ = "lt_system_settings"

    key: Mapped[str] = mapped_column(String(128), primary_key=True)
    value_json: Mapped[str] = mapped_column(Text, nullable=False, default="{}")


class SensitiveWordModel(TimestampMixin, Base):
    __tablename__ = "lt_sensitive_words"
    __table_args__ = (UniqueConstraint("word", name="uq_lt_sensitive_word"),)

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    word: Mapped[str] = mapped_column(String(500), nullable=False)
    enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True, server_default="1")


class StoreModel(TimestampMixin, Base):
    __tablename__ = "lt_stores"
    __table_args__ = (
        UniqueConstraint("owner_username", "store_code", name="uq_lt_store_owner_code"),
        UniqueConstraint("id", "owner_username", name="uq_lt_store_id_owner"),
        Index("ix_lt_store_enabled", "enabled"),
        Index("ix_lt_store_owner_enabled", "owner_username", "enabled"),
    )

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    owner_username: Mapped[str] = mapped_column(
        String(255),
        ForeignKey("lt_user_accounts.username", ondelete="CASCADE"),
        nullable=False,
    )
    store_code: Mapped[str] = mapped_column(String(120), nullable=False)
    store_name: Mapped[str] = mapped_column(String(255), nullable=False)
    alias_name: Mapped[str] = mapped_column(String(255), nullable=False, default="")
    platform: Mapped[str] = mapped_column(String(32), nullable=False, default="rakuten", server_default="rakuten")
    store_url: Mapped[str] = mapped_column(Text, nullable=False, default="")
    enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True, server_default="1")
    contact_name: Mapped[str] = mapped_column(String(120), nullable=False, default="")
    contact_phone: Mapped[str] = mapped_column(String(120), nullable=False, default="")
    description: Mapped[str] = mapped_column(Text, nullable=False, default="")
    rakuten_service_secret_encrypted: Mapped[str] = mapped_column(Text, nullable=False, default="")
    rakuten_license_key_encrypted: Mapped[str] = mapped_column(Text, nullable=False, default="")
    price_multiplier: Mapped[str] = mapped_column(String(32), nullable=False, default="1.00")
    cabinet_used_folder_count: Mapped[int | None] = mapped_column(Integer)
    cabinet_remaining_folder_count: Mapped[int | None] = mapped_column(Integer)
    cabinet_usage_checked_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=False))
    rakuten_product_total_count: Mapped[int | None] = mapped_column(Integer)
    rakuten_product_listed_count: Mapped[int | None] = mapped_column(Integer)
    rakuten_product_unlisted_count: Mapped[int | None] = mapped_column(Integer)
    rakuten_product_total_exceeds_limit: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False, server_default="0")
    last_checked_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=False))
    last_product_synced_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=False))
    last_synced_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=False))
    last_error: Mapped[str | None] = mapped_column(Text)


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
        Index("ix_lt_crawl_task_owner_started", "owner_username", "started_at"),
        Index("ix_lt_crawl_task_owner_finished", "owner_username", "finished_at"),
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
    queue_job_id: Mapped[str | None] = mapped_column(String(64))
    total_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    success_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    failed_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    warning_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    message: Mapped[str] = mapped_column(Text, nullable=False, default="")
    error_detail: Mapped[str | None] = mapped_column(Text)
    warning_detail: Mapped[str | None] = mapped_column(Text)
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=False))
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=False))


class ProductModel(TimestampMixin, Base):
    __tablename__ = "lt_products"
    __table_args__ = (
        UniqueConstraint("owner_username", "source_url_hash", name="uq_lt_product_owner_source_url_hash"),
        UniqueConstraint("store_id", "rakuten_manage_number", name="uq_lt_product_store_manage_number"),
        Index("ix_lt_product_owner_status", "owner_username", "review_status"),
        Index("ix_lt_product_owner_created", "owner_username", "created_at"),
        Index("ix_lt_product_owner_updated", "owner_username", "updated_at"),
        Index("ix_lt_product_owner_title", "owner_username", "title"),
        Index("ix_lt_product_store_status", "store_id", "store_product_status"),
        Index("ix_lt_product_store_listing_listed", "store_id", "review_status", "rakuten_listing_status", "listed_at"),
        Index("ix_lt_product_parent_status", "parent_product_id", "review_status"),
        Index("ix_lt_product_listing_task", "listing_task_id"),
    )

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    owner_username: Mapped[str] = mapped_column(
        String(255),
        ForeignKey("lt_user_accounts.username", ondelete="CASCADE"),
        nullable=False,
    )
    task_id: Mapped[str | None] = mapped_column(String(64), ForeignKey("lt_crawl_tasks.id", ondelete="SET NULL"))
    parent_product_id: Mapped[int | None] = mapped_column(ForeignKey("lt_products.id", ondelete="SET NULL"))
    listing_task_id: Mapped[str | None] = mapped_column(String(64))
    store_id: Mapped[int | None] = mapped_column(ForeignKey("lt_stores.id", ondelete="SET NULL"))
    title: Mapped[str] = mapped_column(String(500), nullable=False)
    source_url: Mapped[str] = mapped_column(Text, nullable=False)
    source_url_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    rakuten_manage_number: Mapped[str | None] = mapped_column(String(255))
    item_number: Mapped[str] = mapped_column(String(255), nullable=False, default="")
    shop_name: Mapped[str] = mapped_column(String(255), nullable=False, default="")
    image_url: Mapped[str] = mapped_column(Text, nullable=False, default="")
    price: Mapped[float | None] = mapped_column(Numeric(12, 2))
    currency: Mapped[str] = mapped_column(String(16), nullable=False, default="JPY")
    genre_id: Mapped[str] = mapped_column(String(64), nullable=False, default="")
    review_status: Mapped[str] = mapped_column(String(32), nullable=False, default="pending")
    store_product_status: Mapped[str] = mapped_column(String(32), nullable=False, default="", server_default="")
    rakuten_listing_status: Mapped[str] = mapped_column(String(32), nullable=False, default="", server_default="")
    listed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=False))
    store_last_seen_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=False))
    raw_payload_json: Mapped[str] = mapped_column(Text().with_variant(LONGTEXT(), "mysql"), nullable=False, default="{}")
    last_error: Mapped[str | None] = mapped_column(Text)


class AiTitleSettingsModel(TimestampMixin, Base):
    __tablename__ = "lt_ai_title_settings"

    id: Mapped[int] = mapped_column(primary_key=True, default=1)
    enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True, server_default="1")
    api_base_url: Mapped[str] = mapped_column(String(500), nullable=False, default="")
    api_key_encrypted: Mapped[str] = mapped_column(Text, nullable=False, default="")
    model_name: Mapped[str] = mapped_column(String(255), nullable=False, default="qwen-vl-max")
    title_prompt: Mapped[str] = mapped_column(Text, nullable=False, default="")
    subtitle_prompt: Mapped[str] = mapped_column(Text, nullable=False, default="")
    temperature: Mapped[str] = mapped_column(String(16), nullable=False, default="0.3")
    max_tokens: Mapped[int] = mapped_column(Integer, nullable=False, default=1000, server_default="1000")


class UserAiTitleSettingsModel(TimestampMixin, Base):
    __tablename__ = "lt_user_ai_title_settings"

    owner_username: Mapped[str] = mapped_column(
        String(255),
        ForeignKey("lt_user_accounts.username", ondelete="CASCADE"),
        primary_key=True,
    )
    provider: Mapped[str] = mapped_column(String(64), nullable=False, default="custom_openai")
    api_base_url: Mapped[str] = mapped_column(String(500), nullable=False, default="")
    api_key_encrypted: Mapped[str] = mapped_column(Text, nullable=False, default="")
    model_name: Mapped[str] = mapped_column(String(255), nullable=False, default="")
    title_prompt: Mapped[str] = mapped_column(Text, nullable=False, default="")
    subtitle_prompt: Mapped[str] = mapped_column(Text, nullable=False, default="")
    verified_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=False))
    last_error: Mapped[str | None] = mapped_column(Text)


class ProductTitleVersionModel(TimestampMixin, Base):
    __tablename__ = "lt_product_title_versions"
    __table_args__ = (
        Index("ix_lt_product_title_version_product_created", "product_id", "created_at"),
    )

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    product_id: Mapped[int] = mapped_column(
        ForeignKey("lt_products.id", ondelete="CASCADE"),
        nullable=False,
    )
    owner_username: Mapped[str] = mapped_column(
        String(255),
        ForeignKey("lt_user_accounts.username", ondelete="CASCADE"),
        nullable=False,
    )
    title: Mapped[str] = mapped_column(String(500), nullable=False)
    subtitle: Mapped[str] = mapped_column(Text, nullable=False, default="")
    source: Mapped[str] = mapped_column(String(32), nullable=False, default="ai", server_default="ai")
    model_name: Mapped[str] = mapped_column(String(255), nullable=False, default="")
    input_snapshot_json: Mapped[str] = mapped_column(Text().with_variant(LONGTEXT(), "mysql"), nullable=False, default="{}")
    is_selected: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False, server_default="0")
    created_by: Mapped[str] = mapped_column(String(255), nullable=False, default="")


class ListingTaskModel(TimestampMixin, Base):
    __tablename__ = "lt_listing_tasks"
    __table_args__ = (
        Index("ix_lt_listing_task_owner_status", "owner_username", "status"),
        Index("ix_lt_listing_task_owner_created", "owner_username", "created_at"),
        Index("ix_lt_listing_task_owner_started", "owner_username", "started_at"),
        Index("ix_lt_listing_task_owner_finished", "owner_username", "finished_at"),
        Index("ix_lt_listing_task_owner_updated", "owner_username", "updated_at"),
    )

    id: Mapped[str] = mapped_column(String(64), primary_key=True, default=lambda: uuid.uuid4().hex)
    owner_username: Mapped[str] = mapped_column(
        String(255),
        ForeignKey("lt_user_accounts.username", ondelete="CASCADE"),
        nullable=False,
    )
    store_id: Mapped[int | None] = mapped_column(ForeignKey("lt_stores.id", ondelete="SET NULL"))
    task_name: Mapped[str] = mapped_column(String(255), nullable=False, default="")
    status: Mapped[str] = mapped_column(String(32), nullable=False, default="queued", server_default="queued")
    total_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    success_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    failed_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    product_ids_json: Mapped[str] = mapped_column(Text, nullable=False, default="[]")
    message: Mapped[str] = mapped_column(Text, nullable=False, default="")
    error_detail: Mapped[str | None] = mapped_column(Text)
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=False))
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=False))


class SyncTaskModel(TimestampMixin, Base):
    __tablename__ = "lt_sync_tasks"
    __table_args__ = (
        Index("ix_lt_sync_task_owner_status", "owner_username", "status"),
        Index("ix_lt_sync_task_owner_created", "owner_username", "created_at"),
        Index("ix_lt_sync_task_owner_started", "owner_username", "started_at"),
        Index("ix_lt_sync_task_owner_finished", "owner_username", "finished_at"),
        Index("ix_lt_sync_task_owner_updated", "owner_username", "updated_at"),
    )

    id: Mapped[str] = mapped_column(String(64), primary_key=True, default=lambda: uuid.uuid4().hex)
    owner_username: Mapped[str] = mapped_column(
        String(255),
        ForeignKey("lt_user_accounts.username", ondelete="CASCADE"),
        nullable=False,
    )
    store_id: Mapped[int | None] = mapped_column(ForeignKey("lt_stores.id", ondelete="SET NULL"))
    store_name: Mapped[str] = mapped_column(String(255), nullable=False, default="")
    task_name: Mapped[str] = mapped_column(String(255), nullable=False, default="")
    task_type: Mapped[str] = mapped_column(String(32), nullable=False, default="store_sync", server_default="store_sync")
    payload_json: Mapped[str] = mapped_column(Text().with_variant(LONGTEXT(), "mysql"), nullable=False, default="{}")
    status: Mapped[str] = mapped_column(String(32), nullable=False, default="queued", server_default="queued")
    total_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    success_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    failed_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    message: Mapped[str] = mapped_column(Text, nullable=False, default="")
    error_detail: Mapped[str | None] = mapped_column(Text)
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=False))
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=False))


class ScheduledCrawlModel(TimestampMixin, Base):
    __tablename__ = "lt_scheduled_crawls"
    __table_args__ = (
        Index("ix_lt_schedule_owner_enabled", "owner_username", "enabled"),
        Index("ix_lt_schedule_owner_next", "owner_username", "next_run_at"),
    )

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    owner_username: Mapped[str] = mapped_column(
        String(255),
        ForeignKey("lt_user_accounts.username", ondelete="CASCADE"),
        nullable=False,
    )
    source_id: Mapped[int | None] = mapped_column(ForeignKey("lt_crawl_sources.id", ondelete="SET NULL"))
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    crawl_content: Mapped[str] = mapped_column(Text, nullable=False, default="")
    crawl_condition: Mapped[str] = mapped_column(Text, nullable=False, default="")
    source_type: Mapped[str] = mapped_column(String(32), nullable=False, default="keyword")
    target: Mapped[str] = mapped_column(Text, nullable=False, default="")
    enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True, server_default="1")
    interval_minutes: Mapped[int] = mapped_column(Integer, nullable=False, default=60, server_default="60")
    schedule_time: Mapped[str] = mapped_column(String(5), nullable=False, default="09:00", server_default="09:00")
    last_run_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=False))
    next_run_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=False))
    status: Mapped[str] = mapped_column(String(32), nullable=False, default="idle", server_default="idle")
    notes: Mapped[str] = mapped_column(Text, nullable=False, default="")


class CrawlLogModel(TimestampMixin, Base):
    __tablename__ = "lt_crawl_logs"
    __table_args__ = (Index("ix_lt_crawl_log_owner_task", "owner_username", "task_id"),)

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    owner_username: Mapped[str] = mapped_column(String(255), nullable=False)
    task_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    level: Mapped[str] = mapped_column(String(16), nullable=False, default="info")
    message: Mapped[str] = mapped_column(Text, nullable=False)


def _normalize_decimal(value: Decimal | float | int | str) -> Decimal:
    if isinstance(value, Decimal):
        return value
    return Decimal(str(value))


class SalesOrderModel(TimestampMixin, Base):
    __tablename__ = "lt_sales_orders"
    __table_args__ = (
        ForeignKeyConstraint(
            ["store_id", "owner_username"],
            ["lt_stores.id", "lt_stores.owner_username"],
            ondelete="CASCADE",
            name="fk_lt_sales_order_store_owner",
        ),
        UniqueConstraint("store_id", "order_number", name="uq_lt_sales_order_store_order_number"),
        UniqueConstraint("id", "owner_username", "store_id", name="uq_lt_sales_order_id_owner_store"),
        Index("ix_lt_sales_order_owner_store", "owner_username", "store_id"),
        Index("ix_lt_sales_order_store_synced", "store_id", "last_synced_at"),
    )

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    owner_username: Mapped[str] = mapped_column(
        String(255),
        ForeignKey("lt_user_accounts.username", ondelete="CASCADE"),
        nullable=False,
    )
    store_id: Mapped[int] = mapped_column(Integer, nullable=False)
    order_number: Mapped[str] = mapped_column(String(64), nullable=False)
    order_progress: Mapped[str] = mapped_column(String(64), nullable=False, default="", server_default="")
    order_status: Mapped[str] = mapped_column(String(64), nullable=False, default="", server_default="")
    ordered_at: Mapped[datetime] = mapped_column(DateTime(timezone=False), nullable=False)
    updated_at_remote: Mapped[datetime | None] = mapped_column(DateTime(timezone=False))
    total_amount: Mapped[Decimal] = mapped_column(Numeric(12, 2), nullable=False, default=0, server_default="0")
    currency: Mapped[str] = mapped_column(String(16), nullable=False, default="JPY", server_default="JPY")
    is_canceled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False, server_default="0")
    has_unresolved_adjustment: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False, server_default="0")
    raw_order_json: Mapped[str] = mapped_column(
        Text().with_variant(LONGTEXT(), "mysql"),
        nullable=False,
        default="{}",
    )
    last_synced_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=False),
        nullable=False,
        default=datetime.utcnow,
    )

    items: Mapped[list[SalesOrderItemModel]] = relationship(
        back_populates="sales_order",
        cascade="all, delete-orphan",
    )


class SalesOrderItemModel(TimestampMixin, Base):
    __tablename__ = "lt_sales_order_items"
    __table_args__ = (
        ForeignKeyConstraint(
            ["sales_order_id", "owner_username", "store_id"],
            [
                "lt_sales_orders.id",
                "lt_sales_orders.owner_username",
                "lt_sales_orders.store_id",
            ],
            ondelete="CASCADE",
            name="fk_lt_sales_order_item_parent_order",
        ),
        UniqueConstraint(
            "store_id",
            "order_number",
            "item_detail_id",
            name="uq_lt_sales_order_item_store_order_detail",
        ),
        UniqueConstraint("id", "owner_username", "store_id", name="uq_lt_sales_order_item_id_owner_store"),
        Index("ix_lt_sales_order_item_owner_store", "owner_username", "store_id"),
        Index("ix_lt_sales_order_item_store_manage", "store_id", "manage_number"),
        Index("ix_lt_sales_order_item_store_manage_sku", "store_id", "manage_number", "sku_key"),
    )

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    owner_username: Mapped[str] = mapped_column(
        String(255),
        ForeignKey("lt_user_accounts.username", ondelete="CASCADE"),
        nullable=False,
    )
    store_id: Mapped[int] = mapped_column(ForeignKey("lt_stores.id", ondelete="CASCADE"), nullable=False)
    sales_order_id: Mapped[int] = mapped_column(Integer, nullable=False)
    order_number: Mapped[str] = mapped_column(String(64), nullable=False)
    item_detail_id: Mapped[str] = mapped_column(String(255), nullable=False)
    manage_number: Mapped[str] = mapped_column(String(255), nullable=False, default="", server_default="")
    item_number: Mapped[str] = mapped_column(String(255), nullable=False, default="", server_default="")
    item_id: Mapped[str] = mapped_column(String(255), nullable=False, default="", server_default="")
    sku_key: Mapped[str] = mapped_column(String(255), nullable=False, default="", server_default="")
    sku_json: Mapped[str] = mapped_column(
        Text().with_variant(LONGTEXT(), "mysql"),
        nullable=False,
        default="{}",
    )
    item_name: Mapped[str] = mapped_column(String(500), nullable=False, default="", server_default="")
    unit_price: Mapped[Decimal] = mapped_column(Numeric(12, 2), nullable=False, default=0, server_default="0")
    ordered_units: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    latest_units: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    canceled_units: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    refunded_units: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    returned_units: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    effective_units: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    effective_amount: Mapped[Decimal] = mapped_column(Numeric(12, 2), nullable=False, default=0, server_default="0")
    delete_item_flag: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False, server_default="0")
    restore_inventory_flag: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False, server_default="0")
    ordered_at: Mapped[datetime] = mapped_column(DateTime(timezone=False), nullable=False)

    sales_order: Mapped[SalesOrderModel] = relationship(back_populates="items")
    adjustments: Mapped[list[SalesItemAdjustmentModel]] = relationship(
        back_populates="sales_order_item",
        cascade="all, delete-orphan",
    )

    @staticmethod
    def calculate_effective_units(
        ordered_units: int,
        canceled_units: int,
        refunded_units: int,
        returned_units: int,
    ) -> int:
        return max(0, ordered_units - canceled_units - refunded_units - returned_units)

    @staticmethod
    def validate_deductions(
        *,
        ordered_units: int,
        canceled_units: int,
        refunded_units: int,
        returned_units: int,
        unresolved_refunded_units: int = 0,
        return_refund_units: int = 0,
    ) -> None:
        SalesOrderItemModel.normalize_deductions(
            ordered_units=ordered_units,
            canceled_units=canceled_units,
            refunded_units=refunded_units,
            returned_units=returned_units,
            unresolved_refunded_units=unresolved_refunded_units,
            return_refund_units=return_refund_units,
        )

    @staticmethod
    def normalize_deductions(
        *,
        ordered_units: int,
        canceled_units: int,
        refunded_units: int,
        returned_units: int,
        unresolved_refunded_units: int = 0,
        return_refund_units: int = 0,
    ) -> tuple[int, int, int]:
        counts = {
            "ordered_units": ordered_units,
            "canceled_units": canceled_units,
            "refunded_units": refunded_units,
            "returned_units": returned_units,
            "unresolved_refunded_units": unresolved_refunded_units,
            "return_refund_units": return_refund_units,
        }
        for field_name, value in counts.items():
            if value < 0:
                raise ValueError(f"{field_name} must be >= 0")

        if return_refund_units > refunded_units or return_refund_units > returned_units:
            raise ValueError(
                "return_refund_units cannot exceed refunded_units or returned_units"
            )

        normalized_refunded_units = refunded_units - return_refund_units
        total_confirmed_deductions = (
            canceled_units + normalized_refunded_units + returned_units
        )
        if total_confirmed_deductions > ordered_units:
            raise ValueError("confirmed deductions cannot exceed ordered_units")
        return canceled_units, normalized_refunded_units, returned_units

    @classmethod
    def from_service_payload(
        cls,
        *,
        owner_username: str,
        store_id: int,
        sales_order_id: int,
        order_number: str,
        item_detail_id: str,
        manage_number: str = "",
        item_number: str = "",
        item_id: str = "",
        sku_key: str = "",
        sku_json: str = "{}",
        item_name: str = "",
        unit_price: Decimal | float | int | str = 0,
        ordered_units: int = 0,
        latest_units: int | None = None,
        canceled_units: int = 0,
        refunded_units: int = 0,
        returned_units: int = 0,
        unresolved_refunded_units: int = 0,
        return_refund_units: int = 0,
        delete_item_flag: bool = False,
        restore_inventory_flag: bool = False,
        ordered_at: datetime | None = None,
    ) -> SalesOrderItemModel:
        (
            normalized_canceled_units,
            normalized_refunded_units,
            normalized_returned_units,
        ) = cls.normalize_deductions(
            ordered_units=ordered_units,
            canceled_units=canceled_units,
            refunded_units=refunded_units,
            returned_units=returned_units,
            unresolved_refunded_units=unresolved_refunded_units,
            return_refund_units=return_refund_units,
        )
        normalized_unit_price = _normalize_decimal(unit_price)
        computed_effective_units = cls.calculate_effective_units(
            ordered_units=ordered_units,
            canceled_units=normalized_canceled_units,
            refunded_units=normalized_refunded_units,
            returned_units=normalized_returned_units,
        )
        return cls(
            owner_username=owner_username,
            store_id=store_id,
            sales_order_id=sales_order_id,
            order_number=order_number,
            item_detail_id=item_detail_id,
            manage_number=manage_number,
            item_number=item_number,
            item_id=item_id,
            sku_key=sku_key,
            sku_json=sku_json,
            item_name=item_name,
            unit_price=normalized_unit_price,
            ordered_units=ordered_units,
            latest_units=ordered_units if latest_units is None else latest_units,
            canceled_units=normalized_canceled_units,
            refunded_units=normalized_refunded_units,
            returned_units=normalized_returned_units,
            effective_units=computed_effective_units,
            effective_amount=normalized_unit_price * computed_effective_units,
            delete_item_flag=delete_item_flag,
            restore_inventory_flag=restore_inventory_flag,
            ordered_at=ordered_at or datetime.utcnow(),
        )


class SalesItemAdjustmentModel(TimestampMixin, Base):
    __tablename__ = "lt_sales_item_adjustments"
    __table_args__ = (
        ForeignKeyConstraint(
            ["sales_order_item_id", "owner_username", "store_id"],
            [
                "lt_sales_order_items.id",
                "lt_sales_order_items.owner_username",
                "lt_sales_order_items.store_id",
            ],
            ondelete="CASCADE",
            name="fk_lt_sales_adjustment_parent_item",
        ),
        Index("ix_lt_sales_item_adjustment_owner_store", "owner_username", "store_id"),
        Index("ix_lt_sales_item_adjustment_item_status", "sales_order_item_id", "status"),
    )

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    owner_username: Mapped[str] = mapped_column(
        String(255),
        ForeignKey("lt_user_accounts.username", ondelete="CASCADE"),
        nullable=False,
    )
    store_id: Mapped[int] = mapped_column(ForeignKey("lt_stores.id", ondelete="CASCADE"), nullable=False)
    sales_order_item_id: Mapped[int] = mapped_column(Integer, nullable=False)
    adjustment_type: Mapped[str] = mapped_column(String(32), nullable=False)
    units: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    amount: Mapped[Decimal] = mapped_column(Numeric(12, 2), nullable=False, default=0, server_default="0")
    source: Mapped[str] = mapped_column(String(64), nullable=False, default="", server_default="")
    status: Mapped[str] = mapped_column(String(32), nullable=False, default="confirmed", server_default="confirmed")
    reason: Mapped[str] = mapped_column(Text, nullable=False, default="")
    remote_updated_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=False))
    raw_payload_json: Mapped[str] = mapped_column(
        Text().with_variant(LONGTEXT(), "mysql"),
        nullable=False,
        default="{}",
    )

    sales_order_item: Mapped[SalesOrderItemModel] = relationship(back_populates="adjustments")


class ProductSalesDailyModel(TimestampMixin, Base):
    __tablename__ = "lt_product_sales_daily"
    __table_args__ = (
        ForeignKeyConstraint(
            ["store_id", "owner_username"],
            ["lt_stores.id", "lt_stores.owner_username"],
            ondelete="CASCADE",
            name="fk_lt_product_sales_daily_store_owner",
        ),
        UniqueConstraint(
            "store_id",
            "sales_date",
            "manage_number",
            "sku_key",
            name="uq_lt_product_sales_daily_store_date_manage_sku",
        ),
        Index("ix_lt_product_sales_daily_owner_store_date", "owner_username", "store_id", "sales_date"),
        Index("ix_lt_product_sales_daily_store_manage", "store_id", "manage_number"),
    )

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    owner_username: Mapped[str] = mapped_column(
        String(255),
        ForeignKey("lt_user_accounts.username", ondelete="CASCADE"),
        nullable=False,
    )
    store_id: Mapped[int] = mapped_column(Integer, nullable=False)
    sales_date: Mapped[date] = mapped_column(nullable=False)
    manage_number: Mapped[str] = mapped_column(String(255), nullable=False, default="", server_default="")
    item_number: Mapped[str] = mapped_column(String(255), nullable=False, default="", server_default="")
    sku_key: Mapped[str] = mapped_column(String(255), nullable=False, default="", server_default="")
    item_name_snapshot: Mapped[str] = mapped_column(String(500), nullable=False, default="", server_default="")
    order_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    ordered_units: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    canceled_units: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    refunded_units: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    returned_units: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    effective_units: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    gross_sales_amount: Mapped[Decimal] = mapped_column(Numeric(12, 2), nullable=False, default=0, server_default="0")
    effective_sales_amount: Mapped[Decimal] = mapped_column(Numeric(12, 2), nullable=False, default=0, server_default="0")


class SalesSyncStateModel(TimestampMixin, Base):
    __tablename__ = "lt_sales_sync_states"
    __table_args__ = (
        ForeignKeyConstraint(
            ["store_id", "owner_username"],
            ["lt_stores.id", "lt_stores.owner_username"],
            ondelete="CASCADE",
            name="fk_lt_sales_sync_state_store_owner",
        ),
        Index("ix_lt_sales_sync_state_owner_status", "owner_username", "sync_status"),
    )

    store_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    owner_username: Mapped[str] = mapped_column(
        String(255),
        ForeignKey("lt_user_accounts.username", ondelete="CASCADE"),
        nullable=False,
    )
    initial_sync_completed: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False, server_default="0")
    last_successful_sync_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=False))
    last_remote_updated_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=False))
    sync_status: Mapped[str] = mapped_column(String(32), nullable=False, default="idle", server_default="idle")
    progress_current: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    progress_total: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    last_error: Mapped[str | None] = mapped_column(Text)


class SalesAnalysisConversationModel(TimestampMixin, Base):
    __tablename__ = "lt_sales_analysis_conversations"
    __table_args__ = (
        UniqueConstraint("id", "owner_username", name="uq_lt_sales_analysis_conversation_id_owner"),
        Index("ix_lt_sales_analysis_conversation_owner_updated", "owner_username", "updated_at"),
    )

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    owner_username: Mapped[str] = mapped_column(
        String(255),
        ForeignKey("lt_user_accounts.username", ondelete="CASCADE"),
        nullable=False,
    )
    title: Mapped[str] = mapped_column(String(255), nullable=False, default="新分析", server_default="新分析")
    store_scope_json: Mapped[str] = mapped_column(
        Text().with_variant(LONGTEXT(), "mysql"),
        nullable=False,
        default="[]",
    )
    last_message_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=False))

    messages: Mapped[list[SalesAnalysisMessageModel]] = relationship(
        back_populates="conversation",
        cascade="all, delete-orphan",
    )


class SalesAnalysisMessageModel(TimestampMixin, Base):
    __tablename__ = "lt_sales_analysis_messages"
    __table_args__ = (
        ForeignKeyConstraint(
            ["conversation_id", "owner_username"],
            [
                "lt_sales_analysis_conversations.id",
                "lt_sales_analysis_conversations.owner_username",
            ],
            ondelete="CASCADE",
            name="fk_lt_sales_analysis_message_conversation_owner",
        ),
        Index("ix_lt_sales_analysis_message_conversation_created", "conversation_id", "created_at"),
        Index("ix_lt_sales_analysis_message_owner_created", "owner_username", "created_at"),
    )

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    conversation_id: Mapped[int] = mapped_column(Integer, nullable=False)
    owner_username: Mapped[str] = mapped_column(
        String(255),
        ForeignKey("lt_user_accounts.username", ondelete="CASCADE"),
        nullable=False,
    )
    question_text: Mapped[str] = mapped_column(
        Text().with_variant(LONGTEXT(), "mysql"),
        nullable=False,
        default="",
    )
    answer_text: Mapped[str] = mapped_column(
        Text().with_variant(LONGTEXT(), "mysql"),
        nullable=False,
        default="",
    )
    tool_name: Mapped[str] = mapped_column(String(128), nullable=False, default="", server_default="")
    tool_arguments_json: Mapped[str] = mapped_column(
        Text().with_variant(LONGTEXT(), "mysql"),
        nullable=False,
        default="{}",
    )
    result_summary_json: Mapped[str] = mapped_column(
        Text().with_variant(LONGTEXT(), "mysql"),
        nullable=False,
        default="{}",
    )
    model_name: Mapped[str] = mapped_column(String(255), nullable=False, default="", server_default="")
    store_scope_json: Mapped[str] = mapped_column(
        Text().with_variant(LONGTEXT(), "mysql"),
        nullable=False,
        default="[]",
    )
    statistics_window_json: Mapped[str] = mapped_column(
        Text().with_variant(LONGTEXT(), "mysql"),
        nullable=False,
        default="{}",
    )

    conversation: Mapped[SalesAnalysisConversationModel] = relationship(back_populates="messages")


def make_source_url_hash(source_url: str) -> str:
    return hashlib.sha256(source_url.strip().encode("utf-8")).hexdigest()
