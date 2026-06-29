from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager

from sqlalchemy import create_engine, text
from sqlalchemy.engine import make_url
from sqlalchemy.orm import DeclarativeBase, Session, sessionmaker

from app.core.config import settings


class Base(DeclarativeBase):
    pass


def _quote_mysql_identifier(value: str) -> str:
    return f"`{value.replace('`', '``')}`"


def ensure_mysql_database_exists() -> None:
    url = make_url(settings.database_url)
    if not url.drivername.startswith("mysql") or not url.database:
        return
    admin_engine = create_engine(
        url.set(database=""),
        echo=settings.database_echo,
        pool_pre_ping=True,
        future=True,
    )
    try:
        with admin_engine.begin() as connection:
            database = _quote_mysql_identifier(url.database)
            connection.execute(
                text(f"CREATE DATABASE IF NOT EXISTS {database} CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci")
            )
    finally:
        admin_engine.dispose()


def ensure_schema_compatibility() -> None:
    url = make_url(settings.database_url)
    if not url.drivername.startswith("mysql"):
        return
    with engine.begin() as connection:
        store_columns = set(
            connection.execute(
                text(
                    """
                    SELECT COLUMN_NAME
                    FROM INFORMATION_SCHEMA.COLUMNS
                    WHERE TABLE_SCHEMA = DATABASE()
                      AND TABLE_NAME = 'lt_stores'
                    """
                )
            ).scalars()
        )
        if "cabinet_used_folder_count" not in store_columns:
            connection.execute(text("ALTER TABLE lt_stores ADD COLUMN cabinet_used_folder_count INT NULL"))
        if "cabinet_remaining_folder_count" not in store_columns:
            connection.execute(text("ALTER TABLE lt_stores ADD COLUMN cabinet_remaining_folder_count INT NULL"))
        if "cabinet_usage_checked_at" not in store_columns:
            connection.execute(text("ALTER TABLE lt_stores ADD COLUMN cabinet_usage_checked_at DATETIME NULL"))

        product_columns = set(
            connection.execute(
                text(
                    """
                    SELECT COLUMN_NAME
                    FROM INFORMATION_SCHEMA.COLUMNS
                    WHERE TABLE_SCHEMA = DATABASE()
                      AND TABLE_NAME = 'lt_products'
                    """
                )
            ).scalars()
        )
        if "store_id" not in product_columns:
            connection.execute(text("ALTER TABLE lt_products ADD COLUMN store_id INT NULL"))
        if "rakuten_manage_number" not in product_columns:
            connection.execute(text("ALTER TABLE lt_products ADD COLUMN rakuten_manage_number VARCHAR(255) NULL"))
        if "store_product_status" not in product_columns:
            connection.execute(text("ALTER TABLE lt_products ADD COLUMN store_product_status VARCHAR(32) NOT NULL DEFAULT ''"))
        if "rakuten_listing_status" not in product_columns:
            connection.execute(text("ALTER TABLE lt_products ADD COLUMN rakuten_listing_status VARCHAR(32) NOT NULL DEFAULT ''"))
        if "listed_at" not in product_columns:
            connection.execute(text("ALTER TABLE lt_products ADD COLUMN listed_at DATETIME NULL"))
        if "store_last_seen_at" not in product_columns:
            connection.execute(text("ALTER TABLE lt_products ADD COLUMN store_last_seen_at DATETIME NULL"))

        connection.execute(
            text(
                """
                UPDATE lt_products
                SET rakuten_manage_number = NULLIF(item_number, '')
                WHERE store_id IS NOT NULL
                  AND review_status = 'listed'
                  AND (rakuten_manage_number IS NULL OR rakuten_manage_number = '')
                """
            )
        )
        connection.execute(
            text(
                """
                UPDATE lt_products
                SET rakuten_listing_status = 'listed'
                WHERE store_id IS NOT NULL
                  AND review_status = 'listed'
                  AND rakuten_listing_status = ''
                """
            )
        )
        connection.execute(
            text(
                """
                UPDATE lt_products
                SET store_product_status = 'active'
                WHERE store_id IS NOT NULL
                  AND review_status = 'listed'
                  AND store_product_status = ''
                """
            )
        )
        connection.execute(
            text(
                """
                UPDATE lt_products
                SET listed_at = STR_TO_DATE(
                    LEFT(REPLACE(JSON_UNQUOTE(JSON_EXTRACT(raw_payload_json, '$.created')), 'T', ' '), 19),
                    '%Y-%m-%d %H:%i:%s'
                )
                WHERE listed_at IS NULL
                  AND JSON_VALID(raw_payload_json)
                  AND JSON_UNQUOTE(JSON_EXTRACT(raw_payload_json, '$.created')) IS NOT NULL
                """
            )
        )

        raw_payload_type = connection.execute(
            text(
                """
                SELECT DATA_TYPE
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = DATABASE()
                  AND TABLE_NAME = 'lt_products'
                  AND COLUMN_NAME = 'raw_payload_json'
                """
            )
        ).scalar()
        if raw_payload_type and str(raw_payload_type).lower() != "longtext":
            connection.execute(text("ALTER TABLE lt_products MODIFY COLUMN raw_payload_json LONGTEXT NOT NULL"))

        product_indexes = set(
            connection.execute(
                text(
                    """
                    SELECT INDEX_NAME
                    FROM INFORMATION_SCHEMA.STATISTICS
                    WHERE TABLE_SCHEMA = DATABASE()
                      AND TABLE_NAME = 'lt_products'
                    """
                )
            ).scalars()
        )
        if "ix_lt_product_owner_store" not in product_indexes:
            connection.execute(text("CREATE INDEX ix_lt_product_owner_store ON lt_products (owner_username, store_id)"))
        if "ix_lt_product_store_status" not in product_indexes:
            connection.execute(text("CREATE INDEX ix_lt_product_store_status ON lt_products (store_id, store_product_status)"))
        if "ix_lt_product_store_listing_listed" not in product_indexes:
            connection.execute(
                text(
                    """
                    CREATE INDEX ix_lt_product_store_listing_listed
                    ON lt_products (store_id, review_status, rakuten_listing_status, listed_at)
                    """
                )
            )

        product_unique_constraints = set(
            connection.execute(
                text(
                    """
                    SELECT CONSTRAINT_NAME
                    FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS
                    WHERE TABLE_SCHEMA = DATABASE()
                      AND TABLE_NAME = 'lt_products'
                      AND CONSTRAINT_TYPE = 'UNIQUE'
                    """
                )
            ).scalars()
        )
        if "uq_lt_product_store_manage_number" not in product_unique_constraints:
            connection.execute(
                text(
                    """
                    ALTER TABLE lt_products
                    ADD CONSTRAINT uq_lt_product_store_manage_number
                    UNIQUE (store_id, rakuten_manage_number)
                    """
                )
            )

        sync_task_columns = set(
            connection.execute(
                text(
                    """
                    SELECT COLUMN_NAME
                    FROM INFORMATION_SCHEMA.COLUMNS
                    WHERE TABLE_SCHEMA = DATABASE()
                      AND TABLE_NAME = 'lt_sync_tasks'
                    """
                )
            ).scalars()
        )
        if sync_task_columns:
            if "task_type" not in sync_task_columns:
                connection.execute(text("ALTER TABLE lt_sync_tasks ADD COLUMN task_type VARCHAR(32) NOT NULL DEFAULT 'store_sync'"))
            if "payload_json" not in sync_task_columns:
                connection.execute(text("ALTER TABLE lt_sync_tasks ADD COLUMN payload_json TEXT NULL"))
                connection.execute(text("UPDATE lt_sync_tasks SET payload_json = '{}' WHERE payload_json IS NULL OR payload_json = ''"))
                connection.execute(text("ALTER TABLE lt_sync_tasks MODIFY COLUMN payload_json TEXT NOT NULL"))

        sync_task_indexes = set(
            connection.execute(
                text(
                    """
                    SELECT INDEX_NAME
                    FROM INFORMATION_SCHEMA.STATISTICS
                    WHERE TABLE_SCHEMA = DATABASE()
                      AND TABLE_NAME = 'lt_sync_tasks'
                    """
                )
            ).scalars()
        )
        if sync_task_indexes:
            if "ix_lt_sync_task_owner_status" not in sync_task_indexes:
                connection.execute(text("CREATE INDEX ix_lt_sync_task_owner_status ON lt_sync_tasks (owner_username, status)"))
            if "ix_lt_sync_task_owner_created" not in sync_task_indexes:
                connection.execute(text("CREATE INDEX ix_lt_sync_task_owner_created ON lt_sync_tasks (owner_username, created_at)"))

        schedule_columns = set(
            connection.execute(
                text(
                    """
                    SELECT COLUMN_NAME
                    FROM INFORMATION_SCHEMA.COLUMNS
                    WHERE TABLE_SCHEMA = DATABASE()
                      AND TABLE_NAME = 'lt_scheduled_crawls'
                    """
                )
            ).scalars()
        )
        if schedule_columns and "schedule_time" not in schedule_columns:
            connection.execute(text("ALTER TABLE lt_scheduled_crawls ADD COLUMN schedule_time VARCHAR(5) NOT NULL DEFAULT '09:00'"))


engine = create_engine(
    settings.database_url,
    echo=settings.database_echo,
    pool_pre_ping=True,
    pool_recycle=1800,
    future=True,
)

SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, expire_on_commit=False, future=True)


def init_database() -> None:
    if settings.database_auto_create:
        ensure_mysql_database_exists()
    from app.db import models  # noqa: F401
    from app.services.crawler_service import ensure_default_roles
    from app.services.user_service import ensure_initial_superadmin

    Base.metadata.create_all(bind=engine)
    ensure_schema_compatibility()
    ensure_initial_superadmin()
    ensure_default_roles()


@contextmanager
def session_scope() -> Iterator[Session]:
    session = SessionLocal()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
