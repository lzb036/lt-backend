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
                SET store_product_status = 'active'
                WHERE store_id IS NOT NULL
                  AND review_status = 'listed'
                  AND store_product_status = ''
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
