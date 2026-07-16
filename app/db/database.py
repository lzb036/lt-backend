from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager

from sqlalchemy import UniqueConstraint, create_engine, inspect, text
from sqlalchemy.engine import Connection
from sqlalchemy.engine import make_url
from sqlalchemy.orm import DeclarativeBase, Session, sessionmaker
from sqlalchemy.schema import AddConstraint, CreateColumn

from app.core.config import settings


class Base(DeclarativeBase):
    pass


def _quote_mysql_identifier(value: str) -> str:
    return f"`{value.replace('`', '``')}`"


def _table_names(connection: Connection) -> set[str]:
    return set(inspect(connection).get_table_names())


def _column_info(connection: Connection, table_name: str) -> dict[str, dict]:
    if table_name not in _table_names(connection):
        return {}
    return {
        column["name"]: column
        for column in inspect(connection).get_columns(table_name)
    }


def _unique_constraint_names(connection: Connection, table_name: str) -> set[str]:
    if table_name not in _table_names(connection):
        return set()
    return {
        constraint["name"]
        for constraint in inspect(connection).get_unique_constraints(table_name)
        if constraint.get("name")
    }


def _index_names(connection: Connection, table_name: str) -> set[str]:
    if table_name not in _table_names(connection):
        return set()
    return {
        index["name"]
        for index in inspect(connection).get_indexes(table_name)
        if index.get("name")
    }


def _has_safe_server_default(column, dialect_name: str) -> bool:
    server_default = getattr(column, "server_default", None)
    if server_default is None:
        return False
    if isinstance(server_default.arg, str):
        return True
    default_sql = str(server_default.arg).strip()
    normalized = default_sql.upper()
    if dialect_name == "sqlite" and normalized in {"CURRENT_TIMESTAMP", "CURRENT_DATE", "CURRENT_TIME"}:
        return False
    if "(" in normalized and not normalized.startswith("("):
        return False
    return True


def _can_add_column_safely(column, dialect_name: str) -> bool:
    if column.primary_key:
        return False
    if column.foreign_keys:
        return False
    if column.nullable:
        return True
    return _has_safe_server_default(column, dialect_name)


def _ensure_table_layout(connection: Connection, table) -> dict[str, list[str]]:
    table_was_missing = table.name not in _table_names(connection)
    if table_was_missing:
        table.create(bind=connection, checkfirst=True)
        return {
            "created_table": [table.name],
            "added_columns": [],
            "skipped_columns": [],
            "added_constraints": [],
            "added_indexes": [],
        }

    added_columns: list[str] = []
    skipped_columns: list[str] = []
    dialect_name = connection.dialect.name

    existing_columns = _column_info(connection, table.name)
    for column in table.columns:
        if column.name in existing_columns:
            continue
        if not _can_add_column_safely(column, dialect_name):
            skipped_columns.append(column.name)
            continue
        compiled_column = str(CreateColumn(column).compile(dialect=connection.dialect))
        connection.execute(
            text(
                f"ALTER TABLE {table.name if dialect_name == 'sqlite' else _quote_mysql_identifier(table.name)} "
                f"ADD COLUMN {compiled_column}"
            )
        )
        added_columns.append(column.name)

    refreshed_columns = _column_info(connection, table.name)
    for column in table.columns:
        compiled_type = str(column.type.compile(dialect=connection.dialect)).strip().lower()
        current_column = refreshed_columns.get(column.name)
        if compiled_type != "longtext" or not current_column or not connection.dialect.supports_alter:
            continue
        current_type = str(current_column["type"]).strip().lower()
        if current_type == "longtext":
            continue
        compiled_column = str(CreateColumn(column).compile(dialect=connection.dialect))
        connection.execute(
            text(
                f"ALTER TABLE {_quote_mysql_identifier(table.name)} "
                f"MODIFY COLUMN {compiled_column}"
            )
        )

    added_constraints: list[str] = []
    if connection.dialect.supports_alter:
        existing_unique_constraints = _unique_constraint_names(connection, table.name)
        available_columns = set(_column_info(connection, table.name))
        for constraint in table.constraints:
            if not isinstance(constraint, UniqueConstraint) or not constraint.name:
                continue
            if constraint.name in existing_unique_constraints:
                continue
            if any(column.name not in available_columns for column in constraint.columns):
                continue
            compiled_constraint = str(AddConstraint(constraint).compile(dialect=connection.dialect)).strip()
            if compiled_constraint:
                connection.execute(text(compiled_constraint))
                added_constraints.append(constraint.name)

    existing_indexes = _index_names(connection, table.name)
    available_columns = set(_column_info(connection, table.name))
    added_indexes: list[str] = []
    for index in table.indexes:
        if not index.name or index.name in existing_indexes:
            continue
        if any(column.name not in available_columns for column in index.columns):
            continue
        index.create(bind=connection, checkfirst=True)
        added_indexes.append(index.name)

    return {
        "created_table": [],
        "added_columns": added_columns,
        "skipped_columns": skipped_columns,
        "added_constraints": added_constraints,
        "added_indexes": added_indexes,
    }


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
    from app.db import models as model_module

    with engine.begin() as connection:
        user_columns = set(
            connection.execute(
                text(
                    """
                    SELECT COLUMN_NAME
                    FROM INFORMATION_SCHEMA.COLUMNS
                    WHERE TABLE_SCHEMA = DATABASE()
                      AND TABLE_NAME = 'lt_user_accounts'
                    """
                )
            ).scalars()
        )
        if user_columns and "permissions_json" not in user_columns:
            connection.execute(text("ALTER TABLE lt_user_accounts ADD COLUMN permissions_json TEXT NULL"))
            connection.execute(
                text(
                    """
                    UPDATE lt_user_accounts
                    SET permissions_json = CASE
                        WHEN role = 'superadmin' THEN '["users.manage","crawler.manage","products.manage","stores.manage","settings.manage"]'
                        ELSE '["crawler.manage","products.manage","stores.manage"]'
                    END
                    WHERE permissions_json IS NULL OR permissions_json = ''
                    """
                )
            )
            connection.execute(text("ALTER TABLE lt_user_accounts MODIFY COLUMN permissions_json TEXT NOT NULL"))
        if user_columns and "crawl_min_price" not in user_columns:
            connection.execute(text("ALTER TABLE lt_user_accounts ADD COLUMN crawl_min_price INT NOT NULL DEFAULT 0"))

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
        if "rakuten_product_total_count" not in store_columns:
            connection.execute(text("ALTER TABLE lt_stores ADD COLUMN rakuten_product_total_count INT NULL"))
        if "rakuten_product_listed_count" not in store_columns:
            connection.execute(text("ALTER TABLE lt_stores ADD COLUMN rakuten_product_listed_count INT NULL"))
        if "rakuten_product_unlisted_count" not in store_columns:
            connection.execute(text("ALTER TABLE lt_stores ADD COLUMN rakuten_product_unlisted_count INT NULL"))
        if "rakuten_product_total_exceeds_limit" not in store_columns:
            connection.execute(text("ALTER TABLE lt_stores ADD COLUMN rakuten_product_total_exceeds_limit TINYINT(1) NOT NULL DEFAULT 0"))
        if "last_checked_at" not in store_columns:
            connection.execute(text("ALTER TABLE lt_stores ADD COLUMN last_checked_at DATETIME NULL"))
        if "last_product_synced_at" not in store_columns:
            connection.execute(text("ALTER TABLE lt_stores ADD COLUMN last_product_synced_at DATETIME NULL"))

        store_unique_constraints = set(
            connection.execute(
                text(
                    """
                    SELECT CONSTRAINT_NAME
                    FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS
                    WHERE TABLE_SCHEMA = DATABASE()
                      AND TABLE_NAME = 'lt_stores'
                      AND CONSTRAINT_TYPE = 'UNIQUE'
                    """
                )
            ).scalars()
        )
        if "uq_lt_store_code" in store_unique_constraints:
            connection.execute(text("ALTER TABLE lt_stores DROP INDEX uq_lt_store_code"))
        if "uq_lt_store_owner_code" not in store_unique_constraints:
            connection.execute(
                text(
                    """
                    ALTER TABLE lt_stores
                    ADD CONSTRAINT uq_lt_store_owner_code UNIQUE (owner_username, store_code)
                    """
                )
            )

        store_indexes = set(
            connection.execute(
                text(
                    """
                    SELECT INDEX_NAME
                    FROM INFORMATION_SCHEMA.STATISTICS
                    WHERE TABLE_SCHEMA = DATABASE()
                      AND TABLE_NAME = 'lt_stores'
                    """
                )
            ).scalars()
        )
        if "ix_lt_store_owner_enabled" not in store_indexes:
            connection.execute(text("CREATE INDEX ix_lt_store_owner_enabled ON lt_stores (owner_username, enabled)"))

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
        if "parent_product_id" not in product_columns:
            connection.execute(text("ALTER TABLE lt_products ADD COLUMN parent_product_id INT NULL"))
        if "listing_task_id" not in product_columns:
            connection.execute(text("ALTER TABLE lt_products ADD COLUMN listing_task_id VARCHAR(64) NULL"))
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
        if "ix_lt_product_owner_created" not in product_indexes:
            connection.execute(text("CREATE INDEX ix_lt_product_owner_created ON lt_products (owner_username, created_at)"))
        if "ix_lt_product_owner_updated" not in product_indexes:
            connection.execute(text("CREATE INDEX ix_lt_product_owner_updated ON lt_products (owner_username, updated_at)"))
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
        if "ix_lt_product_parent_status" not in product_indexes:
            connection.execute(text("CREATE INDEX ix_lt_product_parent_status ON lt_products (parent_product_id, review_status)"))
        if "ix_lt_product_listing_task" not in product_indexes:
            connection.execute(text("CREATE INDEX ix_lt_product_listing_task ON lt_products (listing_task_id)"))

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
            payload_json_type = connection.execute(
                text(
                    """
                    SELECT DATA_TYPE
                    FROM INFORMATION_SCHEMA.COLUMNS
                    WHERE TABLE_SCHEMA = DATABASE()
                      AND TABLE_NAME = 'lt_sync_tasks'
                      AND COLUMN_NAME = 'payload_json'
                    """
                )
            ).scalar()
            if str(payload_json_type or "").strip().lower() != "longtext":
                connection.execute(text("ALTER TABLE lt_sync_tasks MODIFY COLUMN payload_json LONGTEXT NOT NULL"))

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
        if sync_task_columns:
            if "ix_lt_sync_task_owner_status" not in sync_task_indexes:
                connection.execute(text("CREATE INDEX ix_lt_sync_task_owner_status ON lt_sync_tasks (owner_username, status)"))
            if "ix_lt_sync_task_owner_created" not in sync_task_indexes:
                connection.execute(text("CREATE INDEX ix_lt_sync_task_owner_created ON lt_sync_tasks (owner_username, created_at)"))
            if "ix_lt_sync_task_owner_started" not in sync_task_indexes:
                connection.execute(text("CREATE INDEX ix_lt_sync_task_owner_started ON lt_sync_tasks (owner_username, started_at)"))
            if "ix_lt_sync_task_owner_finished" not in sync_task_indexes:
                connection.execute(text("CREATE INDEX ix_lt_sync_task_owner_finished ON lt_sync_tasks (owner_username, finished_at)"))
            if "ix_lt_sync_task_owner_updated" not in sync_task_indexes:
                connection.execute(text("CREATE INDEX ix_lt_sync_task_owner_updated ON lt_sync_tasks (owner_username, updated_at)"))

        listing_task_indexes = set(
            connection.execute(
                text(
                    """
                    SELECT INDEX_NAME
                    FROM INFORMATION_SCHEMA.STATISTICS
                    WHERE TABLE_SCHEMA = DATABASE()
                      AND TABLE_NAME = 'lt_listing_tasks'
                    """
                )
            ).scalars()
        )
        listing_task_columns = set(
            connection.execute(
                text(
                    """
                    SELECT COLUMN_NAME
                    FROM INFORMATION_SCHEMA.COLUMNS
                    WHERE TABLE_SCHEMA = DATABASE()
                      AND TABLE_NAME = 'lt_listing_tasks'
                    """
                )
            ).scalars()
        )
        if listing_task_columns:
            if "ix_lt_listing_task_owner_status" not in listing_task_indexes:
                connection.execute(text("CREATE INDEX ix_lt_listing_task_owner_status ON lt_listing_tasks (owner_username, status)"))
            if "ix_lt_listing_task_owner_created" not in listing_task_indexes:
                connection.execute(text("CREATE INDEX ix_lt_listing_task_owner_created ON lt_listing_tasks (owner_username, created_at)"))
            if "ix_lt_listing_task_owner_started" not in listing_task_indexes:
                connection.execute(text("CREATE INDEX ix_lt_listing_task_owner_started ON lt_listing_tasks (owner_username, started_at)"))
            if "ix_lt_listing_task_owner_finished" not in listing_task_indexes:
                connection.execute(text("CREATE INDEX ix_lt_listing_task_owner_finished ON lt_listing_tasks (owner_username, finished_at)"))
            if "ix_lt_listing_task_owner_updated" not in listing_task_indexes:
                connection.execute(text("CREATE INDEX ix_lt_listing_task_owner_updated ON lt_listing_tasks (owner_username, updated_at)"))

        crawl_task_indexes = set(
            connection.execute(
                text(
                    """
                    SELECT INDEX_NAME
                    FROM INFORMATION_SCHEMA.STATISTICS
                    WHERE TABLE_SCHEMA = DATABASE()
                      AND TABLE_NAME = 'lt_crawl_tasks'
                    """
                )
            ).scalars()
        )
        crawl_task_columns = set(
            connection.execute(
                text(
                    """
                    SELECT COLUMN_NAME
                    FROM INFORMATION_SCHEMA.COLUMNS
                    WHERE TABLE_SCHEMA = DATABASE()
                      AND TABLE_NAME = 'lt_crawl_tasks'
                    """
                )
            ).scalars()
        )
        if crawl_task_columns:
            if "warning_count" not in crawl_task_columns:
                connection.execute(text("ALTER TABLE lt_crawl_tasks ADD COLUMN warning_count INT NOT NULL DEFAULT 0"))
            if "warning_detail" not in crawl_task_columns:
                connection.execute(text("ALTER TABLE lt_crawl_tasks ADD COLUMN warning_detail TEXT NULL"))
            if "queue_job_id" not in crawl_task_columns:
                connection.execute(text("ALTER TABLE lt_crawl_tasks ADD COLUMN queue_job_id VARCHAR(64) NULL"))
            if "ix_lt_crawl_task_owner_status" not in crawl_task_indexes:
                connection.execute(text("CREATE INDEX ix_lt_crawl_task_owner_status ON lt_crawl_tasks (owner_username, status)"))
            if "ix_lt_crawl_task_owner_created" not in crawl_task_indexes:
                connection.execute(text("CREATE INDEX ix_lt_crawl_task_owner_created ON lt_crawl_tasks (owner_username, created_at)"))
            if "ix_lt_crawl_task_owner_started" not in crawl_task_indexes:
                connection.execute(text("CREATE INDEX ix_lt_crawl_task_owner_started ON lt_crawl_tasks (owner_username, started_at)"))
            if "ix_lt_crawl_task_owner_finished" not in crawl_task_indexes:
                connection.execute(text("CREATE INDEX ix_lt_crawl_task_owner_finished ON lt_crawl_tasks (owner_username, finished_at)"))

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

        sales_tables = (
            model_module.SalesOrderModel.__table__,
            model_module.SalesOrderItemModel.__table__,
            model_module.SalesItemAdjustmentModel.__table__,
            model_module.ProductSalesDailyModel.__table__,
            model_module.SalesSyncStateModel.__table__,
            model_module.SalesAnalysisConversationModel.__table__,
            model_module.SalesAnalysisMessageModel.__table__,
        )
        for sales_table in sales_tables:
            _ensure_table_layout(connection, sales_table)


engine = create_engine(
    settings.database_url,
    echo=settings.database_echo,
    pool_size=settings.database_pool_size,
    max_overflow=settings.database_max_overflow,
    pool_timeout=settings.database_pool_timeout,
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
    from app.services.sensitive_word_service import seed_default_sensitive_words
    from app.services.user_service import ensure_initial_superadmin

    Base.metadata.create_all(bind=engine)
    ensure_schema_compatibility()
    ensure_initial_superadmin()
    ensure_default_roles()
    session = SessionLocal()
    try:
        seed_default_sensitive_words(session)
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


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
