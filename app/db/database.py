from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager

from sqlalchemy import (
    ForeignKeyConstraint,
    UniqueConstraint,
    and_,
    create_engine,
    exists,
    func,
    inspect,
    literal,
    or_,
    select,
    text,
)
from sqlalchemy.engine import Connection
from sqlalchemy.engine import make_url
from sqlalchemy.exc import SQLAlchemyError
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


def _primary_key_columns(connection: Connection, table_name: str) -> tuple[str, ...]:
    if table_name not in _table_names(connection):
        return ()
    primary_key = inspect(connection).get_pk_constraint(table_name)
    return tuple(primary_key.get("constrained_columns") or ())


def _unique_constraint_info(connection: Connection, table_name: str) -> list[dict]:
    if table_name not in _table_names(connection):
        return []
    return list(inspect(connection).get_unique_constraints(table_name))


def _foreign_key_info(connection: Connection, table_name: str) -> list[dict]:
    if table_name not in _table_names(connection):
        return []
    return list(inspect(connection).get_foreign_keys(table_name))


def _index_info(connection: Connection, table_name: str) -> dict[str, dict]:
    if table_name not in _table_names(connection):
        return {}
    return {
        index["name"]: index
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
    if normalized in {
        "CURRENT_TIMESTAMP",
        "CURRENT_TIMESTAMP()",
        "NOW()",
    }:
        return dialect_name == "mysql"
    if dialect_name == "sqlite" and normalized in {"CURRENT_DATE", "CURRENT_TIME"}:
        return False
    if "(" in normalized and not normalized.startswith("("):
        return False
    return True


def _can_add_column_safely(column, dialect_name: str) -> bool:
    if column.primary_key:
        return False
    if column.nullable:
        return True
    return _has_safe_server_default(column, dialect_name)


def _compatibility_error(table_name: str, issue: str) -> RuntimeError:
    return RuntimeError(f"{table_name}: {issue}")


def _column_has_nulls(connection: Connection, table, column) -> bool:
    statement = select(column).select_from(table).where(column.is_(None)).limit(1)
    return connection.execute(statement).first() is not None


_NO_BACKFILL = object()
_PRODUCT_KEY_BACKFILL_BATCH_SIZE = 1000


def _safe_backfill_value(column):
    for default_clause in (column.server_default, column.default):
        if default_clause is None:
            continue
        value = default_clause.arg
        if isinstance(value, (str, int, float, bool)):
            return value
    return _NO_BACKFILL


def _compile_modify_column(table, column, dialect) -> str:
    compiled_column = str(CreateColumn(column).compile(dialect=dialect))
    table_name = (
        _quote_mysql_identifier(table.name)
        if dialect.name == "mysql"
        else table.name
    )
    return f"ALTER TABLE {table_name} MODIFY COLUMN {compiled_column}"


def _normalize_longtext_column(
    connection: Connection,
    table,
    column,
    current_column: dict,
) -> bool:
    compiled_type = str(column.type.compile(dialect=connection.dialect)).strip().lower()
    if compiled_type != "longtext":
        return False

    current_type = str(current_column["type"]).strip().lower()
    needs_type_change = current_type != "longtext"
    needs_nullability_change = (
        not column.nullable and current_column.get("nullable") is True
    )
    if not needs_type_change and not needs_nullability_change:
        return False

    if not column.nullable and current_column.get("nullable") is True:
        if _column_has_nulls(connection, table, column):
            backfill_value = _safe_backfill_value(column)
            if backfill_value is _NO_BACKFILL:
                raise _compatibility_error(
                    table.name,
                    f"column {column.name} contains NULL and has no safe backfill",
                )
            try:
                connection.execute(
                    table.update()
                    .where(column.is_(None))
                    .values({column.name: backfill_value})
                )
            except SQLAlchemyError as exc:
                raise _compatibility_error(
                    table.name,
                    f"column {column.name} cannot backfill NULL data",
                ) from exc
            if _column_has_nulls(connection, table, column):
                raise _compatibility_error(
                    table.name,
                    f"column {column.name} still contains NULL after backfill",
                )

    if not connection.dialect.supports_alter:
        raise _compatibility_error(
            table.name,
            f"column {column.name} cannot normalize to LONGTEXT on {connection.dialect.name}",
        )

    try:
        ddl = _compile_modify_column(table, column, connection.dialect)
        connection.execute(text(ddl))
    except SQLAlchemyError as exc:
        raise _compatibility_error(
            table.name,
            f"column {column.name} cannot normalize to LONGTEXT",
        ) from exc
    return True


def _normalize_required_nullability(
    connection: Connection,
    table,
    column,
    current_column: dict,
) -> bool:
    if (
        column.primary_key
        or column.nullable
        or current_column.get("nullable") is not True
    ):
        return False
    if _column_has_nulls(connection, table, column):
        raise _compatibility_error(
            table.name,
            f"column {column.name} contains NULL required data",
        )
    if not connection.dialect.supports_alter or connection.dialect.name != "mysql":
        raise _compatibility_error(
            table.name,
            f"column {column.name} cannot enforce NOT NULL on {connection.dialect.name}",
        )
    try:
        ddl = _compile_modify_column(table, column, connection.dialect)
        connection.execute(text(ddl))
    except SQLAlchemyError as exc:
        raise _compatibility_error(
            table.name,
            f"column {column.name} cannot enforce NOT NULL",
        ) from exc
    return True


def _unique_constraint_matches(existing: dict, constraint: UniqueConstraint) -> bool:
    return tuple(existing.get("column_names") or ()) == tuple(
        column.name for column in constraint.columns
    )


def _normalize_referential_action(value) -> str:
    normalized = str(value or "NO ACTION").strip().upper().replace("_", " ")
    if normalized in {"NO ACTION", "RESTRICT"}:
        return "RESTRICT"
    return normalized


def _foreign_key_structure_matches(
    existing: dict,
    constraint: ForeignKeyConstraint,
) -> bool:
    elements = list(constraint.elements)
    referred_table = elements[0].column.table
    return (
        tuple(existing.get("constrained_columns") or ())
        == tuple(column.name for column in constraint.columns)
        and (existing.get("referred_schema") or None)
        == (referred_table.schema or None)
        and existing.get("referred_table") == referred_table.name
        and tuple(existing.get("referred_columns") or ())
        == tuple(element.column.name for element in elements)
    )


def _foreign_key_actions(existing: dict) -> tuple[str, str]:
    existing_options = existing.get("options") or {}
    return (
        _normalize_referential_action(existing_options.get("ondelete")),
        _normalize_referential_action(existing_options.get("onupdate")),
    )


def _foreign_key_constraint_matches(
    existing: dict,
    constraint: ForeignKeyConstraint,
) -> bool:
    return (
        _foreign_key_structure_matches(existing, constraint)
        and _foreign_key_actions(existing)
        == (
            _normalize_referential_action(constraint.ondelete),
            _normalize_referential_action(constraint.onupdate),
        )
    )


def _constraint_is_present(connection: Connection, table, constraint) -> bool:
    if isinstance(constraint, UniqueConstraint):
        existing_constraints = _unique_constraint_info(connection, table.name)
        for existing in existing_constraints:
            if _unique_constraint_matches(existing, constraint):
                return True
            if constraint.name and existing.get("name") == constraint.name:
                raise _compatibility_error(
                    table.name,
                    f"constraint {constraint.name} has a conflicting definition",
                )
        return False

    if not isinstance(constraint, ForeignKeyConstraint):
        return True

    exact_match = False
    expected_local_columns = tuple(
        column.name for column in constraint.columns
    )
    expected_actions = (
        _normalize_referential_action(constraint.ondelete),
        _normalize_referential_action(constraint.onupdate),
    )
    for existing in _foreign_key_info(connection, table.name):
        existing_name = existing.get("name") or "<unnamed>"
        existing_local_columns = tuple(
            existing.get("constrained_columns") or ()
        )
        same_structure = _foreign_key_structure_matches(
            existing,
            constraint,
        )
        if same_structure:
            existing_actions = _foreign_key_actions(existing)
            if existing_actions != expected_actions:
                raise _compatibility_error(
                    table.name,
                    f"constraint {constraint.name} conflicts with existing foreign key "
                    f"{existing_name}: referential action mismatch "
                    f"(expected ON DELETE {expected_actions[0]} / ON UPDATE {expected_actions[1]}, "
                    f"found ON DELETE {existing_actions[0]} / ON UPDATE {existing_actions[1]})",
                )
            exact_match = True
            continue

        if constraint.name and existing.get("name") == constraint.name:
            raise _compatibility_error(
                table.name,
                f"constraint {constraint.name} has a conflicting definition",
            )
        if existing_local_columns == expected_local_columns:
            raise _compatibility_error(
                table.name,
                f"constraint {constraint.name} conflicts with existing foreign key "
                f"{existing_name}: incompatible referenced target",
            )
    return exact_match


def _normalize_index_option_value(value):
    if isinstance(value, dict):
        return tuple(
            sorted(
                (
                    key,
                    _normalize_index_option_value(item),
                )
                for key, item in value.items()
            )
        )
    if isinstance(value, (list, tuple)):
        return tuple(_normalize_index_option_value(item) for item in value)
    if isinstance(value, str):
        return value.lower()
    return value


def _meaningful_index_options(options: dict, dialect_name: str) -> dict:
    prefix = f"{dialect_name}_"
    normalized: dict[str, object] = {}

    nested_options = options.get(dialect_name)
    if isinstance(nested_options, dict):
        options = {
            **options,
            **{
                f"{prefix}{key}": value
                for key, value in nested_options.items()
            },
        }

    for key, value in options.items():
        if key == dialect_name and isinstance(value, dict):
            continue
        normalized_key = key if key.startswith(prefix) else f"{prefix}{key}"
        if value is None or value == {} or value == [] or value == ():
            continue
        normalized[normalized_key] = _normalize_index_option_value(value)
    return normalized


_REFLECTED_INDEX_DIALECT_OPTIONS = {
    "mysql": frozenset(
        {
            "mysql_length",
            "mysql_prefix",
            "mysql_with_parser",
        }
    ),
    "mariadb": frozenset(
        {
            "mariadb_length",
            "mariadb_prefix",
            "mariadb_with_parser",
        }
    ),
}


def _reflectable_index_options(options: dict, dialect_name: str) -> dict:
    normalized = _meaningful_index_options(options, dialect_name)
    reflected_keys = _REFLECTED_INDEX_DIALECT_OPTIONS.get(dialect_name)
    if reflected_keys is None:
        return normalized
    return {
        key: value
        for key, value in normalized.items()
        if key in reflected_keys
    }


def _expected_index_dialect_options(index, dialect_name: str) -> dict:
    prefix = f"{dialect_name}_"
    options = {
        key: value
        for key, value in dict(index.dialect_kwargs).items()
        if key.startswith(prefix)
    }
    length_key = f"{prefix}length"
    length_value = options.get(length_key)
    if length_value is not None and not isinstance(length_value, dict):
        options[length_key] = {
            column.name: length_value
            for column in index.columns
        }
    return _reflectable_index_options(
        options,
        dialect_name,
    )


def _existing_index_dialect_options(existing: dict, dialect_name: str) -> dict:
    return _reflectable_index_options(
        existing.get("dialect_options") or {},
        dialect_name,
    )


def _index_definition_matches(existing: dict, index, dialect_name: str) -> bool:
    return (
        tuple(existing.get("column_names") or ())
        == tuple(column.name for column in index.columns)
        and bool(existing.get("unique"))
        == bool(index.unique)
        and _existing_index_dialect_options(existing, dialect_name)
        == _expected_index_dialect_options(index, dialect_name)
    )


def _validate_unique_constraint_data(
    connection: Connection,
    table,
    constraint: UniqueConstraint,
) -> None:
    columns = [table.c[column.name] for column in constraint.columns]
    statement = (
        select(literal(1))
        .select_from(table)
        .where(and_(*(column.is_not(None) for column in columns)))
        .group_by(*columns)
        .having(func.count() > 1)
        .limit(1)
    )
    if connection.execute(statement).first() is not None:
        raise _compatibility_error(
            table.name,
            f"constraint {constraint.name} has duplicate data",
        )


def _parent_key_is_available(
    connection: Connection,
    parent_table,
    referred_columns: tuple[str, ...],
) -> bool:
    if _primary_key_columns(connection, parent_table.name) == referred_columns:
        return True
    return any(
        tuple(item.get("column_names") or ()) == referred_columns
        for item in _unique_constraint_info(connection, parent_table.name)
    )


def _validate_foreign_key_constraint_data(
    connection: Connection,
    table,
    constraint: ForeignKeyConstraint,
) -> None:
    elements = list(constraint.elements)
    parent_table = elements[0].column.table
    local_columns = tuple(column.name for column in constraint.columns)
    referred_columns = tuple(element.column.name for element in elements)

    if parent_table.name not in _table_names(connection):
        raise _compatibility_error(
            table.name,
            f"constraint {constraint.name} parent table {parent_table.name} is missing",
        )
    parent_columns = set(_column_info(connection, parent_table.name))
    missing_parent_columns = [
        column for column in referred_columns if column not in parent_columns
    ]
    if missing_parent_columns:
        raise _compatibility_error(
            table.name,
            f"constraint {constraint.name} parent columns missing: {', '.join(missing_parent_columns)}",
        )
    if not _parent_key_is_available(connection, parent_table, referred_columns):
        raise _compatibility_error(
            table.name,
            f"constraint {constraint.name} parent key is not unique",
        )

    child_alias = table.alias("compat_child")
    parent_alias = parent_table.alias("compat_parent")
    parent_match = and_(
        *(
            parent_alias.c[remote_name] == child_alias.c[local_name]
            for local_name, remote_name in zip(
                local_columns,
                referred_columns,
                strict=True,
            )
        )
    )
    child_values_present = and_(
        *(child_alias.c[column_name].is_not(None) for column_name in local_columns)
    )
    statement = (
        select(literal(1))
        .select_from(child_alias)
        .where(
            child_values_present,
            ~exists(select(literal(1)).select_from(parent_alias).where(parent_match)),
        )
        .limit(1)
    )
    if connection.execute(statement).first() is not None:
        raise _compatibility_error(
            table.name,
            f"constraint {constraint.name} has conflicting foreign key data",
        )


def _compile_add_constraint(constraint, dialect) -> str:
    return str(AddConstraint(constraint).compile(dialect=dialect)).strip()


def _ensure_constraint(connection: Connection, table, constraint) -> bool:
    if _constraint_is_present(connection, table, constraint):
        return False

    available_columns = set(_column_info(connection, table.name))
    missing_columns = [
        column.name
        for column in constraint.columns
        if column.name not in available_columns
    ]
    if missing_columns:
        raise _compatibility_error(
            table.name,
            f"constraint {constraint.name} columns missing: {', '.join(missing_columns)}",
        )

    if isinstance(constraint, UniqueConstraint):
        _validate_unique_constraint_data(connection, table, constraint)
    elif isinstance(constraint, ForeignKeyConstraint):
        _validate_foreign_key_constraint_data(connection, table, constraint)

    if not connection.dialect.supports_alter:
        raise _compatibility_error(
            table.name,
            f"constraint {constraint.name} cannot install on {connection.dialect.name}",
        )

    try:
        ddl = _compile_add_constraint(constraint, connection.dialect)
        if not ddl:
            raise _compatibility_error(
                table.name,
                f"constraint {constraint.name} cannot install",
            )
        connection.execute(text(ddl))
    except RuntimeError:
        raise
    except SQLAlchemyError as exc:
        raise _compatibility_error(
            table.name,
            f"constraint {constraint.name} cannot install",
        ) from exc
    return True


def _ensure_table_layout(connection: Connection, table) -> dict[str, list[str]]:
    table_was_missing = table.name not in _table_names(connection)
    if table_was_missing:
        try:
            table.create(bind=connection, checkfirst=True)
        except SQLAlchemyError as exc:
            raise _compatibility_error(
                table.name,
                "table cannot be created with required constraints",
            ) from exc
        return {
            "created_table": [table.name],
            "added_columns": [],
            "skipped_columns": [],
            "added_constraints": [],
            "added_indexes": [],
        }

    added_columns: list[str] = []
    dialect_name = connection.dialect.name

    expected_primary_key = tuple(column.name for column in table.primary_key.columns)
    actual_primary_key = _primary_key_columns(connection, table.name)
    if actual_primary_key != expected_primary_key:
        raise _compatibility_error(
            table.name,
            "primary key mismatch "
            f"(expected {expected_primary_key or 'none'}, found {actual_primary_key or 'none'})",
        )

    existing_columns = _column_info(connection, table.name)
    for column in table.columns:
        if column.name in existing_columns:
            continue
        if not _can_add_column_safely(column, dialect_name):
            raise _compatibility_error(
                table.name,
                f"required column {column.name} has no safely addable server default",
            )
        try:
            compiled_column = str(
                CreateColumn(column).compile(dialect=connection.dialect)
            )
            connection.execute(
                text(
                    f"ALTER TABLE {table.name if dialect_name == 'sqlite' else _quote_mysql_identifier(table.name)} "
                    f"ADD COLUMN {compiled_column}"
                )
            )
        except SQLAlchemyError as exc:
            raise _compatibility_error(
                table.name,
                f"column {column.name} cannot be added safely",
            ) from exc
        added_columns.append(column.name)

    refreshed_columns = _column_info(connection, table.name)
    for column in table.columns:
        current_column = refreshed_columns.get(column.name)
        if not current_column:
            raise _compatibility_error(
                table.name,
                f"column {column.name} is still missing after compatibility update",
            )
        compiled_type = str(
            column.type.compile(dialect=connection.dialect)
        ).strip().lower()
        if compiled_type == "longtext":
            _normalize_longtext_column(
                connection,
                table,
                column,
                current_column,
            )
            continue
        _normalize_required_nullability(
            connection,
            table,
            column,
            current_column,
        )

    added_constraints: list[str] = []
    required_constraints = [
        constraint
        for constraint in table.constraints
        if isinstance(constraint, (UniqueConstraint, ForeignKeyConstraint))
    ]
    unnamed_constraints = [
        constraint
        for constraint in required_constraints
        if not constraint.name
    ]
    if unnamed_constraints:
        descriptions = ", ".join(
            f"{type(constraint).__name__}({', '.join(column.name for column in constraint.columns)})"
            for constraint in unnamed_constraints
        )
        raise _compatibility_error(
            table.name,
            f"required constraints must be named: {descriptions}",
        )
    required_constraints.sort(
        key=lambda constraint: (
            0 if isinstance(constraint, UniqueConstraint) else 1,
            constraint.name,
        )
    )
    for constraint in required_constraints:
        if _ensure_constraint(connection, table, constraint):
            added_constraints.append(constraint.name)

    existing_indexes = _index_info(connection, table.name)
    available_columns = set(_column_info(connection, table.name))
    added_indexes: list[str] = []
    for index in table.indexes:
        if not index.name:
            raise _compatibility_error(
                table.name,
                "required indexes must be named",
            )
        existing_index = existing_indexes.get(index.name)
        if existing_index is not None:
            if not _index_definition_matches(
                existing_index,
                index,
                connection.dialect.name,
            ):
                raise _compatibility_error(
                    table.name,
                    f"index {index.name} has a conflicting definition",
                )
            continue
        if any(column.name not in available_columns for column in index.columns):
            raise _compatibility_error(
                table.name,
                f"index {index.name} references missing columns",
            )
        try:
            index.create(bind=connection, checkfirst=True)
        except SQLAlchemyError as exc:
            raise _compatibility_error(
                table.name,
                f"index {index.name} cannot install",
            ) from exc
        added_indexes.append(index.name)

    return {
        "created_table": [],
        "added_columns": added_columns,
        "skipped_columns": [],
        "added_constraints": added_constraints,
        "added_indexes": added_indexes,
    }


def _backfill_sales_order_item_product_keys(
    connection: Connection,
    table,
) -> int:
    columns = _column_info(connection, table.name)
    required_columns = {
        "id",
        "product_key",
        "manage_number",
        "item_number",
        "item_id",
        "item_detail_id",
    }
    if not required_columns <= set(columns):
        return 0

    from app.db.models import canonical_sales_order_item_product_key

    total = 0
    while True:
        rows = connection.execute(
            select(
                table.c.id,
                table.c.manage_number,
                table.c.item_number,
                table.c.item_id,
                table.c.item_detail_id,
            )
            .where(
                or_(
                    table.c.product_key.is_(None),
                    table.c.product_key == "",
                )
            )
            .order_by(table.c.id.asc())
            .limit(_PRODUCT_KEY_BACKFILL_BATCH_SIZE)
        ).mappings().all()
        if not rows:
            return total
        for row in rows:
            connection.execute(
                table.update()
                .where(table.c.id == row["id"])
                .values(
                    product_key=canonical_sales_order_item_product_key(
                        manage_number=row["manage_number"],
                        item_number=row["item_number"],
                        item_id=row["item_id"],
                        item_detail_id=row["item_detail_id"],
                    )
                )
            )
        total += len(rows)


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


def ensure_sales_parent_keys_before_create_all(bind=None) -> None:
    target_bind = bind or engine
    from app.db import models as model_module

    store_table = model_module.StoreModel.__table__
    store_identity_constraint = next(
        constraint
        for constraint in store_table.constraints
        if constraint.name == "uq_lt_store_id_owner"
    )
    expected_columns = tuple(
        column.name for column in store_identity_constraint.columns
    )

    with target_bind.begin() as connection:
        if store_table.name not in _table_names(connection):
            return
        available_columns = set(
            _column_info(connection, store_table.name)
        )
        missing_columns = [
            column_name
            for column_name in expected_columns
            if column_name not in available_columns
        ]
        if missing_columns:
            raise _compatibility_error(
                store_table.name,
                "sales parent key columns missing: "
                + ", ".join(missing_columns),
            )

        if any(
            tuple(existing.get("column_names") or ())
            == expected_columns
            for existing in _unique_constraint_info(
                connection,
                store_table.name,
            )
        ):
            return
        existing_indexes = _index_info(
            connection,
            store_table.name,
        )
        for existing in existing_indexes.values():
            if (
                bool(existing.get("unique"))
                and tuple(existing.get("column_names") or ())
                == expected_columns
            ):
                return
        conflicting = existing_indexes.get(
            store_identity_constraint.name
        )
        if conflicting is not None:
            raise _compatibility_error(
                store_table.name,
                "index uq_lt_store_id_owner has a conflicting definition",
            )

        _validate_unique_constraint_data(
            connection,
            store_table,
            store_identity_constraint,
        )
        quote = connection.dialect.identifier_preparer.quote
        columns_sql = ", ".join(
            quote(column_name)
            for column_name in expected_columns
        )
        try:
            connection.execute(
                text(
                    f"CREATE UNIQUE INDEX "
                    f"{quote(store_identity_constraint.name)} "
                    f"ON {quote(store_table.name)} ({columns_sql})"
                )
            )
        except SQLAlchemyError as exc:
            raise _compatibility_error(
                store_table.name,
                "sales parent key uq_lt_store_id_owner cannot install",
            ) from exc


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
        store_identity_constraint = next(
            constraint
            for constraint in model_module.StoreModel.__table__.constraints
            if constraint.name == "uq_lt_store_id_owner"
        )
        _ensure_constraint(
            connection,
            model_module.StoreModel.__table__,
            store_identity_constraint,
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
            model_module.SalesOrderSyncRunModel.__table__,
        )
        for sales_table in sales_tables:
            _ensure_table_layout(connection, sales_table)
        _backfill_sales_order_item_product_keys(
            connection,
            model_module.SalesOrderItemModel.__table__,
        )


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

    ensure_sales_parent_keys_before_create_all()
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
