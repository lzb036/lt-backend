from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal

import pytest
from sqlalchemy import (
    Column,
    ForeignKeyConstraint,
    Index,
    Integer,
    MetaData,
    String,
    Table,
    Text,
    UniqueConstraint,
    create_engine,
    event,
    inspect,
    select,
    text,
)
from sqlalchemy.dialects import mysql
from sqlalchemy.exc import IntegrityError, OperationalError
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.schema import CreateColumn

import app.db.models  # noqa: F401
import app.db.database as database_module
from app.db.database import Base
from app.db.models import (
    ProductSalesDailyModel,
    SalesAnalysisConversationModel,
    SalesAnalysisMessageModel,
    SalesItemAdjustmentModel,
    SalesOrderItemModel,
    SalesOrderModel,
    SalesSyncStateModel,
    StoreModel,
    UserAccountModel,
)


SALES_TABLES = (
    SalesOrderModel.__table__,
    SalesOrderItemModel.__table__,
    SalesItemAdjustmentModel.__table__,
    ProductSalesDailyModel.__table__,
    SalesSyncStateModel.__table__,
    SalesAnalysisConversationModel.__table__,
    SalesAnalysisMessageModel.__table__,
)


@pytest.fixture()
def sqlite_engine():
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    @event.listens_for(engine, "connect")
    def _set_sqlite_pragma(dbapi_connection, _):  # pragma: no cover - driver hook
        cursor = dbapi_connection.cursor()
        cursor.execute("PRAGMA foreign_keys=ON")
        cursor.close()
    try:
        Base.metadata.create_all(engine)
        yield engine
    finally:
        engine.dispose()


def seed_owner_store_and_order(session: Session) -> tuple[StoreModel, SalesOrderModel]:
    user = UserAccountModel(
        username="alice",
        display_name="Alice",
        password_salt_b64="salt",
        password_hash_b64="hash",
    )
    session.add(user)
    session.flush()
    store = StoreModel(
        owner_username="alice",
        store_code="demo-shop",
        store_name="Demo Shop",
    )
    session.add(store)
    session.flush()
    order = SalesOrderModel(
        owner_username="alice",
        store_id=store.id,
        order_number="1001",
        order_progress="100",
        order_status="sent",
        ordered_at=datetime(2026, 7, 16, 9, 0, 0),
        updated_at_remote=datetime(2026, 7, 16, 9, 5, 0),
        total_amount=Decimal("299.00"),
        currency="JPY",
        is_canceled=False,
        has_unresolved_adjustment=False,
        raw_order_json='{"orderNumber":"1001"}',
        last_synced_at=datetime(2026, 7, 16, 9, 6, 0),
    )
    session.add(order)
    session.flush()
    return store, order


def seed_owner_store_order_item(
    session: Session,
    *,
    owner_username: str = "alice",
    store_code: str = "demo-shop",
    order_number: str = "1001",
    item_detail_id: str = "detail-1",
) -> tuple[StoreModel, SalesOrderModel, SalesOrderItemModel]:
    user = session.get(UserAccountModel, owner_username)
    if user is None:
        user = UserAccountModel(
            username=owner_username,
            display_name=owner_username.title(),
            password_salt_b64="salt",
            password_hash_b64="hash",
        )
        session.add(user)
        session.flush()

    store = StoreModel(
        owner_username=owner_username,
        store_code=store_code,
        store_name=f"{owner_username}-{store_code}",
    )
    session.add(store)
    session.flush()

    order = SalesOrderModel(
        owner_username=owner_username,
        store_id=store.id,
        order_number=order_number,
        order_progress="100",
        order_status="sent",
        ordered_at=datetime(2026, 7, 16, 9, 0, 0),
        updated_at_remote=datetime(2026, 7, 16, 9, 5, 0),
        total_amount=Decimal("299.00"),
        currency="JPY",
        is_canceled=False,
        has_unresolved_adjustment=False,
        raw_order_json='{"orderNumber":"1001"}',
        last_synced_at=datetime(2026, 7, 16, 9, 6, 0),
    )
    session.add(order)
    session.flush()

    item = SalesOrderItemModel.from_service_payload(
        owner_username=owner_username,
        store_id=store.id,
        sales_order_id=order.id,
        order_number=order.order_number,
        item_detail_id=item_detail_id,
        manage_number="MN-1",
        item_number="ITEM-1",
        item_id="rakuten-item-1",
        sku_key="default",
        sku_json='{"sku":"default"}',
        item_name="Demo Item",
        unit_price=Decimal("149.50"),
        ordered_units=2,
        latest_units=2,
        canceled_units=0,
        refunded_units=0,
        returned_units=0,
        ordered_at=order.ordered_at,
    )
    session.add(item)
    session.flush()
    return store, order, item


def test_sales_tables_are_created(sqlite_engine):
    names = set(inspect(sqlite_engine).get_table_names())

    assert {
        "lt_sales_orders",
        "lt_sales_order_items",
        "lt_sales_item_adjustments",
        "lt_product_sales_daily",
        "lt_sales_sync_states",
        "lt_sales_analysis_conversations",
        "lt_sales_analysis_messages",
    } <= names


def test_store_exposes_composite_identity_for_tenant_foreign_keys(sqlite_engine):
    constraints = {
        constraint["name"]: tuple(constraint["column_names"])
        for constraint in inspect(sqlite_engine).get_unique_constraints("lt_stores")
    }

    assert constraints["uq_lt_store_id_owner"] == ("id", "owner_username")


def test_all_sales_foreign_keys_are_named_for_compatibility():
    unnamed = {
        table.name: [
            tuple(column.name for column in constraint.columns)
            for constraint in table.constraints
            if isinstance(constraint, ForeignKeyConstraint)
            and not constraint.name
        ]
        for table in SALES_TABLES
    }

    assert unnamed == {table.name: [] for table in SALES_TABLES}


def test_sales_order_item_parent_key_includes_order_number():
    parent_constraint = next(
        constraint
        for constraint in SalesOrderModel.__table__.constraints
        if constraint.name == "uq_lt_sales_order_id_owner_store_number"
    )
    child_constraint = next(
        constraint
        for constraint in SalesOrderItemModel.__table__.constraints
        if constraint.name == "fk_lt_sales_order_item_parent_order_number"
    )

    assert tuple(column.name for column in parent_constraint.columns) == (
        "id",
        "owner_username",
        "store_id",
        "order_number",
    )
    assert tuple(column.name for column in child_constraint.columns) == (
        "sales_order_id",
        "owner_username",
        "store_id",
        "order_number",
    )
    assert tuple(
        element.target_fullname for element in child_constraint.elements
    ) == (
        "lt_sales_orders.id",
        "lt_sales_orders.owner_username",
        "lt_sales_orders.store_id",
        "lt_sales_orders.order_number",
    )


def test_sales_order_enforces_store_and_order_number_uniqueness(sqlite_engine):
    with Session(sqlite_engine, future=True) as session:
        store, _ = seed_owner_store_and_order(session)
        store_id = store.id
        session.commit()

    with Session(sqlite_engine, future=True) as session:
        duplicate = SalesOrderModel(
            owner_username="alice",
            store_id=store_id,
            order_number="1001",
            order_progress="200",
            order_status="closed",
            ordered_at=datetime(2026, 7, 16, 10, 0, 0),
            updated_at_remote=datetime(2026, 7, 16, 10, 5, 0),
            total_amount=Decimal("199.00"),
            currency="JPY",
            raw_order_json='{"orderNumber":"1001","status":"closed"}',
            last_synced_at=datetime(2026, 7, 16, 10, 6, 0),
        )
        session.add(duplicate)

        with pytest.raises(IntegrityError):
            session.commit()


def test_sales_order_item_enforces_store_order_item_detail_uniqueness(sqlite_engine):
    with Session(sqlite_engine, future=True) as session:
        store, order = seed_owner_store_and_order(session)
        store_id = store.id
        order_id = order.id
        order_number = order.order_number
        ordered_at = order.ordered_at
        session.add(
            SalesOrderItemModel.from_service_payload(
                owner_username="alice",
                store_id=store_id,
                sales_order_id=order_id,
                order_number=order_number,
                item_detail_id="detail-1",
                manage_number="MN-1",
                item_number="ITEM-1",
                item_id="rakuten-item-1",
                sku_key="default",
                sku_json='{"sku":"default"}',
                item_name="Demo Item",
                unit_price=Decimal("149.50"),
                ordered_units=2,
                latest_units=2,
                canceled_units=0,
                refunded_units=0,
                returned_units=0,
                ordered_at=ordered_at,
            )
        )
        session.commit()

    with Session(sqlite_engine, future=True) as session:
        duplicate = SalesOrderItemModel.from_service_payload(
            owner_username="alice",
            store_id=store_id,
            sales_order_id=order_id,
            order_number=order_number,
            item_detail_id="detail-1",
            manage_number="MN-1",
            item_number="ITEM-1",
            item_id="rakuten-item-1",
            sku_key="alternate",
            sku_json='{"sku":"alternate"}',
            item_name="Demo Item",
            unit_price=Decimal("149.50"),
            ordered_units=1,
            latest_units=1,
            canceled_units=0,
            refunded_units=0,
            returned_units=0,
            ordered_at=datetime(2026, 7, 16, 10, 0, 0),
        )
        session.add(duplicate)

        with pytest.raises(IntegrityError):
            session.commit()


def test_sales_order_item_service_constructor_clamps_effective_units_to_zero(sqlite_engine):
    with Session(sqlite_engine, future=True) as session:
        store, order = seed_owner_store_and_order(session)
        store_id = store.id
        order_id = order.id
        order_number = order.order_number
        ordered_at = order.ordered_at
        item = SalesOrderItemModel.from_service_payload(
            owner_username="alice",
            store_id=store_id,
            sales_order_id=order_id,
            order_number=order_number,
            item_detail_id="detail-2",
            manage_number="MN-2",
            item_number="ITEM-2",
            item_id="rakuten-item-2",
            sku_key="default",
            sku_json='{"sku":"default"}',
            item_name="Adjustment Item",
            unit_price=Decimal("100.00"),
            ordered_units=2,
            latest_units=2,
            canceled_units=1,
            refunded_units=1,
            returned_units=0,
            ordered_at=ordered_at,
        )
        session.add(item)
        session.commit()

        persisted = session.scalar(
            select(SalesOrderItemModel).where(SalesOrderItemModel.id == item.id)
        )

    assert persisted is not None
    assert persisted.effective_units == 0
    assert persisted.effective_amount == Decimal("0")


def test_product_sales_daily_enforces_daily_product_sku_uniqueness(sqlite_engine):
    with Session(sqlite_engine, future=True) as session:
        store, _ = seed_owner_store_and_order(session)
        store_id = store.id
        session.add(
            ProductSalesDailyModel(
                owner_username="alice",
                store_id=store_id,
                sales_date=date(2026, 7, 16),
                manage_number="MN-DAILY",
                item_number="ITEM-DAILY",
                sku_key="default",
                item_name_snapshot="Daily Item",
                order_count=1,
                ordered_units=2,
                canceled_units=0,
                refunded_units=0,
                returned_units=0,
                effective_units=2,
                gross_sales_amount=Decimal("200.00"),
                effective_sales_amount=Decimal("200.00"),
            )
        )
        session.commit()

    with Session(sqlite_engine, future=True) as session:
        session.add(
            ProductSalesDailyModel(
                owner_username="alice",
                store_id=store_id,
                sales_date=date(2026, 7, 16),
                manage_number="MN-DAILY",
                item_number="ITEM-DAILY-2",
                sku_key="default",
                item_name_snapshot="Daily Item",
                order_count=2,
                ordered_units=3,
                canceled_units=0,
                refunded_units=0,
                returned_units=0,
                effective_units=3,
                gross_sales_amount=Decimal("300.00"),
                effective_sales_amount=Decimal("300.00"),
            )
        )

        with pytest.raises(IntegrityError):
            session.commit()


def test_sales_order_item_service_constructor_rejects_deductions_above_ordered_units():
    with pytest.raises(ValueError, match="cannot exceed ordered_units"):
        SalesOrderItemModel.from_service_payload(
            owner_username="alice",
            store_id=1,
            sales_order_id=1,
            order_number="1002",
            item_detail_id="detail-overflow",
            unit_price=Decimal("100.00"),
            ordered_units=2,
            latest_units=2,
            canceled_units=1,
            refunded_units=1,
            returned_units=1,
            ordered_at=datetime(2026, 7, 16, 11, 0, 0),
        )


def test_sales_order_item_service_constructor_does_not_reduce_effective_units_for_unresolved_refunds(sqlite_engine):
    with Session(sqlite_engine, future=True) as session:
        store, order = seed_owner_store_and_order(session)
        item = SalesOrderItemModel.from_service_payload(
            owner_username="alice",
            store_id=store.id,
            sales_order_id=order.id,
            order_number=order.order_number,
            item_detail_id="detail-unresolved",
            manage_number="MN-3",
            item_number="ITEM-3",
            item_id="rakuten-item-3",
            sku_key="default",
            sku_json='{"sku":"default"}',
            item_name="Unresolved Refund Item",
            unit_price=Decimal("100.00"),
            ordered_units=5,
            latest_units=5,
            canceled_units=0,
            refunded_units=0,
            returned_units=0,
            unresolved_refunded_units=2,
            ordered_at=order.ordered_at,
        )
        session.add(item)
        session.commit()

        persisted = session.scalar(
            select(SalesOrderItemModel).where(SalesOrderItemModel.id == item.id)
        )

    assert persisted is not None
    assert persisted.effective_units == 5
    assert persisted.effective_amount == Decimal("500")


def test_sales_order_item_service_constructor_deducts_return_refund_once():
    common_payload = {
        "owner_username": "alice",
        "store_id": 1,
        "sales_order_id": 1,
        "order_number": "1003",
        "unit_price": Decimal("100.00"),
        "ordered_units": 5,
        "latest_units": 5,
        "canceled_units": 0,
        "refunded_units": 2,
        "returned_units": 2,
        "ordered_at": datetime(2026, 7, 16, 11, 30, 0),
    }

    same_return_refund = SalesOrderItemModel.from_service_payload(
        **common_payload,
        item_detail_id="detail-return-refund",
        return_refund_units=2,
    )
    independent_refund_and_return = SalesOrderItemModel.from_service_payload(
        **common_payload,
        item_detail_id="detail-independent",
        return_refund_units=0,
    )

    assert same_return_refund.refunded_units == 0
    assert same_return_refund.returned_units == 2
    assert same_return_refund.effective_units == 3
    assert independent_refund_and_return.refunded_units == 2
    assert independent_refund_and_return.returned_units == 2
    assert independent_refund_and_return.effective_units == 1


def test_sales_order_rejects_cross_owner_store_reference(sqlite_engine):
    with Session(sqlite_engine, future=True) as session:
        alice_store, _ = seed_owner_store_and_order(session)
        session.add(
            UserAccountModel(
                username="bob",
                display_name="Bob",
                password_salt_b64="salt",
                password_hash_b64="hash",
            )
        )
        session.flush()

        session.add(
            SalesOrderModel(
                owner_username="bob",
                store_id=alice_store.id,
                order_number="cross-owner-order",
                order_progress="100",
                order_status="sent",
                ordered_at=datetime(2026, 7, 16, 12, 0, 0),
                total_amount=Decimal("100.00"),
                currency="JPY",
                raw_order_json="{}",
                last_synced_at=datetime(2026, 7, 16, 12, 1, 0),
            )
        )

        with pytest.raises(IntegrityError):
            session.commit()


def test_product_sales_daily_rejects_cross_owner_store_reference(sqlite_engine):
    with Session(sqlite_engine, future=True) as session:
        alice_store, _ = seed_owner_store_and_order(session)
        session.add(
            UserAccountModel(
                username="bob",
                display_name="Bob",
                password_salt_b64="salt",
                password_hash_b64="hash",
            )
        )
        session.flush()

        session.add(
            ProductSalesDailyModel(
                owner_username="bob",
                store_id=alice_store.id,
                sales_date=date(2026, 7, 16),
                manage_number="MN-CROSS",
                item_number="ITEM-CROSS",
                sku_key="default",
                item_name_snapshot="Cross Owner Daily",
                order_count=1,
                ordered_units=1,
                effective_units=1,
                gross_sales_amount=Decimal("100.00"),
                effective_sales_amount=Decimal("100.00"),
            )
        )

        with pytest.raises(IntegrityError):
            session.commit()


def test_sales_sync_state_rejects_cross_owner_store_reference(sqlite_engine):
    with Session(sqlite_engine, future=True) as session:
        alice_store, _ = seed_owner_store_and_order(session)
        session.add(
            UserAccountModel(
                username="bob",
                display_name="Bob",
                password_salt_b64="salt",
                password_hash_b64="hash",
            )
        )
        session.flush()

        session.add(
            SalesSyncStateModel(
                owner_username="bob",
                store_id=alice_store.id,
                initial_sync_completed=False,
                sync_status="idle",
            )
        )

        with pytest.raises(IntegrityError):
            session.commit()


def test_sales_order_item_rejects_cross_owner_or_store_order_reference(sqlite_engine):
    with Session(sqlite_engine, future=True) as session:
        alice_store, alice_order, _ = seed_owner_store_order_item(session)
        bob_user = UserAccountModel(
            username="bob",
            display_name="Bob",
            password_salt_b64="salt",
            password_hash_b64="hash",
        )
        bob_store = StoreModel(
            owner_username="bob",
            store_code="bob-shop",
            store_name="Bob Shop",
        )
        session.add(bob_user)
        session.flush()
        session.add(bob_store)
        session.flush()

        session.add(
            SalesOrderItemModel.from_service_payload(
                owner_username="bob",
                store_id=bob_store.id,
                sales_order_id=alice_order.id,
                order_number=alice_order.order_number,
                item_detail_id="detail-cross-order",
                manage_number="MN-X",
                item_number="ITEM-X",
                item_id="rakuten-item-x",
                sku_key="default",
                sku_json='{"sku":"default"}',
                item_name="Cross Order Item",
                unit_price=Decimal("10.00"),
                ordered_units=1,
                latest_units=1,
                canceled_units=0,
                refunded_units=0,
                returned_units=0,
                ordered_at=alice_order.ordered_at,
            )
        )

        with pytest.raises(IntegrityError):
            session.commit()


def test_sales_order_item_rejects_mismatched_parent_order_number(sqlite_engine):
    with Session(sqlite_engine, future=True) as session:
        store, order_a = seed_owner_store_and_order(session)
        order_b = SalesOrderModel(
            owner_username="alice",
            store_id=store.id,
            order_number="ORDER-B",
            order_progress="100",
            order_status="sent",
            ordered_at=datetime(2026, 7, 16, 13, 0, 0),
            total_amount=Decimal("100.00"),
            currency="JPY",
            raw_order_json='{"orderNumber":"ORDER-B"}',
            last_synced_at=datetime(2026, 7, 16, 13, 1, 0),
        )
        session.add(order_b)
        session.flush()

        session.add(
            SalesOrderItemModel.from_service_payload(
                owner_username="alice",
                store_id=store.id,
                sales_order_id=order_a.id,
                order_number=order_b.order_number,
                item_detail_id="detail-wrong-parent-number",
                manage_number="MN-WRONG",
                item_number="ITEM-WRONG",
                item_id="rakuten-item-wrong",
                sku_key="default",
                sku_json='{"sku":"default"}',
                item_name="Wrong Parent Number",
                unit_price=Decimal("10.00"),
                ordered_units=1,
                latest_units=1,
                ordered_at=order_a.ordered_at,
            )
        )

        with pytest.raises(IntegrityError):
            session.commit()


def test_sales_adjustment_rejects_cross_owner_or_store_item_reference(sqlite_engine):
    with Session(sqlite_engine, future=True) as session:
        _, _, alice_item = seed_owner_store_order_item(session)
        bob_user = UserAccountModel(
            username="bob",
            display_name="Bob",
            password_salt_b64="salt",
            password_hash_b64="hash",
        )
        bob_store = StoreModel(
            owner_username="bob",
            store_code="bob-adjustment-shop",
            store_name="Bob Adjustment Shop",
        )
        session.add(bob_user)
        session.flush()
        session.add(bob_store)
        session.flush()

        session.add(
            SalesItemAdjustmentModel(
                owner_username="bob",
                store_id=bob_store.id,
                sales_order_item_id=alice_item.id,
                adjustment_type="refund",
                units=1,
                amount=Decimal("10.00"),
                source="test",
                status="confirmed",
                reason="cross store mismatch",
                raw_payload_json="{}",
            )
        )

        with pytest.raises(IntegrityError):
            session.commit()


def test_sales_analysis_message_rejects_cross_owner_conversation_reference(sqlite_engine):
    with Session(sqlite_engine, future=True) as session:
        session.add_all(
            [
                UserAccountModel(
                    username="alice",
                    display_name="Alice",
                    password_salt_b64="salt",
                    password_hash_b64="hash",
                ),
                UserAccountModel(
                    username="bob",
                    display_name="Bob",
                    password_salt_b64="salt",
                    password_hash_b64="hash",
                ),
            ]
        )
        session.flush()

        conversation = SalesAnalysisConversationModel(
            owner_username="alice",
            title="Alice Analysis",
            store_scope_json="[1]",
        )
        session.add(conversation)
        session.flush()

        session.add(
            SalesAnalysisMessageModel(
                conversation_id=conversation.id,
                owner_username="bob",
                question_text="Who owns this?",
                answer_text="Should fail",
                tool_name="overview",
                tool_arguments_json="{}",
                result_summary_json="{}",
                model_name="demo",
                store_scope_json="[1]",
                statistics_window_json="{}",
            )
        )

        with pytest.raises(IntegrityError):
            session.commit()


def test_ensure_table_layout_fails_for_required_no_default_column(sqlite_engine):
    partial_engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    try:
        with partial_engine.begin() as connection:
            connection.execute(
                text(
                    """
                    CREATE TABLE lt_sales_analysis_messages (
                        id INTEGER PRIMARY KEY,
                        owner_username VARCHAR(255) NOT NULL,
                        question_text TEXT NOT NULL DEFAULT ''
                    )
                    """
                )
            )
            connection.execute(
                text(
                    """
                    INSERT INTO lt_sales_analysis_messages (id, owner_username, question_text)
                    VALUES (1, 'alice', 'legacy row')
                    """
                )
            )

            with pytest.raises(
                RuntimeError,
                match=r"lt_sales_analysis_messages.*conversation_id.*server default",
            ):
                database_module._ensure_table_layout(
                    connection,
                    SalesAnalysisMessageModel.__table__,
                )
    finally:
        partial_engine.dispose()


def test_ensure_table_layout_adds_safe_defaulted_columns_to_partial_populated_table():
    target_metadata = MetaData()
    target_table = Table(
        "lt_compat_safe_columns",
        target_metadata,
        Column("id", Integer, primary_key=True),
        Column("status", String(32), nullable=False, server_default="pending"),
        Column("attempt_count", Integer, nullable=False, server_default="0"),
        Column("notes", Text, nullable=True),
    )
    partial_engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    try:
        with partial_engine.begin() as connection:
            connection.execute(
                text(
                    """
                    CREATE TABLE lt_compat_safe_columns (
                        id INTEGER PRIMARY KEY
                    )
                    """
                )
            )
            connection.execute(
                text(
                    """
                    INSERT INTO lt_compat_safe_columns (id)
                    VALUES (1)
                    """
                )
            )

            result = database_module._ensure_table_layout(
                connection,
                target_table,
            )

        columns = {
            column["name"]
            for column in inspect(partial_engine).get_columns("lt_compat_safe_columns")
        }
        with partial_engine.connect() as connection:
            row = connection.execute(
                text(
                    """
                    SELECT status, attempt_count, notes
                    FROM lt_compat_safe_columns
                    WHERE id = 1
                    """
                )
            ).one()

        assert {"status", "attempt_count", "notes"} <= columns
        assert row == ("pending", 0, None)
        assert set(result["added_columns"]) == {"status", "attempt_count", "notes"}
    finally:
        partial_engine.dispose()


def _create_complete_legacy_conversation_engine(
    *,
    include_owner_fk: bool,
    owner_fk_name: str | None = None,
    owner_fk_ondelete: str | None = "CASCADE",
    owner_fk_onupdate: str | None = None,
):
    legacy_engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    if include_owner_fk:
        constraint_name = (
            f"CONSTRAINT {owner_fk_name} "
            if owner_fk_name
            else ""
        )
        ondelete_sql = (
            f" ON DELETE {owner_fk_ondelete}"
            if owner_fk_ondelete
            else ""
        )
        onupdate_sql = (
            f" ON UPDATE {owner_fk_onupdate}"
            if owner_fk_onupdate
            else ""
        )
        owner_fk_sql = (
            f", {constraint_name}FOREIGN KEY (owner_username) "
            "REFERENCES lt_user_accounts(username)"
            f"{ondelete_sql}{onupdate_sql}"
        )
    else:
        owner_fk_sql = ""
    with legacy_engine.begin() as connection:
        connection.execute(
            text(
                """
                CREATE TABLE lt_user_accounts (
                    username VARCHAR(255) PRIMARY KEY
                )
                """
            )
        )
        connection.execute(
            text(
                f"""
                CREATE TABLE lt_sales_analysis_conversations (
                    id INTEGER PRIMARY KEY,
                    owner_username VARCHAR(255) NOT NULL,
                    title VARCHAR(255) NOT NULL DEFAULT 'Legacy',
                    store_scope_json TEXT NOT NULL,
                    last_message_at DATETIME NULL,
                    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    CONSTRAINT uq_lt_sales_analysis_conversation_id_owner
                        UNIQUE (id, owner_username)
                    {owner_fk_sql}
                )
                """
            )
        )
        connection.execute(
            text(
                """
                CREATE INDEX ix_lt_sales_analysis_conversation_owner_updated
                ON lt_sales_analysis_conversations (owner_username, updated_at)
                """
            )
        )
        connection.execute(
            text(
                """
                INSERT INTO lt_user_accounts (username)
                VALUES ('alice')
                """
            )
        )
        connection.execute(
            text(
                """
                INSERT INTO lt_sales_analysis_conversations (
                    id,
                    owner_username,
                    title,
                    store_scope_json
                )
                VALUES (1, 'alice', 'Legacy', '[]')
                """
            )
        )
    return legacy_engine


def test_complete_legacy_conversation_owner_fk_is_detected():
    legacy_engine = _create_complete_legacy_conversation_engine(
        include_owner_fk=True
    )
    try:
        owner_constraint = next(
            constraint
            for constraint in SalesAnalysisConversationModel.__table__.constraints
            if constraint.name
            == "fk_lt_sales_analysis_conversation_owner_user"
        )
        with legacy_engine.begin() as connection:
            assert database_module._constraint_is_present(
                connection,
                SalesAnalysisConversationModel.__table__,
                owner_constraint,
            )
            result = database_module._ensure_table_layout(
                connection,
                SalesAnalysisConversationModel.__table__,
            )

        assert result["added_constraints"] == []
    finally:
        legacy_engine.dispose()


def test_complete_legacy_named_equivalent_conversation_owner_fk_is_detected():
    legacy_engine = _create_complete_legacy_conversation_engine(
        include_owner_fk=True,
        owner_fk_name="fk_legacy_conversation_owner",
    )
    try:
        with legacy_engine.begin() as connection:
            result = database_module._ensure_table_layout(
                connection,
                SalesAnalysisConversationModel.__table__,
            )

        assert result["added_constraints"] == []
    finally:
        legacy_engine.dispose()


def test_complete_legacy_conversation_owner_fk_action_conflict_fails():
    legacy_engine = _create_complete_legacy_conversation_engine(
        include_owner_fk=True,
        owner_fk_name="fk_legacy_conversation_owner_no_action",
        owner_fk_ondelete=None,
    )
    try:
        with legacy_engine.begin() as connection:
            with pytest.raises(
                RuntimeError,
                match=(
                    r"lt_sales_analysis_conversations.*"
                    r"fk_lt_sales_analysis_conversation_owner_user.*"
                    r"fk_legacy_conversation_owner_no_action.*"
                    r"referential action"
                ),
            ):
                database_module._ensure_table_layout(
                    connection,
                    SalesAnalysisConversationModel.__table__,
                )
    finally:
        legacy_engine.dispose()


def test_complete_legacy_conversation_owner_fk_onupdate_conflict_fails():
    legacy_engine = _create_complete_legacy_conversation_engine(
        include_owner_fk=True,
        owner_fk_name="fk_legacy_conversation_owner_onupdate",
        owner_fk_onupdate="CASCADE",
    )
    try:
        with legacy_engine.begin() as connection:
            with pytest.raises(
                RuntimeError,
                match=(
                    r"lt_sales_analysis_conversations.*"
                    r"fk_lt_sales_analysis_conversation_owner_user.*"
                    r"fk_legacy_conversation_owner_onupdate.*"
                    r"referential action"
                ),
            ):
                database_module._ensure_table_layout(
                    connection,
                    SalesAnalysisConversationModel.__table__,
                )
    finally:
        legacy_engine.dispose()


def test_complete_legacy_conversation_missing_owner_fk_fails_clearly():
    legacy_engine = _create_complete_legacy_conversation_engine(
        include_owner_fk=False
    )
    try:
        with legacy_engine.begin() as connection:
            with pytest.raises(
                RuntimeError,
                match=(
                    r"lt_sales_analysis_conversations.*"
                    r"fk_lt_sales_analysis_conversation_owner_user.*"
                    r"cannot install"
                ),
            ):
                database_module._ensure_table_layout(
                    connection,
                    SalesAnalysisConversationModel.__table__,
                )
    finally:
        legacy_engine.dispose()


def _compat_default_action_foreign_key_tables() -> tuple[Table, Table]:
    metadata = MetaData()
    parent = Table(
        "lt_compat_default_action_parent",
        metadata,
        Column("id", Integer, primary_key=True),
    )
    child = Table(
        "lt_compat_default_action_child",
        metadata,
        Column("id", Integer, primary_key=True),
        Column("parent_id", Integer, nullable=False),
        ForeignKeyConstraint(
            ["parent_id"],
            ["lt_compat_default_action_parent.id"],
            name="fk_lt_compat_default_action_child_parent",
        ),
    )
    return parent, child


def _reflected_default_action_foreign_key(
    action: str | None,
) -> dict:
    return {
        "name": "fk_legacy_default_action_child_parent",
        "constrained_columns": ["parent_id"],
        "referred_schema": None,
        "referred_table": "lt_compat_default_action_parent",
        "referred_columns": ["id"],
        "options": {"ondelete": action} if action else {},
    }


def test_foreign_key_restrict_matches_required_omitted_action(monkeypatch):
    assert {
        database_module._normalize_referential_action(None),
        database_module._normalize_referential_action("NO ACTION"),
        database_module._normalize_referential_action("RESTRICT"),
    } == {"RESTRICT"}

    _, child = _compat_default_action_foreign_key_tables()
    constraint = next(
        constraint
        for constraint in child.constraints
        if constraint.name
        == "fk_lt_compat_default_action_child_parent"
    )
    for action in (None, "NO ACTION", "RESTRICT"):
        assert database_module._foreign_key_constraint_matches(
            _reflected_default_action_foreign_key(action),
            constraint,
        )

    monkeypatch.setattr(
        database_module,
        "_foreign_key_info",
        lambda connection, table_name: [
            _reflected_default_action_foreign_key("RESTRICT")
        ],
    )
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    try:
        with engine.connect() as connection:
            assert database_module._constraint_is_present(
                connection,
                child,
                constraint,
            )
    finally:
        engine.dispose()


def test_foreign_key_cascade_conflicts_with_required_omitted_action(
    monkeypatch,
):
    _, child = _compat_default_action_foreign_key_tables()
    constraint = next(
        constraint
        for constraint in child.constraints
        if constraint.name
        == "fk_lt_compat_default_action_child_parent"
    )
    reflected_cascade = _reflected_default_action_foreign_key(
        "CASCADE"
    )
    assert not database_module._foreign_key_constraint_matches(
        reflected_cascade,
        constraint,
    )
    monkeypatch.setattr(
        database_module,
        "_foreign_key_info",
        lambda connection, table_name: [reflected_cascade],
    )
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    try:
        with engine.connect() as connection:
            with pytest.raises(
                RuntimeError,
                match=(
                    r"lt_compat_default_action_child.*"
                    r"fk_lt_compat_default_action_child_parent.*"
                    r"fk_legacy_default_action_child_parent.*"
                    r"referential action"
                ),
            ):
                database_module._constraint_is_present(
                    connection,
                    child,
                    constraint,
                )
    finally:
        engine.dispose()


def test_complete_legacy_order_item_missing_order_number_fk_fails_clearly():
    legacy_metadata = MetaData()
    for table in (
        UserAccountModel.__table__,
        StoreModel.__table__,
        SalesOrderModel.__table__,
        SalesOrderItemModel.__table__,
    ):
        table.to_metadata(legacy_metadata)

    legacy_item = legacy_metadata.tables["lt_sales_order_items"]
    order_number_constraint = next(
        constraint
        for constraint in legacy_item.constraints
        if constraint.name == "fk_lt_sales_order_item_parent_order_number"
    )
    legacy_item.constraints.remove(order_number_constraint)

    legacy_engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    try:
        legacy_metadata.create_all(legacy_engine)
        reflected_foreign_keys = inspect(legacy_engine).get_foreign_keys(
            "lt_sales_order_items"
        )
        assert not any(
            tuple(item["constrained_columns"])
            == (
                "sales_order_id",
                "owner_username",
                "store_id",
                "order_number",
            )
            for item in reflected_foreign_keys
        )

        with legacy_engine.begin() as connection:
            with pytest.raises(
                RuntimeError,
                match=(
                    r"lt_sales_order_items.*"
                    r"fk_lt_sales_order_item_parent_order_number.*"
                    r"cannot install"
                ),
            ):
                database_module._ensure_table_layout(
                    connection,
                    SalesOrderItemModel.__table__,
                )
    finally:
        legacy_engine.dispose()


def test_ensure_table_layout_fails_for_named_index_with_wrong_ordered_columns():
    target_metadata = MetaData()
    target_table = Table(
        "lt_compat_wrong_index_columns",
        target_metadata,
        Column("id", Integer, primary_key=True),
        Column("owner_username", String(255), nullable=False),
        Column("status", String(32), nullable=False),
        Index(
            "ix_lt_compat_wrong_index_columns_owner_status",
            "owner_username",
            "status",
        ),
    )
    legacy_engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    try:
        with legacy_engine.begin() as connection:
            connection.execute(
                text(
                    """
                    CREATE TABLE lt_compat_wrong_index_columns (
                        id INTEGER PRIMARY KEY,
                        owner_username VARCHAR(255) NOT NULL,
                        status VARCHAR(32) NOT NULL
                    )
                    """
                )
            )
            connection.execute(
                text(
                    """
                    CREATE INDEX ix_lt_compat_wrong_index_columns_owner_status
                    ON lt_compat_wrong_index_columns (status, owner_username)
                    """
                )
            )

            with pytest.raises(
                RuntimeError,
                match=(
                    r"lt_compat_wrong_index_columns.*"
                    r"ix_lt_compat_wrong_index_columns_owner_status.*"
                    r"conflicting definition"
                ),
            ):
                database_module._ensure_table_layout(
                    connection,
                    target_table,
                )
    finally:
        legacy_engine.dispose()


def test_ensure_table_layout_fails_for_named_index_with_wrong_uniqueness():
    target_metadata = MetaData()
    target_table = Table(
        "lt_compat_wrong_index_unique",
        target_metadata,
        Column("id", Integer, primary_key=True),
        Column("remote_key", String(64), nullable=False),
        Index(
            "ix_lt_compat_wrong_index_unique_remote_key",
            "remote_key",
            unique=True,
        ),
    )
    legacy_engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    try:
        with legacy_engine.begin() as connection:
            connection.execute(
                text(
                    """
                    CREATE TABLE lt_compat_wrong_index_unique (
                        id INTEGER PRIMARY KEY,
                        remote_key VARCHAR(64) NOT NULL
                    )
                    """
                )
            )
            connection.execute(
                text(
                    """
                    CREATE INDEX ix_lt_compat_wrong_index_unique_remote_key
                    ON lt_compat_wrong_index_unique (remote_key)
                    """
                )
            )

            with pytest.raises(
                RuntimeError,
                match=(
                    r"lt_compat_wrong_index_unique.*"
                    r"ix_lt_compat_wrong_index_unique_remote_key.*"
                    r"conflicting definition"
                ),
            ):
                database_module._ensure_table_layout(
                    connection,
                    target_table,
                )
    finally:
        legacy_engine.dispose()


def test_index_definition_matching_uses_mysql_reflected_options_only():
    target_metadata = MetaData()
    target_table = Table(
        "lt_compat_mysql_index_options",
        target_metadata,
        Column("code", String(255), nullable=False),
    )
    target_index = Index(
        "ix_lt_compat_mysql_index_options_code",
        target_table.c.code,
        mysql_length=32,
        mysql_using="hash",
    )
    exact_existing = {
        "name": target_index.name,
        "column_names": ["code"],
        "unique": False,
        "dialect_options": {
            "mysql_length": {"code": 32},
        },
    }
    wrong_existing = {
        **exact_existing,
        "dialect_options": {
            "mysql_length": {"code": 16},
        },
    }

    assert database_module._index_definition_matches(
        exact_existing,
        target_index,
        "mysql",
    )
    assert not database_module._index_definition_matches(
        wrong_existing,
        target_index,
        "mysql",
    )


def test_ensure_table_layout_is_idempotent_for_complete_sales_tables(sqlite_engine):
    with sqlite_engine.begin() as connection:
        results = [
            database_module._ensure_table_layout(connection, table)
            for table in SALES_TABLES
        ]

    assert all(
        not result[key]
        for result in results
        for key in (
            "created_table",
            "added_columns",
            "skipped_columns",
            "added_constraints",
            "added_indexes",
        )
    )


def test_mysql_timestamp_default_is_safe_to_add_but_sqlite_is_not():
    created_at = SalesOrderModel.__table__.c.created_at

    assert database_module._has_safe_server_default(created_at, "mysql")
    assert not database_module._has_safe_server_default(created_at, "sqlite")


def test_ensure_table_layout_fails_when_primary_key_is_missing():
    target_metadata = MetaData()
    target_table = Table(
        "lt_compat_missing_pk",
        target_metadata,
        Column("id", Integer, primary_key=True),
        Column("payload", String(32), nullable=False, server_default=""),
    )
    partial_engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    try:
        with partial_engine.begin() as connection:
            connection.execute(
                text(
                    """
                    CREATE TABLE lt_compat_missing_pk (
                        id INTEGER NOT NULL,
                        payload VARCHAR(32) NOT NULL DEFAULT ''
                    )
                    """
                )
            )

            with pytest.raises(RuntimeError, match=r"lt_compat_missing_pk.*primary key"):
                database_module._ensure_table_layout(connection, target_table)
    finally:
        partial_engine.dispose()


def test_ensure_table_layout_fails_on_null_required_data_without_backfill():
    target_metadata = MetaData()
    target_table = Table(
        "lt_compat_null_required",
        target_metadata,
        Column("id", Integer, primary_key=True),
        Column("required_value", String(32), nullable=False),
    )
    partial_engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    try:
        with partial_engine.begin() as connection:
            connection.execute(
                text(
                    """
                    CREATE TABLE lt_compat_null_required (
                        id INTEGER PRIMARY KEY,
                        required_value VARCHAR(32) NULL
                    )
                    """
                )
            )
            connection.execute(
                text(
                    """
                    INSERT INTO lt_compat_null_required (id, required_value)
                    VALUES (1, NULL)
                    """
                )
            )

            with pytest.raises(
                RuntimeError,
                match=r"lt_compat_null_required.*required_value.*NULL",
            ):
                database_module._ensure_table_layout(connection, target_table)
    finally:
        partial_engine.dispose()


def test_ensure_table_layout_fails_on_duplicate_unique_constraint_data():
    target_metadata = MetaData()
    target_table = Table(
        "lt_compat_duplicate_unique",
        target_metadata,
        Column("id", Integer, primary_key=True),
        Column("owner_username", String(255), nullable=False),
        Column("remote_key", String(64), nullable=False),
        UniqueConstraint(
            "owner_username",
            "remote_key",
            name="uq_lt_compat_duplicate_unique_owner_key",
        ),
    )
    partial_engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    try:
        with partial_engine.begin() as connection:
            connection.execute(
                text(
                    """
                    CREATE TABLE lt_compat_duplicate_unique (
                        id INTEGER PRIMARY KEY,
                        owner_username VARCHAR(255) NOT NULL,
                        remote_key VARCHAR(64) NOT NULL
                    )
                    """
                )
            )
            connection.execute(
                text(
                    """
                    INSERT INTO lt_compat_duplicate_unique (id, owner_username, remote_key)
                    VALUES
                        (1, 'alice', 'same'),
                        (2, 'alice', 'same')
                    """
                )
            )

            with pytest.raises(
                RuntimeError,
                match=r"lt_compat_duplicate_unique.*uq_lt_compat_duplicate_unique_owner_key.*duplicate",
            ):
                database_module._ensure_table_layout(connection, target_table)
    finally:
        partial_engine.dispose()


def _compat_parent_child_tables() -> tuple[MetaData, Table, Table]:
    metadata = MetaData()
    parent = Table(
        "lt_compat_parent",
        metadata,
        Column("id", Integer, primary_key=True),
        Column("owner_username", String(255), nullable=False),
        UniqueConstraint(
            "id",
            "owner_username",
            name="uq_lt_compat_parent_id_owner",
        ),
    )
    child = Table(
        "lt_compat_child",
        metadata,
        Column("id", Integer, primary_key=True),
        Column("parent_id", Integer, nullable=False),
        Column("owner_username", String(255), nullable=False),
        ForeignKeyConstraint(
            ["parent_id", "owner_username"],
            ["lt_compat_parent.id", "lt_compat_parent.owner_username"],
            name="fk_lt_compat_child_parent_owner",
        ),
    )
    return metadata, parent, child


def test_ensure_table_layout_fails_on_conflicting_foreign_key_data():
    _, parent, child = _compat_parent_child_tables()
    partial_engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    try:
        with partial_engine.begin() as connection:
            parent.create(connection)
            connection.execute(
                text(
                    """
                    CREATE TABLE lt_compat_child (
                        id INTEGER PRIMARY KEY,
                        parent_id INTEGER NOT NULL,
                        owner_username VARCHAR(255) NOT NULL
                    )
                    """
                )
            )
            connection.execute(
                parent.insert().values(id=1, owner_username="alice")
            )
            connection.execute(
                text(
                    """
                    INSERT INTO lt_compat_child (id, parent_id, owner_username)
                    VALUES (1, 1, 'bob')
                    """
                )
            )

            with pytest.raises(
                RuntimeError,
                match=r"lt_compat_child.*fk_lt_compat_child_parent_owner.*conflicting",
            ):
                database_module._ensure_table_layout(connection, child)
    finally:
        partial_engine.dispose()


def test_ensure_table_layout_fails_when_constraint_cannot_be_installed():
    _, parent, child = _compat_parent_child_tables()
    partial_engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    try:
        with partial_engine.begin() as connection:
            parent.create(connection)
            connection.execute(
                text(
                    """
                    CREATE TABLE lt_compat_child (
                        id INTEGER PRIMARY KEY,
                        parent_id INTEGER NOT NULL,
                        owner_username VARCHAR(255) NOT NULL
                    )
                    """
                )
            )
            connection.execute(
                parent.insert().values(id=1, owner_username="alice")
            )
            connection.execute(
                text(
                    """
                    INSERT INTO lt_compat_child (id, parent_id, owner_username)
                    VALUES (1, 1, 'alice')
                    """
                )
            )

            with pytest.raises(
                RuntimeError,
                match=r"lt_compat_child.*fk_lt_compat_child_parent_owner.*cannot install",
            ):
                database_module._ensure_table_layout(connection, child)
    finally:
        partial_engine.dispose()


def test_mysql_root_sales_foreign_key_ddl_is_composite():
    constraint = next(
        constraint
        for constraint in SalesOrderModel.__table__.constraints
        if constraint.name == "fk_lt_sales_order_store_owner"
    )

    ddl = database_module._compile_add_constraint(
        constraint,
        mysql.dialect(),
    )

    assert "FOREIGN KEY(store_id, owner_username)" in ddl
    assert "REFERENCES lt_stores (id, owner_username)" in ddl


def test_mysql_longtext_column_ddl_does_not_require_a_server_default():
    ddl = str(
        CreateColumn(SalesOrderModel.__table__.c.raw_order_json).compile(
            dialect=mysql.dialect()
        )
    )

    assert "LONGTEXT NOT NULL" in ddl
    assert "DEFAULT" not in ddl


def _legacy_store_schema_engine():
    legacy_engine = create_engine(
        "sqlite+pysqlite:///:memory:",
        future=True,
    )

    @event.listens_for(legacy_engine, "connect")
    def _set_sqlite_pragma(dbapi_connection, _):
        cursor = dbapi_connection.cursor()
        cursor.execute("PRAGMA foreign_keys=ON")
        cursor.close()

    with legacy_engine.begin() as connection:
        connection.execute(
            text(
                """
                CREATE TABLE lt_user_accounts (
                    username VARCHAR(255) PRIMARY KEY
                )
                """
            )
        )
        connection.execute(
            text(
                """
                CREATE TABLE lt_stores (
                    id INTEGER PRIMARY KEY,
                    owner_username VARCHAR(255) NOT NULL
                )
                """
            )
        )
        connection.execute(
            text(
                """
                INSERT INTO lt_user_accounts (username)
                VALUES ('alice')
                """
            )
        )
        connection.execute(
            text(
                """
                INSERT INTO lt_stores (id, owner_username)
                VALUES (1, 'alice')
                """
            )
        )
    return legacy_engine


def test_existing_store_schema_needs_parent_preflight_before_sales_child_use():
    without_preflight = _legacy_store_schema_engine()
    try:
        with pytest.raises(OperationalError, match="foreign key mismatch"):
            with without_preflight.begin() as connection:
                SalesSyncStateModel.__table__.create(connection)
                connection.execute(
                    SalesSyncStateModel.__table__.insert().values(
                        store_id=1,
                        owner_username="alice",
                    )
                )
    finally:
        without_preflight.dispose()

    with_preflight = _legacy_store_schema_engine()
    try:
        database_module.ensure_sales_parent_keys_before_create_all(
            with_preflight
        )
        with with_preflight.begin() as connection:
            SalesSyncStateModel.__table__.create(connection)
            connection.execute(
                SalesSyncStateModel.__table__.insert().values(
                    store_id=1,
                    owner_username="alice",
                )
            )
        with Session(with_preflight) as session:
            state = session.get(SalesSyncStateModel, 1)
        assert state is not None
        assert state.owner_username == "alice"
    finally:
        with_preflight.dispose()


def test_init_database_preflights_sales_parent_before_create_all(
    monkeypatch,
):
    from app.services import (
        crawler_service,
        sensitive_word_service,
        user_service,
    )

    events: list[str] = []

    class _Session:
        def commit(self):
            events.append("seed")

        def rollback(self):
            raise AssertionError("rollback must not be needed")

        def close(self):
            pass

    monkeypatch.setattr(
        database_module.settings,
        "database_auto_create",
        False,
    )
    monkeypatch.setattr(
        database_module,
        "ensure_sales_parent_keys_before_create_all",
        lambda *_args, **_kwargs: events.append("parent-preflight"),
    )
    monkeypatch.setattr(
        database_module.Base.metadata,
        "create_all",
        lambda **_kwargs: events.append("create-all"),
    )
    monkeypatch.setattr(
        database_module,
        "ensure_schema_compatibility",
        lambda: events.append("compatibility"),
    )
    monkeypatch.setattr(
        user_service,
        "ensure_initial_superadmin",
        lambda: None,
    )
    monkeypatch.setattr(
        crawler_service,
        "ensure_default_roles",
        lambda: None,
    )
    monkeypatch.setattr(
        sensitive_word_service,
        "seed_default_sensitive_words",
        lambda _session: None,
    )
    monkeypatch.setattr(
        database_module,
        "SessionLocal",
        _Session,
    )

    database_module.init_database()

    assert events[:3] == [
        "parent-preflight",
        "create-all",
        "compatibility",
    ]


def test_init_database_real_create_all_sees_existing_store_parent_key(
    monkeypatch,
):
    from app.services import (
        crawler_service,
        sensitive_word_service,
        user_service,
    )

    legacy_engine = _legacy_store_schema_engine()
    local_session_factory = sessionmaker(
        bind=legacy_engine,
        autoflush=False,
        expire_on_commit=False,
        future=True,
    )
    parent_key_seen_before_child_create: list[bool] = []

    def record_parent_key(_target, connection, **_kwargs):
        indexes = inspect(connection).get_indexes("lt_stores")
        parent_key_seen_before_child_create.append(
            any(
                bool(index.get("unique"))
                and tuple(index.get("column_names") or ())
                == ("id", "owner_username")
                for index in indexes
            )
        )

    event.listen(
        SalesOrderModel.__table__,
        "before_create",
        record_parent_key,
    )
    try:
        monkeypatch.setattr(
            database_module.settings,
            "database_auto_create",
            False,
        )
        monkeypatch.setattr(database_module, "engine", legacy_engine)
        monkeypatch.setattr(
            database_module,
            "SessionLocal",
            local_session_factory,
        )
        monkeypatch.setattr(
            database_module,
            "ensure_schema_compatibility",
            lambda: None,
        )
        monkeypatch.setattr(
            user_service,
            "ensure_initial_superadmin",
            lambda: None,
        )
        monkeypatch.setattr(
            crawler_service,
            "ensure_default_roles",
            lambda: None,
        )
        monkeypatch.setattr(
            sensitive_word_service,
            "seed_default_sensitive_words",
            lambda _session: None,
        )

        database_module.init_database()

        with Session(legacy_engine) as session:
            session.add(
                SalesSyncStateModel(
                    store_id=1,
                    owner_username="alice",
                )
            )
            session.commit()
    finally:
        event.remove(
            SalesOrderModel.__table__,
            "before_create",
            record_parent_key,
        )
        legacy_engine.dispose()

    assert parent_key_seen_before_child_create == [True]


class _FakeResult:
    def __init__(self, row):
        self._row = row

    def first(self):
        return self._row


class _RecordingMysqlConnection:
    def __init__(self):
        self.dialect = mysql.dialect()
        self.statements: list[str] = []
        self.select_count = 0

    def execute(self, statement):
        compiled = str(
            statement.compile(
                dialect=self.dialect,
                compile_kwargs={"literal_binds": True},
            )
        )
        self.statements.append(compiled)
        if compiled.lstrip().upper().startswith("SELECT"):
            self.select_count += 1
            return _FakeResult((1,) if self.select_count == 1 else None)
        return _FakeResult(None)


def test_longtext_normalization_backfills_nulls_before_not_null_modify():
    connection = _RecordingMysqlConnection()
    table = SalesOrderModel.__table__
    column = table.c.raw_order_json

    database_module._normalize_longtext_column(
        connection,
        table,
        column,
        {"type": Text(), "nullable": True},
    )

    update_index = next(
        index
        for index, statement in enumerate(connection.statements)
        if statement.lstrip().upper().startswith("UPDATE")
    )
    alter_index = next(
        index
        for index, statement in enumerate(connection.statements)
        if "MODIFY COLUMN" in statement.upper()
    )
    assert update_index < alter_index
    assert connection.select_count == 2
    assert "raw_order_json='{}'" in connection.statements[update_index]
    assert "LONGTEXT NOT NULL" in connection.statements[alter_index]
