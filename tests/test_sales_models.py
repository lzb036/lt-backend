from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal

import pytest
from sqlalchemy import event
from sqlalchemy import create_engine, inspect, select, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

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
    StoreModel,
    UserAccountModel,
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


def test_ensure_table_layout_skips_unsafe_non_null_foreign_key_columns_for_partial_populated_table(sqlite_engine):
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

            result = database_module._ensure_table_layout(
                connection,
                SalesAnalysisMessageModel.__table__,
            )
            second_result = database_module._ensure_table_layout(
                connection,
                SalesAnalysisMessageModel.__table__,
            )

        columns = {column["name"] for column in inspect(partial_engine).get_columns("lt_sales_analysis_messages")}
        with partial_engine.connect() as connection:
            rows = list(connection.execute(text("SELECT id, owner_username, question_text FROM lt_sales_analysis_messages")))

        assert "answer_text" in columns
        assert "conversation_id" not in columns
        assert "created_at" not in columns
        assert "answer_text" in result["added_columns"]
        assert result["skipped_columns"]
        assert "conversation_id" in result["skipped_columns"]
        assert second_result["added_columns"] == []
        assert rows == [(1, "alice", "legacy row")]
    finally:
        partial_engine.dispose()


def test_ensure_table_layout_adds_safe_defaulted_columns_to_partial_populated_daily_sales_table(sqlite_engine):
    partial_engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    try:
        with partial_engine.begin() as connection:
            connection.execute(
                text(
                    """
                    CREATE TABLE lt_product_sales_daily (
                        id INTEGER PRIMARY KEY,
                        owner_username VARCHAR(255) NOT NULL,
                        store_id INTEGER NOT NULL,
                        sales_date DATE NOT NULL
                    )
                    """
                )
            )
            connection.execute(
                text(
                    """
                    INSERT INTO lt_product_sales_daily (id, owner_username, store_id, sales_date)
                    VALUES (1, 'alice', 10, '2026-07-16')
                    """
                )
            )

            result = database_module._ensure_table_layout(
                connection,
                ProductSalesDailyModel.__table__,
            )

        columns = {column["name"] for column in inspect(partial_engine).get_columns("lt_product_sales_daily")}
        with partial_engine.connect() as connection:
            row = connection.execute(
                text(
                    """
                    SELECT manage_number, sku_key, order_count, effective_units
                    FROM lt_product_sales_daily
                    WHERE id = 1
                    """
                )
            ).one()

        assert {"manage_number", "sku_key", "order_count", "effective_units"} <= columns
        assert row == ("", "", 0, 0)
        assert "store_id" not in result["skipped_columns"]
    finally:
        partial_engine.dispose()
