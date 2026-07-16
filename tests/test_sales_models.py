from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal

import pytest
from sqlalchemy import create_engine, inspect, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

import app.db.models  # noqa: F401
from app.db.database import Base
from app.db.models import (
    ProductSalesDailyModel,
    SalesOrderItemModel,
    SalesOrderModel,
    StoreModel,
    UserAccountModel,
)


@pytest.fixture()
def sqlite_engine():
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
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
    store = StoreModel(
        owner_username="alice",
        store_code="demo-shop",
        store_name="Demo Shop",
    )
    session.add(user)
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
            returned_units=1,
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
