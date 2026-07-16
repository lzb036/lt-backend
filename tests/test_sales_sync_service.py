from __future__ import annotations

from contextlib import contextmanager
from datetime import date, datetime
from decimal import Decimal

import pytest
from sqlalchemy import create_engine, event, func, select
from sqlalchemy.orm import Session, sessionmaker

from app.db.database import Base
from app.db.models import (
    ProductSalesDailyModel,
    SalesItemAdjustmentModel,
    SalesOrderItemModel,
    SalesOrderModel,
    SalesSyncStateModel,
    StoreModel,
    UserAccountModel,
)
from app.services import sales_sync_service


@pytest.fixture()
def session_factory():
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)

    @event.listens_for(engine, "connect")
    def _enable_foreign_keys(dbapi_connection, _):
        cursor = dbapi_connection.cursor()
        cursor.execute("PRAGMA foreign_keys=ON")
        cursor.close()

    Base.metadata.create_all(engine)
    factory = sessionmaker(
        bind=engine,
        autoflush=False,
        expire_on_commit=False,
        future=True,
    )
    try:
        yield factory
    finally:
        engine.dispose()


def seed_store(
    session: Session,
    *,
    owner_username: str = "alice",
    store_code: str = "alice-shop",
) -> StoreModel:
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
        store_name=f"{owner_username.title()} Shop",
        rakuten_service_secret_encrypted="encrypted-secret",
        rakuten_license_key_encrypted="encrypted-key",
    )
    session.add(store)
    session.flush()
    return store


def order_snapshot(
    *,
    order_number: str = "ORDER-1",
    units: int = 5,
    item_overrides: dict | None = None,
    order_overrides: dict | None = None,
) -> dict:
    item = {
        "itemDetailId": "detail-1",
        "itemId": "item-1",
        "manageNumber": "MN-1",
        "itemNumber": "ITEM-1",
        "itemName": "Demo Item",
        "SkuModelList": [{"variantId": "sku-blue"}],
        "units": units,
        "price": 100,
        "priceTaxIncl": 110,
        "deleteItemFlag": False,
        "restoreInventoryFlag": False,
    }
    item.update(item_overrides or {})
    order = {
        "orderNumber": order_number,
        "orderProgress": 300,
        "orderStatus": "normal",
        "orderDatetime": "2026-07-15T10:00:00",
        "updateDatetime": "2026-07-15T10:05:00",
        "totalPrice": units * 100,
        "currencyCode": "JPY",
        "PackageModelList": [{"ItemModelList": [item]}],
    }
    order.update(order_overrides or {})
    return order


def patch_local_sync_dependencies(
    monkeypatch,
    session_factory,
    snapshots: list[dict],
):
    @contextmanager
    def local_session_scope():
        session = session_factory()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    monkeypatch.setattr(sales_sync_service, "session_scope", local_session_scope)
    monkeypatch.setattr(sales_sync_service, "decrypt_text", lambda value: value)
    monkeypatch.setattr(
        sales_sync_service.rakuten_order_service,
        "search_order_numbers",
        lambda *_args, **_kwargs: [
            str(snapshot["orderNumber"]) for snapshot in snapshots
        ],
    )
    monkeypatch.setattr(
        sales_sync_service.rakuten_order_service,
        "get_orders",
        lambda *_args, **_kwargs: snapshots,
    )


@pytest.mark.parametrize(
    (
        "kwargs",
        "expected",
    ),
    [
        (
            {"ordered_units": 5},
            (0, 0, 0, 5),
        ),
        (
            {"ordered_units": 5, "order_canceled": True},
            (5, 0, 0, 0),
        ),
        (
            {"ordered_units": 5, "delete_item": True},
            (5, 0, 0, 0),
        ),
        (
            {"ordered_units": 5, "latest_units": 3},
            (2, 0, 0, 3),
        ),
        (
            {"ordered_units": 5, "refund_units": 2},
            (0, 2, 0, 3),
        ),
        (
            {"ordered_units": 5, "return_units": 2},
            (0, 0, 2, 3),
        ),
    ],
)
def test_derive_adjustments_calculates_confirmed_deductions(kwargs, expected):
    result = sales_sync_service.derive_adjustments(**kwargs)

    assert (
        result.canceled_units,
        result.refunded_units,
        result.returned_units,
        result.effective_units,
    ) == expected


def test_return_refund_is_deducted_once():
    result = sales_sync_service.derive_adjustments(
        ordered_units=5,
        refund_units=2,
        return_units=2,
        return_refund=True,
    )

    assert result.refunded_units == 0
    assert result.returned_units == 2
    assert result.effective_units == 3


def test_unresolved_partial_refund_does_not_reduce_effective_units():
    result = sales_sync_service.derive_adjustments(
        ordered_units=5,
        unresolved_refund_units=2,
    )

    assert result.refunded_units == 0
    assert result.effective_units == 5
    assert result.unresolved_refund_units == 2


def test_attributed_refund_explains_quantity_reduction_without_double_deduction():
    result = sales_sync_service.derive_adjustments(
        ordered_units=5,
        latest_units=3,
        refund_units=2,
    )

    assert result.canceled_units == 0
    assert result.refunded_units == 2
    assert result.effective_units == 3


def test_calculate_effective_units_clamps_at_zero():
    assert sales_sync_service.calculate_effective_units(2, 1, 1, 1) == 0


def test_sync_owned_store_enforces_owner_and_store_lookup(
    monkeypatch,
    session_factory,
):
    with session_factory() as session:
        store = seed_store(session, owner_username="alice")
        store_id = store.id
        session.commit()

    called = False

    @contextmanager
    def local_session_scope():
        session = session_factory()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def unexpected_search(*_args, **_kwargs):
        nonlocal called
        called = True
        return []

    monkeypatch.setattr(sales_sync_service, "session_scope", local_session_scope)
    monkeypatch.setattr(
        sales_sync_service.rakuten_order_service,
        "search_order_numbers",
        unexpected_search,
    )

    with pytest.raises(LookupError, match="店铺不存在或无权访问"):
        sales_sync_service.sync_owned_store("bob", store_id)

    assert not called


def test_sync_owned_store_returns_running_state_without_duplicate_api_call(
    monkeypatch,
    session_factory,
):
    with session_factory() as session:
        store = seed_store(session)
        session.add(
            SalesSyncStateModel(
                owner_username="alice",
                store_id=store.id,
                sync_status="running",
                progress_current=3,
                progress_total=10,
            )
        )
        store_id = store.id
        session.commit()

    called = False

    @contextmanager
    def local_session_scope():
        session = session_factory()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def unexpected_search(*_args, **_kwargs):
        nonlocal called
        called = True
        return []

    monkeypatch.setattr(sales_sync_service, "session_scope", local_session_scope)
    monkeypatch.setattr(
        sales_sync_service.rakuten_order_service,
        "search_order_numbers",
        unexpected_search,
    )

    result = sales_sync_service.sync_owned_store("alice", store_id)

    assert result["status"] == "running"
    assert result["alreadyRunning"] is True
    assert result["progressCurrent"] == 3
    assert result["progressTotal"] == 10
    assert not called


def test_syncing_same_snapshot_twice_is_idempotent(
    monkeypatch,
    session_factory,
):
    with session_factory() as session:
        store = seed_store(session)
        store_id = store.id
        session.commit()

    snapshot = order_snapshot(item_overrides={"refundUnits": 2})
    patch_local_sync_dependencies(
        monkeypatch,
        session_factory,
        [snapshot],
    )

    first = sales_sync_service.sync_owned_store("alice", store_id)
    second = sales_sync_service.sync_owned_store("alice", store_id)

    with session_factory() as session:
        assert session.scalar(select(func.count(SalesOrderModel.id))) == 1
        assert session.scalar(select(func.count(SalesOrderItemModel.id))) == 1
        assert (
            session.scalar(select(func.count(SalesItemAdjustmentModel.id)))
            == 1
        )
        daily = session.scalars(select(ProductSalesDailyModel)).one()
        item = session.scalars(select(SalesOrderItemModel)).one()

    assert first["status"] == "completed"
    assert second["status"] == "completed"
    assert item.ordered_units == 5
    assert item.latest_units == 5
    assert daily.ordered_units == 5
    assert item.refunded_units == 2
    assert item.effective_units == 3
    assert daily.refunded_units == 2
    assert daily.effective_units == 3
    assert daily.order_count == 1


def test_reconciliation_preserves_first_units_and_reverts_removed_adjustment(
    monkeypatch,
    session_factory,
):
    with session_factory() as session:
        store = seed_store(session)
        store_id = store.id
        session.commit()

    snapshots = [order_snapshot(item_overrides={"refundUnits": 2})]
    patch_local_sync_dependencies(monkeypatch, session_factory, snapshots)
    sales_sync_service.sync_owned_store("alice", store_id)

    snapshots[0] = order_snapshot(units=5)
    sales_sync_service.sync_owned_store("alice", store_id)

    with session_factory() as session:
        item = session.scalars(select(SalesOrderItemModel)).one()
        adjustment = session.scalars(select(SalesItemAdjustmentModel)).one()
        daily = session.scalars(select(ProductSalesDailyModel)).one()

    assert item.ordered_units == 5
    assert item.latest_units == 5
    assert item.refunded_units == 0
    assert item.effective_units == 5
    assert adjustment.adjustment_type == "refund"
    assert adjustment.status == "reverted"
    assert daily.effective_units == 5


def test_reconciliation_marks_missing_existing_item_as_canceled(
    monkeypatch,
    session_factory,
):
    with session_factory() as session:
        store = seed_store(session)
        store_id = store.id
        session.commit()

    snapshots = [order_snapshot()]
    patch_local_sync_dependencies(monkeypatch, session_factory, snapshots)
    sales_sync_service.sync_owned_store("alice", store_id)

    snapshots[0] = order_snapshot()
    snapshots[0]["PackageModelList"] = []
    sales_sync_service.sync_owned_store("alice", store_id)

    with session_factory() as session:
        item = session.scalars(select(SalesOrderItemModel)).one()
        adjustment = session.scalars(select(SalesItemAdjustmentModel)).one()

    assert item.ordered_units == 5
    assert item.latest_units == 0
    assert item.canceled_units == 5
    assert item.effective_units == 0
    assert adjustment.adjustment_type == "cancel"
    assert adjustment.status == "confirmed"


def test_unattributed_partial_refund_is_unresolved_and_not_deducted(
    monkeypatch,
    session_factory,
):
    with session_factory() as session:
        store = seed_store(session)
        store_id = store.id
        session.commit()

    snapshot = order_snapshot(
        order_overrides={
            "partialRefund": True,
            "refundAmount": 200,
        },
    )
    patch_local_sync_dependencies(
        monkeypatch,
        session_factory,
        [snapshot],
    )

    sales_sync_service.sync_owned_store("alice", store_id)

    with session_factory() as session:
        order = session.scalars(select(SalesOrderModel)).one()
        item = session.scalars(select(SalesOrderItemModel)).one()
        adjustment = session.scalars(select(SalesItemAdjustmentModel)).one()

    assert order.has_unresolved_adjustment is True
    assert item.effective_units == 5
    assert adjustment.adjustment_type == "refund"
    assert adjustment.status == "unresolved"
    assert adjustment.amount == Decimal("200")


def test_rebuild_daily_sales_replaces_range_without_committing(
    session_factory,
):
    with session_factory() as session:
        store = seed_store(session)
        order = SalesOrderModel(
            owner_username="alice",
            store_id=store.id,
            order_number="ORDER-DAILY",
            ordered_at=datetime(2026, 7, 15, 10, 0, 0),
            raw_order_json="{}",
            last_synced_at=datetime(2026, 7, 16, 9, 0, 0),
        )
        session.add(order)
        session.flush()
        session.add(
            SalesOrderItemModel.from_service_payload(
                owner_username="alice",
                store_id=store.id,
                sales_order_id=order.id,
                order_number=order.order_number,
                item_detail_id="daily-detail",
                manage_number="MN-DAILY",
                item_number="ITEM-DAILY",
                sku_key="sku",
                item_name="Daily Item",
                unit_price=Decimal("100"),
                ordered_units=3,
                refunded_units=1,
                ordered_at=order.ordered_at,
            )
        )
        session.add(
            ProductSalesDailyModel(
                owner_username="alice",
                store_id=store.id,
                sales_date=date(2026, 7, 15),
                manage_number="STALE",
                sku_key="",
                item_name_snapshot="Stale",
                order_count=99,
            )
        )
        store_id = store.id
        session.commit()

    with session_factory() as session:
        sales_sync_service.rebuild_daily_sales(
            session,
            store_id,
            date(2026, 7, 15),
            date(2026, 7, 15),
        )
        session.flush()
        rebuilt = session.scalars(select(ProductSalesDailyModel)).one()
        assert rebuilt.manage_number == "MN-DAILY"
        assert rebuilt.ordered_units == 3
        assert rebuilt.refunded_units == 1
        assert rebuilt.effective_units == 2
        assert rebuilt.gross_sales_amount == Decimal("300")
        assert rebuilt.effective_sales_amount == Decimal("200")
        session.rollback()

    with session_factory() as session:
        persisted = session.scalars(
            select(ProductSalesDailyModel).where(
                ProductSalesDailyModel.store_id == store_id
            )
        ).one()

    assert persisted.manage_number == "STALE"
    assert persisted.order_count == 99
