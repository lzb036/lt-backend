from __future__ import annotations

from contextlib import contextmanager
from datetime import date, datetime, timedelta

import pytest
from sqlalchemy import create_engine, event
from sqlalchemy.orm import sessionmaker

from app.db.database import Base
from app.db.models import (
    ProductModel,
    ProductSalesDailyModel,
    SalesSyncStateModel,
    StoreModel,
    UserAccountModel,
)
from app.services import crawler_service


@pytest.fixture()
def session_factory():
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)

    @event.listens_for(engine, "connect")
    def _enable_foreign_keys(dbapi_connection, _):
        cursor = dbapi_connection.cursor()
        cursor.execute("PRAGMA foreign_keys=ON")
        cursor.close()

    Base.metadata.create_all(engine)
    factory = sessionmaker(bind=engine, expire_on_commit=False, future=True)
    try:
        yield factory
    finally:
        engine.dispose()


def test_list_store_products_aggregates_effective_sales_for_selected_period(
    monkeypatch,
    session_factory,
):
    now = datetime(2026, 7, 18, 12, 0, 0)

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

    with session_factory() as session:
        session.add(UserAccountModel(
            username="alice",
            display_name="Alice",
            password_salt_b64="salt",
            password_hash_b64="hash",
        ))
        session.flush()
        store = StoreModel(
            owner_username="alice",
            store_code="shop",
            store_name="Shop",
        )
        session.add(store)
        session.flush()
        session.add(SalesSyncStateModel(
            owner_username="alice",
            store_id=store.id,
            initial_sync_completed=True,
            sync_status="idle",
        ))
        session.add(ProductModel(
            owner_username="alice",
            store_id=store.id,
            title="Product",
            source_url="https://example.com/product",
            source_url_hash="hash",
            rakuten_manage_number="manage-1",
            item_number="item-1",
            review_status="listed",
        ))
        session.add_all([
            ProductSalesDailyModel(
                owner_username="alice",
                store_id=store.id,
                sales_date=date(2026, 7, 18),
                manage_number="manage-1",
                sku_key="",
                effective_units=3,
            ),
            ProductSalesDailyModel(
                owner_username="alice",
                store_id=store.id,
                sales_date=date(2026, 6, 1),
                manage_number="manage-1",
                sku_key="",
                effective_units=5,
            ),
        ])
        store_id = store.id
        session.commit()

    monkeypatch.setattr(crawler_service, "session_scope", local_session_scope)
    monkeypatch.setattr(crawler_service, "sales_now_naive", lambda: now)

    week = crawler_service.list_products(
        "alice",
        status="listed",
        store_id=store_id,
        sales_period_days=7,
    )
    year = crawler_service.list_products(
        "alice",
        status="listed",
        store_id=store_id,
        sales_period_days=365,
    )

    assert week[0]["periodSalesCount"] == 3
    assert year[0]["periodSalesCount"] == 8
