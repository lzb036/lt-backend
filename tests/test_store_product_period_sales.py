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
        session.add_all([
            ProductModel(
                owner_username="alice",
                store_id=store.id,
                title="Product 1",
                source_url="https://example.com/product-1",
                source_url_hash="hash-1",
                rakuten_manage_number="manage-1",
                item_number="item-1",
                review_status="listed",
                listed_at=datetime(2026, 7, 18, 10, 0, 0),
            ),
            ProductModel(
                owner_username="alice",
                store_id=store.id,
                title="Product 2",
                source_url="https://example.com/product-2",
                source_url_hash="hash-2",
                rakuten_manage_number="manage-2",
                item_number="item-2",
                review_status="listed",
                listed_at=datetime(2026, 7, 1, 10, 0, 0),
            ),
        ])
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
            ProductSalesDailyModel(
                owner_username="alice",
                store_id=store.id,
                sales_date=date(2026, 7, 18),
                manage_number="manage-2",
                sku_key="",
                effective_units=1,
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

    assert sorted(row["periodSalesCount"] for row in week) == [1, 3]
    assert sorted(row["periodSalesCount"] for row in year) == [1, 8]
    assert [row["rakutenManageNumber"] for row in week] == ["manage-2", "manage-1"]

    sales_sorted = crawler_service.list_products(
        "alice",
        status="listed",
        store_id=store_id,
        sales_period_days=7,
        sales_sort="desc",
    )
    assert [row["rakutenManageNumber"] for row in sales_sorted] == ["manage-1", "manage-2"]

    filtered = crawler_service.list_products(
        "alice",
        status="listed",
        store_id=store_id,
        sales_period_days=7,
        sales_sort="desc",
        sales_min=2,
        sales_max=4,
        page=1,
        page_size=1,
    )

    assert filtered["total"] == 1
    assert filtered["products"][0]["rakutenManageNumber"] == "manage-1"
    assert filtered["products"][0]["periodSalesCount"] == 3


def test_list_store_products_applies_sales_filter_before_page_serialization(
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
        session.add_all([
            ProductModel(
                owner_username="alice",
                store_id=store.id,
                title=f"Product {index}",
                source_url=f"https://example.com/product-{index}",
                source_url_hash=f"hash-{index}",
                rakuten_manage_number=f"manage-{index}",
                item_number=f"item-{index}",
                review_status="listed",
                listed_at=datetime(2026, 7, 1, 10, 0, index),
            )
            for index in range(40)
        ])
        session.add(ProductSalesDailyModel(
            owner_username="alice",
            store_id=store.id,
            sales_date=date(2026, 7, 18),
            manage_number="manage-0",
            item_number="item-0",
            sku_key="",
            effective_units=1,
        ))
        store_id = store.id
        session.commit()

    monkeypatch.setattr(crawler_service, "session_scope", local_session_scope)
    monkeypatch.setattr(crawler_service, "sales_now_naive", lambda: now)
    serialized_ids = []
    original_product_to_public = crawler_service.product_to_public

    def tracked_product_to_public(row, **kwargs):
        serialized_ids.append(int(row.id))
        return original_product_to_public(row, **kwargs)

    monkeypatch.setattr(
        crawler_service,
        "product_to_public",
        tracked_product_to_public,
    )

    result = crawler_service.list_products(
        "alice",
        status="listed",
        store_id=store_id,
        sales_period_days=365,
        sales_min=0,
        sales_max=0,
        page=2,
        page_size=10,
    )

    assert result["total"] == 39
    assert result["page"] == 2
    assert len(result["products"]) == 10
    assert len(serialized_ids) == 10
    assert all(row["periodSalesCount"] == 0 for row in result["products"])


def test_zero_sales_filter_excludes_store_without_completed_initial_sync(
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
            initial_sync_completed=False,
            sync_status="idle",
        ))
        session.add(ProductModel(
            owner_username="alice",
            store_id=store.id,
            title="Product",
            source_url="https://example.com/product",
            source_url_hash="hash",
            rakuten_manage_number="manage",
            item_number="item",
            review_status="listed",
            listed_at=datetime(2026, 7, 1, 10, 0, 0),
        ))
        store_id = store.id
        session.commit()

    monkeypatch.setattr(crawler_service, "session_scope", local_session_scope)
    monkeypatch.setattr(crawler_service, "sales_now_naive", lambda: now)

    unfiltered = crawler_service.list_products(
        "alice",
        status="listed",
        store_id=store_id,
        sales_period_days=365,
        page=1,
        page_size=30,
    )
    zero_sales = crawler_service.list_products(
        "alice",
        status="listed",
        store_id=store_id,
        sales_period_days=365,
        sales_min=0,
        sales_max=0,
        page=1,
        page_size=30,
    )

    assert unfiltered["total"] == 1
    assert unfiltered["products"][0]["periodSalesCount"] is None
    assert zero_sales["total"] == 0
    assert zero_sales["products"] == []


def test_product_page_serializes_only_current_page(
    monkeypatch,
    session_factory,
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

    with session_factory() as session:
        session.add(UserAccountModel(
            username="alice",
            display_name="Alice",
            password_salt_b64="salt",
            password_hash_b64="hash",
        ))
        session.flush()
        session.add_all([
            ProductModel(
                owner_username="alice",
                title=f"Product {index}",
                source_url=f"https://example.com/product-{index}",
                source_url_hash=f"hash-{index}",
                item_number=f"item-{index}",
                review_status="pending",
                created_at=datetime(2026, 7, 1, 10, 0, index),
            )
            for index in range(40)
        ])
        session.commit()

    monkeypatch.setattr(crawler_service, "session_scope", local_session_scope)
    serialized_ids = []
    original_product_to_public = crawler_service.product_to_public

    def tracked_product_to_public(row, **kwargs):
        serialized_ids.append(int(row.id))
        return original_product_to_public(row, **kwargs)

    monkeypatch.setattr(
        crawler_service,
        "product_to_public",
        tracked_product_to_public,
    )

    result = crawler_service.list_products(
        "alice",
        status="pending",
        page=2,
        page_size=10,
    )

    assert result["total"] == 40
    assert len(result["products"]) == 10
    assert len(serialized_ids) == 10


def test_listed_master_store_filter_uses_child_product_relationship(
    monkeypatch,
    session_factory,
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
        linked_master = ProductModel(
            owner_username="alice",
            title="Linked master",
            source_url="https://example.com/linked-master",
            source_url_hash="linked-master",
            item_number="linked-master",
            review_status="listed_master",
            raw_payload_json='{"listedStores":"invalid legacy value"}',
        )
        unlinked_master = ProductModel(
            owner_username="alice",
            title="Unlinked master",
            source_url="https://example.com/unlinked-master",
            source_url_hash="unlinked-master",
            item_number="unlinked-master",
            review_status="listed_master",
        )
        session.add_all([linked_master, unlinked_master])
        session.flush()
        session.add(ProductModel(
            owner_username="alice",
            store_id=store.id,
            parent_product_id=linked_master.id,
            title="Store product",
            source_url="https://example.com/store-product",
            source_url_hash="store-product",
            rakuten_manage_number="manage",
            item_number="item",
            review_status="listed",
            store_product_status="active",
        ))
        store_id = store.id
        linked_master_id = linked_master.id
        unlinked_master_id = unlinked_master.id
        session.commit()

    monkeypatch.setattr(crawler_service, "session_scope", local_session_scope)

    linked = crawler_service.list_products(
        "alice",
        status="listed_master",
        listed_store_id=str(store_id),
        page=1,
        page_size=30,
    )
    unlinked = crawler_service.list_products(
        "alice",
        status="listed_master",
        listed_store_id=crawler_service.LISTED_STORE_NONE_FILTER,
        page=1,
        page_size=30,
    )

    assert [row["id"] for row in linked["products"]] == [
        linked_master_id
    ]
    assert [row["id"] for row in unlinked["products"]] == [
        unlinked_master_id
    ]
