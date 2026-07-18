from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta

import pytest
from sqlalchemy import create_engine, event
from sqlalchemy.orm import sessionmaker

from app.db.database import Base
from app.db.models import (
    SalesOrderModel,
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


def test_list_stores_includes_recent_year_order_count_after_initial_sync(
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
        session.add(
            UserAccountModel(
                username="alice",
                display_name="Alice",
                password_salt_b64="salt",
                password_hash_b64="hash",
            )
        )
        session.flush()
        stores = [
            StoreModel(
                owner_username="alice",
                store_code=f"store-{index}",
                store_name=f"Store {index}",
            )
            for index in range(1, 4)
        ]
        session.add_all(stores)
        session.flush()
        session.add_all(
            [
                SalesSyncStateModel(
                    owner_username="alice",
                    store_id=stores[0].id,
                    initial_sync_completed=True,
                    sync_status="idle",
                ),
                SalesSyncStateModel(
                    owner_username="alice",
                    store_id=stores[1].id,
                    initial_sync_completed=True,
                    sync_status="idle",
                ),
            ]
        )
        session.add_all(
            [
                SalesOrderModel(
                    owner_username="alice",
                    store_id=stores[0].id,
                    order_number="RECENT",
                    ordered_at=now - timedelta(days=10),
                    raw_order_json="{}",
                    last_synced_at=now,
                ),
                SalesOrderModel(
                    owner_username="alice",
                    store_id=stores[0].id,
                    order_number="EXPIRED",
                    ordered_at=now - timedelta(days=366),
                    raw_order_json="{}",
                    last_synced_at=now,
                ),
            ]
        )
        session.commit()

    monkeypatch.setattr(crawler_service, "session_scope", local_session_scope)
    monkeypatch.setattr(crawler_service, "decrypt_text", lambda value: value)
    monkeypatch.setattr(crawler_service, "sales_now_naive", lambda: now)

    result = crawler_service.list_stores("alice")

    assert isinstance(result, list)
    assert [row["recentYearOrderCount"] for row in result] == [1, 0, None]
