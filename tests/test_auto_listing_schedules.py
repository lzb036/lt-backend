from __future__ import annotations

import json
from contextlib import contextmanager
from datetime import datetime
from types import SimpleNamespace

import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

from app.db.database import Base
from app.db.models import (
    AutoListingScheduleModel,
    ProductModel,
    StoreModel,
    UserAccountModel,
)
from app.services import crawler_service


@pytest.fixture
def database(monkeypatch):
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    Base.metadata.create_all(engine)
    factory = sessionmaker(bind=engine, expire_on_commit=False, future=True)

    @contextmanager
    def local_session_scope():
        session = factory()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    monkeypatch.setattr(crawler_service, "session_scope", local_session_scope)
    monkeypatch.setattr(crawler_service, "decrypt_text", lambda value: value or "")
    with local_session_scope() as session:
        session.add(
            UserAccountModel(
                username="alice",
                display_name="Alice",
                password_salt_b64="salt",
                password_hash_b64="hash",
            )
        )
        session.add(
            StoreModel(
                owner_username="alice",
                store_code="shop-a",
                store_name="店铺 A",
                alias_name="A 店",
                enabled=True,
                rakuten_service_secret_encrypted="secret",
                rakuten_license_key_encrypted="key",
            )
        )
    yield local_session_scope
    engine.dispose()


def test_next_monthly_run_uses_last_valid_day() -> None:
    assert crawler_service.next_monthly_run_at(
        31,
        "09:30",
        now=datetime(2026, 2, 1, 8, 0),
    ) == datetime(2026, 2, 28, 9, 30)
    assert crawler_service.next_monthly_run_at(
        31,
        "09:30",
        now=datetime(2026, 2, 28, 10, 0),
    ) == datetime(2026, 3, 31, 9, 30)


def test_create_schedule_rejects_duplicate_store(database) -> None:
    with database() as session:
        store_id = session.scalar(select(StoreModel.id))
    payload = SimpleNamespace(
        storeId=store_id,
        scheduleType="weekly",
        scheduleTime="10:15",
        weekday=7,
        monthDay=None,
        quantity=30,
    )
    created = crawler_service.create_auto_listing_schedule("alice", payload)
    assert created["storeAliasName"] == "A 店"
    assert created["quantity"] == 30
    assert created["nextRunAt"]

    with pytest.raises(RuntimeError, match="已经创建过"):
        crawler_service.create_auto_listing_schedule("alice", payload)


def test_due_schedule_uses_actual_available_quantity(database, monkeypatch) -> None:
    with database() as session:
        store_id = session.scalar(select(StoreModel.id))
        session.add(
            AutoListingScheduleModel(
                owner_username="alice",
                store_id=store_id,
                schedule_type="daily",
                schedule_time="09:00",
                quantity=5,
                enabled=True,
                status="idle",
                next_run_at=datetime(2026, 1, 1, 9, 0),
            )
        )
        session.add_all(
            [
                ProductModel(
                    owner_username="alice",
                    title=f"商品 {index}",
                    source_url=f"https://example.com/{index}",
                    source_url_hash=f"hash-{index}",
                    review_status="approved",
                    raw_payload_json="{}",
                    created_at=datetime(2026, 1, index, 8, 0),
                )
                for index in range(1, 4)
            ]
        )
        session.add(
            ProductModel(
                owner_username="alice",
                title="正在上架",
                source_url="https://example.com/locked",
                source_url_hash="hash-locked",
                review_status="approved",
                listing_task_id="busy",
                raw_payload_json="{}",
            )
        )
        session.add(
            ProductModel(
                owner_username="alice",
                title="已上架当前店铺",
                source_url="https://example.com/listed",
                source_url_hash="hash-listed",
                review_status="approved",
                raw_payload_json=json.dumps({"listedStores": [{"storeId": store_id}]}),
            )
        )

    captured: dict[str, object] = {}
    monkeypatch.setattr(
        crawler_service,
        "listing_preflight_product_check",
        lambda product, store: {"productId": product.id, "issues": []},
    )

    def fake_create_listing_task(owner_username, payload):
        captured["owner"] = owner_username
        captured["productIds"] = payload.productIds
        captured["storeIds"] = payload.storeIds
        return {
            "listingTask": {"id": "task-1"},
            "listingTasks": [{"id": "task-1"}],
            "summary": {"total": len(payload.productIds), "taskCount": 1},
        }

    monkeypatch.setattr(crawler_service, "create_listing_task", fake_create_listing_task)

    assert crawler_service.run_due_auto_listing_schedules_once() == 1
    assert captured["owner"] == "alice"
    assert len(captured["productIds"]) == 3
    assert captured["storeIds"] == [store_id]

    with database() as session:
        schedule = session.scalar(select(AutoListingScheduleModel))
        assert schedule.status == "idle"
        assert json.loads(schedule.last_task_ids_json) == ["task-1"]
        assert "计划上架 5 件，实际已创建 3 件" in schedule.last_message
        assert schedule.next_run_at > datetime(2026, 1, 1, 9, 0)


def test_due_schedule_records_no_available_products(database) -> None:
    with database() as session:
        store_id = session.scalar(select(StoreModel.id))
        session.add(
            AutoListingScheduleModel(
                owner_username="alice",
                store_id=store_id,
                schedule_type="daily",
                schedule_time="09:00",
                quantity=10,
                enabled=True,
                status="idle",
                next_run_at=datetime(2026, 1, 1, 9, 0),
            )
        )

    assert crawler_service.run_due_auto_listing_schedules_once() == 1

    with database() as session:
        schedule = session.scalar(select(AutoListingScheduleModel))
        assert schedule.status == "idle"
        assert schedule.last_message == "当前没有可自动上架的已审核商品。"
        assert schedule.last_task_ids_json == "[]"
