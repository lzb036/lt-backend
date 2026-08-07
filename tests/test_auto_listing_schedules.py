from __future__ import annotations

import json
from contextlib import contextmanager
from datetime import datetime, timedelta
from types import SimpleNamespace

import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

from app.db.database import Base
from app.db.models import (
    AutoDeletionTaskModel,
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
    monkeypatch.setattr(
        crawler_service,
        "dispatch_auto_listing_schedule",
        lambda schedule_id, **kwargs: None,
    )
    monkeypatch.setattr(
        crawler_service,
        "dispatch_auto_deletion_task",
        lambda task_id, **kwargs: None,
    )
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
        quantity=10000,
    )
    created = crawler_service.create_auto_listing_schedule("alice", payload)
    assert created["storeAliasName"] == "A 店"
    assert created["quantity"] == 10000
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


def test_run_schedule_now_keeps_disabled_schedule_disabled(database, monkeypatch) -> None:
    with database() as session:
        store_id = session.scalar(select(StoreModel.id))
        session.add(
            AutoListingScheduleModel(
                owner_username="alice",
                store_id=store_id,
                schedule_type="daily",
                schedule_time="09:00",
                quantity=10,
                enabled=False,
                status="disabled",
                next_run_at=None,
            )
        )
        session.add(
            ProductModel(
                owner_username="alice",
                title="立即执行商品",
                source_url="https://example.com/run-now",
                source_url_hash="run-now",
                review_status="approved",
                raw_payload_json="{}",
            )
        )

    monkeypatch.setattr(
        crawler_service,
        "listing_preflight_product_check",
        lambda product, store: {"productId": product.id, "issues": []},
    )
    monkeypatch.setattr(
        crawler_service,
        "create_listing_task",
        lambda owner_username, payload: {
            "listingTask": {"id": "manual-task"},
            "listingTasks": [{"id": "manual-task"}],
            "summary": {"total": len(payload.productIds), "taskCount": 1},
        },
    )

    result = crawler_service.run_auto_listing_schedule_now("alice", 1)

    assert result["status"] == "disabled"
    assert result["nextRunAt"] is None
    assert result["lastTaskIds"] == ["manual-task"]
    assert "实际已创建 1 件" in result["lastMessage"]


def test_run_schedule_now_is_owner_scoped(database) -> None:
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
                next_run_at=datetime(2026, 8, 4, 9, 0),
            )
        )

    with pytest.raises(RuntimeError, match="不存在"):
        crawler_service.run_auto_listing_schedule_now("bob", 1)


def test_manual_tasks_reject_repeated_active_store_and_allow_after_completion(
    database,
) -> None:
    with database() as session:
        store_id = session.scalar(select(StoreModel.id))

    automatic = crawler_service.create_auto_listing_schedule(
        "alice",
        SimpleNamespace(
            storeId=store_id,
            scheduleType="daily",
            scheduleTime="09:00",
            weekday=None,
            monthDay=None,
            quantity=10,
        ),
    )
    first_manual = crawler_service.create_manual_listing_task(
        "alice",
        SimpleNamespace(storeId=store_id, quantity=10),
    )
    with pytest.raises(RuntimeError, match="请勿重复提交"):
        crawler_service.create_manual_listing_task(
            "alice",
            SimpleNamespace(storeId=store_id, quantity=20),
        )

    with database() as session:
        row = session.get(AutoListingScheduleModel, first_manual["id"])
        row.status = "completed"

    second_manual = crawler_service.create_manual_listing_task(
        "alice",
        SimpleNamespace(storeId=store_id, quantity=20),
    )

    assert automatic["taskType"] == "automatic"
    assert first_manual["taskType"] == "manual"
    assert first_manual["status"] == "idle"
    assert first_manual["lastMessage"] == "任务已受理，后台正在创建上架任务。"
    assert second_manual["taskType"] == "manual"
    assert first_manual["id"] != second_manual["id"]

    all_tasks = crawler_service.list_auto_listing_schedules("alice")
    manual_tasks = crawler_service.list_auto_listing_schedules(
        "alice",
        store_id=store_id,
        task_type="manual",
    )

    assert [task["taskType"] for task in all_tasks] == [
        "automatic",
        "manual",
        "manual",
    ]
    assert {task["id"] for task in manual_tasks} == {
        first_manual["id"],
        second_manual["id"],
    }


def test_immediate_manual_listing_task_dispatches_after_commit(
    database,
    monkeypatch,
) -> None:
    with database() as session:
        store_id = session.scalar(select(StoreModel.id))

    calls: list[tuple[int, str | None, bool]] = []

    def fake_dispatch(
        schedule_id: int,
        *,
        owner_username: str | None = None,
        advance_next_run: bool = False,
    ) -> None:
        calls.append((schedule_id, owner_username, advance_next_run))

    monkeypatch.setattr(
        crawler_service,
        "dispatch_auto_listing_schedule",
        fake_dispatch,
    )

    created = crawler_service.create_manual_listing_task(
        "alice",
        SimpleNamespace(storeId=store_id, quantity=30),
    )

    assert created["status"] == "idle"
    assert calls == [(created["id"], "alice", False)]


def test_pending_immediate_manual_listing_task_cannot_be_deleted(database) -> None:
    with database() as session:
        store_id = session.scalar(select(StoreModel.id))

    created = crawler_service.create_manual_listing_task(
        "alice",
        SimpleNamespace(storeId=store_id, quantity=10),
    )

    with pytest.raises(RuntimeError, match="暂时不能删除"):
        crawler_service.delete_auto_listing_schedule("alice", created["id"])

    with database() as session:
        row = session.get(AutoListingScheduleModel, created["id"])
        row.status = "completed"

    crawler_service.delete_auto_listing_schedule("alice", created["id"])
    with database() as session:
        assert session.get(AutoListingScheduleModel, created["id"]) is None


def test_auto_listing_model_uses_nullable_automatic_store_unique_key() -> None:
    constraint_names = {
        constraint.name
        for constraint in AutoListingScheduleModel.__table__.constraints
    }
    assert "uq_lt_auto_listing_owner_auto_store" in constraint_names
    assert "uq_lt_auto_listing_owner_store" not in constraint_names
    assert AutoListingScheduleModel.__table__.columns["automatic_store_id"].nullable


def test_scheduled_manual_listing_task_runs_once_when_due(
    database,
    monkeypatch,
) -> None:
    with database() as session:
        store_id = session.scalar(select(StoreModel.id))

    created = crawler_service.create_manual_listing_task(
        "alice",
        SimpleNamespace(
            storeId=store_id,
            quantity=25,
            executionMode="scheduled",
            executeAt=datetime.now() + timedelta(hours=1),
        ),
    )

    assert created["taskType"] == "manual"
    assert created["executionMode"] == "scheduled"
    assert created["status"] == "idle"
    assert created["enabled"] is True

    with database() as session:
        row = session.get(AutoListingScheduleModel, created["id"])
        row.next_run_at = datetime.now() - timedelta(minutes=1)
        scheduled_at = row.next_run_at

    monkeypatch.setattr(
        crawler_service,
        "auto_listing_candidate_product_ids",
        lambda owner_username, store_id, quantity: [],
    )

    assert crawler_service.run_due_auto_listing_schedules_once() == 1
    assert crawler_service.run_due_auto_listing_schedules_once() == 0

    with database() as session:
        row = session.get(AutoListingScheduleModel, created["id"])
        assert row.task_type == "manual"
        assert row.schedule_type == "once"
        assert row.status == "completed"
        assert row.enabled is False
        assert row.next_run_at == scheduled_at


def test_scheduled_manual_deletion_task_runs_once_when_due(
    database,
    monkeypatch,
) -> None:
    with database() as session:
        store_id = session.scalar(select(StoreModel.id))

    created = crawler_service.create_manual_deletion_task(
        "alice",
        SimpleNamespace(
            storeId=store_id,
            quantity=30,
            executionMode="scheduled",
            executeAt=datetime.now() + timedelta(hours=1),
        ),
    )

    assert created["taskType"] == "manual"
    assert created["executionMode"] == "scheduled"
    assert created["status"] == "idle"

    with database() as session:
        row = session.get(AutoDeletionTaskModel, created["id"])
        row.next_run_at = datetime.now() - timedelta(minutes=1)
        scheduled_at = row.next_run_at

    monkeypatch.setattr(
        crawler_service,
        "auto_deletion_candidate_product_ids",
        lambda owner_username, store_id, quantity: [],
    )

    assert crawler_service.run_due_auto_deletion_tasks_once() == 1
    assert crawler_service.run_due_auto_deletion_tasks_once() == 0

    with database() as session:
        row = session.get(AutoDeletionTaskModel, created["id"])
        assert row.task_type == "manual"
        assert row.schedule_type == "once"
        assert row.status == "completed"
        assert row.enabled is False
        assert row.next_run_at == scheduled_at


def test_immediate_manual_deletion_task_dispatches_and_rejects_duplicate(
    database,
    monkeypatch,
) -> None:
    with database() as session:
        store_id = session.scalar(select(StoreModel.id))

    calls: list[tuple[int, str | None, bool]] = []

    def fake_dispatch(
        task_id: int,
        *,
        owner_username: str | None = None,
        advance_next_run: bool = False,
    ) -> None:
        calls.append((task_id, owner_username, advance_next_run))

    monkeypatch.setattr(
        crawler_service,
        "dispatch_auto_deletion_task",
        fake_dispatch,
    )

    created = crawler_service.create_manual_deletion_task(
        "alice",
        SimpleNamespace(storeId=store_id, quantity=30),
    )

    assert created["status"] == "idle"
    assert created["lastMessage"] == "任务已受理，后台正在创建删除任务。"
    assert calls == [(created["id"], "alice", False)]

    with pytest.raises(RuntimeError, match="请勿重复提交"):
        crawler_service.create_manual_deletion_task(
            "alice",
            SimpleNamespace(storeId=store_id, quantity=20),
        )

    with pytest.raises(RuntimeError, match="暂时不能删除"):
        crawler_service.delete_auto_deletion_task("alice", created["id"])

    with database() as session:
        row = session.get(AutoDeletionTaskModel, created["id"])
        row.status = "completed"

    second = crawler_service.create_manual_deletion_task(
        "alice",
        SimpleNamespace(storeId=store_id, quantity=20),
    )
    assert second["id"] != created["id"]

    crawler_service.delete_auto_deletion_task("alice", created["id"])
    with database() as session:
        assert session.get(AutoDeletionTaskModel, created["id"]) is None


def test_scheduled_manual_task_requires_future_execute_at(database) -> None:
    with database() as session:
        store_id = session.scalar(select(StoreModel.id))

    with pytest.raises(RuntimeError, match="必须晚于当前时间"):
        crawler_service.create_manual_listing_task(
            "alice",
            SimpleNamespace(
                storeId=store_id,
                quantity=10,
                executionMode="scheduled",
                executeAt=datetime.now() - timedelta(minutes=1),
            ),
        )


def test_automatic_listing_task_can_be_edited_and_toggled(database) -> None:
    with database() as session:
        store_id = session.scalar(select(StoreModel.id))

    created = crawler_service.create_auto_listing_schedule(
        "alice",
        SimpleNamespace(
            storeId=store_id,
            scheduleType="daily",
            scheduleTime="09:00",
            weekday=None,
            monthDay=None,
            quantity=10,
        ),
    )
    updated = crawler_service.update_auto_listing_schedule(
        "alice",
        created["id"],
        SimpleNamespace(
            scheduleType="weekly",
            scheduleTime="16:30",
            weekday=5,
            monthDay=None,
            quantity=88,
        ),
    )
    assert updated["scheduleType"] == "weekly"
    assert updated["scheduleTime"] == "16:30"
    assert updated["weekday"] == 5
    assert updated["quantity"] == 88
    assert updated["nextRunAt"]

    disabled = crawler_service.update_auto_listing_schedule_status(
        "alice",
        created["id"],
        False,
    )
    assert disabled["enabled"] is False
    assert disabled["status"] == "disabled"
    assert disabled["nextRunAt"] is None

    enabled = crawler_service.update_auto_listing_schedule_status(
        "alice",
        created["id"],
        True,
    )
    assert enabled["enabled"] is True
    assert enabled["status"] == "idle"
    assert enabled["nextRunAt"]


def test_automatic_deletion_task_can_be_edited_and_toggled(database) -> None:
    with database() as session:
        store_id = session.scalar(select(StoreModel.id))

    created = crawler_service.create_auto_deletion_task(
        "alice",
        SimpleNamespace(
            storeId=store_id,
            scheduleType="daily",
            scheduleTime="09:00",
            weekday=None,
            monthDay=None,
            quantity=10,
        ),
    )
    updated = crawler_service.update_auto_deletion_task(
        "alice",
        created["id"],
        SimpleNamespace(
            scheduleType="monthly",
            scheduleTime="18:45",
            weekday=None,
            monthDay=20,
            quantity=66,
        ),
    )
    assert updated["scheduleType"] == "monthly"
    assert updated["scheduleTime"] == "18:45"
    assert updated["monthDay"] == 20
    assert updated["quantity"] == 66

    disabled = crawler_service.update_auto_deletion_task_status(
        "alice",
        created["id"],
        False,
    )
    assert disabled["enabled"] is False
    assert disabled["status"] == "disabled"
    assert disabled["nextRunAt"] is None

    enabled = crawler_service.update_auto_deletion_task_status(
        "alice",
        created["id"],
        True,
    )
    assert enabled["enabled"] is True
    assert enabled["status"] == "idle"
    assert enabled["nextRunAt"]


def test_manual_tasks_reject_automatic_edit_and_toggle(database) -> None:
    with database() as session:
        store_id = session.scalar(select(StoreModel.id))

    listing = crawler_service.create_manual_listing_task(
        "alice",
        SimpleNamespace(storeId=store_id, quantity=10),
    )
    deletion = crawler_service.create_manual_deletion_task(
        "alice",
        SimpleNamespace(storeId=store_id, quantity=10),
    )
    update_payload = SimpleNamespace(
        scheduleType="daily",
        scheduleTime="09:00",
        weekday=None,
        monthDay=None,
        quantity=20,
    )

    with pytest.raises(RuntimeError, match="不支持编辑"):
        crawler_service.update_auto_listing_schedule(
            "alice",
            listing["id"],
            update_payload,
        )
    with pytest.raises(RuntimeError, match="不支持启用或停用"):
        crawler_service.update_auto_listing_schedule_status(
            "alice",
            listing["id"],
            True,
        )
    with pytest.raises(RuntimeError, match="不支持编辑"):
        crawler_service.update_auto_deletion_task(
            "alice",
            deletion["id"],
            update_payload,
        )
    with pytest.raises(RuntimeError, match="不支持启用或关闭"):
        crawler_service.update_auto_deletion_task_status(
            "alice",
            deletion["id"],
            True,
        )
