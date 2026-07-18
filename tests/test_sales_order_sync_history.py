from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta
from types import SimpleNamespace

import pytest
from sqlalchemy import create_engine, event, func, select
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from app.db.database import Base
from app.db.models import (
    SalesOrderModel,
    SalesOrderSyncRunModel,
    StoreModel,
    SystemSettingModel,
    UserAccountModel,
)
from app.services import sales_order_sync_history_service


@pytest.fixture()
def history_session_factory(monkeypatch: pytest.MonkeyPatch):
    engine = create_engine(
        "sqlite+pysqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
        future=True,
    )

    @event.listens_for(engine, "connect")
    def _enable_foreign_keys(dbapi_connection, _record):
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

    @contextmanager
    def _session_scope():
        with factory() as session:
            try:
                yield session
                session.commit()
            except Exception:
                session.rollback()
                raise

    monkeypatch.setattr(
        sales_order_sync_history_service,
        "session_scope",
        _session_scope,
    )
    try:
        yield factory
    finally:
        engine.dispose()


def _seed_owner(
    session: Session,
    username: str,
    store_code: str,
) -> StoreModel:
    session.add(
        UserAccountModel(
            username=username,
            display_name=username,
            password_salt_b64="salt",
            password_hash_b64="hash",
        )
    )
    session.flush()
    store = StoreModel(
        owner_username=username,
        store_code=store_code,
        store_name=f"{username} store",
    )
    session.add(store)
    session.flush()
    return store


def _add_run(
    session: Session,
    *,
    run_id: str,
    owner_username: str,
    store: StoreModel,
    status: str,
    trigger_type: str = "manual",
    created_at: datetime | None = None,
) -> SalesOrderSyncRunModel:
    row = SalesOrderSyncRunModel(
        id=run_id,
        owner_username=owner_username,
        store_id=store.id,
        store_name=store.store_name,
        trigger_type=trigger_type,
        status=status,
        created_at=created_at or datetime(2026, 7, 17, 8, 0, 0),
        updated_at=created_at or datetime(2026, 7, 17, 8, 0, 0),
    )
    session.add(row)
    session.flush()
    return row


def test_get_global_settings_returns_defaults_without_writing(
    history_session_factory,
) -> None:
    assert sales_order_sync_history_service.get_global_settings() == {
        "enabled": True,
        "intervalMinutes": 30,
        "successRetentionDays": 30,
    }

    with history_session_factory() as session:
        count = session.scalar(
            select(func.count()).select_from(SystemSettingModel)
        )
    assert count == 0


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("intervalMinutes", 4),
        ("intervalMinutes", 1441),
        ("successRetentionDays", 0),
        ("successRetentionDays", 366),
    ],
)
def test_save_global_settings_rejects_out_of_range_values(
    history_session_factory,
    field: str,
    value: int,
) -> None:
    payload = SimpleNamespace(
        enabled=True,
        intervalMinutes=30,
        successRetentionDays=30,
    )
    setattr(payload, field, value)

    with pytest.raises(ValueError):
        sales_order_sync_history_service.save_global_settings(payload)

    with history_session_factory() as session:
        assert session.get(
            SystemSettingModel,
            sales_order_sync_history_service.GLOBAL_SETTINGS_KEY,
        ) is None


def test_save_global_settings_persists_complete_payload(
    history_session_factory,
) -> None:
    saved = sales_order_sync_history_service.save_global_settings(
        SimpleNamespace(
            enabled=False,
            intervalMinutes=75,
            successRetentionDays=45,
        )
    )

    assert saved == {
        "enabled": False,
        "intervalMinutes": 75,
        "successRetentionDays": 45,
    }
    assert sales_order_sync_history_service.get_global_settings() == saved


def test_list_runs_is_owner_scoped_and_server_paginated(
    history_session_factory,
) -> None:
    with history_session_factory() as session:
        store_a = _seed_owner(session, "owner-a", "a")
        store_a.alias_name = "Owner A Alias"
        store_b = _seed_owner(session, "owner-b", "b")
        _add_run(
            session,
            run_id="run-a-old",
            owner_username="owner-a",
            store=store_a,
            status="failed",
            created_at=datetime(2026, 7, 17, 8, 0, 0),
        )
        session.add_all([
            SalesOrderModel(
                owner_username="owner-a",
                store_id=store_a.id,
                order_number="recent-1",
                ordered_at=datetime(2026, 7, 10, 8, 0, 0),
                raw_order_json="{}",
                last_synced_at=datetime(2026, 7, 17, 8, 0, 0),
            ),
            SalesOrderModel(
                owner_username="owner-a",
                store_id=store_a.id,
                order_number="recent-2",
                ordered_at=datetime(2026, 7, 11, 8, 0, 0),
                raw_order_json="{}",
                last_synced_at=datetime(2026, 7, 17, 8, 0, 0),
            ),
        ])
        _add_run(
            session,
            run_id="run-a-new",
            owner_username="owner-a",
            store=store_a,
            status="success",
            created_at=datetime(2026, 7, 17, 9, 0, 0),
        )
        _add_run(
            session,
            run_id="run-b",
            owner_username="owner-b",
            store=store_b,
            status="success",
            created_at=datetime(2026, 7, 17, 10, 0, 0),
        )
        session.commit()

    result = sales_order_sync_history_service.list_runs(
        "owner-a",
        page=1,
        page_size=1,
    )

    assert result["total"] == 2
    assert result["page"] == 1
    assert result["pageSize"] == 1
    assert [row["id"] for row in result["rows"]] == ["run-a-new"]
    assert result["rows"][0]["storeAliasName"] == "Owner A Alias"
    assert result["rows"][0]["totalOrderCount"] == 2


def test_list_runs_applies_owner_before_all_filters(
    history_session_factory,
) -> None:
    with history_session_factory() as session:
        store_a = _seed_owner(session, "owner-a", "a")
        store_b = _seed_owner(session, "owner-b", "b")
        _add_run(
            session,
            run_id="run-a",
            owner_username="owner-a",
            store=store_a,
            status="failed",
            trigger_type="retry",
        )
        _add_run(
            session,
            run_id="run-b",
            owner_username="owner-b",
            store=store_b,
            status="failed",
            trigger_type="retry",
        )
        session.commit()

    result = sales_order_sync_history_service.list_runs(
        "owner-a",
        store_id=store_a.id,
        trigger_type="retry",
        status="failed",
        created_at_from=datetime(2026, 7, 17, 0, 0, 0),
        created_at_to=datetime(2026, 7, 17, 23, 59, 59),
    )

    assert result["total"] == 1
    assert [row["id"] for row in result["rows"]] == ["run-a"]


def test_delete_runs_rejects_running_rows_and_deletes_nothing(
    history_session_factory,
) -> None:
    with history_session_factory() as session:
        store = _seed_owner(session, "owner-a", "a")
        _add_run(
            session,
            run_id="finished",
            owner_username="owner-a",
            store=store,
            status="success",
        )
        _add_run(
            session,
            run_id="active",
            owner_username="owner-a",
            store=store,
            status="running",
        )
        session.commit()

    with pytest.raises(ValueError, match="运行中"):
        sales_order_sync_history_service.delete_runs(
            "owner-a",
            ["finished", "active"],
        )

    with history_session_factory() as session:
        assert set(
            session.scalars(select(SalesOrderSyncRunModel.id)).all()
        ) == {"finished", "active"}


def test_delete_runs_never_deletes_another_owners_rows(
    history_session_factory,
) -> None:
    with history_session_factory() as session:
        store_a = _seed_owner(session, "owner-a", "a")
        store_b = _seed_owner(session, "owner-b", "b")
        _add_run(
            session,
            run_id="run-a",
            owner_username="owner-a",
            store=store_a,
            status="success",
        )
        _add_run(
            session,
            run_id="run-b",
            owner_username="owner-b",
            store=store_b,
            status="failed",
        )
        session.commit()

    result = sales_order_sync_history_service.delete_runs(
        "owner-a",
        ["run-a", "run-b"],
    )

    assert result == {"deletedCount": 1}
    with history_session_factory() as session:
        assert session.get(SalesOrderSyncRunModel, "run-a") is None
        assert session.get(SalesOrderSyncRunModel, "run-b") is not None


def test_retry_run_creates_linked_retry_through_queue(
    monkeypatch: pytest.MonkeyPatch,
    history_session_factory,
) -> None:
    with history_session_factory() as session:
        store = _seed_owner(session, "owner-a", "a")
        _add_run(
            session,
            run_id="failed-run",
            owner_username="owner-a",
            store=store,
            status="failed",
        )
        session.commit()
        store_id = store.id

    calls: list[tuple] = []
    monkeypatch.setattr(
        sales_order_sync_history_service,
        "_queue_sales_analysis_sync",
        lambda owner, target_store_id, **kwargs: calls.append(
            (owner, target_store_id, kwargs)
        )
        or {"id": "sales-1", "runId": "retry-run"},
        raising=False,
    )

    result = sales_order_sync_history_service.retry_run(
        "owner-a",
        "failed-run",
    )

    assert result["runId"] == "retry-run"
    assert calls == [
        (
            "owner-a",
            store_id,
            {
                "trigger_type": "retry",
                "parent_run_id": "failed-run",
            },
        )
    ]


@pytest.mark.parametrize("status", ["queued", "running", "success"])
def test_retry_run_rejects_non_retryable_status(
    monkeypatch: pytest.MonkeyPatch,
    history_session_factory,
    status: str,
) -> None:
    with history_session_factory() as session:
        store = _seed_owner(session, "owner-a", "a")
        _add_run(
            session,
            run_id="source-run",
            owner_username="owner-a",
            store=store,
            status=status,
        )
        session.commit()

    monkeypatch.setattr(
        sales_order_sync_history_service,
        "_queue_sales_analysis_sync",
        lambda *_args, **_kwargs: pytest.fail("must reject before queue"),
        raising=False,
    )

    with pytest.raises(ValueError, match="重试"):
        sales_order_sync_history_service.retry_run(
            "owner-a",
            "source-run",
        )


def test_cleanup_only_deletes_expired_success_rows(
    history_session_factory,
) -> None:
    now = datetime(2026, 7, 17, 12, 0, 0)
    with history_session_factory() as session:
        store = _seed_owner(session, "owner-a", "a")
        for run_id, status, finished_at in (
            ("expired-success", "success", now - timedelta(days=31)),
            ("recent-success", "success", now - timedelta(days=29)),
            ("old-failed", "failed", now - timedelta(days=60)),
            ("old-partial", "partial", now - timedelta(days=60)),
        ):
            row = _add_run(
                session,
                run_id=run_id,
                owner_username="owner-a",
                store=store,
                status=status,
            )
            row.finished_at = finished_at
        session.commit()

    deleted = (
        sales_order_sync_history_service.cleanup_successful_runs_if_due(
            now=now,
            force=True,
        )
    )

    assert deleted == 1
    with history_session_factory() as session:
        remaining = set(
            session.scalars(select(SalesOrderSyncRunModel.id)).all()
        )
    assert remaining == {
        "recent-success",
        "old-failed",
        "old-partial",
    }


def test_stale_queued_and_running_runs_are_recovered_as_failed(
    history_session_factory,
) -> None:
    now = datetime(2026, 7, 17, 12, 0, 0)
    with history_session_factory() as session:
        store = _seed_owner(session, "owner-a", "a")
        for run_id, status, updated_at in (
            ("stale-queued", "queued", now - timedelta(minutes=11)),
            ("stale-running", "running", now - timedelta(minutes=11)),
            ("active-running", "running", now - timedelta(minutes=9)),
        ):
            row = _add_run(
                session,
                run_id=run_id,
                owner_username="owner-a",
                store=store,
                status=status,
            )
            row.updated_at = updated_at
        session.commit()

    recovered = sales_order_sync_history_service.recover_stale_runs(
        now=now,
        stale_after=timedelta(minutes=10),
    )

    assert recovered == 2
    with history_session_factory() as session:
        rows = {
            row.id: row
            for row in session.scalars(
                select(SalesOrderSyncRunModel)
            ).all()
        }
    assert rows["stale-queued"].status == "failed"
    assert rows["stale-running"].status == "failed"
    assert rows["stale-running"].finished_at == now
    assert rows["active-running"].status == "running"


def test_history_heartbeat_refreshes_running_run_without_progress(
    history_session_factory,
) -> None:
    with history_session_factory() as session:
        store = _seed_owner(session, "owner-a", "a")
        row = _add_run(
            session,
            run_id="running-run",
            owner_username="owner-a",
            store=store,
            status="running",
            created_at=datetime(2026, 7, 17, 8, 0, 0),
        )
        row.updated_at = datetime(2026, 7, 17, 8, 0, 0)
        session.commit()

    sales_order_sync_history_service.update_run_progress(
        "owner-a",
        "running-run",
        progress_current=None,
        progress_total=None,
    )

    with history_session_factory() as session:
        row = session.get(SalesOrderSyncRunModel, "running-run")
    assert row is not None
    assert row.updated_at > datetime(2026, 7, 17, 8, 0, 0)
    assert row.progress_current == 0
    assert row.progress_total == 0


def test_complete_run_marks_mixed_result_partial(
    history_session_factory,
) -> None:
    with history_session_factory() as session:
        store = _seed_owner(session, "owner-a", "a")
        _add_run(
            session,
            run_id="partial-run",
            owner_username="owner-a",
            store=store,
            status="running",
        )
        session.commit()

    sales_order_sync_history_service.complete_run(
        "owner-a",
        "partial-run",
        {
            "totalOrderCount": 5,
            "newOrderCount": 2,
            "updatedOrderCount": 1,
            "unchangedOrderCount": 1,
            "failedOrderCount": 1,
        },
    )

    with history_session_factory() as session:
        row = session.get(SalesOrderSyncRunModel, "partial-run")
    assert row is not None
    assert row.status == "partial"
    assert row.total_order_count == 5
    assert row.failed_order_count == 1


def test_complete_run_marks_all_failed_result_failed(
    history_session_factory,
) -> None:
    with history_session_factory() as session:
        store = _seed_owner(session, "owner-a", "a")
        _add_run(
            session,
            run_id="failed-result",
            owner_username="owner-a",
            store=store,
            status="running",
        )
        session.commit()

    sales_order_sync_history_service.complete_run(
        "owner-a",
        "failed-result",
        {
            "totalOrderCount": 3,
            "newOrderCount": 0,
            "updatedOrderCount": 0,
            "unchangedOrderCount": 0,
            "failedOrderCount": 3,
        },
    )

    with history_session_factory() as session:
        row = session.get(SalesOrderSyncRunModel, "failed-result")
    assert row is not None
    assert row.status == "failed"
    assert row.message == "订单同步失败，请稍后重试。"
