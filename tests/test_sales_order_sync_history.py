from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime
from types import SimpleNamespace

import pytest
from sqlalchemy import create_engine, event, func, select
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from app.db.database import Base
from app.db.models import (
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
        store_b = _seed_owner(session, "owner-b", "b")
        _add_run(
            session,
            run_id="run-a-old",
            owner_username="owner-a",
            store=store_a,
            status="failed",
            created_at=datetime(2026, 7, 17, 8, 0, 0),
        )
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
