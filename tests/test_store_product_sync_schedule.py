from __future__ import annotations

import json
from contextlib import contextmanager
from datetime import datetime

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.db.database import Base
from app.db.models import StoreModel, SystemSettingModel, UserAccountModel
from app.services import crawler_service


@pytest.fixture()
def session_factory():
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
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


@pytest.fixture()
def local_session_scope(monkeypatch, session_factory):
    @contextmanager
    def _session_scope():
        session = session_factory()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    monkeypatch.setattr(crawler_service, "session_scope", _session_scope)
    return _session_scope


def freeze_now(monkeypatch, value: datetime) -> None:
    class FrozenDateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            return value.replace(tzinfo=tz) if tz is not None else value

    monkeypatch.setattr(crawler_service, "datetime", FrozenDateTime)


def test_product_sync_defaults_to_sunday_at_21():
    payload = crawler_service.default_time_settings_value(
        now=datetime(2026, 7, 18, 12, 0, 0)
    )

    assert payload["productSyncEnabled"] is True
    assert payload["productSyncWeekday"] == 6
    assert payload["productSyncTime"] == "21:00"
    assert payload["productSyncNextAt"] == "2026-07-19 21:00:00"


@pytest.mark.parametrize(
    ("enabled", "next_at"),
    [
        (False, "2026-07-18 11:00:00"),
        (True, "2026-07-19 21:00:00"),
    ],
)
def test_product_sync_does_not_queue_when_disabled_or_not_due(
    monkeypatch,
    session_factory,
    local_session_scope,
    enabled,
    next_at,
):
    now = datetime(2026, 7, 18, 12, 0, 0)
    freeze_now(monkeypatch, now)
    payload = crawler_service.default_time_settings_value(now=now)
    payload["productSyncEnabled"] = enabled
    payload["productSyncNextAt"] = next_at
    with session_factory() as session:
        session.add(
            SystemSettingModel(
                key=crawler_service.SCHEDULED_CRAWL_TASK_CLEANUP_SETTING_KEY,
                value_json=json.dumps(payload),
            )
        )
        session.commit()

    queued = []
    monkeypatch.setattr(
        crawler_service,
        "create_sync_task",
        lambda owner_username, store_id: queued.append((owner_username, store_id)),
    )

    assert crawler_service.run_due_store_product_syncs_once() == 0
    assert queued == []


def test_due_product_sync_queues_only_enabled_stores_with_credentials(
    monkeypatch,
    session_factory,
    local_session_scope,
):
    now = datetime(2026, 7, 18, 21, 0, 0)
    freeze_now(monkeypatch, now)
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
                store_code="ready",
                store_name="Ready",
                enabled=True,
                rakuten_service_secret_encrypted="secret",
                rakuten_license_key_encrypted="key",
            ),
            StoreModel(
                owner_username="alice",
                store_code="missing-key",
                store_name="Missing Key",
                enabled=True,
                rakuten_service_secret_encrypted="secret",
                rakuten_license_key_encrypted="",
            ),
            StoreModel(
                owner_username="alice",
                store_code="disabled",
                store_name="Disabled",
                enabled=False,
                rakuten_service_secret_encrypted="secret",
                rakuten_license_key_encrypted="key",
            ),
        ]
        session.add_all(stores)
        session.flush()
        ready_store_id = stores[0].id
        payload = crawler_service.default_time_settings_value(now=now)
        payload["productSyncNextAt"] = "2026-07-18 20:59:00"
        session.add(
            SystemSettingModel(
                key=crawler_service.SCHEDULED_CRAWL_TASK_CLEANUP_SETTING_KEY,
                value_json=json.dumps(payload),
            )
        )
        session.commit()

    queued = []
    monkeypatch.setattr(
        crawler_service,
        "create_sync_task",
        lambda owner_username, store_id: queued.append((owner_username, store_id)),
    )

    assert crawler_service.run_due_store_product_syncs_once() == 1
    assert queued == [("alice", ready_store_id)]

    with session_factory() as session:
        row = session.get(
            SystemSettingModel,
            crawler_service.SCHEDULED_CRAWL_TASK_CLEANUP_SETTING_KEY,
        )
        saved = json.loads(row.value_json)
    assert saved["productSyncNextAt"] == "2026-07-19 21:00:00"
    assert saved["productSyncLastAt"] == "2026-07-18 21:00:00"
    assert saved["productSyncLastTaskCount"] == 1

    assert crawler_service.run_due_store_product_syncs_once() == 0
    assert queued == [("alice", ready_store_id)]
