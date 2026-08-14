from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime
import json
from types import SimpleNamespace

import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

from app.db.database import Base
from app.db.models import (
    CrawlTaskModel,
    StoreModel,
    SystemSettingModel,
    UserAccountModel,
)
from app.services import crawler_service, sales_order_sync_history_service


@pytest.fixture()
def session_factory(monkeypatch):
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    Base.metadata.create_all(engine)
    factory = sessionmaker(
        bind=engine,
        expire_on_commit=False,
        future=True,
    )

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
    monkeypatch.setattr(
        sales_order_sync_history_service,
        "session_scope",
        local_session_scope,
    )
    try:
        yield factory
    finally:
        engine.dispose()


def seed_user_and_store(
    session_factory,
    username: str,
    store_code: str,
    *,
    role: str = "operator",
) -> int:
    with session_factory() as session:
        session.add(
            UserAccountModel(
                username=username,
                display_name=username.title(),
                role=role,
                password_salt_b64="salt",
                password_hash_b64="hash",
            )
        )
        store = StoreModel(
            owner_username=username,
            store_code=store_code,
            store_name=f"{username} Store",
            alias_name=f"{username} Alias",
            enabled=True,
            rakuten_service_secret_encrypted="secret",
            rakuten_license_key_encrypted="key",
        )
        session.add(store)
        session.flush()
        store_id = int(store.id)
        session.commit()
        return store_id


def test_owner_store_update_changes_only_alias_and_enabled(
    session_factory,
) -> None:
    alice_store_id = seed_user_and_store(
        session_factory,
        "alice",
        "alice-shop",
    )
    bob_store_id = seed_user_and_store(
        session_factory,
        "bob",
        "bob-shop",
    )

    result = crawler_service.update_owned_store_settings(
        "alice",
        alice_store_id,
        alias_name="Alice New Alias",
        enabled=False,
    )

    assert result["aliasName"] == "Alice New Alias"
    assert result["enabled"] is False
    with session_factory() as session:
        alice_store = session.get(StoreModel, alice_store_id)
        bob_store = session.get(StoreModel, bob_store_id)
        assert alice_store.store_code == "alice-shop"
        assert alice_store.store_name == "alice Store"
        assert alice_store.rakuten_service_secret_encrypted == "secret"
        assert bob_store.alias_name == "bob Alias"
        assert bob_store.enabled is True

    with pytest.raises(RuntimeError):
        crawler_service.update_owned_store_settings(
            "alice",
            bob_store_id,
            alias_name="Not Allowed",
            enabled=False,
        )


def test_user_time_settings_are_independent(
    session_factory,
) -> None:
    seed_user_and_store(session_factory, "alice", "alice-shop")
    seed_user_and_store(session_factory, "bob", "bob-shop")

    alice_settings = crawler_service.save_time_settings(
        "alice",
        SimpleNamespace(
            cleanupWeekday=1,
            cleanupTime="03:00",
            cleanupEnabled=False,
            productSyncEnabled=False,
            productSyncWeekday=2,
            productSyncTime="04:00",
            unlistedCleanupEnabled=False,
            deletedImageCleanupEnabled=False,
            deletedImageCleanupWeekday=3,
            deletedImageCleanupTime="05:00",
        ),
        include_queue_health=False,
    )
    bob_settings = crawler_service.get_time_settings(
        "bob",
        include_queue_health=False,
    )

    assert alice_settings["cleanupTime"] == "03:00"
    assert alice_settings["productSyncEnabled"] is False
    assert bob_settings["cleanupTime"] == "09:00"
    assert bob_settings["productSyncEnabled"] is True


def test_manual_scheduled_cleanup_is_owner_scoped(
    session_factory,
) -> None:
    seed_user_and_store(session_factory, "alice", "alice-shop")
    seed_user_and_store(session_factory, "bob", "bob-shop")
    with session_factory() as session:
        session.add_all(
            [
                CrawlTaskModel(
                    id="alice-task",
                    owner_username="alice",
                    mode="scheduled",
                    source_type="shop",
                    target="https://example.com/alice",
                    status="success",
                    created_at=datetime(2026, 8, 1, 0, 0, 0),
                ),
                CrawlTaskModel(
                    id="bob-task",
                    owner_username="bob",
                    mode="scheduled",
                    source_type="shop",
                    target="https://example.com/bob",
                    status="success",
                    created_at=datetime(2026, 8, 1, 0, 0, 0),
                ),
            ]
        )
        session.commit()

    result = crawler_service.run_completed_scheduled_crawl_tasks_cleanup_now(
        "alice",
        include_queue_health=False,
    )

    assert result["lastCleanupDeletedCount"] == 1
    with session_factory() as session:
        task_ids = set(session.scalars(select(CrawlTaskModel.id)).all())
    assert task_ids == {"bob-task"}


def test_order_sync_settings_are_independent(
    session_factory,
) -> None:
    seed_user_and_store(session_factory, "alice", "alice-shop")
    seed_user_and_store(session_factory, "bob", "bob-shop")

    alice = sales_order_sync_history_service.save_user_settings(
        "alice",
        SimpleNamespace(
            enabled=False,
            intervalMinutes=60,
            successRetentionDays=15,
        ),
    )
    bob = sales_order_sync_history_service.get_user_settings("bob")

    assert alice["enabled"] is True
    assert alice["intervalMinutes"] == 60
    assert bob["enabled"] is True
    assert bob["intervalMinutes"] == 30


def test_time_settings_migration_resets_copied_global_history(
    session_factory,
) -> None:
    seed_user_and_store(session_factory, "alice", "alice-shop")
    seed_user_and_store(session_factory, "bob", "bob-shop")
    legacy_payload = {
        **crawler_service.default_time_settings_value(
            now=datetime(2026, 8, 14, 10, 0, 0)
        ),
        "cleanupWeekday": 2,
        "cleanupTime": "04:00",
        "lastCleanupAt": "2026-08-13 01:38:16",
        "lastCleanupDeletedCount": 1288,
        "productSyncLastAt": "2026-08-09 21:00:54",
        "productSyncLastTaskCount": 2,
        "unlistedLastCleanupAt": "2026-08-01 01:00:35",
        "unlistedLastDeletedCount": 9,
        "unlistedLastTaskCount": 2,
        "deletedImageCleanupLastAt": "2026-08-08 00:00:33",
        "deletedImageCleanupLastProductCount": 62,
        "deletedImageCleanupLastTaskCount": 3,
    }
    with session_factory() as session:
        for owner_username in ("alice", "bob"):
            session.add(
                SystemSettingModel(
                    key=crawler_service.user_time_settings_key(owner_username),
                    value_json=json.dumps(legacy_payload),
                )
            )
        migrated = crawler_service.migrate_user_time_settings_to_owner_scope(
            session,
            now=datetime(2026, 8, 14, 10, 0, 0),
        )
        session.commit()

    assert migrated == 2
    for owner_username in ("alice", "bob"):
        settings = crawler_service.get_time_settings(
            owner_username,
            include_queue_health=False,
        )
        assert settings["cleanupWeekday"] == 2
        assert settings["cleanupTime"] == "04:00"
        assert settings["lastCleanupAt"] is None
        assert settings["lastCleanupDeletedCount"] == 0
        assert settings["productSyncLastAt"] is None
        assert settings["productSyncLastTaskCount"] == 0
        assert settings["unlistedLastCleanupAt"] is None
        assert settings["unlistedLastDeletedCount"] == 0
        assert settings["unlistedLastTaskCount"] == 0
        assert settings["deletedImageCleanupLastAt"] is None
        assert settings["deletedImageCleanupLastProductCount"] == 0
        assert settings["deletedImageCleanupLastTaskCount"] == 0

    with session_factory() as session:
        assert session.get(
            SystemSettingModel,
            crawler_service.USER_TIME_SETTINGS_OWNER_SCOPE_MIGRATION_KEY,
        ) is not None
        assert session.get(
            SystemSettingModel,
            crawler_service.deleted_product_image_cleanup_user_setting_key("alice"),
        ) is not None


def test_order_sync_settings_migration_gives_each_user_a_personal_row(
    session_factory,
) -> None:
    seed_user_and_store(
        session_factory,
        "superadmin",
        "superadmin-shop",
        role="superadmin",
    )
    seed_user_and_store(session_factory, "alice", "alice-shop")
    with session_factory() as session:
        session.add(
            SystemSettingModel(
                key=sales_order_sync_history_service.GLOBAL_SETTINGS_KEY,
                value_json=json.dumps(
                    {
                        "enabled": True,
                        "intervalMinutes": 720,
                        "successRetentionDays": 7,
                    }
                ),
            )
        )
        migrated = (
            sales_order_sync_history_service
            .migrate_user_settings_to_owner_scope(session)
        )
        session.commit()

    assert migrated == 2
    superadmin = sales_order_sync_history_service.get_user_settings("superadmin")
    alice = sales_order_sync_history_service.get_user_settings("alice")
    assert superadmin["intervalMinutes"] == 720
    assert superadmin["successRetentionDays"] == 7
    assert alice["intervalMinutes"] == 30
    assert alice["successRetentionDays"] == 30

    with session_factory() as session:
        assert session.get(
            SystemSettingModel,
            sales_order_sync_history_service.user_settings_key("superadmin"),
        ) is not None
        assert session.get(
            SystemSettingModel,
            sales_order_sync_history_service.user_settings_key("alice"),
        ) is not None
        assert session.get(
            SystemSettingModel,
            sales_order_sync_history_service.USER_SETTINGS_OWNER_SCOPE_MIGRATION_KEY,
        ) is not None
