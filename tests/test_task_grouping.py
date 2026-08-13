from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.db.database import Base
from app.db.models import ListingTaskModel, SyncTaskModel, UserAccountModel
from app.services import crawler_service


def install_session_scope(monkeypatch, session_factory) -> None:
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

    monkeypatch.setattr(crawler_service, "session_scope", local_session_scope)
    monkeypatch.setattr(crawler_service, "dispatch_next_sync_task_safely", lambda: None)
    monkeypatch.setattr(crawler_service, "dispatch_next_listing_task_safely", lambda: None)
    monkeypatch.setattr(crawler_service, "finalize_stale_cancel_requested_tasks", lambda *args, **kwargs: None)
    monkeypatch.setattr(crawler_service, "reconcile_interrupted_running_tasks", lambda *args, **kwargs: None)


def seed_user(session_factory, username: str = "alice") -> None:
    with session_factory() as session:
        session.add(
            UserAccountModel(
                username=username,
                display_name=username.title(),
                password_salt_b64="salt",
                password_hash_b64="hash",
            )
        )
        session.commit()


def test_listing_page_groups_complete_legacy_split_tasks(monkeypatch):
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    Base.metadata.create_all(engine)
    session_factory = sessionmaker(bind=engine, expire_on_commit=False, future=True)
    install_session_scope(monkeypatch, session_factory)
    seed_user(session_factory)
    created_at = datetime(2026, 8, 12, 7, 3, 35)
    with session_factory() as session:
        session.add_all(
            [
                ListingTaskModel(
                    id=f"listing-{index}",
                    owner_username="alice",
                    task_name=f"自动上架 2026-08-12 07:00 {index}/3",
                    status=status,
                    total_count=50,
                    success_count=success_count,
                    failed_count=failed_count,
                    created_at=created_at,
                    updated_at=created_at + timedelta(minutes=index),
                )
                for index, status, success_count, failed_count in (
                    (1, "success", 50, 0),
                    (2, "failed", 40, 10),
                    (3, "queued", 0, 0),
                )
            ]
        )
        session.commit()

    page = crawler_service.list_listing_tasks("alice", page=1, page_size=30)

    assert page["total"] == 1
    assert len(page["listingTasks"]) == 1
    group = page["listingTasks"][0]
    assert group["isGroup"] is True
    assert group["taskName"] == "自动上架 2026-08-12 07:00"
    assert group["status"] == "queued"
    assert group["totalCount"] == 150
    assert group["successCount"] == 90
    assert group["failedCount"] == 10
    assert group["childTaskIds"] == ["listing-1", "listing-2", "listing-3"]
    assert [child["id"] for child in group["children"]] == group["childTaskIds"]
    assert [child["taskGroupIndex"] for child in group["children"]] == [1, 2, 3]
    assert all(child["taskGroupSize"] == 3 for child in group["children"])

    engine.dispose()


def test_incomplete_legacy_split_tasks_remain_standalone(monkeypatch):
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    Base.metadata.create_all(engine)
    session_factory = sessionmaker(bind=engine, expire_on_commit=False, future=True)
    install_session_scope(monkeypatch, session_factory)
    seed_user(session_factory)
    created_at = datetime(2026, 8, 12, 7, 3, 35)
    with session_factory() as session:
        session.add_all(
            [
                ListingTaskModel(
                    id=f"listing-{index}",
                    owner_username="alice",
                    task_name=f"自动上架 2026-08-12 07:00 {index}/3",
                    status="success",
                    total_count=50,
                    success_count=50,
                    created_at=created_at,
                )
                for index in (1, 3)
            ]
        )
        session.commit()

    page = crawler_service.list_listing_tasks("alice", page=1, page_size=30)

    assert page["total"] == 2
    assert all(not task.get("isGroup") for task in page["listingTasks"])

    engine.dispose()


def test_sync_group_pagination_counts_logical_tasks(monkeypatch):
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    Base.metadata.create_all(engine)
    session_factory = sessionmaker(bind=engine, expire_on_commit=False, future=True)
    install_session_scope(monkeypatch, session_factory)
    seed_user(session_factory)
    created_at = datetime(2026, 8, 13, 9, 0, 0)
    with session_factory() as session:
        session.add_all(
            [
                SyncTaskModel(
                    id=f"sync-{index}",
                    owner_username="alice",
                    task_group_id="batch-1",
                    task_group_index=index,
                    task_group_size=2,
                    task_name=f"批量删除 {index}/2 店铺 2026-08-13 09:00",
                    task_type="product_delete",
                    status="success",
                    total_count=50,
                    success_count=50,
                    created_at=created_at,
                )
                for index in (1, 2)
            ]
        )
        session.add(
            SyncTaskModel(
                id="sync-single",
                owner_username="alice",
                task_name="商品同步 店铺 2026-08-13 08:00",
                task_type="store_sync",
                status="success",
                created_at=created_at - timedelta(hours=1),
            )
        )
        session.commit()

    first_page = crawler_service.list_sync_tasks("alice", page=1, page_size=1)
    second_page = crawler_service.list_sync_tasks("alice", page=2, page_size=1)

    assert first_page["total"] == 2
    assert first_page["syncTasks"][0]["isGroup"] is True
    assert second_page["syncTasks"][0]["id"] == "sync-single"

    raw_tasks = crawler_service.list_sync_tasks(
        "alice",
        task_ids=["sync-1", "sync-2"],
    )
    assert [task["id"] for task in raw_tasks] == ["sync-2", "sync-1"]
    assert all(not task.get("isGroup") for task in raw_tasks)

    engine.dispose()
