from __future__ import annotations

from contextlib import contextmanager

import pytest
from fastapi import HTTPException
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.api import crawler as crawler_api
from app.db.database import Base
from app.db.models import ListingTaskModel, SyncTaskModel, UserAccountModel
from app.services import crawler_service


@pytest.fixture()
def session_factory():
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    Base.metadata.create_all(engine)
    factory = sessionmaker(
        bind=engine,
        expire_on_commit=False,
        future=True,
    )
    try:
        yield factory
    finally:
        engine.dispose()


def install_session_scope(monkeypatch, session_factory):
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

    monkeypatch.setattr(
        crawler_service,
        "session_scope",
        local_session_scope,
    )


def test_task_id_filters_are_owner_scoped(
    monkeypatch,
    session_factory,
):
    install_session_scope(monkeypatch, session_factory)
    monkeypatch.setattr(
        crawler_service,
        "dispatch_next_sync_task_safely",
        lambda: None,
    )
    monkeypatch.setattr(
        crawler_service,
        "dispatch_next_listing_task_safely",
        lambda: None,
    )
    monkeypatch.setattr(
        crawler_service,
        "finalize_stale_cancel_requested_tasks",
        lambda *args, **kwargs: None,
    )
    monkeypatch.setattr(
        crawler_service,
        "reconcile_interrupted_running_tasks",
        lambda *args, **kwargs: None,
    )
    with session_factory() as session:
        session.add_all([
            UserAccountModel(
                username="alice",
                display_name="Alice",
                password_salt_b64="salt",
                password_hash_b64="hash",
            ),
            UserAccountModel(
                username="bob",
                display_name="Bob",
                password_salt_b64="salt",
                password_hash_b64="hash",
            ),
        ])
        session.add_all([
            SyncTaskModel(
                id="sync-alice",
                owner_username="alice",
                task_name="Alice sync",
            ),
            SyncTaskModel(
                id="sync-bob",
                owner_username="bob",
                task_name="Bob sync",
            ),
            ListingTaskModel(
                id="listing-alice",
                owner_username="alice",
                task_name="Alice listing",
            ),
            ListingTaskModel(
                id="listing-bob",
                owner_username="bob",
                task_name="Bob listing",
            ),
        ])
        session.commit()

    sync_tasks = crawler_service.list_sync_tasks(
        "alice",
        task_ids=["sync-alice", "sync-bob"],
    )
    listing_tasks = crawler_service.list_listing_tasks(
        "alice",
        task_ids=["listing-alice", "listing-bob"],
    )

    assert [row["id"] for row in sync_tasks] == ["sync-alice"]
    assert [row["id"] for row in listing_tasks] == [
        "listing-alice"
    ]


def test_task_id_query_filter_deduplicates_and_limits_input():
    assert crawler_api.parse_task_ids_filter(
        "task-1,task-1, task-2"
    ) == ["task-1", "task-2"]

    with pytest.raises(HTTPException) as exc_info:
        crawler_api.parse_task_ids_filter(
            ",".join(f"task-{index}" for index in range(101))
        )

    assert exc_info.value.status_code == 400
