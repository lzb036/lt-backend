from __future__ import annotations

from datetime import datetime, timedelta

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.db.database import Base
from app.db.models import (
    CrawlTaskModel,
    ListingTaskModel,
    SalesOrderSyncRunModel,
    SyncTaskModel,
    UserAccountModel,
)


def test_all_task_finished_times_are_write_once():
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    Base.metadata.create_all(engine)
    factory = sessionmaker(bind=engine, expire_on_commit=False, future=True)
    first_finished_at = datetime(2026, 8, 18, 8, 30, 0)
    retry_finished_at = first_finished_at + timedelta(hours=2)

    with factory() as session:
        session.add(
            UserAccountModel(
                username="alice",
                display_name="Alice",
                password_salt_b64="salt",
                password_hash_b64="hash",
            )
        )
        rows = [
            CrawlTaskModel(
                id="crawl-task",
                owner_username="alice",
                source_type="url",
                target="https://example.com",
                status="success",
                finished_at=first_finished_at,
            ),
            ListingTaskModel(
                id="listing-task",
                owner_username="alice",
                task_name="Listing",
                status="success",
                finished_at=first_finished_at,
            ),
            SyncTaskModel(
                id="sync-task",
                owner_username="alice",
                store_name="Store",
                task_name="Sync",
                status="success",
                finished_at=first_finished_at,
            ),
            SalesOrderSyncRunModel(
                id="order-sync-task",
                owner_username="alice",
                store_name="Store",
                trigger_type="manual",
                status="success",
                finished_at=first_finished_at,
            ),
        ]
        session.add_all(rows)
        session.commit()

        for row in rows:
            row.status = "queued"
            row.finished_at = None
        session.commit()

        for row in rows:
            assert row.finished_at == first_finished_at
            row.status = "success"
            row.finished_at = retry_finished_at
        session.commit()

        for row in rows:
            assert row.finished_at == first_finished_at
