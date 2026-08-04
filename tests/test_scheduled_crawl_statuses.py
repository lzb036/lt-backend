from __future__ import annotations

from contextlib import contextmanager

from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

from app.db.database import Base
from app.db.models import ScheduledCrawlModel, UserAccountModel
from app.services import crawler_service


def test_update_all_scheduled_crawl_statuses_is_owner_scoped(monkeypatch) -> None:
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
    with local_session_scope() as session:
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
            ScheduledCrawlModel(
                owner_username="alice",
                name="Alice enabled",
                source_type="shop",
                target="店铺:alice-a 日榜 全部",
                enabled=True,
                status="idle",
                schedule_time="09:00",
            ),
            ScheduledCrawlModel(
                owner_username="alice",
                name="Alice disabled",
                source_type="shop",
                target="店铺:alice-b 日榜 全部",
                enabled=False,
                status="disabled",
                schedule_time="10:00",
            ),
            ScheduledCrawlModel(
                owner_username="alice",
                name="Alice unrelated",
                source_type="keyword",
                target="keyword",
                enabled=True,
                status="idle",
                schedule_time="11:00",
            ),
            ScheduledCrawlModel(
                owner_username="bob",
                name="Bob shop",
                source_type="shop",
                target="店铺:bob 日榜 全部",
                enabled=True,
                status="idle",
                schedule_time="12:00",
            ),
        ])

    result = crawler_service.update_all_scheduled_crawl_statuses("alice", False)

    assert result == {"matchedCount": 2, "updatedCount": 1, "enabled": False}
    with local_session_scope() as session:
        rows = session.scalars(select(ScheduledCrawlModel).order_by(ScheduledCrawlModel.id)).all()
        assert [(row.owner_username, row.source_type, row.enabled, row.status) for row in rows] == [
            ("alice", "shop", False, "disabled"),
            ("alice", "shop", False, "disabled"),
            ("alice", "keyword", True, "idle"),
            ("bob", "shop", True, "idle"),
        ]

    result = crawler_service.update_all_scheduled_crawl_statuses("alice", True)

    assert result == {"matchedCount": 2, "updatedCount": 2, "enabled": True}
    with local_session_scope() as session:
        rows = session.scalars(select(ScheduledCrawlModel).order_by(ScheduledCrawlModel.id)).all()
        assert [(row.owner_username, row.source_type, row.enabled, row.status) for row in rows] == [
            ("alice", "shop", True, "idle"),
            ("alice", "shop", True, "idle"),
            ("alice", "keyword", True, "idle"),
            ("bob", "shop", True, "idle"),
        ]
        assert rows[0].next_run_at is not None
        assert rows[1].next_run_at is not None

    engine.dispose()
