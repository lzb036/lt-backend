from __future__ import annotations

import unittest
from contextlib import contextmanager
from datetime import datetime, timedelta
from types import SimpleNamespace
from unittest.mock import Mock, patch

from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session, sessionmaker

from app.db.database import Base
from app.db.models import CrawlTaskModel
from app.services import crawler_service


class CrawlDispatchModelTests(unittest.TestCase):
    def test_crawl_task_persists_reserved_queue_job_id(self):
        self.assertIn("queue_job_id", CrawlTaskModel.__table__.columns)
        column = CrawlTaskModel.__table__.columns["queue_job_id"]
        self.assertTrue(column.nullable)
        self.assertEqual(column.type.length, 64)


class FakeRedisLock:
    def __init__(self, *, acquired: bool = True):
        self.acquired = acquired
        self.acquire_calls: list[tuple[bool, float | None]] = []
        self.release_calls = 0

    def acquire(self, blocking: bool = True, blocking_timeout: float | None = None) -> bool:
        self.acquire_calls.append((blocking, blocking_timeout))
        return self.acquired

    def release(self) -> None:
        self.release_calls += 1


class FakeRedisConnection:
    def __init__(self, lock: FakeRedisLock):
        self.lock_instance = lock
        self.lock_calls: list[tuple[str, int]] = []

    def lock(self, name: str, *, timeout: int):
        self.lock_calls.append((name, timeout))
        return self.lock_instance


class CrawlDispatcherTests(unittest.TestCase):
    def setUp(self):
        self.engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
        Base.metadata.create_all(self.engine)
        self.session_factory = sessionmaker(
            bind=self.engine,
            expire_on_commit=False,
            future=True,
        )

    def tearDown(self):
        self.engine.dispose()

    @contextmanager
    def session_scope(self):
        session = self.session_factory()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def add_task(
        self,
        task_id: str,
        *,
        status: str = "queued",
        queue_job_id: str | None = None,
        created_at: datetime | None = None,
    ) -> None:
        with self.session_scope() as session:
            session.add(
                CrawlTaskModel(
                    id=task_id,
                    owner_username="owner",
                    source_type="shop",
                    target=f"店铺:https://www.rakuten.co.jp/{task_id}/ 全部",
                    mode="scheduled",
                    status=status,
                    queue_job_id=queue_job_id,
                    total_count=0,
                    success_count=0,
                    failed_count=0,
                    warning_count=0,
                    message="等待执行",
                    created_at=created_at or datetime.now(),
                )
            )

    def get_task(self, task_id: str) -> CrawlTaskModel:
        with Session(self.engine, future=True) as session:
            return session.scalar(
                select(CrawlTaskModel).where(CrawlTaskModel.id == task_id)
            )

    @contextmanager
    def dispatcher_context(
        self,
        enqueue: Mock,
        lock: FakeRedisLock,
    ):
        connection = FakeRedisConnection(lock)
        with (
            patch.object(crawler_service, "session_scope", self.session_scope),
            patch.object(crawler_service, "redis_connection", return_value=connection),
            patch.object(crawler_service, "enqueue_task", enqueue),
            patch.object(crawler_service.settings, "task_queue_mode", "redis"),
            patch.object(
                crawler_service.settings,
                "max_running_crawl_tasks_per_user",
                3,
            ),
        ):
            yield

    def test_dispatch_reserves_only_available_oldest_tasks(self):
        now = datetime.now()
        self.add_task("running", status="running", created_at=now - timedelta(minutes=5))
        self.add_task(
            "reserved",
            queue_job_id="crawl-reserved-existing",
            created_at=now - timedelta(minutes=4),
        )
        self.add_task("oldest-unreserved", created_at=now - timedelta(minutes=3))
        self.add_task("newer-unreserved", created_at=now - timedelta(minutes=2))
        enqueue = Mock(return_value="job")
        lock = FakeRedisLock()

        with (
            self.dispatcher_context(enqueue, lock),
            patch.object(crawler_service.uuid, "uuid4") as uuid4,
        ):
            uuid4.return_value.hex = "12345678abcdef"
            dispatched = crawler_service.dispatch_queued_crawl_tasks("owner")

        self.assertEqual(dispatched, 1)
        self.assertEqual(enqueue.call_count, 1)
        self.assertEqual(enqueue.call_args.args[1], "oldest-unreserved")
        reserved = self.get_task("oldest-unreserved")
        newer = self.get_task("newer-unreserved")
        self.assertEqual(
            reserved.queue_job_id,
            "crawl-oldest-unreserved-12345678",
        )
        self.assertIsNone(newer.queue_job_id)
        self.assertEqual(lock.release_calls, 1)

    def test_dispatch_does_not_exceed_capacity(self):
        now = datetime.now()
        self.add_task("running-1", status="running", created_at=now - timedelta(minutes=5))
        self.add_task("running-2", status="running", created_at=now - timedelta(minutes=4))
        self.add_task(
            "reserved",
            queue_job_id="crawl-reserved-existing",
            created_at=now - timedelta(minutes=3),
        )
        self.add_task("waiting", created_at=now - timedelta(minutes=2))
        enqueue = Mock(return_value="job")
        lock = FakeRedisLock()

        with self.dispatcher_context(enqueue, lock):
            dispatched = crawler_service.dispatch_queued_crawl_tasks("owner")

        self.assertEqual(dispatched, 0)
        enqueue.assert_not_called()
        self.assertIsNone(self.get_task("waiting").queue_job_id)
        self.assertEqual(lock.release_calls, 1)

    def test_enqueue_failure_clears_matching_reservation(self):
        self.add_task("waiting")
        enqueue = Mock(side_effect=RuntimeError("redis unavailable"))
        lock = FakeRedisLock()

        with (
            self.dispatcher_context(enqueue, lock),
            patch.object(crawler_service.uuid, "uuid4") as uuid4,
        ):
            uuid4.return_value.hex = "abcdef12345678"
            dispatched = crawler_service.dispatch_queued_crawl_tasks("owner")

        self.assertEqual(dispatched, 0)
        task = self.get_task("waiting")
        self.assertIsNone(task.queue_job_id)
        self.assertIn("等待系统重试", task.message)
        self.assertEqual(lock.release_calls, 1)

    def test_create_task_uses_capacity_dispatcher_in_redis_mode(self):
        payload = SimpleNamespace(
            sourceId=None,
            sourceType="product_url",
            target="https://item.rakuten.co.jp/shop/item/",
            mode="manual",
        )
        capacity_dispatch = Mock(return_value=1)
        direct_dispatch = Mock()

        with (
            patch.object(crawler_service, "session_scope", self.session_scope),
            patch.object(crawler_service.settings, "task_queue_mode", "redis"),
            patch.object(
                crawler_service,
                "normalize_rakuten_product_target",
                return_value=payload.target,
            ),
            patch.object(
                crawler_service,
                "dispatch_queued_crawl_tasks_safely",
                capacity_dispatch,
            ),
            patch.object(crawler_service, "dispatch_crawl_task", direct_dispatch),
        ):
            crawler_service.create_task("owner", payload)

        capacity_dispatch.assert_called_once_with("owner")
        direct_dispatch.assert_not_called()

    def test_run_existing_task_uses_capacity_dispatcher_in_redis_mode(self):
        self.add_task("retry", status="success")
        capacity_dispatch = Mock(return_value=1)
        direct_dispatch = Mock()

        with (
            patch.object(crawler_service, "session_scope", self.session_scope),
            patch.object(crawler_service.settings, "task_queue_mode", "redis"),
            patch.object(
                crawler_service,
                "dispatch_queued_crawl_tasks_safely",
                capacity_dispatch,
            ),
            patch.object(crawler_service, "dispatch_crawl_task", direct_dispatch),
        ):
            crawler_service.run_existing_task("owner", "retry")

        capacity_dispatch.assert_called_once_with("owner")
        direct_dispatch.assert_not_called()
        task = self.get_task("retry")
        self.assertEqual(task.status, "queued")
        self.assertIsNone(task.queue_job_id)


if __name__ == "__main__":
    unittest.main()
