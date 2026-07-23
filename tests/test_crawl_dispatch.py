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


class CrawlDispatchDatabaseTestCase(unittest.TestCase):
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
        finished_at: datetime | None = None,
        mode: str = "scheduled",
    ) -> None:
        with self.session_scope() as session:
            session.add(
                CrawlTaskModel(
                    id=task_id,
                    owner_username="owner",
                    source_type="shop",
                    target=f"店铺:https://www.rakuten.co.jp/{task_id}/ 全部",
                    mode=mode,
                    status=status,
                    queue_job_id=queue_job_id,
                    total_count=0,
                    success_count=0,
                    failed_count=0,
                    warning_count=0,
                    message="等待执行",
                    created_at=created_at or datetime.now(),
                    finished_at=finished_at,
                )
            )

    def get_task(self, task_id: str) -> CrawlTaskModel:
        with Session(self.engine, future=True) as session:
            return session.scalar(
                select(CrawlTaskModel).where(CrawlTaskModel.id == task_id)
            )


class CrawlDispatcherTests(CrawlDispatchDatabaseTestCase):
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
        self.assertEqual(
            enqueue.call_args.args[2],
            "crawl-oldest-unreserved-12345678",
        )
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


class CrawlWorkerDispatchTests(CrawlDispatchDatabaseTestCase):
    def test_concurrency_rejected_task_is_not_requeued(self):
        now = datetime.now()
        self.add_task(
            "running",
            status="running",
            created_at=now - timedelta(minutes=2),
        )
        self.add_task(
            "waiting",
            queue_job_id="crawl-waiting-reserved",
            created_at=now - timedelta(minutes=1),
        )
        direct_dispatch = Mock()
        refill = Mock(return_value=0)
        reconcile = Mock(return_value=0)

        with (
            patch.object(crawler_service, "session_scope", self.session_scope),
            patch.object(crawler_service.settings, "task_queue_mode", "redis"),
            patch.object(
                crawler_service.settings,
                "max_running_crawl_tasks_per_user",
                1,
            ),
            patch.object(
                crawler_service,
                "dispatch_crawl_task",
                direct_dispatch,
            ),
            patch.object(
                crawler_service,
                "dispatch_queued_crawl_tasks_safely",
                refill,
            ),
            patch.object(
                crawler_service,
                "reconcile_interrupted_running_tasks",
                reconcile,
            ),
        ):
            crawler_service.run_task("waiting", "crawl-waiting-reserved")

        direct_dispatch.assert_not_called()
        reconcile.assert_not_called()
        refill.assert_called_once_with("owner")
        task = self.get_task("waiting")
        self.assertEqual(task.status, "queued")
        self.assertIsNone(task.queue_job_id)
        self.assertIn("排队中", task.message)

    def test_thread_mode_concurrency_rejection_keeps_delayed_retry(self):
        now = datetime.now()
        self.add_task(
            "running-thread",
            status="running",
            created_at=now - timedelta(minutes=2),
        )
        self.add_task(
            "waiting-thread",
            created_at=now - timedelta(minutes=1),
        )
        direct_dispatch = Mock()

        with (
            patch.object(crawler_service, "session_scope", self.session_scope),
            patch.object(crawler_service.settings, "task_queue_mode", "thread"),
            patch.object(
                crawler_service.settings,
                "max_running_crawl_tasks_per_user",
                1,
            ),
            patch.object(
                crawler_service,
                "dispatch_crawl_task",
                direct_dispatch,
            ),
        ):
            crawler_service.run_task("waiting-thread")

        direct_dispatch.assert_called_once_with(
            "waiting-thread",
            delay_seconds=5.0,
        )
        task = self.get_task("waiting-thread")
        self.assertEqual(task.status, "queued")
        self.assertIn("排队中", task.message)

    def test_stale_job_cannot_clear_newer_reservation(self):
        self.add_task(
            "reserved",
            queue_job_id="crawl-reserved-new",
        )
        collect = Mock(return_value=[])
        refill = Mock(return_value=0)

        with (
            patch.object(crawler_service, "session_scope", self.session_scope),
            patch.object(crawler_service.settings, "task_queue_mode", "redis"),
            patch.object(crawler_service, "collect_items", collect),
            patch.object(
                crawler_service,
                "dispatch_queued_crawl_tasks_safely",
                refill,
            ),
        ):
            crawler_service.run_task("reserved", "crawl-reserved-old")

        collect.assert_not_called()
        task = self.get_task("reserved")
        self.assertEqual(task.status, "queued")
        self.assertEqual(task.queue_job_id, "crawl-reserved-new")

    def test_redis_job_without_reservation_cannot_start_task(self):
        self.add_task("unreserved")
        collect = Mock(return_value=[])
        refill = Mock(return_value=0)

        with (
            patch.object(crawler_service, "session_scope", self.session_scope),
            patch.object(crawler_service.settings, "task_queue_mode", "redis"),
            patch.object(crawler_service, "collect_items", collect),
            patch.object(
                crawler_service,
                "dispatch_queued_crawl_tasks_safely",
                refill,
            ),
        ):
            crawler_service.run_task("unreserved")

        collect.assert_not_called()
        refill.assert_called_once_with("owner")
        task = self.get_task("unreserved")
        self.assertEqual(task.status, "queued")
        self.assertIsNone(task.queue_job_id)

    def test_completed_task_refills_crawl_capacity(self):
        self.add_task(
            "complete",
            queue_job_id="crawl-complete-reserved",
        )
        refill = Mock(return_value=1)
        reconcile = Mock(return_value=0)

        with (
            patch.object(crawler_service, "session_scope", self.session_scope),
            patch.object(crawler_service.settings, "task_queue_mode", "redis"),
            patch.object(
                crawler_service.settings,
                "max_running_crawl_tasks_per_user",
                3,
            ),
            patch.object(crawler_service, "collect_items", return_value=[]),
            patch.object(crawler_service, "log_event"),
            patch.object(
                crawler_service,
                "dispatch_queued_crawl_tasks_safely",
                refill,
            ),
            patch.object(
                crawler_service,
                "reconcile_interrupted_running_tasks",
                reconcile,
            ),
        ):
            crawler_service.run_task("complete", "crawl-complete-reserved")

        reconcile.assert_not_called()
        refill.assert_called_once_with("owner")
        task = self.get_task("complete")
        self.assertEqual(task.status, "success")
        self.assertIsNone(task.queue_job_id)


class CrawlRecoveryTests(CrawlDispatchDatabaseTestCase):
    def test_missing_job_recovery_ignores_unreserved_backlog(self):
        self.add_task(
            "waiting",
            created_at=datetime.now() - timedelta(minutes=10),
        )
        states = Mock(return_value={})
        direct_dispatch = Mock()

        with (
            patch.object(crawler_service.settings, "task_queue_mode", "redis"),
            patch.object(crawler_service, "redis_task_states", states),
            patch.object(crawler_service, "dispatch_crawl_task", direct_dispatch),
        ):
            with self.session_scope() as session:
                recovered = crawler_service.reconcile_missing_queued_tasks(
                    session,
                    CrawlTaskModel,
                )

        self.assertEqual(recovered, 0)
        states.assert_not_called()
        direct_dispatch.assert_not_called()
        self.assertIsNone(self.get_task("waiting").queue_job_id)

    def test_missing_reserved_job_clears_reservation_without_direct_requeue(self):
        self.add_task(
            "reserved",
            queue_job_id="crawl-reserved-missing",
            created_at=datetime.now() - timedelta(minutes=10),
        )
        direct_dispatch = Mock()

        with (
            patch.object(crawler_service.settings, "task_queue_mode", "redis"),
            patch.object(crawler_service, "redis_connection", return_value=object()),
            patch.object(crawler_service, "fetch_rq_job", return_value=None),
            patch.object(crawler_service, "dispatch_crawl_task", direct_dispatch),
        ):
            with self.session_scope() as session:
                recovered = crawler_service.reconcile_missing_queued_tasks(
                    session,
                    CrawlTaskModel,
                )

        self.assertEqual(recovered, 1)
        direct_dispatch.assert_not_called()
        task = self.get_task("reserved")
        self.assertIsNone(task.queue_job_id)
        self.assertEqual(task.status, "queued")
        self.assertIn("等待重新投递", task.message)

    def test_reserved_job_recovery_fetches_exact_persisted_job_id(self):
        self.add_task(
            "exact-reservation",
            queue_job_id="crawl-exact-reservation",
            created_at=datetime.now() - timedelta(minutes=10),
        )
        connection = object()
        fetch = Mock(return_value=None)
        bulk_states = Mock(
            return_value={
                "exact-reservation": {
                    "status": "queued",
                }
            }
        )

        with (
            patch.object(crawler_service.settings, "task_queue_mode", "redis"),
            patch.object(crawler_service, "redis_connection", return_value=connection),
            patch.object(crawler_service, "fetch_rq_job", fetch),
            patch.object(crawler_service, "redis_task_states", bulk_states),
            patch.object(crawler_service, "dispatch_crawl_task"),
        ):
            with self.session_scope() as session:
                recovered = crawler_service.reconcile_missing_queued_tasks(
                    session,
                    CrawlTaskModel,
                )

        self.assertEqual(recovered, 1)
        fetch.assert_called_once_with(connection, "crawl-exact-reservation")
        bulk_states.assert_not_called()
        self.assertIsNone(self.get_task("exact-reservation").queue_job_id)

    def test_failed_reserved_job_clears_reservation(self):
        self.add_task(
            "failed-reservation",
            queue_job_id="crawl-failed-reservation",
            created_at=datetime.now() - timedelta(minutes=10),
        )
        failed_job = SimpleNamespace(
            id="crawl-failed-reservation",
            description="采集任务 failed-reservation",
            started_at=None,
            ended_at=None,
            exc_info="worker failed before task start",
            get_status=lambda refresh=True: "failed",
        )

        with (
            patch.object(crawler_service.settings, "task_queue_mode", "redis"),
            patch.object(crawler_service, "redis_connection", return_value=object()),
            patch.object(crawler_service, "fetch_rq_job", return_value=failed_job),
            patch.object(crawler_service, "dispatch_crawl_task") as direct_dispatch,
        ):
            with self.session_scope() as session:
                recovered = crawler_service.reconcile_missing_queued_tasks(
                    session,
                    CrawlTaskModel,
                )

        self.assertEqual(recovered, 1)
        direct_dispatch.assert_not_called()
        task = self.get_task("failed-reservation")
        self.assertIsNone(task.queue_job_id)
        self.assertEqual(task.status, "queued")

    def test_periodic_maintenance_refills_crawl_capacity(self):
        refill = Mock(return_value=3)

        with (
            patch.object(
                crawler_service,
                "reconcile_interrupted_background_tasks_once",
                return_value=0,
            ),
            patch.object(
                crawler_service,
                "cleanup_expired_product_image_drafts_if_due",
                return_value=0,
            ),
            patch.object(
                crawler_service,
                "cleanup_orphan_product_image_dirs_if_due",
                return_value=0,
            ),
            patch.object(
                crawler_service,
                "cleanup_completed_scheduled_crawl_tasks_if_due",
                return_value=0,
            ),
            patch.object(
                crawler_service,
                "cleanup_store_unlisted_products_if_due",
                return_value=0,
            ),
            patch.object(
                crawler_service,
                "cleanup_deleted_product_images",
                return_value={"taskCount": 0, "productCount": 0},
            ),
            patch.object(
                crawler_service,
                "dispatch_queued_crawl_tasks_safely",
                refill,
            ),
        ):
            crawler_service.run_periodic_maintenance_once()

        refill.assert_called_once_with()


class ScheduledCrawlCleanupTests(CrawlDispatchDatabaseTestCase):
    def test_cleanup_deletes_only_old_terminal_scheduled_tasks(self):
        now = datetime.now()
        old = now - timedelta(days=8)
        recent = now - timedelta(days=1)
        self.add_task(
            "queued-old",
            status="queued",
            queue_job_id="crawl-queued-old",
            created_at=old,
        )
        self.add_task(
            "running-old",
            status="running",
            created_at=old,
        )
        self.add_task(
            "success-recent",
            status="success",
            created_at=old,
            finished_at=recent,
        )
        self.add_task(
            "success-old",
            status="success",
            created_at=old,
            finished_at=old,
        )
        self.add_task(
            "failed-old",
            status="failed",
            created_at=old,
            finished_at=old,
        )
        self.add_task(
            "manual-old",
            status="success",
            created_at=old,
            finished_at=old,
            mode="manual",
        )
        remove_jobs = Mock(return_value=2)

        with (
            patch.object(crawler_service, "session_scope", self.session_scope),
            patch.object(
                crawler_service,
                "remove_crawl_queue_jobs_for_task_ids",
                remove_jobs,
            ),
        ):
            deleted = crawler_service.cleanup_completed_scheduled_crawl_tasks(
                force=True
            )

        self.assertEqual(deleted, 2)
        remove_jobs.assert_called_once_with({"success-old", "failed-old"})
        self.assertIsNotNone(self.get_task("queued-old"))
        self.assertIsNotNone(self.get_task("running-old"))
        self.assertIsNotNone(self.get_task("success-recent"))
        self.assertIsNone(self.get_task("success-old"))
        self.assertIsNone(self.get_task("failed-old"))
        self.assertIsNotNone(self.get_task("manual-old"))


if __name__ == "__main__":
    unittest.main()
