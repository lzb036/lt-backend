from __future__ import annotations

import unittest
from contextlib import contextmanager
from datetime import datetime
from unittest.mock import patch

from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session, sessionmaker

from app.db.database import Base
from app.db.models import CrawlTaskModel, ScheduledCrawlModel
from app.services import crawler_service


class ScheduledCrawlStatusTests(unittest.TestCase):
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

    def add_schedule(
        self,
        *,
        owner: str,
        enabled: bool = True,
        status: str = "idle",
        next_run_at: datetime | None = None,
    ) -> int:
        with self.session_scope() as session:
            row = ScheduledCrawlModel(
                owner_username=owner,
                name=f"{owner} schedule",
                crawl_content=owner,
                crawl_condition="店铺采集",
                source_type="shop",
                target=f"店铺:https://www.rakuten.co.jp/{owner}/ 日榜 全部",
                enabled=enabled,
                interval_minutes=1440,
                schedule_time="20:00",
                next_run_at=next_run_at,
                status=status,
                notes="",
            )
            session.add(row)
            session.flush()
            return int(row.id)

    def add_running_crawl_task(self, task_id: str) -> None:
        with self.session_scope() as session:
            session.add(
                CrawlTaskModel(
                    id=task_id,
                    owner_username="owner",
                    source_type="shop",
                    target="店铺:https://www.rakuten.co.jp/owner/ 日榜 全部",
                    mode="scheduled",
                    status="running",
                    total_count=10,
                    success_count=1,
                    failed_count=0,
                    warning_count=0,
                    message="正在采集",
                    started_at=datetime(2026, 7, 11, 18, 0),
                )
            )

    def get_schedule(self, schedule_id: int) -> ScheduledCrawlModel:
        with Session(self.engine, future=True) as session:
            row = session.get(ScheduledCrawlModel, schedule_id)
            self.assertIsNotNone(row)
            return row

    def get_crawl_task(self, task_id: str) -> CrawlTaskModel:
        with Session(self.engine, future=True) as session:
            row = session.scalar(
                select(CrawlTaskModel).where(CrawlTaskModel.id == task_id)
            )
            self.assertIsNotNone(row)
            return row

    def test_batch_enable_recalculates_next_run(self):
        schedule_id = self.add_schedule(
            owner="owner",
            enabled=False,
            status="disabled",
            next_run_at=None,
        )
        expected_next_run = datetime(2026, 7, 12, 20, 0)

        with (
            patch.object(crawler_service, "session_scope", self.session_scope),
            patch.object(
                crawler_service,
                "next_daily_run_at",
                return_value=expected_next_run,
            ),
        ):
            result = crawler_service.update_scheduled_crawl_statuses(
                "owner",
                [schedule_id],
                True,
            )

        row = self.get_schedule(schedule_id)
        self.assertTrue(row.enabled)
        self.assertEqual(row.status, "idle")
        self.assertEqual(row.next_run_at, expected_next_run)
        self.assertEqual(result["updatedIds"], [schedule_id])
        self.assertEqual(result["failedIds"], [])
        self.assertEqual(result["updatedCount"], 1)
        self.assertTrue(result["enabled"])

    def test_batch_disable_does_not_cancel_running_crawl_task(self):
        schedule_id = self.add_schedule(
            owner="owner",
            enabled=True,
            status="running",
            next_run_at=datetime(2026, 7, 12, 20, 0),
        )
        self.add_running_crawl_task("running-task")

        with patch.object(crawler_service, "session_scope", self.session_scope):
            result = crawler_service.update_scheduled_crawl_statuses(
                "owner",
                [schedule_id],
                False,
            )

        row = self.get_schedule(schedule_id)
        task = self.get_crawl_task("running-task")
        self.assertFalse(row.enabled)
        self.assertEqual(row.status, "disabled")
        self.assertIsNone(row.next_run_at)
        self.assertEqual(task.status, "running")
        self.assertEqual(result["updatedCount"], 1)
        self.assertFalse(result["enabled"])

    def test_batch_status_normalizes_ids_and_reports_inaccessible_rows(self):
        owned_id = self.add_schedule(owner="owner")
        other_id = self.add_schedule(owner="other")

        with patch.object(crawler_service, "session_scope", self.session_scope):
            result = crawler_service.update_scheduled_crawl_statuses(
                "owner",
                [owned_id, owned_id, other_id, 999999, 0, -1],
                False,
            )

        self.assertEqual(result["updatedIds"], [owned_id])
        self.assertEqual(result["failedIds"], [other_id, 999999])
        self.assertEqual(result["updatedCount"], 1)

    def test_batch_status_rejects_empty_normalized_ids(self):
        with (
            patch.object(crawler_service, "session_scope", self.session_scope),
            self.assertRaisesRegex(RuntimeError, "请选择要启用或停用的采集店铺"),
        ):
            crawler_service.update_scheduled_crawl_statuses(
                "owner",
                [0, -1],
                True,
            )


if __name__ == "__main__":
    unittest.main()
