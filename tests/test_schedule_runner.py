from __future__ import annotations

import json
import logging

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.db.database import Base
from app.db.models import SystemTaskControlModel
from app.services import crawler_service


def _reset_health() -> None:
    with crawler_service.SCHEDULE_RUNNER_HEALTH_LOCK:
        crawler_service.SCHEDULE_RUNNER_ACTIVE = False
        crawler_service.SCHEDULE_RUNNER_HEALTH.update(
            {
                "lastTickAt": None,
                "lastSuccessfulTickAt": None,
                "lastError": "",
                "consecutiveFailures": 0,
            }
        )


def test_schedule_runner_continues_after_task_failure(monkeypatch, caplog) -> None:
    _reset_health()
    calls: list[str] = []
    monkeypatch.setattr(
        crawler_service,
        "system_task_dispatch_paused",
        lambda: False,
    )

    def fail() -> None:
        calls.append("crawl")
        raise RuntimeError("sensitive detail")

    monkeypatch.setattr(crawler_service, "run_due_scheduled_crawls_once", fail)
    monkeypatch.setattr(
        crawler_service,
        "run_due_auto_listing_schedules_once",
        lambda: calls.append("auto-listing"),
    )
    monkeypatch.setattr(
        crawler_service,
        "run_due_auto_deletion_tasks_once",
        lambda: calls.append("auto-deletion"),
    )
    monkeypatch.setattr(
        crawler_service,
        "run_due_sales_order_syncs_once",
        lambda: calls.append("sales"),
    )
    monkeypatch.setattr(
        crawler_service,
        "run_due_store_product_syncs_once",
        lambda: calls.append("products"),
    )
    monkeypatch.setattr(
        crawler_service,
        "run_periodic_maintenance_once",
        lambda: calls.append("maintenance"),
    )

    with caplog.at_level(logging.ERROR):
        assert crawler_service.run_schedule_runner_tick() is False

    assert calls == ["crawl", "auto-listing", "auto-deletion", "sales", "products", "maintenance"]
    assert "Schedule runner task failed: scheduled crawls" in caplog.text
    health = crawler_service.schedule_runner_health()
    assert health["consecutiveFailures"] == 1
    assert health["lastError"] == "scheduled crawls: RuntimeError"
    assert "sensitive detail" not in health["lastError"]


def test_successful_schedule_tick_resets_failure_health(monkeypatch) -> None:
    _reset_health()
    monkeypatch.setattr(
        crawler_service,
        "system_task_dispatch_paused",
        lambda: False,
    )
    with crawler_service.SCHEDULE_RUNNER_HEALTH_LOCK:
        crawler_service.SCHEDULE_RUNNER_HEALTH["consecutiveFailures"] = 3
        crawler_service.SCHEDULE_RUNNER_HEALTH["lastError"] = "old error"

    monkeypatch.setattr(crawler_service, "run_due_scheduled_crawls_once", lambda: 0)
    monkeypatch.setattr(crawler_service, "run_due_auto_listing_schedules_once", lambda: 0)
    monkeypatch.setattr(crawler_service, "run_due_auto_deletion_tasks_once", lambda: 0)
    monkeypatch.setattr(crawler_service, "run_due_sales_order_syncs_once", lambda: 0)
    monkeypatch.setattr(crawler_service, "run_due_store_product_syncs_once", lambda: 0)
    monkeypatch.setattr(crawler_service, "run_periodic_maintenance_once", lambda: None)

    assert crawler_service.run_schedule_runner_tick() is True

    health = crawler_service.schedule_runner_health()
    assert health["consecutiveFailures"] == 0
    assert health["lastError"] == ""
    assert health["lastSuccessfulTickAt"]


def test_schedule_runner_continues_while_selected_users_are_paused(
    monkeypatch,
) -> None:
    _reset_health()
    calls: list[str] = []
    monkeypatch.setattr(
        crawler_service,
        "run_due_scheduled_crawls_once",
        lambda: calls.append("crawl"),
    )
    monkeypatch.setattr(
        crawler_service,
        "run_due_auto_listing_schedules_once",
        lambda: calls.append("auto-listing"),
    )
    monkeypatch.setattr(
        crawler_service,
        "run_due_auto_deletion_tasks_once",
        lambda: calls.append("auto-deletion"),
    )
    monkeypatch.setattr(
        crawler_service,
        "run_due_sales_order_syncs_once",
        lambda: calls.append("sales"),
    )
    monkeypatch.setattr(
        crawler_service,
        "run_due_store_product_syncs_once",
        lambda: calls.append("products"),
    )
    monkeypatch.setattr(
        crawler_service,
        "run_periodic_maintenance_once",
        lambda: calls.append("maintenance"),
    )

    assert crawler_service.run_schedule_runner_tick() is True
    assert calls == [
        "crawl",
        "auto-listing",
        "auto-deletion",
        "sales",
        "products",
        "maintenance",
    ]
    assert crawler_service.schedule_runner_health()["active"] is False


def test_system_task_paused_usernames_reads_selected_snapshot(
    monkeypatch,
) -> None:
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    Base.metadata.create_all(engine)
    factory = sessionmaker(bind=engine, expire_on_commit=False, future=True)

    def local_session_scope():
        session = factory()

        class SessionContext:
            def __enter__(self):
                return session

            def __exit__(self, exc_type, exc, traceback):
                session.close()

        return SessionContext()

    with factory() as session:
        session.add(
            SystemTaskControlModel(
                id=1,
                paused=True,
                phase="paused",
                snapshot_json=json.dumps(
                    {"selectedUsernames": ["alice", "bob"]}
                ),
            )
        )
        session.commit()

    monkeypatch.setattr(
        crawler_service,
        "session_scope",
        local_session_scope,
    )

    assert crawler_service.system_task_paused_usernames() == {
        "alice",
        "bob",
    }
