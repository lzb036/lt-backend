from __future__ import annotations

import logging

from app.services import crawler_service


def _reset_health() -> None:
    with crawler_service.SCHEDULE_RUNNER_HEALTH_LOCK:
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

    def fail() -> None:
        calls.append("crawl")
        raise RuntimeError("sensitive detail")

    monkeypatch.setattr(crawler_service, "run_due_scheduled_crawls_once", fail)
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

    assert calls == ["crawl", "sales", "products", "maintenance"]
    assert "Schedule runner task failed: scheduled crawls" in caplog.text
    health = crawler_service.schedule_runner_health()
    assert health["consecutiveFailures"] == 1
    assert health["lastError"] == "scheduled crawls: RuntimeError"
    assert "sensitive detail" not in health["lastError"]


def test_successful_schedule_tick_resets_failure_health(monkeypatch) -> None:
    _reset_health()
    with crawler_service.SCHEDULE_RUNNER_HEALTH_LOCK:
        crawler_service.SCHEDULE_RUNNER_HEALTH["consecutiveFailures"] = 3
        crawler_service.SCHEDULE_RUNNER_HEALTH["lastError"] = "old error"

    monkeypatch.setattr(crawler_service, "run_due_scheduled_crawls_once", lambda: 0)
    monkeypatch.setattr(crawler_service, "run_due_sales_order_syncs_once", lambda: 0)
    monkeypatch.setattr(crawler_service, "run_due_store_product_syncs_once", lambda: 0)
    monkeypatch.setattr(crawler_service, "run_periodic_maintenance_once", lambda: None)

    assert crawler_service.run_schedule_runner_tick() is True

    health = crawler_service.schedule_runner_health()
    assert health["consecutiveFailures"] == 0
    assert health["lastError"] == ""
    assert health["lastSuccessfulTickAt"]
