from __future__ import annotations

from pathlib import Path

from app.core import task_queue
from app.services import crawler_service


ROOT = Path(__file__).resolve().parents[1]


def test_manual_and_scheduled_crawl_queue_names_are_registered() -> None:
    assert task_queue.task_queue_name_for_kind("manual-crawl") == (
        crawler_service.settings.task_queue_manual_crawl_name
    )
    assert task_queue.task_queue_name_for_kind("scheduled-crawl") == (
        crawler_service.settings.task_queue_scheduled_crawl_name
    )
    queue_names = task_queue.all_task_queue_names()
    assert crawler_service.settings.task_queue_manual_crawl_name in queue_names
    assert crawler_service.settings.task_queue_scheduled_crawl_name in queue_names
    assert crawler_service.settings.task_queue_crawl_name in queue_names


def test_queue_health_distinguishes_manual_scheduled_and_legacy_crawl() -> None:
    labels = crawler_service.task_queue_health_kind_by_name()
    assert labels[crawler_service.settings.task_queue_manual_crawl_name] == "手动采集"
    assert labels[crawler_service.settings.task_queue_scheduled_crawl_name] == "定时采集执行"
    assert labels[crawler_service.settings.task_queue_crawl_name] == "采集兼容队列"


def test_supervisor_templates_keep_total_crawl_worker_count_at_three() -> None:
    manual_template = (
        ROOT / "scripts/supervisor/lt-worker-manual-crawl.ini.example"
    ).read_text(encoding="utf-8")
    scheduled_template = (
        ROOT / "scripts/supervisor/lt-worker-scheduled-crawl.ini.example"
    ).read_text(encoding="utf-8")

    assert "worker.py manual-crawl" in manual_template
    assert "numprocs=2" in manual_template
    assert "worker.py scheduled-crawl" in scheduled_template
    assert "numprocs=1" in scheduled_template


def test_environment_example_documents_independent_limits_and_queues() -> None:
    env_example = (ROOT / ".env.example").read_text(encoding="utf-8")
    for setting in (
        "LT_MAX_RUNNING_MANUAL_CRAWL_TASKS_PER_USER=2",
        "LT_MAX_RUNNING_SCHEDULED_CRAWL_TASKS_PER_USER=1",
        "LT_TASK_QUEUE_MANUAL_CRAWL_NAME=lt-tasks-manual-crawl",
        "LT_TASK_QUEUE_SCHEDULED_CRAWL_NAME=lt-tasks-scheduled-crawl",
    ):
        assert setting in env_example
