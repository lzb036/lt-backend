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


def test_crawl_queues_use_independent_long_job_timeout(monkeypatch) -> None:
    monkeypatch.setattr(task_queue.settings, "task_queue_job_timeout_seconds", 10800)
    monkeypatch.setattr(
        task_queue.settings,
        "task_queue_crawl_job_timeout_seconds",
        86400,
    )

    for kind in ("manual-crawl", "scheduled-crawl", "crawl"):
        assert task_queue.task_queue_job_timeout_for_name(
            task_queue.task_queue_name_for_kind(kind)
        ) == 86400
    assert task_queue.task_queue_job_timeout_for_name(
        task_queue.task_queue_name_for_kind("listing")
    ) == 10800
    assert task_queue.task_queue_job_timeout_for_name(
        task_queue.task_queue_name_for_kind("sync")
    ) == 10800


def test_enqueue_uses_crawl_specific_job_timeout(monkeypatch) -> None:
    captured = {}

    class FakeJob:
        id = "job-id"

    class FakeQueue:
        def enqueue(self, *_args, **kwargs):
            captured["immediate"] = kwargs
            return FakeJob()

        def enqueue_in(self, *_args, **kwargs):
            captured["delayed"] = kwargs
            return FakeJob()

    monkeypatch.setattr(task_queue.settings, "task_queue_job_timeout_seconds", 10800)
    monkeypatch.setattr(
        task_queue.settings,
        "task_queue_crawl_job_timeout_seconds",
        86400,
    )
    monkeypatch.setattr(task_queue, "task_queue", lambda _queue_name=None: FakeQueue())

    job_id = task_queue.enqueue_task(
        lambda: None,
        queue_name=task_queue.task_queue_name_for_kind("manual-crawl"),
    )

    assert job_id == "job-id"
    assert captured["immediate"]["job_timeout"] == 86400

    delayed_job_id = task_queue.enqueue_task_in(
        30,
        lambda: None,
        queue_name=task_queue.task_queue_name_for_kind("scheduled-crawl"),
    )

    assert delayed_job_id == "job-id"
    assert captured["delayed"]["job_timeout"] == 86400


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
        "LT_TASK_QUEUE_CRAWL_JOB_TIMEOUT_SECONDS=86400",
    ):
        assert setting in env_example
