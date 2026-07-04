from __future__ import annotations

from collections.abc import Callable
from collections.abc import Iterable
from typing import Any

from app.core.config import settings


QUEUE_KIND_NAMES = {
    "default": "task_queue_name",
    "crawl": "task_queue_crawl_name",
    "sync": "task_queue_sync_name",
    "listing": "task_queue_listing_name",
    "schedule": "task_queue_schedule_name",
}


def redis_connection() -> Any:
    from redis import Redis

    return Redis.from_url(settings.redis_url)


def task_queue_name_for_kind(kind: str | None = None) -> str:
    raw_name = (kind or "default").strip()
    normalized = raw_name.lower()
    setting_name = QUEUE_KIND_NAMES.get(normalized)
    if setting_name is None:
        return raw_name
    return str(getattr(settings, setting_name))


def unique_queue_names(queue_names: Iterable[str]) -> list[str]:
    result: list[str] = []
    seen: set[str] = set()
    for queue_name in queue_names:
        normalized = str(queue_name or "").strip()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        result.append(normalized)
    return result


def all_task_queue_names() -> list[str]:
    return unique_queue_names(
        [
            settings.task_queue_listing_name,
            settings.task_queue_sync_name,
            settings.task_queue_crawl_name,
            settings.task_queue_schedule_name,
            settings.task_queue_name,
        ]
    )


def resolve_worker_queue_names(queue_names: Iterable[str] | None = None) -> list[str]:
    if queue_names is None:
        return all_task_queue_names()
    values: list[str] = []
    for queue_name in queue_names:
        values.extend(part.strip() for part in str(queue_name or "").split(","))
    resolved = [task_queue_name_for_kind(value) for value in values if value]
    return unique_queue_names(resolved) or all_task_queue_names()


def task_queue(queue_name: str | None = None) -> Any:
    from rq import Queue

    return Queue(
        queue_name or settings.task_queue_name,
        connection=redis_connection(),
        default_timeout=settings.task_queue_job_timeout_seconds,
    )


def enqueue_task(
    func: Callable[..., Any],
    *args: Any,
    job_id: str | None = None,
    description: str = "",
    queue_name: str | None = None,
) -> str:
    job = task_queue(queue_name).enqueue(
        func,
        args=args,
        job_id=job_id,
        job_timeout=settings.task_queue_job_timeout_seconds,
        result_ttl=settings.task_queue_result_ttl_seconds,
        failure_ttl=settings.task_queue_failure_ttl_seconds,
        description=description or None,
    )
    return job.id


def run_worker(queue_names: Iterable[str] | None = None) -> None:
    from rq import Queue, Worker

    connection = redis_connection()
    queues = [
        Queue(
            queue_name,
            connection=connection,
            default_timeout=settings.task_queue_job_timeout_seconds,
        )
        for queue_name in resolve_worker_queue_names(queue_names)
    ]
    worker = Worker(queues, connection=connection)
    worker.work(with_scheduler=True)
