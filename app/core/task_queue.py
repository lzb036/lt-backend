from __future__ import annotations

from collections.abc import Callable
from typing import Any

from app.core.config import settings


def redis_connection() -> Any:
    from redis import Redis

    return Redis.from_url(settings.redis_url)


def task_queue() -> Any:
    from rq import Queue

    return Queue(
        settings.task_queue_name,
        connection=redis_connection(),
        default_timeout=settings.task_queue_job_timeout_seconds,
    )


def enqueue_task(
    func: Callable[..., Any],
    *args: Any,
    job_id: str | None = None,
    description: str = "",
) -> str:
    job = task_queue().enqueue(
        func,
        args=args,
        job_id=job_id,
        job_timeout=settings.task_queue_job_timeout_seconds,
        result_ttl=settings.task_queue_result_ttl_seconds,
        failure_ttl=settings.task_queue_failure_ttl_seconds,
        description=description or None,
    )
    return job.id


def run_worker() -> None:
    from rq import Queue, Worker

    connection = redis_connection()
    queue = Queue(
        settings.task_queue_name,
        connection=connection,
        default_timeout=settings.task_queue_job_timeout_seconds,
    )
    worker = Worker([queue], connection=connection)
    worker.work(with_scheduler=True)
