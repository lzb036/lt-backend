from __future__ import annotations

from typing import Any

from sqlalchemy import case


RUNNING_STATUSES = ("running", "processing")
PENDING_STATUSES = ("queued", "idle", "pending", "waiting", "preview_ready")
FAILED_STATUSES = ("failed", "partial", "error", "cancelled")
SUCCESS_STATUSES = ("success", "completed")
DISABLED_STATUSES = ("disabled",)


def task_status_priority(
    status_column: Any,
    *,
    disabled_condition: Any | None = None,
    failure_condition: Any | None = None,
) -> Any:
    conditions: list[tuple[Any, int]] = [
        (status_column.in_(RUNNING_STATUSES), 0),
    ]
    if disabled_condition is not None:
        conditions.append((disabled_condition, 4))
    conditions.append((status_column.in_(PENDING_STATUSES), 1))
    if failure_condition is not None:
        conditions.append((failure_condition, 2))
    conditions.extend(
        [
            (status_column.in_(FAILED_STATUSES), 2),
            (status_column.in_(SUCCESS_STATUSES), 3),
            (status_column.in_(DISABLED_STATUSES), 4),
        ]
    )
    return case(*conditions, else_=5)


def task_status_order_by(
    model: Any,
    *,
    disabled_condition: Any | None = None,
    failure_condition: Any | None = None,
) -> tuple[Any, ...]:
    return (
        task_status_priority(
            model.status,
            disabled_condition=disabled_condition,
            failure_condition=failure_condition,
        ).asc(),
        model.created_at.desc(),
        model.id.desc(),
    )
