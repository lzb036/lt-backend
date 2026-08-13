from __future__ import annotations

from datetime import datetime
from typing import Any

from app.db.database import SessionLocal
from app.db.models import SystemMaintenanceSettingModel


MAINTENANCE_SETTING_ID = 1
DEFAULT_TITLE = "系统维护中"
DEFAULT_MESSAGE = "系统正在进行维护升级，请稍后再试。"


def get_maintenance_status(*, now: datetime | None = None) -> dict[str, Any]:
    with SessionLocal() as session:
        row = session.get(SystemMaintenanceSettingModel, MAINTENANCE_SETTING_ID)
        return maintenance_setting_to_public(row, now=now)


def save_maintenance_settings(
    *,
    enabled: bool,
    title: str,
    message: str,
    starts_at: datetime | None,
    estimated_ends_at: datetime | None,
    updated_by: str,
    now: datetime | None = None,
) -> dict[str, Any]:
    normalized_title = str(title or "").strip()
    normalized_message = str(message or "").strip()
    if not normalized_title:
        raise RuntimeError("请输入维护提示标题。")
    if not normalized_message:
        raise RuntimeError("请输入维护提示内容。")
    starts_at = normalized_local_datetime(starts_at)
    estimated_ends_at = normalized_local_datetime(estimated_ends_at)
    if starts_at and estimated_ends_at and estimated_ends_at <= starts_at:
        raise RuntimeError("预计维护完成时间必须晚于开始维护时间。")

    with SessionLocal() as session:
        row = session.get(SystemMaintenanceSettingModel, MAINTENANCE_SETTING_ID)
        if row is None:
            row = SystemMaintenanceSettingModel(id=MAINTENANCE_SETTING_ID)
            session.add(row)
        row.enabled = bool(enabled)
        row.title = normalized_title
        row.message = normalized_message
        row.starts_at = starts_at
        row.estimated_ends_at = estimated_ends_at
        row.updated_by = str(updated_by or "").strip()
        session.commit()
        session.refresh(row)
        return maintenance_setting_to_public(row, now=now)


def maintenance_setting_to_public(
    row: SystemMaintenanceSettingModel | None,
    *,
    now: datetime | None = None,
) -> dict[str, Any]:
    current_time = now or datetime.now()
    enabled = bool(row.enabled) if row is not None else False
    starts_at = row.starts_at if row is not None else None
    active = enabled and (starts_at is None or starts_at <= current_time)
    scheduled = enabled and starts_at is not None and starts_at > current_time
    return {
        "enabled": enabled,
        "active": active,
        "scheduled": scheduled,
        "title": (row.title if row is not None else DEFAULT_TITLE) or DEFAULT_TITLE,
        "message": (row.message if row is not None else DEFAULT_MESSAGE) or DEFAULT_MESSAGE,
        "startsAt": datetime_to_public(starts_at),
        "estimatedEndsAt": datetime_to_public(row.estimated_ends_at if row is not None else None),
        "updatedBy": row.updated_by if row is not None else "",
        "updatedAt": datetime_to_public(row.updated_at if row is not None else None),
        "serverTime": datetime_to_public(current_time),
    }


def datetime_to_public(value: datetime | None) -> str | None:
    return value.isoformat(timespec="seconds") if value is not None else None


def normalized_local_datetime(value: datetime | None) -> datetime | None:
    if value is None or value.tzinfo is None:
        return value
    return value.astimezone().replace(tzinfo=None)


def get_task_control_status() -> dict[str, Any]:
    from app.services.task_control_service import get_task_control_status as get_status

    return get_status()


def stop_all_tasks(
    *,
    operated_by: str,
    usernames: list[str],
) -> dict[str, Any]:
    from app.services.task_control_service import stop_all_tasks as stop_tasks

    return stop_tasks(operated_by=operated_by, usernames=usernames)


def resume_all_tasks(*, operated_by: str) -> dict[str, Any]:
    from app.services.task_control_service import resume_all_tasks as resume_tasks

    return resume_tasks(operated_by=operated_by)
