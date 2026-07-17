from __future__ import annotations

import json
from datetime import datetime
from types import SimpleNamespace
from typing import Any

from sqlalchemy import func, select

from app.db.database import session_scope
from app.db.models import SalesOrderSyncRunModel, SystemSettingModel


GLOBAL_SETTINGS_KEY = "sales_order_sync_settings"
DEFAULT_GLOBAL_SETTINGS = {
    "enabled": True,
    "intervalMinutes": 30,
    "successRetentionDays": 30,
}
TRIGGER_TYPES = {"automatic", "manual", "retry"}
RUN_STATUSES = {
    "queued",
    "running",
    "success",
    "partial",
    "failed",
    "cancelled",
}
ACTIVE_RUN_STATUSES = {"queued", "running"}


def _validated_settings(payload: Any) -> dict[str, Any]:
    enabled = getattr(payload, "enabled", None)
    if not isinstance(enabled, bool):
        raise ValueError("自动同步启用状态必须是布尔值。")
    interval_minutes = getattr(payload, "intervalMinutes", None)
    if (
        not isinstance(interval_minutes, int)
        or isinstance(interval_minutes, bool)
        or not 5 <= interval_minutes <= 1440
    ):
        raise ValueError("同步间隔必须在 5 至 1440 分钟之间。")
    retention_days = getattr(payload, "successRetentionDays", None)
    if (
        not isinstance(retention_days, int)
        or isinstance(retention_days, bool)
        or not 1 <= retention_days <= 365
    ):
        raise ValueError("成功记录保留天数必须在 1 至 365 天之间。")
    return {
        "enabled": enabled,
        "intervalMinutes": interval_minutes,
        "successRetentionDays": retention_days,
    }


def _settings_from_row(row: SystemSettingModel | None) -> dict[str, Any]:
    if row is None:
        return dict(DEFAULT_GLOBAL_SETTINGS)
    try:
        raw = json.loads(row.value_json or "{}")
        if not isinstance(raw, dict):
            raise ValueError
        return _validated_settings(
            SimpleNamespace(
                enabled=raw.get("enabled"),
                intervalMinutes=raw.get("intervalMinutes"),
                successRetentionDays=raw.get("successRetentionDays"),
            )
        )
    except (TypeError, ValueError):
        return dict(DEFAULT_GLOBAL_SETTINGS)


def get_global_settings() -> dict[str, Any]:
    with session_scope() as session:
        return _settings_from_row(
            session.get(SystemSettingModel, GLOBAL_SETTINGS_KEY)
        )


def save_global_settings(payload: Any) -> dict[str, Any]:
    settings = _validated_settings(payload)
    with session_scope() as session:
        row = session.get(SystemSettingModel, GLOBAL_SETTINGS_KEY)
        if row is None:
            row = SystemSettingModel(key=GLOBAL_SETTINGS_KEY)
            session.add(row)
        row.value_json = json.dumps(settings, ensure_ascii=False)
        session.flush()
    return settings


def _run_to_public(row: SalesOrderSyncRunModel) -> dict[str, Any]:
    return {
        "id": row.id,
        "ownerUsername": row.owner_username,
        "storeId": row.store_id,
        "storeName": row.store_name,
        "triggerType": row.trigger_type,
        "parentRunId": row.parent_run_id,
        "status": row.status,
        "initialSync": bool(row.initial_sync),
        "progressCurrent": row.progress_current,
        "progressTotal": row.progress_total,
        "totalOrderCount": row.total_order_count,
        "newOrderCount": row.new_order_count,
        "updatedOrderCount": row.updated_order_count,
        "unchangedOrderCount": row.unchanged_order_count,
        "failedOrderCount": row.failed_order_count,
        "message": row.message,
        "errorDetail": row.error_detail,
        "startedAt": row.started_at,
        "finishedAt": row.finished_at,
        "createdAt": row.created_at,
        "updatedAt": row.updated_at,
    }


def list_runs(
    owner_username: str,
    *,
    page: int = 1,
    page_size: int = 30,
    store_id: int | None = None,
    trigger_type: str | None = None,
    status: str | None = None,
    created_at_from: datetime | None = None,
    created_at_to: datetime | None = None,
) -> dict[str, Any]:
    if page < 1 or page_size < 1 or page_size > 100:
        raise ValueError("分页参数无效。")
    if trigger_type is not None and trigger_type not in TRIGGER_TYPES:
        raise ValueError("触发方式不受支持。")
    if status is not None and status not in RUN_STATUSES:
        raise ValueError("同步状态不受支持。")
    if (
        created_at_from is not None
        and created_at_to is not None
        and created_at_from > created_at_to
    ):
        raise ValueError("创建时间范围无效。")

    filters = [SalesOrderSyncRunModel.owner_username == owner_username]
    if store_id is not None:
        filters.append(SalesOrderSyncRunModel.store_id == store_id)
    if trigger_type is not None:
        filters.append(SalesOrderSyncRunModel.trigger_type == trigger_type)
    if status is not None:
        filters.append(SalesOrderSyncRunModel.status == status)
    if created_at_from is not None:
        filters.append(SalesOrderSyncRunModel.created_at >= created_at_from)
    if created_at_to is not None:
        filters.append(SalesOrderSyncRunModel.created_at <= created_at_to)

    with session_scope() as session:
        total = session.scalar(
            select(func.count())
            .select_from(SalesOrderSyncRunModel)
            .where(*filters)
        )
        rows = session.scalars(
            select(SalesOrderSyncRunModel)
            .where(*filters)
            .order_by(
                SalesOrderSyncRunModel.created_at.desc(),
                SalesOrderSyncRunModel.id.desc(),
            )
            .offset((page - 1) * page_size)
            .limit(page_size)
        ).all()
        return {
            "rows": [_run_to_public(row) for row in rows],
            "total": int(total or 0),
            "page": page,
            "pageSize": page_size,
        }


def delete_runs(owner_username: str, run_ids: list[str]) -> dict[str, int]:
    normalized_ids = list(
        dict.fromkeys(
            run_id.strip()
            for run_id in run_ids
            if isinstance(run_id, str) and run_id.strip()
        )
    )
    if not normalized_ids:
        raise ValueError("请选择要删除的订单同步记录。")
    if len(normalized_ids) > 100:
        raise ValueError("单次最多删除 100 条订单同步记录。")

    with session_scope() as session:
        rows = session.scalars(
            select(SalesOrderSyncRunModel).where(
                SalesOrderSyncRunModel.owner_username == owner_username,
                SalesOrderSyncRunModel.id.in_(normalized_ids),
            )
        ).all()
        if any(row.status in ACTIVE_RUN_STATUSES for row in rows):
            raise ValueError("运行中的订单同步记录不可删除。")
        for row in rows:
            session.delete(row)
        session.flush()
        return {"deletedCount": len(rows)}
