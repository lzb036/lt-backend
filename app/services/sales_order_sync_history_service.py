from __future__ import annotations

import json
from datetime import datetime, timedelta
from types import SimpleNamespace
from threading import Lock
from typing import Any
import uuid

from sqlalchemy import delete, func, select, update
from sqlalchemy.orm import Session

from app.db.database import session_scope
from app.db.models import (
    SalesOrderModel,
    SalesOrderSyncRunModel,
    StoreModel,
    SystemSettingModel,
)
from app.services.sales_time import sales_now_naive


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
RETRYABLE_RUN_STATUSES = {"failed", "partial", "cancelled"}
RUN_STALE_AFTER = timedelta(minutes=10)
SUCCESS_CLEANUP_INTERVAL = timedelta(hours=1)
SUCCESS_CLEANUP_LOCK = Lock()
SUCCESS_CLEANUP_LAST_RUN_AT: datetime | None = None


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


def create_run(
    session: Session,
    *,
    owner_username: str,
    store: StoreModel,
    trigger_type: str,
    parent_run_id: str | None = None,
    initial_sync: bool = False,
) -> SalesOrderSyncRunModel:
    if trigger_type not in TRIGGER_TYPES:
        raise ValueError("触发方式不受支持。")
    row = SalesOrderSyncRunModel(
        id=f"sales-run-{uuid.uuid4().hex}",
        owner_username=owner_username,
        store_id=store.id,
        store_name=store.store_name,
        trigger_type=trigger_type,
        parent_run_id=parent_run_id,
        status="queued",
        initial_sync=initial_sync,
        message="等待执行。",
    )
    session.add(row)
    session.flush()
    return row


def mark_run_running(
    owner_username: str,
    run_id: str,
    *,
    initial_sync: bool,
) -> None:
    now = sales_now_naive()
    with session_scope() as session:
        updated = session.execute(
            update(SalesOrderSyncRunModel)
            .where(
                SalesOrderSyncRunModel.id == run_id,
                SalesOrderSyncRunModel.owner_username == owner_username,
                SalesOrderSyncRunModel.status == "queued",
            )
            .values(
                status="running",
                initial_sync=initial_sync,
                started_at=now,
                message="正在获取订单。",
                error_detail=None,
                updated_at=now,
            )
        ).rowcount
        if updated != 1:
            raise LookupError("订单同步记录不存在或状态已变化。")


def update_run_progress(
    owner_username: str,
    run_id: str,
    *,
    progress_current: int | None,
    progress_total: int | None,
) -> None:
    with session_scope() as session:
        update_run_progress_in_session(
            session,
            owner_username,
            run_id,
            progress_current=progress_current,
            progress_total=progress_total,
        )


def update_run_progress_in_session(
    session: Session,
    owner_username: str,
    run_id: str,
    *,
    progress_current: int | None,
    progress_total: int | None,
) -> None:
    values: dict[str, Any] = {
        "message": "正在获取订单。",
        "updated_at": sales_now_naive(),
    }
    if progress_current is not None:
        values["progress_current"] = max(
            0, int(progress_current)
        )
    if progress_total is not None:
        values["progress_total"] = max(0, int(progress_total))
    session.execute(
        update(SalesOrderSyncRunModel)
        .where(
            SalesOrderSyncRunModel.id == run_id,
            SalesOrderSyncRunModel.owner_username == owner_username,
            SalesOrderSyncRunModel.status == "running",
        )
        .values(**values)
    )


def _result_values(result: dict[str, Any]) -> dict[str, Any]:
    counts = {
        "total_order_count": max(
            0, int(result.get("totalOrderCount") or 0)
        ),
        "new_order_count": max(
            0, int(result.get("newOrderCount") or 0)
        ),
        "updated_order_count": max(
            0, int(result.get("updatedOrderCount") or 0)
        ),
        "unchanged_order_count": max(
            0, int(result.get("unchangedOrderCount") or 0)
        ),
        "failed_order_count": max(
            0, int(result.get("failedOrderCount") or 0)
        ),
    }
    successful_count = (
        counts["new_order_count"]
        + counts["updated_order_count"]
        + counts["unchanged_order_count"]
    )
    if counts["failed_order_count"] == 0:
        status = "success"
    elif successful_count > 0:
        status = "partial"
    else:
        status = "failed"
    now = sales_now_naive()
    messages = {
        "success": ("订单同步完成。", None),
        "partial": (
            "订单同步部分完成。",
            "部分订单同步失败，请重试。",
        ),
        "failed": (
            "订单同步失败，请稍后重试。",
            "订单同步失败，请稍后重试。",
        ),
    }
    message, error_detail = messages[status]
    return {
        "status": status,
        "progress_current": counts["total_order_count"],
        "progress_total": counts["total_order_count"],
        "message": message,
        "error_detail": error_detail,
        "finished_at": now,
        "updated_at": now,
        **counts,
    }


def complete_run_in_session(
    session: Session,
    owner_username: str,
    run_id: str,
    result: dict[str, Any],
) -> None:
    session.execute(
        update(SalesOrderSyncRunModel)
        .where(
            SalesOrderSyncRunModel.id == run_id,
            SalesOrderSyncRunModel.owner_username == owner_username,
            SalesOrderSyncRunModel.status.in_(("queued", "running")),
        )
        .values(**_result_values(result))
    )


def complete_run(
    owner_username: str,
    run_id: str,
    result: dict[str, Any],
) -> None:
    with session_scope() as session:
        complete_run_in_session(
            session,
            owner_username,
            run_id,
            result,
        )


def fail_run(
    owner_username: str,
    run_id: str,
    *,
    message: str = "订单同步失败，请稍后重试。",
    failed_order_count: int | None = None,
) -> None:
    now = sales_now_naive()
    values: dict[str, Any] = {
        "status": "failed",
        "message": message,
        "error_detail": message,
        "finished_at": now,
        "updated_at": now,
    }
    if failed_order_count is not None:
        values["failed_order_count"] = max(
            0, int(failed_order_count)
        )
        values["total_order_count"] = max(
            0, int(failed_order_count)
        )
    with session_scope() as session:
        session.execute(
            update(SalesOrderSyncRunModel)
            .where(
                SalesOrderSyncRunModel.id == run_id,
                SalesOrderSyncRunModel.owner_username == owner_username,
                SalesOrderSyncRunModel.status.in_(("queued", "running")),
            )
            .values(**values)
        )


def retry_run(owner_username: str, run_id: str) -> dict[str, Any]:
    with session_scope() as session:
        row = session.scalar(
            select(SalesOrderSyncRunModel).where(
                SalesOrderSyncRunModel.id == run_id,
                SalesOrderSyncRunModel.owner_username == owner_username,
            )
        )
        if row is None:
            raise LookupError("订单同步记录不存在或无权访问。")
        if row.status not in RETRYABLE_RUN_STATUSES:
            raise ValueError("当前订单同步记录不可重试。")
        if row.store_id is None:
            raise ValueError("原店铺已删除，无法重试。")
        store_id = row.store_id
    return _queue_sales_order_sync(
        owner_username,
        store_id,
        trigger_type="retry",
        parent_run_id=run_id,
    )


def _queue_sales_order_sync(
    owner_username: str,
    store_id: int,
    **kwargs: Any,
) -> dict[str, Any]:
    from app.services.crawler_service import queue_sales_order_sync

    return queue_sales_order_sync(owner_username, store_id, **kwargs)


def recover_stale_runs(
    *,
    now: datetime | None = None,
    stale_after: timedelta = RUN_STALE_AFTER,
) -> int:
    current = now or sales_now_naive()
    stale_before = current - stale_after
    with session_scope() as session:
        result = session.execute(
            update(SalesOrderSyncRunModel)
            .where(
                SalesOrderSyncRunModel.status.in_(ACTIVE_RUN_STATUSES),
                SalesOrderSyncRunModel.updated_at < stale_before,
            )
            .values(
                status="failed",
                message="订单同步任务已中断，请重试。",
                error_detail="订单同步任务已中断，请重试。",
                finished_at=current,
                updated_at=current,
            )
        )
        return max(0, int(result.rowcount or 0))


def cleanup_successful_runs_if_due(
    *,
    now: datetime | None = None,
    force: bool = False,
) -> int:
    global SUCCESS_CLEANUP_LAST_RUN_AT
    current = now or sales_now_naive()
    if not SUCCESS_CLEANUP_LOCK.acquire(blocking=False):
        return 0
    try:
        if (
            not force
            and SUCCESS_CLEANUP_LAST_RUN_AT is not None
            and current - SUCCESS_CLEANUP_LAST_RUN_AT
            < SUCCESS_CLEANUP_INTERVAL
        ):
            return 0
        settings = get_global_settings()
        cutoff = current - timedelta(
            days=int(settings["successRetentionDays"])
        )
        with session_scope() as session:
            result = session.execute(
                delete(SalesOrderSyncRunModel).where(
                    SalesOrderSyncRunModel.status == "success",
                    SalesOrderSyncRunModel.finished_at.is_not(None),
                    SalesOrderSyncRunModel.finished_at < cutoff,
                )
            )
            deleted_count = max(0, int(result.rowcount or 0))
        SUCCESS_CLEANUP_LAST_RUN_AT = current
        return deleted_count
    finally:
        SUCCESS_CLEANUP_LOCK.release()


def _run_to_public(
    row: SalesOrderSyncRunModel,
    *,
    store_alias_name: str = "",
    current_total_order_count: int | None = None,
) -> dict[str, Any]:
    return {
        "id": row.id,
        "ownerUsername": row.owner_username,
        "storeId": row.store_id,
        "storeName": row.store_name,
        "storeAliasName": store_alias_name,
        "triggerType": row.trigger_type,
        "parentRunId": row.parent_run_id,
        "status": row.status,
        "initialSync": bool(row.initial_sync),
        "progressCurrent": row.progress_current,
        "progressTotal": row.progress_total,
        "totalOrderCount": (
            current_total_order_count
            if current_total_order_count is not None
            else row.total_order_count
        ),
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
        store_ids = {
            int(row.store_id)
            for row in rows
            if row.store_id is not None
        }
        store_aliases = {
            int(store.id): str(
                store.alias_name or store.store_name or store.store_code
            )
            for store in session.scalars(
                select(StoreModel).where(
                    StoreModel.owner_username == owner_username,
                    StoreModel.id.in_(store_ids or {-1}),
                )
            ).all()
        }
        cutoff = sales_now_naive() - timedelta(days=365)
        current_order_counts = {
            int(result.store_id): int(result.order_count or 0)
            for result in session.execute(
                select(
                    SalesOrderModel.store_id,
                    func.count(SalesOrderModel.id).label("order_count"),
                )
                .where(
                    SalesOrderModel.owner_username == owner_username,
                    SalesOrderModel.store_id.in_(store_ids or {-1}),
                    SalesOrderModel.ordered_at >= cutoff,
                )
                .group_by(SalesOrderModel.store_id)
            )
        }
        return {
            "rows": [
                _run_to_public(
                    row,
                    store_alias_name=store_aliases.get(
                        int(row.store_id or 0),
                        row.store_name,
                    ),
                    current_total_order_count=(
                        current_order_counts.get(
                            int(row.store_id),
                            0,
                        )
                        if row.store_id is not None
                        else None
                    ),
                )
                for row in rows
            ],
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
