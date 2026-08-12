from __future__ import annotations

import json
from datetime import datetime
from typing import Any
import uuid

from sqlalchemy import func, select

from app.db.database import SessionLocal
from app.db.models import (
    AutoDeletionTaskModel,
    AutoListingScheduleModel,
    CrawlTaskModel,
    DeletedProductImageCleanupModel,
    ListingTaskModel,
    SalesOrderSyncRunModel,
    SalesSyncStateModel,
    ScheduledCrawlModel,
    SyncTaskModel,
    SystemTaskControlModel,
)


TASK_CONTROL_STOP_MESSAGE = "系统维护期间由超级管理员停止"
TASK_CONTROL_ROW_ID = 1
ACTIVE_TASK_STATUSES = ("queued", "running")


def get_task_control_status() -> dict[str, Any]:
    with SessionLocal() as session:
        state = _load_state(session)
        counts = _active_counts(session)
    return _state_to_public(state, counts=counts)


def task_dispatch_paused() -> bool:
    with SessionLocal() as session:
        return bool(_load_state(session).get("paused"))


def ensure_task_dispatch_allowed() -> None:
    if task_dispatch_paused():
        raise RuntimeError("系统维护期间任务调度已暂停，请等待超级管理员恢复。")


def stop_all_tasks(*, operated_by: str) -> dict[str, Any]:
    from app.services import crawler_service

    stopped_at = datetime.now()
    operation_id = uuid.uuid4().hex
    with SessionLocal() as session:
        current_state = _load_state(session)
        if current_state.get("paused") and current_state.get("phase") != "stopping":
            return _state_to_public(current_state, counts=_active_counts(session))
        if not current_state.get("paused"):
            current_state = {
                "paused": True,
                "phase": "stopping",
                "operationId": operation_id,
                "stoppedAt": _datetime_to_public(stopped_at),
                "stoppedBy": str(operated_by or "").strip(),
                "resumedAt": None,
                "resumedBy": "",
                "snapshot": {},
                "lastResult": {},
            }
            _save_state(session, current_state)
            session.commit()

    with SessionLocal() as session:
        snapshot = _merge_snapshots(
            current_state.get("snapshot"),
            _build_snapshot(session),
        )
        state = _load_state(session)
        state["snapshot"] = snapshot
        _save_state(session, state)
        session.commit()

    with SessionLocal() as session:
        stopped_counts = {
            "crawl": _cancel_model_tasks(
                session,
                CrawlTaskModel,
                snapshot["tasks"]["crawl"],
                clear_crawl_reservation=True,
            ),
            "listing": _cancel_listing_tasks(
                session,
                snapshot["tasks"]["listing"],
                crawler_service.release_listing_task_locks,
            ),
            "sync": _cancel_model_tasks(
                session,
                SyncTaskModel,
                snapshot["tasks"]["sync"],
            ),
            "salesOrderSync": _cancel_sales_order_runs(
                session,
                snapshot["tasks"]["salesOrderSync"],
            ),
            "imageCleanupRecords": _cancel_image_cleanup_records(
                session,
                snapshot["tasks"]["imageCleanupRecords"],
            ),
            "scheduledCrawls": _stop_schedule_rows(
                session,
                ScheduledCrawlModel,
                snapshot["schedules"]["scheduledCrawls"],
            ),
            "autoListing": _stop_schedule_rows(
                session,
                AutoListingScheduleModel,
                snapshot["schedules"]["autoListing"],
            ),
            "autoDeletion": _stop_schedule_rows(
                session,
                AutoDeletionTaskModel,
                snapshot["schedules"]["autoDeletion"],
            ),
        }
        session.commit()
    redis_result = _stop_snapshot_rq_jobs(snapshot)
    with SessionLocal() as session:
        state = _load_state(session)
        state["phase"] = "paused"
        state["lastResult"] = {
            "action": "stop",
            "counts": stopped_counts,
            "redis": redis_result,
        }
        _save_state(session, state)
        session.commit()
        counts = _active_counts(session)
    return _state_to_public(state, counts=counts)


def resume_all_tasks(*, operated_by: str) -> dict[str, Any]:
    from app.services import crawler_service, sales_order_sync_history_service

    with SessionLocal() as session:
        state = _load_state(session)
        snapshot = state.get("snapshot") if isinstance(state.get("snapshot"), dict) else {}
        if not state.get("paused"):
            return _state_to_public(state, counts=_active_counts(session))
        if not snapshot:
            raise RuntimeError("系统正在停止全部任务，请稍后再执行恢复。")
        state["paused"] = False
        state["phase"] = "resuming"
        state["resumedAt"] = _datetime_to_public(datetime.now())
        state["resumedBy"] = str(operated_by or "").strip()
        _save_state(session, state)
        session.commit()

    restored_counts = {
        "crawl": 0,
        "listing": 0,
        "sync": 0,
        "salesOrderSync": 0,
        "imageCleanupRecords": 0,
        "scheduledCrawls": 0,
        "autoListing": 0,
        "autoDeletion": 0,
    }
    errors: list[str] = []

    restored_counts["scheduledCrawls"] = _restore_schedule_rows(
        ScheduledCrawlModel,
        snapshot.get("schedules", {}).get("scheduledCrawls", []),
    )
    restored_counts["autoListing"] = _restore_schedule_rows(
        AutoListingScheduleModel,
        snapshot.get("schedules", {}).get("autoListing", []),
    )
    restored_counts["autoDeletion"] = _restore_schedule_rows(
        AutoDeletionTaskModel,
        snapshot.get("schedules", {}).get("autoDeletion", []),
    )
    restored_counts["imageCleanupRecords"] = _restore_image_cleanup_records(
        snapshot.get("tasks", {}).get("imageCleanupRecords", []),
    )

    for item in snapshot.get("tasks", {}).get("crawl", []):
        try:
            _restore_crawl_task(item, crawler_service)
            restored_counts["crawl"] += 1
        except Exception as exc:
            errors.append(f"采集任务 {item.get('id')}：{exc}")
    for item in snapshot.get("tasks", {}).get("listing", []):
        try:
            crawler_service.retry_listing_task(str(item.get("ownerUsername") or ""), str(item.get("id") or ""))
            restored_counts["listing"] += 1
        except Exception as exc:
            errors.append(f"上架任务 {item.get('id')}：{exc}")
    for item in snapshot.get("tasks", {}).get("sync", []):
        try:
            crawler_service.retry_sync_task(
                str(item.get("ownerUsername") or ""),
                str(item.get("id") or ""),
                allow_all_owners=True,
            )
            restored_counts["sync"] += 1
        except Exception as exc:
            errors.append(f"同步任务 {item.get('id')}：{exc}")
    for item in snapshot.get("tasks", {}).get("salesOrderSync", []):
        try:
            sales_order_sync_history_service.retry_run(
                str(item.get("ownerUsername") or ""),
                str(item.get("id") or ""),
            )
            restored_counts["salesOrderSync"] += 1
        except Exception as exc:
            errors.append(f"订单同步 {item.get('id')}：{exc}")

    crawler_service.dispatch_queued_crawl_tasks_safely()
    crawler_service.dispatch_next_listing_task_safely()
    crawler_service.dispatch_next_sync_task_safely()

    with SessionLocal() as session:
        state = _load_state(session)
        state["phase"] = "running"
        state["lastResult"] = {
            "action": "resume",
            "counts": restored_counts,
            "errors": errors[:100],
        }
        _save_state(session, state)
        session.commit()
        counts = _active_counts(session)
    return _state_to_public(state, counts=counts)


def _build_snapshot(session: Any) -> dict[str, Any]:
    return {
        "tasks": {
            "crawl": _task_snapshot(session, CrawlTaskModel),
            "listing": _task_snapshot(session, ListingTaskModel),
            "sync": _task_snapshot(session, SyncTaskModel),
            "salesOrderSync": _task_snapshot(session, SalesOrderSyncRunModel),
            "imageCleanupRecords": _image_cleanup_snapshot(session),
        },
        "schedules": {
            "scheduledCrawls": _schedule_snapshot(session, ScheduledCrawlModel),
            "autoListing": _schedule_snapshot(session, AutoListingScheduleModel),
            "autoDeletion": _schedule_snapshot(session, AutoDeletionTaskModel),
        },
    }


def _merge_snapshots(existing: Any, current: dict[str, Any]) -> dict[str, Any]:
    existing_snapshot = existing if isinstance(existing, dict) else {}
    merged = {"tasks": {}, "schedules": {}}
    for group_name in ("tasks", "schedules"):
        existing_groups = (
            existing_snapshot.get(group_name)
            if isinstance(existing_snapshot.get(group_name), dict)
            else {}
        )
        current_groups = current.get(group_name) if isinstance(current.get(group_name), dict) else {}
        for key in set(existing_groups) | set(current_groups):
            items_by_id: dict[str, dict[str, Any]] = {}
            for item in [*(existing_groups.get(key) or []), *(current_groups.get(key) or [])]:
                if isinstance(item, dict) and item.get("id") is not None:
                    items_by_id[str(item["id"])] = item
            merged[group_name][key] = list(items_by_id.values())
    return merged


def _task_snapshot(
    session: Any,
    model: Any,
    *,
    statuses: tuple[str, ...] = ACTIVE_TASK_STATUSES,
    include_owner: bool = True,
) -> list[dict[str, Any]]:
    rows = session.scalars(select(model).where(model.status.in_(statuses))).all()
    result = []
    for row in rows:
        item = {
            "id": str(row.id),
            "status": str(row.status),
        }
        if include_owner and hasattr(row, "owner_username"):
            item["ownerUsername"] = str(row.owner_username)
        result.append(item)
    return result


def _schedule_snapshot(session: Any, model: Any) -> list[dict[str, Any]]:
    rows = session.scalars(select(model).where(model.status == "running")).all()
    return [
        {
            "id": int(row.id),
            "status": str(row.status),
            "enabled": bool(row.enabled),
        }
        for row in rows
    ]


def _image_cleanup_snapshot(session: Any) -> list[dict[str, Any]]:
    rows = session.scalars(
        select(DeletedProductImageCleanupModel).where(
            DeletedProductImageCleanupModel.status.in_(("pending", "queued"))
        )
    ).all()
    return [
        {
            "id": int(row.id),
            "ownerUsername": str(row.owner_username),
            "status": str(row.status),
            "syncTaskId": str(row.sync_task_id or ""),
        }
        for row in rows
    ]


def _cancel_model_tasks(
    session: Any,
    model: Any,
    items: list[dict[str, Any]],
    *,
    clear_crawl_reservation: bool = False,
) -> int:
    count = 0
    for item in items:
        row = session.get(model, item.get("id"))
        if row is None or row.status not in ACTIVE_TASK_STATUSES:
            continue
        row.status = "cancelled"
        row.message = TASK_CONTROL_STOP_MESSAGE
        row.error_detail = TASK_CONTROL_STOP_MESSAGE
        row.finished_at = datetime.now()
        if clear_crawl_reservation:
            row.queue_job_id = None
        count += 1
    return count


def _cancel_listing_tasks(session: Any, items: list[dict[str, Any]], release_locks: Any) -> int:
    count = 0
    for item in items:
        row = session.get(ListingTaskModel, item.get("id"))
        if row is None or row.status not in ACTIVE_TASK_STATUSES:
            continue
        release_locks(session, row.owner_username, row)
        row.status = "cancelled"
        row.message = TASK_CONTROL_STOP_MESSAGE
        row.error_detail = TASK_CONTROL_STOP_MESSAGE
        row.finished_at = datetime.now()
        count += 1
    return count


def _cancel_sales_order_runs(session: Any, items: list[dict[str, Any]]) -> int:
    count = 0
    for item in items:
        row = session.get(SalesOrderSyncRunModel, item.get("id"))
        if row is None or row.status not in ACTIVE_TASK_STATUSES:
            continue
        row.status = "cancelled"
        row.message = TASK_CONTROL_STOP_MESSAGE
        row.error_detail = TASK_CONTROL_STOP_MESSAGE
        row.finished_at = datetime.now()
        if row.store_id is not None:
            state = session.get(
                SalesSyncStateModel,
                {
                    "store_id": int(row.store_id),
                    "owner_username": row.owner_username,
                },
            )
            if state is not None and state.owner_username == row.owner_username:
                state.sync_status = "idle"
                state.last_error = TASK_CONTROL_STOP_MESSAGE
        count += 1
    return count


def _cancel_image_cleanup_records(session: Any, items: list[dict[str, Any]]) -> int:
    count = 0
    for item in items:
        row = session.get(DeletedProductImageCleanupModel, int(item.get("id") or 0))
        if row is None or row.status not in {"pending", "queued"}:
            continue
        row.status = "cancelled"
        row.sync_task_id = None
        row.last_error = TASK_CONTROL_STOP_MESSAGE
        count += 1
    return count


def _stop_schedule_rows(session: Any, model: Any, items: list[dict[str, Any]]) -> int:
    count = 0
    for item in items:
        row = session.get(model, int(item.get("id") or 0))
        if row is None or row.status != "running":
            continue
        row.status = "disabled" if not row.enabled else "idle"
        count += 1
    return count


def _restore_schedule_rows(model: Any, items: list[dict[str, Any]]) -> int:
    count = 0
    with SessionLocal() as session:
        for item in items:
            row = session.get(model, int(item.get("id") or 0))
            if row is None:
                continue
            row.status = "idle" if row.enabled else "disabled"
            count += 1
        session.commit()
    return count


def _restore_image_cleanup_records(items: list[dict[str, Any]]) -> int:
    count = 0
    with SessionLocal() as session:
        for item in items:
            row = session.get(DeletedProductImageCleanupModel, int(item.get("id") or 0))
            if row is None or row.status != "cancelled":
                continue
            original_status = str(item.get("status") or "pending")
            row.status = "queued" if original_status == "queued" else "pending"
            row.sync_task_id = (
                str(item.get("syncTaskId") or "") or None
                if original_status == "queued"
                else None
            )
            row.last_error = None
            count += 1
        session.commit()
    return count


def _restore_crawl_task(item: dict[str, Any], crawler_service: Any) -> None:
    task_id = str(item.get("id") or "")
    owner_username = str(item.get("ownerUsername") or "")
    if item.get("status") == "queued":
        with SessionLocal() as session:
            row = session.get(CrawlTaskModel, task_id)
            if row is None or row.status != "cancelled":
                raise RuntimeError("任务状态已变化，未恢复。")
            row.status = "queued"
            row.queue_job_id = None
            row.message = "系统维护结束，等待继续执行"
            row.error_detail = None
            row.warning_detail = None
            row.finished_at = None
            session.commit()
        return
    crawler_service.run_existing_task(owner_username, task_id)


def _stop_snapshot_rq_jobs(snapshot: dict[str, Any]) -> dict[str, int]:
    from rq import Queue
    from rq.command import send_stop_job_command
    from rq.job import Job
    from rq.registry import DeferredJobRegistry, ScheduledJobRegistry, StartedJobRegistry

    from app.core.task_queue import all_task_queue_names, redis_connection

    task_ids = {
        str(item.get("id") or "")
        for key, group in snapshot.get("tasks", {}).items()
        if key != "imageCleanupRecords"
        for item in group
        if item.get("id")
    }
    exact_job_ids = {
        *{
            f"auto-listing-schedule-{int(item.get('id') or 0)}"
            for item in snapshot.get("schedules", {}).get("autoListing", [])
        },
        *{
            f"auto-deletion-task-{int(item.get('id') or 0)}"
            for item in snapshot.get("schedules", {}).get("autoDeletion", [])
        },
    }
    job_id_prefixes = {
        f"schedule-{int(item.get('id') or 0)}-"
        for item in snapshot.get("schedules", {}).get("scheduledCrawls", [])
    }
    connection = redis_connection()
    result = {"queued": 0, "deferred": 0, "scheduled": 0, "started": 0}
    for queue_name in all_task_queue_names():
        queue = Queue(queue_name, connection=connection)
        groups = [
            ("queued", list(queue.job_ids), queue),
            ("deferred", DeferredJobRegistry(queue_name, connection=connection).get_job_ids(), DeferredJobRegistry(queue_name, connection=connection)),
            ("scheduled", ScheduledJobRegistry(queue_name, connection=connection).get_job_ids(), ScheduledJobRegistry(queue_name, connection=connection)),
            ("started", StartedJobRegistry(queue_name, connection=connection).get_job_ids(), StartedJobRegistry(queue_name, connection=connection)),
        ]
        for state, job_ids, holder in groups:
            for job_id in job_ids:
                try:
                    job = Job.fetch(job_id, connection=connection)
                except Exception:
                    continue
                searchable = " ".join(
                    [
                        str(job.id),
                        str(getattr(job, "description", "") or ""),
                        repr(getattr(job, "args", ()) or ()),
                    ]
                )
                if (
                    str(job.id) not in exact_job_ids
                    and not any(str(job.id).startswith(prefix) for prefix in job_id_prefixes)
                    and not any(task_id and task_id in searchable for task_id in task_ids)
                ):
                    continue
                try:
                    if state == "started":
                        send_stop_job_command(connection, job.id)
                    elif state == "queued":
                        holder.remove(job.id)
                        job.cancel()
                    else:
                        holder.remove(job.id, delete_job=False)
                        job.cancel()
                    result[state] += 1
                except Exception:
                    continue
    return result


def _active_counts(session: Any) -> dict[str, int]:
    return {
        "crawl": _count_statuses(session, CrawlTaskModel, ACTIVE_TASK_STATUSES),
        "listing": _count_statuses(session, ListingTaskModel, ACTIVE_TASK_STATUSES),
        "sync": _count_statuses(session, SyncTaskModel, ACTIVE_TASK_STATUSES),
        "salesOrderSync": _count_statuses(session, SalesOrderSyncRunModel, ACTIVE_TASK_STATUSES),
        "imageCleanupRecords": _count_statuses(
            session,
            DeletedProductImageCleanupModel,
            ("pending", "queued"),
        ),
    }


def _count_statuses(session: Any, model: Any, statuses: tuple[str, ...]) -> int:
    return int(session.scalar(select(func.count()).where(model.status.in_(statuses))) or 0)


def _load_state(session: Any) -> dict[str, Any]:
    row = session.get(SystemTaskControlModel, TASK_CONTROL_ROW_ID)
    if row is None:
        return {"paused": False, "snapshot": {}, "lastResult": {}}
    return {
        "paused": bool(row.paused),
        "phase": str(row.phase or ""),
        "operationId": str(row.operation_id or ""),
        "stoppedAt": _datetime_to_public(row.stopped_at),
        "stoppedBy": str(row.stopped_by or ""),
        "resumedAt": _datetime_to_public(row.resumed_at),
        "resumedBy": str(row.resumed_by or ""),
        "snapshot": _json_object(row.snapshot_json),
        "lastResult": _json_object(row.last_result_json),
    }


def _save_state(session: Any, state: dict[str, Any]) -> None:
    row = session.get(SystemTaskControlModel, TASK_CONTROL_ROW_ID)
    if row is None:
        row = SystemTaskControlModel(id=TASK_CONTROL_ROW_ID)
        session.add(row)
    row.paused = bool(state.get("paused"))
    row.phase = str(state.get("phase") or ("paused" if row.paused else "running"))
    row.operation_id = str(state.get("operationId") or "")
    row.stopped_at = _parse_datetime(state.get("stoppedAt"))
    row.stopped_by = str(state.get("stoppedBy") or "")
    row.resumed_at = _parse_datetime(state.get("resumedAt"))
    row.resumed_by = str(state.get("resumedBy") or "")
    row.snapshot_json = json.dumps(
        state.get("snapshot") if isinstance(state.get("snapshot"), dict) else {},
        ensure_ascii=False,
    )
    row.last_result_json = json.dumps(
        state.get("lastResult") if isinstance(state.get("lastResult"), dict) else {},
        ensure_ascii=False,
    )


def _state_to_public(state: dict[str, Any], *, counts: dict[str, int]) -> dict[str, Any]:
    snapshot = state.get("snapshot") if isinstance(state.get("snapshot"), dict) else {}
    task_groups = snapshot.get("tasks") if isinstance(snapshot.get("tasks"), dict) else {}
    resumable_count = sum(len(items) for items in task_groups.values() if isinstance(items, list))
    return {
        "paused": bool(state.get("paused")),
        "phase": str(state.get("phase") or ("paused" if state.get("paused") else "running")),
        "operationId": str(state.get("operationId") or ""),
        "stoppedAt": state.get("stoppedAt"),
        "stoppedBy": str(state.get("stoppedBy") or ""),
        "resumedAt": state.get("resumedAt"),
        "resumedBy": str(state.get("resumedBy") or ""),
        "activeCounts": counts,
        "activeTotal": sum(counts.values()),
        "resumableCount": resumable_count if state.get("paused") else 0,
        "lastResult": state.get("lastResult") if isinstance(state.get("lastResult"), dict) else {},
    }


def _datetime_to_public(value: datetime | None) -> str | None:
    return value.isoformat(timespec="seconds") if value is not None else None


def _parse_datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value
    normalized = str(value or "").strip()
    if not normalized:
        return None
    try:
        return datetime.fromisoformat(normalized)
    except ValueError:
        return None


def _json_object(value: str | None) -> dict[str, Any]:
    try:
        payload = json.loads(value or "{}")
    except ValueError:
        payload = {}
    return payload if isinstance(payload, dict) else {}
