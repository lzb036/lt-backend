from __future__ import annotations

import json
import time
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
    UserAccountModel,
)


TASK_CONTROL_STOP_MESSAGE = "系统维护期间由超级管理员停止"
TASK_CONTROL_ROW_ID = 1
ACTIVE_TASK_STATUSES = ("queued", "running")
TASK_STOP_TIMEOUT_SECONDS = 120.0
TASK_STOP_POLL_SECONDS = 1.0
TASK_STOP_STABLE_CHECKS = 2


def get_task_control_status() -> dict[str, Any]:
    with SessionLocal() as session:
        state = _load_state(session)
        counts = _active_counts(session, _selected_usernames(state))
    return _state_to_public(state, counts=counts)


def paused_usernames() -> set[str]:
    with SessionLocal() as session:
        state = _load_state(session)
    return (
        set(_selected_usernames(state))
        if state.get("paused")
        else set()
    )


def task_dispatch_paused(owner_username: str | None = None) -> bool:
    selected = paused_usernames()
    if owner_username is None:
        return bool(selected)
    return str(owner_username or "").strip() in selected


def ensure_task_dispatch_allowed(owner_username: str | None = None) -> None:
    if owner_username and task_dispatch_paused(owner_username):
        raise RuntimeError("系统维护期间任务调度已暂停，请等待超级管理员恢复。")


def stop_all_tasks(
    *,
    operated_by: str,
    usernames: list[str] | None = None,
) -> dict[str, Any]:
    from app.services import crawler_service

    stopped_at = datetime.now()
    with SessionLocal() as session:
        current_state = _load_state(session)
        requested_usernames = (
            _normalize_selected_usernames(session, usernames)
            if usernames is not None
            else (
                _selected_usernames(current_state)
                or _discover_active_usernames(session)
            )
        )
        if not requested_usernames:
            raise RuntimeError("请至少选择一个用户。")
        locked_usernames = _selected_usernames(current_state)
        if current_state.get("paused") and requested_usernames != locked_usernames:
            raise RuntimeError(
                "已有一组用户处于停止操作中，请先恢复本次停止的用户后再重新选择。"
            )
        if current_state.get("paused") and current_state.get("phase") == "stopping":
            return _state_to_public(
                current_state,
                counts=_active_counts(session, locked_usernames),
            )
        if (
            current_state.get("paused")
            and current_state.get("phase") == "paused"
            and _state_deploy_safe(current_state)
        ):
            return _state_to_public(
                current_state,
                counts=_active_counts(session, locked_usernames),
            )
        if not current_state.get("paused"):
            current_state = {
                "paused": True,
                "phase": "stopping",
                "operationId": uuid.uuid4().hex,
                "stoppedAt": _datetime_to_public(stopped_at),
                "stoppedBy": str(operated_by or "").strip(),
                "resumedAt": None,
                "resumedBy": "",
                "snapshot": {
                    "selectedUsernames": requested_usernames,
                    "tasks": {},
                    "schedules": {},
                },
                "lastResult": {},
            }
            _save_state(session, current_state)
            session.commit()
        else:
            current_state["phase"] = "stopping"
            current_state["lastResult"] = {
                **(
                    current_state.get("lastResult")
                    if isinstance(current_state.get("lastResult"), dict)
                    else {}
                ),
                "deploySafe": False,
            }
            _save_state(session, current_state)
            session.commit()

    deadline = time.monotonic() + TASK_STOP_TIMEOUT_SECONDS
    stopped_counts = _empty_stop_counts()
    redis_result = _empty_redis_counts()
    stop_errors: list[str] = []
    stable_checks = 0
    quiescence = _empty_quiescence()
    while True:
        with SessionLocal() as session:
            state = _load_state(session)
            selected_usernames = _selected_usernames(state)
            snapshot = _merge_snapshots(
                state.get("snapshot"),
                _build_snapshot(session, selected_usernames),
            )
            state["snapshot"] = snapshot
            _save_state(session, state)
            iteration_counts = _cancel_snapshot_rows(
                session,
                snapshot,
                release_listing_locks=crawler_service.release_listing_task_locks,
            )
            session.commit()
        stopped_counts = _sum_counts(stopped_counts, iteration_counts)
        redis_iteration = _stop_all_managed_rq_jobs(snapshot)
        redis_result = _sum_counts(redis_result, redis_iteration["counts"])
        stop_errors = list(redis_iteration["errors"])
        quiescence = _deployment_quiescence(snapshot)
        if quiescence["quiet"]:
            stable_checks += 1
            if stable_checks >= TASK_STOP_STABLE_CHECKS:
                break
        else:
            stable_checks = 0
        if time.monotonic() >= deadline:
            break
        time.sleep(TASK_STOP_POLL_SECONDS)

    deploy_safe = bool(
        quiescence["quiet"]
        and not quiescence["errors"]
        and not stop_errors
        and stable_checks >= TASK_STOP_STABLE_CHECKS
    )
    final_errors = list(
        dict.fromkeys(
            [
                *stop_errors,
                *quiescence["errors"],
                *(
                    []
                    if deploy_safe
                    else ["未能确认所有任务执行载体均已静默，禁止开始部署。"]
                ),
            ]
        )
    )
    with SessionLocal() as session:
        state = _load_state(session)
        state["phase"] = "paused" if deploy_safe else "stop_failed"
        state["lastResult"] = {
            "action": "stop",
            "counts": stopped_counts,
            "redis": redis_result,
            "deploySafe": deploy_safe,
            "quiescence": quiescence,
            "errors": final_errors[:100],
        }
        _save_state(session, state)
        session.commit()
        counts = _active_counts(session, _selected_usernames(state))
    return _state_to_public(state, counts=counts)


def resume_all_tasks(*, operated_by: str) -> dict[str, Any]:
    from app.services import crawler_service

    with SessionLocal() as session:
        state = _load_state(session)
        snapshot = state.get("snapshot") if isinstance(state.get("snapshot"), dict) else {}
        if not state.get("paused"):
            return _state_to_public(
                state,
                counts=_active_counts(session, _selected_usernames(state)),
            )
        if not _state_deploy_safe(state):
            raise RuntimeError(
                "任务尚未确认完全停止，请先重新执行“停止全部任务”直至显示可以部署。"
            )
        if not snapshot:
            raise RuntimeError("系统正在停止全部任务，请稍后再执行恢复。")
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

    try:
        restored_counts, prepare_errors = _prepare_snapshot_for_resume(snapshot)
        errors.extend(prepare_errors)
    except Exception as exc:
        errors.append(str(exc))
        with SessionLocal() as session:
            state = _load_state(session)
            state["phase"] = "resume_failed"
            state["lastResult"] = {
                "action": "resume",
                "counts": restored_counts,
                "deploySafe": True,
                "errors": errors[:100],
            }
            _save_state(session, state)
            session.commit()
            counts = _active_counts(session, _selected_usernames(state))
        return _state_to_public(state, counts=counts)

    with SessionLocal() as session:
        state = _load_state(session)
        state["paused"] = False
        state["phase"] = "running"
        _save_state(session, state)
        session.commit()
    errors.extend(_dispatch_restored_snapshot(snapshot, crawler_service))
    with SessionLocal() as session:
        state = _load_state(session)
        state["lastResult"] = {
            "action": "resume",
            "counts": restored_counts,
            "errors": errors[:100],
        }
        _save_state(session, state)
        session.commit()
        counts = _active_counts(session, _selected_usernames(state))
    return _state_to_public(state, counts=counts)


def _empty_stop_counts() -> dict[str, int]:
    return {
        "crawl": 0,
        "listing": 0,
        "sync": 0,
        "salesOrderSync": 0,
        "imageCleanupRecords": 0,
        "scheduledCrawls": 0,
        "autoListing": 0,
        "autoDeletion": 0,
    }


def _empty_redis_counts() -> dict[str, int]:
    return {"queued": 0, "deferred": 0, "scheduled": 0, "started": 0}


def _empty_quiescence() -> dict[str, Any]:
    return {
        "quiet": False,
        "databaseActive": 0,
        "queue": _empty_redis_counts(),
        "errors": [],
    }


def _sum_counts(
    current: dict[str, int],
    additional: dict[str, int],
) -> dict[str, int]:
    return {
        key: int(current.get(key) or 0) + int(additional.get(key) or 0)
        for key in set(current) | set(additional)
    }


def _cancel_snapshot_rows(
    session: Any,
    snapshot: dict[str, Any],
    *,
    release_listing_locks: Any,
) -> dict[str, int]:
    return {
        "crawl": _cancel_model_tasks(
            session,
            CrawlTaskModel,
            snapshot["tasks"]["crawl"],
            clear_crawl_reservation=True,
        ),
        "listing": _cancel_listing_tasks(
            session,
            snapshot["tasks"]["listing"],
            release_listing_locks,
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


def _build_snapshot(
    session: Any,
    selected_usernames: list[str],
) -> dict[str, Any]:
    return {
        "selectedUsernames": selected_usernames,
        "tasks": {
            "crawl": _task_snapshot(session, CrawlTaskModel, selected_usernames),
            "listing": _task_snapshot(session, ListingTaskModel, selected_usernames),
            "sync": _task_snapshot(session, SyncTaskModel, selected_usernames),
            "salesOrderSync": _task_snapshot(
                session,
                SalesOrderSyncRunModel,
                selected_usernames,
            ),
            "imageCleanupRecords": _image_cleanup_snapshot(
                session,
                selected_usernames,
            ),
        },
        "schedules": {
            "scheduledCrawls": _schedule_snapshot(
                session,
                ScheduledCrawlModel,
                selected_usernames,
            ),
            "autoListing": _schedule_snapshot(
                session,
                AutoListingScheduleModel,
                selected_usernames,
            ),
            "autoDeletion": _schedule_snapshot(
                session,
                AutoDeletionTaskModel,
                selected_usernames,
            ),
        },
    }


def _merge_snapshots(existing: Any, current: dict[str, Any]) -> dict[str, Any]:
    existing_snapshot = existing if isinstance(existing, dict) else {}
    merged = {
        "selectedUsernames": list(
            existing_snapshot.get("selectedUsernames")
            or current.get("selectedUsernames")
            or []
        ),
        "tasks": {},
        "schedules": {},
    }
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
    selected_usernames: list[str],
    *,
    statuses: tuple[str, ...] = ACTIVE_TASK_STATUSES,
    include_owner: bool = True,
) -> list[dict[str, Any]]:
    rows = session.scalars(
        select(model).where(
            model.status.in_(statuses),
            model.owner_username.in_(selected_usernames),
        )
    ).all()
    result = []
    for row in rows:
        item = {
            "id": str(row.id),
            "status": str(row.status),
        }
        if include_owner and hasattr(row, "owner_username"):
            item["ownerUsername"] = str(row.owner_username)
        if hasattr(row, "store_id"):
            item["storeId"] = (
                int(row.store_id)
                if row.store_id is not None
                else None
            )
        result.append(item)
    return result


def _schedule_snapshot(
    session: Any,
    model: Any,
    selected_usernames: list[str],
) -> list[dict[str, Any]]:
    rows = session.scalars(
        select(model).where(
            model.status == "running",
            model.owner_username.in_(selected_usernames),
        )
    ).all()
    return [
        {
            "id": int(row.id),
            "status": str(row.status),
            "enabled": bool(row.enabled),
            "ownerUsername": str(row.owner_username),
        }
        for row in rows
    ]


def _image_cleanup_snapshot(
    session: Any,
    selected_usernames: list[str],
) -> list[dict[str, Any]]:
    rows = session.scalars(
        select(DeletedProductImageCleanupModel).where(
            DeletedProductImageCleanupModel.status.in_(("pending", "queued")),
            DeletedProductImageCleanupModel.owner_username.in_(
                selected_usernames
            ),
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


def _prepare_snapshot_for_resume(
    snapshot: dict[str, Any],
) -> tuple[dict[str, int], list[str]]:
    from app.services import crawler_service

    counts = _empty_stop_counts()
    errors: list[str] = []
    with SessionLocal() as session:
        for model, key in (
            (ScheduledCrawlModel, "scheduledCrawls"),
            (AutoListingScheduleModel, "autoListing"),
            (AutoDeletionTaskModel, "autoDeletion"),
        ):
            for item in snapshot.get("schedules", {}).get(key, []):
                row = session.get(model, int(item.get("id") or 0))
                if row is None:
                    errors.append(f"{key} 计划 {item.get('id')} 已不存在，已跳过。")
                    continue
                row.status = "idle" if bool(item.get("enabled")) else "disabled"
                counts[key] += 1

        for item in snapshot.get("tasks", {}).get("imageCleanupRecords", []):
            row = session.get(
                DeletedProductImageCleanupModel,
                int(item.get("id") or 0),
            )
            if row is None or row.status != "cancelled":
                errors.append(f"图片清理记录 {item.get('id')} 状态已变化，已跳过。")
                continue
            original_status = str(item.get("status") or "pending")
            row.status = "queued" if original_status == "queued" else "pending"
            row.sync_task_id = (
                str(item.get("syncTaskId") or "") or None
                if original_status == "queued"
                else None
            )
            row.last_error = None
            counts["imageCleanupRecords"] += 1

        for item in snapshot.get("tasks", {}).get("crawl", []):
            row = session.get(CrawlTaskModel, str(item.get("id") or ""))
            if row is None or row.status != "cancelled":
                errors.append(f"采集任务 {item.get('id')} 状态已变化，已跳过。")
                continue
            row.status = "queued"
            row.queue_job_id = None
            row.message = "系统维护结束，等待继续执行"
            row.error_detail = None
            row.warning_detail = None
            row.started_at = None
            row.finished_at = None
            counts["crawl"] += 1

        for item in snapshot.get("tasks", {}).get("listing", []):
            task_id = str(item.get("id") or "")
            row = session.get(ListingTaskModel, task_id)
            if row is None or row.status != "cancelled":
                errors.append(f"上架任务 {task_id} 状态已变化，已跳过。")
                continue
            product_ids_payload = crawler_service.listing_task_product_ids_payload(
                row.product_ids_json
            )
            retry_product_ids = (
                product_ids_payload["retryIds"]
                or product_ids_payload["failedIds"]
                or [
                    product_id
                    for product_id in product_ids_payload["productIds"]
                    if product_id not in set(product_ids_payload["successIds"])
                ]
            )
            if not retry_product_ids:
                errors.append(f"上架任务 {task_id} 没有可恢复商品，已跳过。")
                continue
            task_product_ids = (
                product_ids_payload["productIds"] or retry_product_ids
            )
            store_ids = product_ids_payload["storeIds"] or (
                [int(row.store_id)] if row.store_id else []
            )
            retry_set = set(retry_product_ids)
            success_ids = [
                product_id
                for product_id in product_ids_payload["successIds"]
                if product_id not in retry_set
            ]
            products = session.scalars(
                select(crawler_service.ProductModel).where(
                    crawler_service.ProductModel.owner_username
                    == row.owner_username,
                    crawler_service.ProductModel.id.in_(retry_product_ids),
                )
            ).all()
            for product in products:
                if (
                    product.review_status in {"approved", "listed_master"}
                    and not product.listing_task_id
                ):
                    product.listing_task_id = task_id
                    product.last_error = None
            row.status = "queued"
            row.total_count = len(task_product_ids) * max(1, len(store_ids))
            row.success_count = len(success_ids)
            row.failed_count = len(retry_product_ids) * max(1, len(store_ids))
            row.message = "系统维护结束，等待继续上架"
            row.error_detail = None
            row.product_ids_json = json.dumps(
                crawler_service.listing_task_result_payload(
                    task_product_ids,
                    success_ids,
                    retry_product_ids,
                    retry_ids=retry_product_ids,
                    store_ids=store_ids,
                ),
                ensure_ascii=False,
            )
            row.started_at = None
            row.finished_at = None
            counts["listing"] += 1

        for item in snapshot.get("tasks", {}).get("sync", []):
            task_id = str(item.get("id") or "")
            row = session.get(SyncTaskModel, task_id)
            if row is None or row.status != "cancelled":
                errors.append(f"同步任务 {task_id} 状态已变化，已跳过。")
                continue
            payload = crawler_service.sync_task_payload(row)
            payload.pop("result", None)
            row.status = "queued"
            row.message = "系统维护结束，等待继续同步"
            row.error_detail = None
            row.payload_json = json.dumps(payload, ensure_ascii=False)
            row.success_count = 0
            row.failed_count = 0
            row.started_at = None
            row.finished_at = None
            counts["sync"] += 1

        for item in snapshot.get("tasks", {}).get("salesOrderSync", []):
            run_id = str(item.get("id") or "")
            row = session.get(SalesOrderSyncRunModel, run_id)
            if row is None or row.status != "cancelled" or row.store_id is None:
                errors.append(f"订单同步 {run_id} 状态已变化或店铺不存在，已跳过。")
                continue
            row.status = "queued"
            row.message = "系统维护结束，等待继续同步订单。"
            row.error_detail = None
            row.started_at = None
            row.finished_at = None
            state = session.get(
                SalesSyncStateModel,
                {
                    "store_id": int(row.store_id),
                    "owner_username": row.owner_username,
                },
            )
            if state is not None:
                state.sync_status = "queued"
                state.last_error = None
            counts["salesOrderSync"] += 1
        session.commit()
    return counts, errors


def _dispatch_restored_snapshot(
    snapshot: dict[str, Any],
    crawler_service: Any,
) -> list[str]:
    errors: list[str] = []
    for owner_username in _snapshot_selected_usernames(snapshot):
        try:
            crawler_service.dispatch_queued_crawl_tasks(owner_username)
        except Exception as exc:
            errors.append(f"用户 {owner_username} 的采集任务重新投递失败：{exc}")
    for label, dispatch in (
        (
            "上架任务",
            getattr(
                crawler_service,
                "dispatch_next_listing_task_safely",
                crawler_service.dispatch_next_listing_task,
            ),
        ),
        (
            "同步任务",
            getattr(
                crawler_service,
                "dispatch_next_sync_task_safely",
                crawler_service.dispatch_next_sync_task,
            ),
        ),
    ):
        try:
            dispatch()
        except Exception as exc:
            errors.append(f"{label}重新投递失败：{exc}")
    for item in snapshot.get("tasks", {}).get("salesOrderSync", []):
        store_id = item.get("storeId")
        if store_id is None:
            errors.append(f"订单同步 {item.get('id')} 缺少店铺信息，未重新投递。")
            continue
        try:
            crawler_service.dispatch_sales_order_sync_task(
                str(item.get("ownerUsername") or ""),
                int(store_id),
                str(item.get("id") or ""),
            )
        except Exception as exc:
            errors.append(f"订单同步 {item.get('id')} 重新投递失败：{exc}")
    return errors


def _stop_all_managed_rq_jobs(
    snapshot: dict[str, Any] | None = None,
) -> dict[str, Any]:
    from rq import Queue
    from rq.command import send_stop_job_command
    from rq.job import Job
    from rq.registry import DeferredJobRegistry, ScheduledJobRegistry, StartedJobRegistry

    from app.core.task_queue import all_task_queue_names, redis_connection

    result = _empty_redis_counts()
    errors: list[str] = []
    try:
        connection = redis_connection()
    except Exception as exc:
        return {
            "counts": result,
            "errors": [f"连接 Redis 失败：{exc}"],
        }
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
                except Exception as exc:
                    errors.append(f"读取队列任务 {job_id} 失败：{exc}")
                    continue
                if snapshot is not None and not _job_matches_snapshot(job, snapshot):
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
                except Exception as exc:
                    errors.append(f"停止队列任务 {job.id} 失败：{exc}")
    return {"counts": result, "errors": errors}


def _selected_managed_queue_activity(
    snapshot: dict[str, Any],
) -> dict[str, Any]:
    from rq import Queue
    from rq.job import Job
    from rq.registry import DeferredJobRegistry, ScheduledJobRegistry, StartedJobRegistry

    from app.core.task_queue import all_task_queue_names, redis_connection

    counts = _empty_redis_counts()
    errors: list[str] = []
    try:
        connection = redis_connection()
    except Exception as exc:
        return {"counts": counts, "errors": [f"连接 Redis 失败：{exc}"]}
    for queue_name in all_task_queue_names():
        groups = {
            "queued": list(Queue(queue_name, connection=connection).job_ids),
            "deferred": DeferredJobRegistry(
                queue_name,
                connection=connection,
            ).get_job_ids(),
            "scheduled": ScheduledJobRegistry(
                queue_name,
                connection=connection,
            ).get_job_ids(),
            "started": StartedJobRegistry(
                queue_name,
                connection=connection,
            ).get_job_ids(),
        }
        for state, job_ids in groups.items():
            for job_id in job_ids:
                try:
                    job = Job.fetch(job_id, connection=connection)
                    if _job_matches_snapshot(job, snapshot):
                        counts[state] += 1
                except Exception as exc:
                    errors.append(f"检查队列任务 {job_id} 失败：{exc}")
    return {"counts": counts, "errors": errors}


def _deployment_quiescence(
    snapshot: dict[str, Any] | None = None,
) -> dict[str, Any]:
    if not _redis_task_queue_enabled():
        return {
            "quiet": False,
            "databaseActive": 0,
            "queue": _empty_redis_counts(),
            "errors": ["当前不是 Redis/RQ 任务模式，无法确认后台线程已经完全停止。"],
        }
    with SessionLocal() as session:
        selected_usernames = (
            _snapshot_selected_usernames(snapshot)
            if snapshot is not None
            else _discover_active_usernames(session)
        )
        counts = _active_counts(
            session,
            selected_usernames,
        )
    queue_activity = (
        _selected_managed_queue_activity(snapshot)
        if snapshot is not None
        else _all_managed_queue_activity()
    )
    database_active = sum(counts.values())
    queue_counts = queue_activity["counts"]
    return {
        "quiet": (
            database_active == 0
            and sum(queue_counts.values()) == 0
            and not queue_activity["errors"]
        ),
        "databaseActive": database_active,
        "queue": queue_counts,
        "errors": queue_activity["errors"],
    }


def _all_managed_queue_activity() -> dict[str, Any]:
    from rq import Queue
    from rq.registry import DeferredJobRegistry, ScheduledJobRegistry, StartedJobRegistry

    from app.core.task_queue import all_task_queue_names, redis_connection

    counts = _empty_redis_counts()
    errors: list[str] = []
    try:
        connection = redis_connection()
    except Exception as exc:
        return {"counts": counts, "errors": [f"连接 Redis 失败：{exc}"]}
    for queue_name in all_task_queue_names():
        try:
            counts["queued"] += len(Queue(queue_name, connection=connection).job_ids)
            counts["deferred"] += len(
                DeferredJobRegistry(queue_name, connection=connection).get_job_ids()
            )
            counts["scheduled"] += len(
                ScheduledJobRegistry(queue_name, connection=connection).get_job_ids()
            )
            counts["started"] += len(
                StartedJobRegistry(queue_name, connection=connection).get_job_ids()
            )
        except Exception as exc:
            errors.append(f"检查队列 {queue_name} 失败：{exc}")
    return {"counts": counts, "errors": errors}


def _job_matches_snapshot(job: Any, snapshot: dict[str, Any]) -> bool:
    selected_usernames = set(_snapshot_selected_usernames(snapshot))
    meta = getattr(job, "meta", None)
    if isinstance(meta, dict):
        owner_username = str(meta.get("ownerUsername") or "").strip()
        if owner_username:
            return owner_username in selected_usernames

    args = list(getattr(job, "args", None) or [])
    if any(str(value or "").strip() in selected_usernames for value in args):
        return True

    task_ids = {
        str(item.get("id"))
        for group in (snapshot.get("tasks") or {}).values()
        for item in group
        if isinstance(item, dict) and item.get("id") is not None
    }
    schedule_ids = {
        str(item.get("id"))
        for group in (snapshot.get("schedules") or {}).values()
        for item in group
        if isinstance(item, dict) and item.get("id") is not None
    }
    searchable_values = {
        str(getattr(job, "id", "") or ""),
        str(getattr(job, "description", "") or ""),
        *(str(value or "") for value in args),
    }
    return any(
        task_id in value
        for task_id in task_ids
        for value in searchable_values
    ) or any(
        (
            f"schedule-{schedule_id}-" in value
            or f"auto-listing-schedule-{schedule_id}" in value
            or f"auto-deletion-task-{schedule_id}" in value
        )
        for schedule_id in schedule_ids
        for value in searchable_values
    )


def _redis_task_queue_enabled() -> bool:
    from app.core.config import settings

    return settings.task_queue_mode == "redis"


def _active_counts(
    session: Any,
    selected_usernames: list[str],
) -> dict[str, int]:
    return {
        "crawl": _count_statuses(
            session,
            CrawlTaskModel,
            ACTIVE_TASK_STATUSES,
            selected_usernames,
        ),
        "listing": _count_statuses(
            session,
            ListingTaskModel,
            ACTIVE_TASK_STATUSES,
            selected_usernames,
        ),
        "sync": _count_statuses(
            session,
            SyncTaskModel,
            ACTIVE_TASK_STATUSES,
            selected_usernames,
        ),
        "salesOrderSync": _count_statuses(
            session,
            SalesOrderSyncRunModel,
            ACTIVE_TASK_STATUSES,
            selected_usernames,
        ),
        "imageCleanupRecords": _count_statuses(
            session,
            DeletedProductImageCleanupModel,
            ("pending", "queued"),
            selected_usernames,
        ),
    }


def _count_statuses(
    session: Any,
    model: Any,
    statuses: tuple[str, ...],
    selected_usernames: list[str],
) -> int:
    if not selected_usernames:
        return 0
    return int(
        session.scalar(
            select(func.count()).where(
                model.status.in_(statuses),
                model.owner_username.in_(selected_usernames),
            )
        )
        or 0
    )


def _normalize_selected_usernames(
    session: Any,
    usernames: list[str] | None,
) -> list[str]:
    requested = sorted(
        {
            str(username or "").strip()
            for username in (usernames or [])
            if str(username or "").strip()
        }
    )
    if not requested:
        raise RuntimeError("请至少选择一个用户。")
    existing = set(
        session.scalars(
            select(UserAccountModel.username).where(
                UserAccountModel.username.in_(requested)
            )
        ).all()
    )
    missing = [username for username in requested if username not in existing]
    if missing:
        raise RuntimeError(f"用户不存在：{', '.join(missing[:10])}")
    return requested


def _discover_active_usernames(session: Any) -> list[str]:
    usernames: set[str] = set()
    for model, statuses in (
        (CrawlTaskModel, ACTIVE_TASK_STATUSES),
        (ListingTaskModel, ACTIVE_TASK_STATUSES),
        (SyncTaskModel, ACTIVE_TASK_STATUSES),
        (SalesOrderSyncRunModel, ACTIVE_TASK_STATUSES),
        (DeletedProductImageCleanupModel, ("pending", "queued")),
    ):
        usernames.update(
            str(value)
            for value in session.scalars(
                select(model.owner_username).where(
                    model.status.in_(statuses)
                )
            ).all()
        )
    return sorted(usernames)


def _snapshot_selected_usernames(
    snapshot: dict[str, Any],
) -> list[str]:
    return sorted(
        {
            str(username or "").strip()
            for username in snapshot.get("selectedUsernames", [])
            if str(username or "").strip()
        }
    )


def _selected_usernames(state: dict[str, Any]) -> list[str]:
    snapshot = (
        state.get("snapshot")
        if isinstance(state.get("snapshot"), dict)
        else {}
    )
    return _snapshot_selected_usernames(snapshot)


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
    schedule_groups = (
        snapshot.get("schedules")
        if isinstance(snapshot.get("schedules"), dict)
        else {}
    )
    resumable_count = sum(
        len(items)
        for groups in (task_groups, schedule_groups)
        for items in groups.values()
        if isinstance(items, list)
    )
    last_result = (
        state.get("lastResult")
        if isinstance(state.get("lastResult"), dict)
        else {}
    )
    return {
        "paused": bool(state.get("paused")),
        "phase": str(state.get("phase") or ("paused" if state.get("paused") else "running")),
        "operationId": str(state.get("operationId") or ""),
        "stoppedAt": state.get("stoppedAt"),
        "stoppedBy": str(state.get("stoppedBy") or ""),
        "resumedAt": state.get("resumedAt"),
        "resumedBy": str(state.get("resumedBy") or ""),
        "selectedUsernames": _selected_usernames(state),
        "selectionLocked": bool(state.get("paused")),
        "activeCounts": counts,
        "activeTotal": sum(counts.values()),
        "resumableCount": resumable_count if state.get("paused") else 0,
        "deploySafe": _state_deploy_safe(state),
        "lastResult": last_result,
    }


def _state_deploy_safe(state: dict[str, Any]) -> bool:
    last_result = (
        state.get("lastResult")
        if isinstance(state.get("lastResult"), dict)
        else {}
    )
    return bool(
        state.get("paused")
        and state.get("phase") in {"paused", "resume_failed"}
        and last_result.get("deploySafe") is True
    )


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
