from __future__ import annotations

import asyncio
import json
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import pytest
from fastapi.routing import APIRoute
from starlette.requests import Request
from starlette.responses import JSONResponse
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.api import maintenance as maintenance_api
from app.core.auth import require_superadmin
from app.core.config import settings
from app.db.database import Base
from app.db.models import SystemMaintenanceSettingModel
from app.main import enforce_system_maintenance, request_is_maintenance_bypass_user
from app.services import maintenance_service, task_control_service


def test_maintenance_routes_and_permissions() -> None:
    routes = {
        (method, route.path): route
        for route in maintenance_api.router.routes
        if isinstance(route, APIRoute)
        for method in route.methods
    }
    assert ("GET", "/maintenance/status") in routes
    assert ("GET", "/maintenance/settings") in routes
    assert ("PUT", "/maintenance/settings") in routes
    assert ("GET", "/maintenance/task-control") in routes
    assert ("POST", "/maintenance/task-control/stop-all") in routes
    assert ("POST", "/maintenance/task-control/resume-all") in routes
    assert not routes[("GET", "/maintenance/status")].dependant.dependencies
    for key in [
        ("GET", "/maintenance/settings"),
        ("PUT", "/maintenance/settings"),
        ("GET", "/maintenance/task-control"),
        ("POST", "/maintenance/task-control/stop-all"),
        ("POST", "/maintenance/task-control/resume-all"),
    ]:
        dependency_calls = [dependency.call for dependency in routes[key].dependant.dependencies]
        assert require_superadmin in dependency_calls


def test_maintenance_status_becomes_active_at_start_time() -> None:
    now = datetime(2026, 8, 12, 10, 0, 0)
    row = SystemMaintenanceSettingModel(
        id=1,
        enabled=True,
        title="计划维护",
        message="正在升级",
        starts_at=now + timedelta(hours=1),
        estimated_ends_at=now + timedelta(hours=2),
        updated_by="superadmin",
    )

    scheduled = maintenance_service.maintenance_setting_to_public(row, now=now)
    active = maintenance_service.maintenance_setting_to_public(
        row,
        now=now + timedelta(hours=1),
    )

    assert scheduled["scheduled"] is True
    assert scheduled["active"] is False
    assert active["scheduled"] is False
    assert active["active"] is True


def test_save_maintenance_settings_persists_singleton() -> None:
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    Base.metadata.create_all(engine)
    test_session = sessionmaker(bind=engine, expire_on_commit=False, future=True)
    starts_at = datetime(2026, 8, 12, 21, 0, 0)
    ends_at = starts_at + timedelta(hours=2)

    with patch.object(maintenance_service, "SessionLocal", test_session):
        first = maintenance_service.save_maintenance_settings(
            enabled=True,
            title="系统升级",
            message="正在升级商品处理服务。",
            starts_at=starts_at,
            estimated_ends_at=ends_at,
            updated_by="superadmin",
            now=starts_at,
        )
        second = maintenance_service.save_maintenance_settings(
            enabled=False,
            title="维护结束",
            message="系统已恢复。",
            starts_at=None,
            estimated_ends_at=None,
            updated_by="superadmin",
        )
        with test_session() as session:
            rows = session.query(SystemMaintenanceSettingModel).all()

    assert first["active"] is True
    assert first["updatedBy"] == "superadmin"
    assert second["active"] is False
    assert len(rows) == 1


def test_timezone_aware_values_are_normalized_before_storage() -> None:
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    Base.metadata.create_all(engine)
    test_session = sessionmaker(bind=engine, expire_on_commit=False, future=True)
    starts_at = datetime(2026, 8, 12, 12, 0, 0, tzinfo=timezone.utc)

    with patch.object(maintenance_service, "SessionLocal", test_session):
        maintenance_service.save_maintenance_settings(
            enabled=True,
            title="系统升级",
            message="正在升级。",
            starts_at=starts_at,
            estimated_ends_at=starts_at + timedelta(hours=1),
            updated_by="superadmin",
        )
        with test_session() as session:
            row = session.get(SystemMaintenanceSettingModel, 1)

    assert row is not None
    assert row.starts_at is not None
    assert row.starts_at.tzinfo is None


def test_active_maintenance_blocks_ordinary_api_requests() -> None:
    request = build_request("/api/crawler/dashboard/summary")
    maintenance = {"active": True, "title": "系统维护中"}

    async def call_next(_request: Request):
        return JSONResponse({"status": "passed"})

    with (
        patch("app.main.get_maintenance_status", return_value=maintenance),
        patch("app.main.request_is_maintenance_bypass_user", return_value=False),
    ):
        response = asyncio.run(enforce_system_maintenance(request, call_next))

    assert response.status_code == 503
    assert response.headers["retry-after"] == "60"


def test_active_maintenance_allows_superadmin_api_requests() -> None:
    request = build_request("/api/crawler/dashboard/summary")

    async def call_next(_request: Request):
        return JSONResponse({"status": "passed"})

    with (
        patch("app.main.get_maintenance_status", return_value={"active": True}),
        patch("app.main.request_is_maintenance_bypass_user", return_value=True),
    ):
        response = asyncio.run(enforce_system_maintenance(request, call_next))

    assert response.status_code == 200


@pytest.mark.parametrize(
    ("user", "allowed"),
    [
        ({"username": "superadmin", "role": "superadmin"}, True),
        ({"username": "test", "role": "operator"}, True),
        ({"username": "Test", "role": "operator"}, False),
        ({"username": "operator", "role": "operator"}, False),
    ],
)
def test_maintenance_bypass_users_are_exact(user, allowed) -> None:
    request = build_request("/api/crawler/dashboard/summary")
    request.scope["headers"] = [
        (
            b"cookie",
            f"{settings.session_cookie_name}=session-token".encode(),
        )
    ]
    with (
        patch("app.main.read_session_token", return_value={"sub": user["username"]}),
        patch("app.main.require_existing_account", return_value=user),
    ):
        assert request_is_maintenance_bypass_user(request) is allowed


def test_maintenance_status_endpoint_is_always_available() -> None:
    request = build_request("/api/maintenance/status")

    async def call_next(_request: Request):
        return JSONResponse({"status": "passed"})

    with patch("app.main.get_maintenance_status") as get_status:
        response = asyncio.run(enforce_system_maintenance(request, call_next))

    assert response.status_code == 200
    get_status.assert_not_called()


def test_task_control_status_defaults_to_running() -> None:
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    Base.metadata.create_all(engine)
    test_session = sessionmaker(bind=engine, expire_on_commit=False, future=True)

    with patch.object(task_control_service, "SessionLocal", test_session):
        status = task_control_service.get_task_control_status()

    assert status["paused"] is False
    assert status["activeTotal"] == 0
    assert status["resumableCount"] == 0


def test_stop_all_tasks_snapshots_and_cancels_active_rows() -> None:
    from app.db.models import CrawlTaskModel, ListingTaskModel, SyncTaskModel

    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    Base.metadata.create_all(engine)
    test_session = sessionmaker(bind=engine, expire_on_commit=False, future=True)
    with test_session() as session:
        session.add_all(
            [
                CrawlTaskModel(
                    id="crawl-active",
                    owner_username="alice",
                    source_type="keyword",
                    target="test",
                    mode="manual",
                    status="queued",
                    message="等待执行",
                ),
                ListingTaskModel(
                    id="listing-active",
                    owner_username="alice",
                    status="running",
                    product_ids_json="[]",
                    message="执行中",
                ),
                SyncTaskModel(
                    id="sync-active",
                    owner_username="alice",
                    task_type="store_sync",
                    payload_json="{}",
                    status="queued",
                    message="等待执行",
                ),
                SyncTaskModel(
                    id="sync-old-cancelled",
                    owner_username="alice",
                    task_type="store_sync",
                    payload_json="{}",
                    status="cancelled",
                    message="历史取消",
                ),
            ]
        )
        session.commit()

    with (
        patch.object(task_control_service, "SessionLocal", test_session),
        patch.object(task_control_service, "_stop_snapshot_rq_jobs", return_value={"queued": 3, "deferred": 0, "scheduled": 0, "started": 0}),
        patch("app.services.crawler_service.release_listing_task_locks"),
    ):
        status = task_control_service.stop_all_tasks(operated_by="superadmin")

    with test_session() as session:
        crawl = session.get(CrawlTaskModel, "crawl-active")
        listing = session.get(ListingTaskModel, "listing-active")
        sync = session.get(SyncTaskModel, "sync-active")
        historical = session.get(SyncTaskModel, "sync-old-cancelled")

    assert status["paused"] is True
    assert status["phase"] == "paused"
    assert status["activeTotal"] == 0
    assert status["resumableCount"] == 3
    assert crawl is not None and crawl.status == "cancelled"
    assert listing is not None and listing.status == "cancelled"
    assert sync is not None and sync.status == "cancelled"
    assert historical is not None and historical.message == "历史取消"


def test_resume_all_only_uses_current_stop_snapshot() -> None:
    from app.db.models import CrawlTaskModel, SystemTaskControlModel

    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    Base.metadata.create_all(engine)
    test_session = sessionmaker(bind=engine, expire_on_commit=False, future=True)
    snapshot = {
        "tasks": {
            "crawl": [
                {
                    "id": "crawl-paused",
                    "ownerUsername": "alice",
                    "status": "queued",
                }
            ],
            "listing": [],
            "sync": [],
            "salesOrderSync": [],
            "imageCleanupRecords": [],
        },
        "schedules": {
            "scheduledCrawls": [],
            "autoListing": [],
            "autoDeletion": [],
        },
    }
    with test_session() as session:
        session.add(
            CrawlTaskModel(
                id="crawl-paused",
                owner_username="alice",
                source_type="keyword",
                target="test",
                mode="manual",
                status="cancelled",
                message=task_control_service.TASK_CONTROL_STOP_MESSAGE,
            )
        )
        session.add(
            CrawlTaskModel(
                id="crawl-historical",
                owner_username="alice",
                source_type="keyword",
                target="old",
                mode="manual",
                status="cancelled",
                message="历史取消",
            )
        )
        session.add(
            SystemTaskControlModel(
                id=task_control_service.TASK_CONTROL_ROW_ID,
                paused=True,
                phase="paused",
                operation_id="operation",
                snapshot_json=json.dumps(snapshot),
                last_result_json="{}",
            )
        )
        session.commit()

    with (
        patch.object(task_control_service, "SessionLocal", test_session),
        patch("app.services.crawler_service.dispatch_queued_crawl_tasks_safely"),
        patch("app.services.crawler_service.dispatch_next_listing_task_safely"),
        patch("app.services.crawler_service.dispatch_next_sync_task_safely"),
    ):
        status = task_control_service.resume_all_tasks(operated_by="superadmin")

    with test_session() as session:
        resumed = session.get(CrawlTaskModel, "crawl-paused")
        historical = session.get(CrawlTaskModel, "crawl-historical")

    assert status["paused"] is False
    assert status["phase"] == "running"
    assert status["resumableCount"] == 0
    assert resumed is not None and resumed.status == "queued"
    assert historical is not None and historical.status == "cancelled"


def build_request(path: str) -> Request:
    return Request(
        {
            "type": "http",
            "method": "GET",
            "path": path,
            "headers": [],
            "query_string": b"",
            "server": ("testserver", 80),
            "client": ("127.0.0.1", 12345),
            "scheme": "http",
        }
    )
