from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

from fastapi.routing import APIRoute
from starlette.requests import Request
from starlette.responses import JSONResponse
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.api import maintenance as maintenance_api
from app.core.auth import require_superadmin
from app.db.database import Base
from app.db.models import SystemMaintenanceSettingModel
from app.main import enforce_system_maintenance
from app.services import maintenance_service


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
    assert not routes[("GET", "/maintenance/status")].dependant.dependencies
    for key in [("GET", "/maintenance/settings"), ("PUT", "/maintenance/settings")]:
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
        patch("app.main.request_is_superadmin", return_value=False),
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
        patch("app.main.request_is_superadmin", return_value=True),
    ):
        response = asyncio.run(enforce_system_maintenance(request, call_next))

    assert response.status_code == 200


def test_maintenance_status_endpoint_is_always_available() -> None:
    request = build_request("/api/maintenance/status")

    async def call_next(_request: Request):
        return JSONResponse({"status": "passed"})

    with patch("app.main.get_maintenance_status") as get_status:
        response = asyncio.run(enforce_system_maintenance(request, call_next))

    assert response.status_code == 200
    get_status.assert_not_called()


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
