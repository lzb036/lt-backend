from __future__ import annotations

import inspect
import json
import time
from contextlib import contextmanager
from datetime import datetime, timedelta
from pathlib import Path
from threading import Event
from types import SimpleNamespace
from typing import Any, Iterator

import pytest
from fastapi import FastAPI
from fastapi.routing import APIRoute
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, event
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from app.api import crawler as crawler_api
from app.core.auth import require_authenticated_account
from app.db.database import Base
from app.db.models import (
    SalesAnalysisMessageModel,
    SalesSyncStateModel,
    StoreModel,
    UserAccountModel,
)
from app.services import (
    crawler_service,
    sales_ai_service,
    sales_sync_service,
)


AI_USER = {
    "username": "owner-a",
    "role": "operator",
    "permissionCodes": ["ai.manage"],
}


def _wait_for_owner_stream_capacity_release(
    timeout_seconds: float = 1.0,
) -> None:
    deadline = time.monotonic() + timeout_seconds
    while (
        crawler_service.SALES_ANALYSIS_STREAM_OWNER_ACTIVE
        and time.monotonic() < deadline
    ):
        time.sleep(0.005)


def test_requirements_declares_testclient_httpx_dependency() -> None:
    requirements = (
        Path(__file__).resolve().parents[1] / "requirements.txt"
    ).read_text(encoding="utf-8").splitlines()

    assert "httpx==0.28.1" in requirements


def _client(user: dict[str, Any]) -> TestClient:
    app = FastAPI()
    app.include_router(crawler_api.router)
    app.dependency_overrides[require_authenticated_account] = lambda: user
    return TestClient(app)


@pytest.fixture()
def sales_session_factory(monkeypatch: pytest.MonkeyPatch):
    engine = create_engine(
        "sqlite+pysqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
        future=True,
    )

    @event.listens_for(engine, "connect")
    def _enable_foreign_keys(dbapi_connection, _record):
        cursor = dbapi_connection.cursor()
        cursor.execute("PRAGMA foreign_keys=ON")
        cursor.close()

    Base.metadata.create_all(engine)
    factory = sessionmaker(
        bind=engine,
        autoflush=False,
        expire_on_commit=False,
        future=True,
    )

    @contextmanager
    def _session_scope():
        with factory() as session:
            try:
                yield session
                session.commit()
            except Exception:
                session.rollback()
                raise

    monkeypatch.setattr(crawler_service, "session_scope", _session_scope)
    monkeypatch.setattr(sales_ai_service, "session_scope", _session_scope)
    monkeypatch.setattr(sales_sync_service, "session_scope", _session_scope)
    try:
        yield factory
    finally:
        engine.dispose()


def _add_user(session: Session, username: str) -> None:
    session.add(
        UserAccountModel(
            username=username,
            display_name=username,
            password_salt_b64="salt",
            password_hash_b64="hash",
            created_at=datetime(2026, 7, 16, 8, 0, 0),
            updated_at=datetime(2026, 7, 16, 8, 0, 0),
        )
    )
    session.flush()


def _add_store(
    session: Session,
    owner_username: str,
    code: str,
    *,
    enabled: bool = True,
    credentials: bool = True,
) -> StoreModel:
    row = StoreModel(
        owner_username=owner_username,
        store_code=code,
        store_name=code,
        enabled=enabled,
        rakuten_service_secret_encrypted=(
            f"secret-{code}" if credentials else ""
        ),
        rakuten_license_key_encrypted=(
            f"license-{code}" if credentials else ""
        ),
        created_at=datetime(2026, 7, 16, 8, 0, 0),
        updated_at=datetime(2026, 7, 16, 8, 0, 0),
    )
    session.add(row)
    session.flush()
    return row


def test_sales_analysis_routes_require_ai_manage_permission() -> None:
    expected = {
        ("GET", "/crawler/sales-analysis/stores"),
        ("GET", "/crawler/sales-analysis/sync-state"),
        ("POST", "/crawler/sales-analysis/sync"),
        ("GET", "/crawler/sales-analysis/sync/{task_id}"),
        ("GET", "/crawler/sales-analysis/conversations"),
        ("POST", "/crawler/sales-analysis/conversations"),
        (
            "DELETE",
            "/crawler/sales-analysis/conversations/{conversation_id}",
        ),
        (
            "GET",
            "/crawler/sales-analysis/conversations/{conversation_id}/messages",
        ),
        (
            "POST",
            "/crawler/sales-analysis/conversations/{conversation_id}/messages",
        ),
    }
    routes = [
        route
        for route in crawler_api.router.routes
        if isinstance(route, APIRoute)
        and route.path.startswith("/crawler/sales-analysis")
    ]
    actual = {
        (method, route.path)
        for route in routes
        for method in route.methods
    }

    assert actual == expected
    for route in routes:
        dependencies = [item.call for item in route.dependant.dependencies]
        assert crawler_api.require_ai_permission in dependencies

    response = _client(
        {
            "username": "owner-a",
            "role": "operator",
            "permissionCodes": [],
        }
    ).get("/crawler/sales-analysis/stores")
    assert response.status_code == 403


def test_list_sales_analysis_stores_uses_current_tenant(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[str] = []

    def list_stores(owner_username: str) -> dict[str, Any]:
        calls.append(owner_username)
        return {
            "dataUpdatedAt": "2026-07-16T12:00:00",
            "rows": [{"id": 3, "name": "Owned Store"}],
        }

    monkeypatch.setattr(
        crawler_service,
        "list_sales_analysis_stores",
        list_stores,
        raising=False,
    )

    response = _client(AI_USER).get("/crawler/sales-analysis/stores")

    assert response.status_code == 200
    assert response.json() == {
        "stores": [{"id": 3, "name": "Owned Store"}],
        "dataUpdatedAt": "2026-07-16T12:00:00",
    }
    assert calls == ["owner-a"]


@pytest.mark.parametrize(
    ("method", "path", "json_body"),
    [
        (
            "get",
            "/crawler/sales-analysis/sync-state?storeId=99",
            None,
        ),
        (
            "post",
            "/crawler/sales-analysis/sync",
            {"storeId": 99},
        ),
    ],
)
def test_cross_owner_store_ids_are_not_disclosed(
    monkeypatch: pytest.MonkeyPatch,
    method: str,
    path: str,
    json_body: dict[str, Any] | None,
) -> None:
    hidden_calls = 0

    def hidden(*_args: Any, **_kwargs: Any) -> dict[str, Any]:
        nonlocal hidden_calls
        hidden_calls += 1
        raise LookupError("Other Owner Secret Store")

    monkeypatch.setattr(
        crawler_service,
        "get_sales_analysis_sync_state",
        hidden,
        raising=False,
    )
    monkeypatch.setattr(
        crawler_service,
        "queue_sales_analysis_sync",
        hidden,
        raising=False,
    )

    client_method = getattr(_client(AI_USER), method)
    response = (
        client_method(path)
        if json_body is None
        else client_method(path, json=json_body)
    )

    assert response.status_code in {403, 404}
    assert "Other Owner Secret Store" not in response.text
    assert "owner" not in response.text.casefold()
    assert hidden_calls == 1


def test_manual_sync_returns_bounded_task_status(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[str, int]] = []

    def queue_sync(owner_username: str, store_id: int) -> dict[str, Any]:
        calls.append((owner_username, store_id))
        return {
            "id": "sales-7",
            "storeId": 7,
            "status": "queued",
            "progressCurrent": 0,
            "progressTotal": 0,
        }

    monkeypatch.setattr(
        crawler_service,
        "queue_sales_analysis_sync",
        queue_sync,
        raising=False,
    )

    response = _client(AI_USER).post(
        "/crawler/sales-analysis/sync",
        json={"storeId": 7},
    )

    assert response.status_code == 200
    assert response.json()["syncTask"] == {
        "id": "sales-7",
        "storeId": 7,
        "status": "queued",
        "progressCurrent": 0,
        "progressTotal": 0,
    }
    assert calls == [("owner-a", 7)]


def test_sync_task_status_uses_current_tenant(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[str, str]] = []

    def get_task(owner_username: str, task_id: str) -> dict[str, Any]:
        calls.append((owner_username, task_id))
        return {
            "id": task_id,
            "storeId": 7,
            "status": "running",
        }

    monkeypatch.setattr(
        crawler_service,
        "get_sales_analysis_sync_task",
        get_task,
        raising=False,
    )

    response = _client(AI_USER).get(
        "/crawler/sales-analysis/sync/sales-7"
    )

    assert response.status_code == 200
    assert response.json()["syncTask"]["status"] == "running"
    assert calls == [("owner-a", "sales-7")]


def test_conversation_crud_and_messages_delegate_with_owner_scope(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[Any, ...]] = []

    monkeypatch.setattr(
        crawler_service,
        "list_sales_analysis_conversations",
        lambda owner: calls.append(("list", owner))
        or [{"id": 11, "title": "分析"}],
        raising=False,
    )
    monkeypatch.setattr(
        crawler_service,
        "create_sales_analysis_conversation",
        lambda owner, title: calls.append(("create", owner, title))
        or {"id": 12, "title": title},
        raising=False,
    )
    monkeypatch.setattr(
        crawler_service,
        "delete_sales_analysis_conversation",
        lambda owner, conversation_id: calls.append(
            ("delete", owner, conversation_id)
        ),
        raising=False,
    )
    monkeypatch.setattr(
        crawler_service,
        "list_sales_analysis_messages",
        lambda owner, conversation_id, page, limit: calls.append(
            ("messages", owner, conversation_id, page, limit)
        )
        or {
            "messages": [{"id": 21, "conversationId": conversation_id}],
            "total": 1,
            "page": page,
            "pageSize": limit,
            "truncated": False,
        },
        raising=False,
    )
    client = _client(AI_USER)

    listed = client.get("/crawler/sales-analysis/conversations")
    created = client.post(
        "/crawler/sales-analysis/conversations",
        json={"title": "  月度分析  "},
    )
    messages = client.get(
        "/crawler/sales-analysis/conversations/11/messages?page=1&limit=50"
    )
    deleted = client.delete(
        "/crawler/sales-analysis/conversations/11"
    )

    assert listed.json() == {
        "conversations": [{"id": 11, "title": "分析"}]
    }
    assert created.json() == {
        "conversation": {"id": 12, "title": "月度分析"}
    }
    assert messages.json() == {
        "messages": [{"id": 21, "conversationId": 11}],
        "total": 1,
        "page": 1,
        "pageSize": 50,
        "truncated": False,
    }
    assert deleted.json() == {"deleted": True}
    assert calls == [
        ("list", "owner-a"),
        ("create", "owner-a", "月度分析"),
        ("messages", "owner-a", 11, 1, 50),
        ("delete", "owner-a", 11),
    ]


def test_cross_owner_conversation_is_rejected_before_streaming(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    model_called = False
    ownership_checked = False

    def require_conversation(
        _owner_username: str,
        _conversation_id: int,
    ) -> dict[str, Any]:
        nonlocal ownership_checked
        ownership_checked = True
        raise LookupError("Other Owner Conversation")

    def stream_analysis(
        _owner_username: str,
        _conversation_id: int,
        _message: str,
    ) -> Iterator[dict[str, Any]]:
        nonlocal model_called
        model_called = True
        yield {"type": "completed"}

    monkeypatch.setattr(
        crawler_service,
        "require_sales_analysis_conversation",
        require_conversation,
        raising=False,
    )
    monkeypatch.setattr(
        crawler_service,
        "stream_sales_analysis",
        stream_analysis,
        raising=False,
    )

    response = _client(AI_USER).post(
        "/crawler/sales-analysis/conversations/91/messages",
        json={"message": "分析近期销量"},
    )

    assert response.status_code in {403, 404}
    assert "Other Owner Conversation" not in response.text
    assert ownership_checked is True
    assert model_called is False


def test_message_stream_serializes_events_and_stops_at_terminal_event(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    serialized: list[dict[str, Any]] = []
    original_serializer = crawler_api.serialize_sse_event

    monkeypatch.setattr(
        crawler_service,
        "require_sales_analysis_conversation",
        lambda owner, conversation_id: {
            "id": conversation_id,
            "owner": owner,
        },
        raising=False,
    )

    def stream_analysis(
        owner_username: str,
        conversation_id: int,
        message: str,
        **_kwargs: Any,
    ) -> Iterator[dict[str, Any]]:
        assert (owner_username, conversation_id, message) == (
            "owner-a",
            11,
            "分析近期销量",
        )
        yield {"type": "status", "message": "准备中"}
        yield {"type": "delta", "content": "结果"}
        yield {"type": "completed", "message": {"id": 21}}
        yield {"type": "delta", "content": "不应发送"}

    def serialize(event: dict[str, Any]) -> bytes:
        serialized.append(event)
        return original_serializer(event)

    monkeypatch.setattr(
        crawler_service,
        "stream_sales_analysis",
        stream_analysis,
        raising=False,
    )
    monkeypatch.setattr(crawler_api, "serialize_sse_event", serialize)

    response = _client(AI_USER).post(
        "/crawler/sales-analysis/conversations/11/messages",
        json={"message": "分析近期销量"},
    )

    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/event-stream")
    assert serialized == [
        {"type": "status", "message": "准备中"},
        {"type": "delta", "content": "结果"},
        {"type": "completed", "message": {"id": 21}},
    ]
    assert response.content == b"".join(
        original_serializer(event) for event in serialized
    )


def test_message_stream_converts_iterator_failure_to_terminal_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        crawler_service,
        "require_sales_analysis_conversation",
        lambda _owner, conversation_id: {"id": conversation_id},
        raising=False,
    )

    def broken_stream(
        _owner_username: str,
        _conversation_id: int,
        _message: str,
        **_kwargs: Any,
    ) -> Iterator[dict[str, Any]]:
        yield {"type": "status", "message": "准备中"}
        raise RuntimeError("secret failure detail")

    monkeypatch.setattr(
        crawler_service,
        "stream_sales_analysis",
        broken_stream,
        raising=False,
    )

    response = _client(AI_USER).post(
        "/crawler/sales-analysis/conversations/11/messages",
        json={"message": "分析近期销量"},
    )

    assert response.status_code == 200
    assert '"type": "status"' in response.text
    assert '"type": "error"' in response.text
    assert "secret failure detail" not in response.text
    assert response.text.rstrip().endswith("}")


def test_message_stream_rejects_invalid_events_and_closes_iterator(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    closed = False

    class InvalidIterator:
        def __iter__(self):
            return self

        def __next__(self):
            return {
                "type": "internal_debug",
                "secret": "must-not-leak",
            }

        def close(self):
            nonlocal closed
            closed = True

    monkeypatch.setattr(
        crawler_service,
        "require_sales_analysis_conversation",
        lambda _owner, conversation_id: {"id": conversation_id},
    )
    monkeypatch.setattr(
        crawler_service,
        "stream_sales_analysis",
        lambda *_args, **_kwargs: InvalidIterator(),
    )

    response = _client(AI_USER).post(
        "/crawler/sales-analysis/conversations/11/messages",
        json={"message": "分析近期销量"},
    )

    assert response.status_code == 200
    assert '"type": "error"' in response.text
    assert "internal_debug" not in response.text
    assert "must-not-leak" not in response.text
    assert closed is True


def test_background_stream_emits_status_heartbeat_while_producer_blocks(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    release = Event()
    started = Event()
    cancel_event = Event()

    def blocking_stream(
        _owner: str,
        _conversation_id: int,
        _message: str,
        *,
        is_cancelled,
    ) -> Iterator[dict[str, Any]]:
        started.set()
        release.wait(timeout=2)
        if is_cancelled():
            return
        yield {"type": "completed", "message": {"id": 1}}

    monkeypatch.setattr(
        sales_ai_service,
        "stream_analysis",
        blocking_stream,
    )
    stream = crawler_service.stream_sales_analysis(
        "owner-a",
        11,
        "分析近期销量",
        cancel_event=cancel_event,
        heartbeat_interval=0.01,
    )

    heartbeat = next(stream)

    assert started.wait(timeout=1)
    assert heartbeat == {
        "type": "status",
        "message": "销量分析仍在处理中。",
        "heartbeat": True,
    }
    release.set()
    assert next(stream)["type"] == "completed"
    stream.close()


def test_closing_background_stream_cancels_and_closes_inner_iterator(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cancel_event = Event()
    inner_closed = Event()
    cancellation_seen = Event()
    monkeypatch.setattr(
        crawler_service,
        "SALES_ANALYSIS_STREAM_OWNER_ACTIVE",
        {},
        raising=False,
    )

    def cancellable_stream(
        _owner: str,
        _conversation_id: int,
        _message: str,
        *,
        is_cancelled,
    ) -> Iterator[dict[str, Any]]:
        try:
            while not is_cancelled():
                time.sleep(0.005)
            cancellation_seen.set()
            return
            yield
        finally:
            inner_closed.set()

    monkeypatch.setattr(
        sales_ai_service,
        "stream_analysis",
        cancellable_stream,
    )
    stream = crawler_service.stream_sales_analysis(
        "owner-a",
        11,
        "分析近期销量",
        cancel_event=cancel_event,
        heartbeat_interval=0.01,
    )

    assert next(stream)["heartbeat"] is True
    stream.close()

    assert cancel_event.is_set()
    assert cancellation_seen.wait(timeout=1)
    assert inner_closed.wait(timeout=1)
    _wait_for_owner_stream_capacity_release()
    assert crawler_service.SALES_ANALYSIS_STREAM_OWNER_ACTIVE == {}


def test_background_stream_rejects_when_producer_slots_are_full(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    slots = SimpleNamespace(acquire=lambda **_kwargs: False)
    monkeypatch.setattr(
        crawler_service,
        "SALES_ANALYSIS_STREAM_SLOTS",
        slots,
        raising=False,
    )
    monkeypatch.setattr(
        sales_ai_service,
        "stream_analysis",
        lambda *_args, **_kwargs: pytest.fail(
            "full producer slots must reject before inner service access"
        ),
    )

    events = list(
        crawler_service.stream_sales_analysis(
            "owner-a",
            11,
            "分析近期销量",
        )
    )

    assert events == [
        {
            "type": "error",
            "message": "当前销量分析任务较多，请稍后重试。",
        }
    ]


def test_background_stream_enforces_per_owner_fairness(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    release = Event()
    first_started = Event()
    second_started = Event()
    monkeypatch.setattr(
        crawler_service,
        "SALES_ANALYSIS_STREAM_OWNER_ACTIVE",
        {},
        raising=False,
    )

    def owner_stream(
        owner: str,
        conversation_id: int,
        _message: str,
        *,
        is_cancelled,
    ) -> Iterator[dict[str, Any]]:
        if owner == "owner-a":
            if conversation_id == 11:
                first_started.set()
            if conversation_id == 12:
                second_started.set()
            release.wait(timeout=2)
            if is_cancelled():
                return
        yield {"type": "completed", "message": {"owner": owner}}

    monkeypatch.setattr(
        sales_ai_service,
        "stream_analysis",
        owner_stream,
    )
    first = crawler_service.stream_sales_analysis(
        "owner-a",
        11,
        "分析近期销量",
        heartbeat_interval=0.01,
    )

    assert next(first)["heartbeat"] is True
    assert first_started.wait(timeout=1)
    second = crawler_service.stream_sales_analysis(
        "owner-a",
        12,
        "同一用户的第二个请求",
        heartbeat_interval=0.01,
    )
    assert next(second)["heartbeat"] is True
    assert second_started.wait(timeout=1)
    assert list(
        crawler_service.stream_sales_analysis(
            "owner-a",
            13,
            "同一用户的第三个请求",
            heartbeat_interval=0.01,
        )
    ) == [
        {
            "type": "error",
            "message": "当前用户已有销量分析正在进行，请稍后重试。",
        }
    ]
    other_owner_events = list(
        crawler_service.stream_sales_analysis(
            "owner-b",
            13,
            "其他用户请求",
            heartbeat_interval=0.01,
        )
    )
    assert other_owner_events[-1]["type"] == "completed"

    release.set()
    assert next(first)["type"] == "completed"
    assert next(second)["type"] == "completed"
    first.close()
    second.close()
    assert crawler_service.SALES_ANALYSIS_STREAM_MAX_WORKERS == 4
    assert crawler_service.SALES_ANALYSIS_STREAM_MAX_PER_OWNER == 2
    assert crawler_service.SALES_ANALYSIS_STREAM_OWNER_ACTIVE == {}


def test_background_stream_error_releases_owner_capacity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        crawler_service,
        "SALES_ANALYSIS_STREAM_OWNER_ACTIVE",
        {},
        raising=False,
    )

    def broken_stream(
        _owner: str,
        _conversation_id: int,
        _message: str,
        *,
        is_cancelled,
    ) -> Iterator[dict[str, Any]]:
        del is_cancelled
        raise RuntimeError("producer failed")
        yield

    monkeypatch.setattr(
        sales_ai_service,
        "stream_analysis",
        broken_stream,
    )

    events = list(
        crawler_service.stream_sales_analysis(
            "owner-a",
            11,
            "分析近期销量",
            heartbeat_interval=0.01,
        )
    )

    assert events == [
        {
            "type": "error",
            "message": "销量分析失败，请稍后重试。",
        }
    ]
    _wait_for_owner_stream_capacity_release()
    assert crawler_service.SALES_ANALYSIS_STREAM_OWNER_ACTIVE == {}


def test_cancelled_pending_producer_releases_stream_slot(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    releases = 0

    class Slots:
        def acquire(self, **_kwargs):
            return True

        def release(self):
            nonlocal releases
            releases += 1

    class PendingFuture:
        def __init__(self):
            self.callback = None
            self.was_cancelled = False

        def add_done_callback(self, callback):
            self.callback = callback

        def done(self):
            return True

        def cancel(self):
            self.was_cancelled = True
            if self.callback is not None:
                self.callback(self)
            return True

        def cancelled(self):
            return self.was_cancelled

    future = PendingFuture()
    monkeypatch.setattr(
        crawler_service,
        "SALES_ANALYSIS_STREAM_SLOTS",
        Slots(),
    )
    monkeypatch.setattr(
        crawler_service,
        "SALES_ANALYSIS_STREAM_EXECUTOR",
        SimpleNamespace(submit=lambda _producer: future),
    )

    events = list(
        crawler_service.stream_sales_analysis(
            "owner-a",
            11,
            "分析近期销量",
            heartbeat_interval=0.01,
        )
    )

    assert events == []
    assert future.was_cancelled is True
    assert releases == 1


def test_sales_ai_cancellation_after_tool_event_prevents_tool_call(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cancelled = False
    executed = False
    monkeypatch.setattr(
        sales_ai_service,
        "_start_turn",
        lambda *_args: {
            "question": "分析近期销量",
            "messageId": 1,
            "selectedStore": {
                "id": 7,
                "name": "Store",
                "code": "store",
            },
            "stores": [],
            "sensitiveValues": [],
            "history": [],
        },
    )
    monkeypatch.setattr(
        sales_ai_service,
        "_load_model_configuration",
        lambda _owner: {
            "apiKey": "key",
            "apiBase": "",
            "modelName": "model",
        },
    )
    monkeypatch.setattr(
        sales_ai_service,
        "litellm_completion",
        lambda **_kwargs: {
            "choices": [
                {
                    "message": {
                        "content": "",
                        "tool_calls": [
                            {
                                "id": "call-1",
                                "type": "function",
                                "function": {
                                    "name": "get_product_sales_ranking",
                                    "arguments": '{"limit":10}',
                                },
                            }
                        ],
                    }
                }
            ]
        },
    )

    def execute(*_args):
        nonlocal executed
        executed = True
        return {}

    monkeypatch.setattr(
        sales_ai_service,
        "execute_sales_tool",
        execute,
    )
    stream = sales_ai_service.stream_analysis(
        "owner-a",
        11,
        "分析近期销量",
        is_cancelled=lambda: cancelled,
    )

    assert next(stream)["type"] == "status"
    assert next(stream)["type"] == "tool_call"
    cancelled = True
    assert list(stream) == []
    assert executed is False


@pytest.mark.parametrize(
    ("path", "payload"),
    [
        ("/crawler/sales-analysis/sync", {"storeId": 0}),
        (
            "/crawler/sales-analysis/conversations",
            {"title": "x" * 256},
        ),
        (
            "/crawler/sales-analysis/conversations/1/messages",
            {"message": "x" * 100_001},
        ),
    ],
)
def test_sales_analysis_payloads_are_bounded(
    monkeypatch: pytest.MonkeyPatch,
    path: str,
    payload: dict[str, Any],
) -> None:
    monkeypatch.setattr(
        crawler_service,
        "require_sales_analysis_conversation",
        lambda _owner, conversation_id: {"id": conversation_id},
        raising=False,
    )

    response = _client(AI_USER).post(path, json=payload)

    assert response.status_code == 422


def test_history_limit_is_bounded(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        crawler_service,
        "list_sales_analysis_messages",
        lambda *_args: pytest.fail(
            "invalid history limit must fail before service access"
        ),
    )
    response = _client(AI_USER).get(
        "/crawler/sales-analysis/conversations/1/messages?limit=101"
    )

    assert response.status_code == 422


def test_history_page_is_bounded(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        crawler_service,
        "list_sales_analysis_messages",
        lambda *_args: pytest.fail(
            "invalid history page must fail before service access"
        ),
    )
    response = _client(AI_USER).get(
        "/crawler/sales-analysis/conversations/1/messages?page=10001"
    )

    assert response.status_code == 422


def test_crawler_facade_uses_public_sales_ai_crud_interfaces(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[Any, ...]] = []
    monkeypatch.setattr(
        sales_ai_service,
        "get_conversation",
        lambda owner, conversation_id: calls.append(
            ("get", owner, conversation_id)
        )
        or {"id": conversation_id},
        raising=False,
    )
    monkeypatch.setattr(
        sales_ai_service,
        "delete_conversation",
        lambda owner, conversation_id: calls.append(
            ("delete", owner, conversation_id)
        ),
        raising=False,
    )
    monkeypatch.setattr(
        sales_ai_service,
        "list_messages",
        lambda owner, conversation_id, page, page_size: calls.append(
            ("messages", owner, conversation_id, page, page_size)
        )
        or {
            "messages": [],
            "total": 0,
            "page": page,
            "pageSize": page_size,
            "truncated": False,
        },
        raising=False,
    )
    monkeypatch.setattr(
        sales_ai_service,
        "_conversation_to_public",
        lambda *_args: pytest.fail(
            "crawler facade must not use private conversation serializers"
        ),
    )
    monkeypatch.setattr(
        sales_ai_service,
        "_message_to_public",
        lambda *_args, **_kwargs: pytest.fail(
            "history must not use the full private message serializer"
        ),
    )
    monkeypatch.setattr(
        crawler_service,
        "session_scope",
        lambda: pytest.fail(
            "crawler facade must delegate DB access to sales_ai_service"
        ),
    )

    assert crawler_service.require_sales_analysis_conversation(
        "owner-a",
        11,
    ) == {"id": 11}
    crawler_service.delete_sales_analysis_conversation("owner-a", 11)
    assert crawler_service.list_sales_analysis_messages(
        "owner-a",
        11,
        2,
        25,
    )["page"] == 2
    assert calls == [
        ("get", "owner-a", 11),
        ("delete", "owner-a", 11),
        ("messages", "owner-a", 11, 2, 25),
    ]


def test_public_conversation_crud_is_tenant_safe_in_database(
    sales_session_factory,
) -> None:
    with sales_session_factory() as session:
        _add_user(session, "crud-owner")
        _add_user(session, "other-owner")
        session.commit()

    own = sales_ai_service.create_conversation(
        "crud-owner",
        "Own",
    )
    other = sales_ai_service.create_conversation(
        "other-owner",
        "Other",
    )
    with sales_session_factory() as session:
        session.add(
            SalesAnalysisMessageModel(
                conversation_id=other["id"],
                owner_username="other-owner",
                question_text="private question",
                answer_text="private answer",
                tool_name="",
                tool_arguments_json="[]",
                result_summary_json="[]",
                model_name="",
                store_scope_json="[]",
                statistics_window_json="{}",
                created_at=datetime(2026, 7, 16, 9, 0, 0),
                updated_at=datetime(2026, 7, 16, 9, 0, 0),
            )
        )
        session.commit()

    with pytest.raises(LookupError):
        sales_ai_service.get_conversation(
            "crud-owner",
            other["id"],
        )
    with pytest.raises(LookupError):
        sales_ai_service.list_messages(
            "crud-owner",
            other["id"],
            1,
            20,
        )
    with pytest.raises(LookupError):
        sales_ai_service.delete_conversation(
            "crud-owner",
            other["id"],
        )

    assert sales_ai_service.get_conversation(
        "crud-owner",
        own["id"],
    )["title"] == "Own"
    assert sales_ai_service.get_conversation(
        "other-owner",
        other["id"],
    )["title"] == "Other"


def test_history_response_has_per_message_and_total_byte_bounds(
    sales_session_factory,
) -> None:
    with sales_session_factory() as session:
        _add_user(session, "history-owner")
        session.commit()
    conversation = sales_ai_service.create_conversation(
        "history-owner",
        "History",
    )
    huge_rows = [
        {
            "manageNumber": f"item-{index}",
            "itemName": "商品" * 500,
            "effectiveUnits": index,
        }
        for index in range(100)
    ]
    with sales_session_factory() as session:
        for index in range(12):
            session.add(
                SalesAnalysisMessageModel(
                    conversation_id=conversation["id"],
                    owner_username="history-owner",
                    question_text=f"question-{index}-" + ("问" * 8_000),
                    answer_text=f"answer-{index}-" + ("答" * 30_000),
                    tool_name="get_product_sales_ranking",
                    tool_arguments_json=json.dumps(
                        [{"storeId": 1, "query": "x" * 10_000}],
                        ensure_ascii=False,
                    ),
                    result_summary_json=json.dumps(
                        [
                            {
                                "toolName": "get_product_sales_ranking",
                                "result": {"rows": huge_rows},
                            }
                        ],
                        ensure_ascii=False,
                    ),
                    model_name="model",
                    store_scope_json=json.dumps(
                        list(range(10_000))
                    ),
                    statistics_window_json=json.dumps(
                        {
                            "start": "2026-06-01",
                            "end": "2026-06-30",
                            "unexpected": "x" * 100_000,
                        }
                    ),
                    created_at=datetime(2026, 7, 16, 9, index, 0),
                    updated_at=datetime(2026, 7, 16, 9, index, 0),
                )
            )
        session.commit()

    history = sales_ai_service.list_messages(
        "history-owner",
        conversation["id"],
        1,
        1_000,
    )

    assert history["total"] == 12
    assert history["page"] == 1
    assert history["pageSize"] == 100
    assert history["truncated"] is True
    assert history["messages"]
    assert all(
        len(
            json.dumps(
                message,
                ensure_ascii=False,
                separators=(",", ":"),
            ).encode("utf-8")
        )
        <= sales_ai_service.HISTORY_MESSAGE_MAX_BYTES
        for message in history["messages"]
    )
    assert len(
        json.dumps(
            history,
            ensure_ascii=False,
            separators=(",", ":"),
        ).encode("utf-8")
    ) <= sales_ai_service.HISTORY_RESPONSE_MAX_BYTES
    assert all(
        len(
            message.get("resultSummary", [{}])[0]
            .get("result", {})
            .get("rows", [])
        )
        <= sales_ai_service.HISTORY_RESULT_MAX_ROWS
        for message in history["messages"]
        if message.get("resultSummary")
    )


def test_sales_sync_due_threshold_and_active_states() -> None:
    now = datetime(2026, 7, 16, 12, 0, 0)
    stale_at = now - sales_sync_service.SALES_SYNC_LEASE_TIMEOUT

    assert crawler_service.sales_analysis_sync_is_due(
        None,
        "idle",
        now=now,
    )
    assert crawler_service.sales_analysis_sync_is_due(
        now - timedelta(minutes=30),
        "idle",
        now=now,
    )
    assert not crawler_service.sales_analysis_sync_is_due(
        now - timedelta(minutes=29, seconds=59),
        "idle",
        now=now,
    )
    assert not crawler_service.sales_analysis_sync_is_due(
        now - timedelta(hours=1),
        "queued",
        now=now,
        state_updated_at=now,
    )
    assert not crawler_service.sales_analysis_sync_is_due(
        now - timedelta(hours=1),
        "running:lease",
        now=now,
        state_updated_at=now,
    )
    assert not crawler_service.sales_analysis_sync_is_due(
        now - timedelta(hours=1),
        "queued",
        now=now,
        state_updated_at=stale_at + timedelta(seconds=1),
    )
    assert not crawler_service.sales_analysis_sync_is_due(
        now - timedelta(hours=1),
        "running:stale-lease",
        now=now,
        state_updated_at=stale_at,
    )
    assert crawler_service.sales_analysis_sync_is_due(
        now - timedelta(hours=1),
        "queued",
        now=now,
        state_updated_at=stale_at - timedelta(seconds=1),
    )


def test_due_sales_sync_candidates_are_batched_and_deterministic(
    sales_session_factory,
) -> None:
    with sales_session_factory() as session:
        _add_user(session, "batch-owner")
        stores = [
            _add_store(
                session,
                "batch-owner",
                f"store-{index:02d}",
            )
            for index in range(25)
        ]
        session.commit()

    candidates = crawler_service.sales_analysis_sync_due_candidates(
        datetime(2026, 7, 16, 12, 0, 0)
    )

    assert crawler_service.SALES_ANALYSIS_SYNC_BATCH_SIZE == 20
    assert len(candidates) == 20
    assert candidates == [
        ("batch-owner", store.id)
        for store in stores[:20]
    ]


def test_due_sales_sync_candidates_respect_global_active_capacity(
    sales_session_factory,
) -> None:
    now = datetime(2026, 7, 16, 12, 0, 0)
    with sales_session_factory() as session:
        _add_user(session, "capacity-owner")
        active_stores = [
            _add_store(
                session,
                "capacity-owner",
                f"active-{index:02d}",
            )
            for index in range(5)
        ]
        due_stores = [
            _add_store(
                session,
                "capacity-owner",
                f"due-{index:02d}",
            )
            for index in range(25)
        ]
        session.add_all(
            [
                SalesSyncStateModel(
                    owner_username="capacity-owner",
                    store_id=store.id,
                    sync_status=(
                        "queued"
                        if index % 2 == 0
                        else f"running:lease-{index}"
                    ),
                    created_at=now,
                    updated_at=now,
                )
                for index, store in enumerate(active_stores)
            ]
        )
        session.commit()

    candidates = crawler_service.sales_analysis_sync_due_candidates(now)

    assert len(candidates) == 15
    assert candidates == [
        ("capacity-owner", store.id)
        for store in due_stores[:15]
    ]


def test_twenty_stale_active_rows_do_not_block_scheduled_recovery(
    sales_session_factory,
) -> None:
    now = datetime(2026, 7, 16, 12, 0, 0)
    stale_at = (
        now
        - sales_sync_service.SALES_SYNC_LEASE_TIMEOUT
        - timedelta(seconds=1)
    )
    with sales_session_factory() as session:
        _add_user(session, "stale-capacity-owner")
        due_store = _add_store(
            session,
            "stale-capacity-owner",
            "due-first",
        )
        stale_stores = [
            _add_store(
                session,
                "stale-capacity-owner",
                f"stale-{index:02d}",
            )
            for index in range(20)
        ]
        session.add_all(
            [
                SalesSyncStateModel(
                    owner_username="stale-capacity-owner",
                    store_id=store.id,
                    sync_status=(
                        "queued"
                        if index % 2 == 0
                        else f"running:stale-{index}"
                    ),
                    created_at=stale_at,
                    updated_at=stale_at,
                )
                for index, store in enumerate(stale_stores)
            ]
        )
        session.commit()

    candidates = crawler_service.sales_analysis_sync_due_candidates(now)

    assert len(candidates) == crawler_service.SALES_ANALYSIS_SYNC_BATCH_SIZE
    assert candidates[0] == (
        "stale-capacity-owner",
        due_store.id,
    )
    assert {
        store_id for _owner, store_id in candidates[1:]
    } <= {store.id for store in stale_stores}


def test_due_sales_sync_sql_filters_credentials_active_and_error_cooldown(
    sales_session_factory,
) -> None:
    now = datetime(2026, 7, 16, 12, 0, 0)
    with sales_session_factory() as session:
        _add_user(session, "filter-owner")
        due_no_state = _add_store(
            session,
            "filter-owner",
            "due-no-state",
        )
        due_idle = _add_store(session, "filter-owner", "due-idle")
        recent_success = _add_store(
            session,
            "filter-owner",
            "recent-success",
        )
        error_recent = _add_store(
            session,
            "filter-owner",
            "error-recent",
        )
        error_cooled = _add_store(
            session,
            "filter-owner",
            "error-cooled",
        )
        queued = _add_store(session, "filter-owner", "queued")
        running = _add_store(session, "filter-owner", "running")
        _add_store(
            session,
            "filter-owner",
            "missing-credentials",
            credentials=False,
        )
        _add_store(
            session,
            "filter-owner",
            "disabled",
            enabled=False,
        )
        session.add_all(
            [
                SalesSyncStateModel(
                    owner_username="filter-owner",
                    store_id=due_idle.id,
                    sync_status="idle",
                    last_successful_sync_at=now - timedelta(minutes=30),
                    created_at=now - timedelta(hours=1),
                    updated_at=now - timedelta(hours=1),
                ),
                SalesSyncStateModel(
                    owner_username="filter-owner",
                    store_id=recent_success.id,
                    sync_status="idle",
                    last_successful_sync_at=(
                        now - timedelta(minutes=29, seconds=59)
                    ),
                    created_at=now - timedelta(hours=1),
                    updated_at=now - timedelta(hours=1),
                ),
                SalesSyncStateModel(
                    owner_username="filter-owner",
                    store_id=error_recent.id,
                    sync_status="error",
                    last_successful_sync_at=None,
                    created_at=now - timedelta(hours=1),
                    updated_at=now - timedelta(minutes=29, seconds=59),
                ),
                SalesSyncStateModel(
                    owner_username="filter-owner",
                    store_id=error_cooled.id,
                    sync_status="error",
                    last_successful_sync_at=None,
                    created_at=now - timedelta(hours=1),
                    updated_at=now - timedelta(minutes=30),
                ),
                SalesSyncStateModel(
                    owner_username="filter-owner",
                    store_id=queued.id,
                    sync_status="queued",
                    last_successful_sync_at=None,
                    created_at=now - timedelta(hours=2),
                    updated_at=now - timedelta(hours=2),
                ),
                SalesSyncStateModel(
                    owner_username="filter-owner",
                    store_id=running.id,
                    sync_status="running:lease",
                    last_successful_sync_at=None,
                    created_at=now - timedelta(hours=2),
                    updated_at=now - timedelta(hours=2),
                ),
            ]
        )
        session.commit()

    candidates = crawler_service.sales_analysis_sync_due_candidates(now)

    assert candidates == [
        ("filter-owner", due_no_state.id),
        ("filter-owner", due_idle.id),
        ("filter-owner", error_cooled.id),
        ("filter-owner", queued.id),
        ("filter-owner", running.id),
    ]


def test_manual_sync_missing_credentials_returns_actionable_message(
    monkeypatch: pytest.MonkeyPatch,
    sales_session_factory,
) -> None:
    with sales_session_factory() as session:
        _add_user(session, "credential-owner")
        store = _add_store(
            session,
            "credential-owner",
            "missing",
            credentials=False,
        )
        session.commit()

    monkeypatch.setattr(
        crawler_service,
        "dispatch_sales_analysis_sync_task",
        lambda *_args: pytest.fail(
            "missing credentials must fail before dispatch"
        ),
    )
    response = _client(
        {
            "username": "credential-owner",
            "role": "operator",
            "permissionCodes": ["ai.manage"],
        }
    ).post(
        "/crawler/sales-analysis/sync",
        json={"storeId": store.id},
    )

    assert response.status_code == 400
    assert response.json()["detail"] == (
        "店铺未配置乐天订单同步凭证，"
        "请先在店铺管理中检查并保存店铺配置。"
    )


def test_manual_sync_rejects_disabled_store_before_dispatch(
    monkeypatch: pytest.MonkeyPatch,
    sales_session_factory,
) -> None:
    with sales_session_factory() as session:
        _add_user(session, "disabled-sync-owner")
        store = _add_store(
            session,
            "disabled-sync-owner",
            "disabled",
            enabled=False,
            credentials=True,
        )
        session.commit()

    monkeypatch.setattr(
        crawler_service,
        "dispatch_sales_analysis_sync_task",
        lambda *_args: pytest.fail("disabled store must fail before dispatch"),
    )

    response = _client(
        {
            "username": "disabled-sync-owner",
            "role": "operator",
            "permissionCodes": ["ai.manage"],
        }
    ).post(
        "/crawler/sales-analysis/sync",
        json={"storeId": store.id},
    )

    assert response.status_code == 400
    assert response.json()["detail"] == (
        "店铺已停用，无法立即同步销量；请先启用店铺后再重试。"
    )


def test_sales_sync_service_rejects_missing_credentials_before_lease(
    monkeypatch: pytest.MonkeyPatch,
    sales_session_factory,
) -> None:
    with sales_session_factory() as session:
        _add_user(session, "service-owner")
        store = _add_store(
            session,
            "service-owner",
            "missing",
            credentials=False,
        )
        session.commit()

    monkeypatch.setattr(
        sales_sync_service,
        "_PeriodicLeaseHeartbeat",
        lambda *_args, **_kwargs: pytest.fail(
            "missing credentials must fail before lease heartbeat"
        ),
    )

    with pytest.raises(
        ValueError,
        match="请先在店铺管理中检查并保存店铺配置",
    ):
        sales_sync_service.sync_owned_store(
            "service-owner",
            store.id,
        )


def test_non_redis_sales_sync_dispatch_uses_bounded_executor(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    submitted: list[tuple[Any, ...]] = []
    executor = SimpleNamespace(
        submit=lambda *args: submitted.append(args)
    )
    monkeypatch.setattr(
        crawler_service,
        "should_use_redis_task_queue",
        lambda: False,
    )
    monkeypatch.setattr(
        crawler_service,
        "SALES_ANALYSIS_SYNC_EXECUTOR",
        executor,
        raising=False,
    )
    monkeypatch.setattr(
        crawler_service,
        "start_background_task",
        lambda *_args, **_kwargs: pytest.fail(
            "sales sync must not create one thread per store"
        ),
    )

    crawler_service.dispatch_sales_analysis_sync_task(
        "owner-a",
        7,
    )

    assert submitted == [
        (
            crawler_service.run_sales_analysis_sync_task,
            "owner-a",
            7,
        )
    ]


def test_manual_sales_sync_locks_state_before_queue_decision() -> None:
    source = inspect.getsource(
        crawler_service.queue_sales_analysis_sync
    )

    assert ".with_for_update()" in source


def test_manual_sales_sync_recovers_stale_running_state(
    monkeypatch: pytest.MonkeyPatch,
    sales_session_factory,
) -> None:
    now = datetime(2026, 7, 16, 12, 0, 0)
    stale_at = (
        now
        - sales_sync_service.SALES_SYNC_LEASE_TIMEOUT
        - timedelta(seconds=1)
    )
    with sales_session_factory() as session:
        _add_user(session, "manual-recovery-owner")
        store = _add_store(
            session,
            "manual-recovery-owner",
            "manual-recovery",
        )
        session.add(
            SalesSyncStateModel(
                owner_username="manual-recovery-owner",
                store_id=store.id,
                sync_status="running:stale-owner",
                created_at=stale_at,
                updated_at=stale_at,
            )
        )
        session.commit()
        store_id = store.id

    dispatched: list[tuple[str, int]] = []
    monkeypatch.setattr(
        crawler_service,
        "dispatch_sales_analysis_sync_task",
        lambda owner, target_store_id: dispatched.append(
            (owner, target_store_id)
        ),
    )

    result = crawler_service.queue_sales_analysis_sync(
        "manual-recovery-owner",
        store_id,
    )

    with sales_session_factory() as session:
        state = session.get(SalesSyncStateModel, store_id)
    assert result["status"] == "queued"
    assert result["alreadyRunning"] is False
    assert dispatched == [("manual-recovery-owner", store_id)]
    assert state is not None
    assert state.sync_status == "queued"


def test_sales_sync_state_timestamps_have_explicit_shanghai_offset() -> None:
    payload = crawler_service._sales_analysis_sync_state_to_public(
        SimpleNamespace(
            store_id=7,
            sync_status="idle",
            initial_sync_completed=True,
            progress_current=0,
            progress_total=0,
            last_successful_sync_at=datetime(2026, 7, 16, 12, 0, 0),
            last_remote_updated_at=datetime(2026, 7, 16, 11, 30, 0),
            last_error="",
        )
    )

    assert payload["lastSuccessfulSyncAt"] == (
        "2026-07-16T12:00:00+08:00"
    )
    assert payload["lastRemoteUpdatedAt"] == (
        "2026-07-16T11:30:00+08:00"
    )


def test_scheduled_sales_sync_continues_after_one_store_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    queued: list[tuple[str, int]] = []
    monkeypatch.setattr(
        crawler_service,
        "sales_analysis_sync_due_candidates",
        lambda _now: [("owner-a", 1), ("owner-b", 2), ("owner-c", 3)],
        raising=False,
    )

    def queue(owner_username: str, store_id: int) -> dict[str, Any]:
        if store_id == 1:
            raise RuntimeError("first store failed")
        queued.append((owner_username, store_id))
        return {"id": f"sales-{store_id}", "status": "queued"}

    monkeypatch.setattr(
        crawler_service,
        "queue_sales_analysis_sync",
        queue,
        raising=False,
    )

    assert crawler_service.run_due_sales_analysis_syncs_once() == 2
    assert queued == [("owner-b", 2), ("owner-c", 3)]


def test_sales_sync_worker_reuses_task_3_lease_service(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[str, int]] = []
    monkeypatch.setattr(
        crawler_service,
        "sales_sync_service",
        SimpleNamespace(
            sync_owned_store=lambda owner, store_id: calls.append(
                (owner, store_id)
            )
        ),
        raising=False,
    )

    crawler_service.run_sales_analysis_sync_task("owner-a", 7)

    assert calls == [("owner-a", 7)]


def test_existing_schedule_runner_contains_sales_sync_tick() -> None:
    source = inspect.getsource(crawler_service.start_schedule_runner)

    assert "run_due_sales_analysis_syncs_once()" in source
