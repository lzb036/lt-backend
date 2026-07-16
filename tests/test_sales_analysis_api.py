from __future__ import annotations

import inspect
from datetime import datetime, timedelta
from types import SimpleNamespace
from typing import Any, Iterator

import pytest
from fastapi import FastAPI
from fastapi.routing import APIRoute
from fastapi.testclient import TestClient

from app.api import crawler as crawler_api
from app.core.auth import require_authenticated_account
from app.services import crawler_service


AI_USER = {
    "username": "owner-a",
    "role": "operator",
    "permissionCodes": ["ai.manage"],
}


def _client(user: dict[str, Any]) -> TestClient:
    app = FastAPI()
    app.include_router(crawler_api.router)
    app.dependency_overrides[require_authenticated_account] = lambda: user
    return TestClient(app)


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
        lambda owner, conversation_id, limit: calls.append(
            ("messages", owner, conversation_id, limit)
        )
        or [{"id": 21, "conversationId": conversation_id}],
        raising=False,
    )
    client = _client(AI_USER)

    listed = client.get("/crawler/sales-analysis/conversations")
    created = client.post(
        "/crawler/sales-analysis/conversations",
        json={"title": "  月度分析  "},
    )
    messages = client.get(
        "/crawler/sales-analysis/conversations/11/messages?limit=50"
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
    }
    assert deleted.json() == {"deleted": True}
    assert calls == [
        ("list", "owner-a"),
        ("create", "owner-a", "月度分析"),
        ("messages", "owner-a", 11, 50),
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


def test_sales_sync_due_threshold_and_active_states() -> None:
    now = datetime(2026, 7, 16, 12, 0, 0)

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
    assert crawler_service.sales_analysis_sync_is_due(
        now - timedelta(hours=1),
        "queued",
        now=now,
        state_updated_at=now - timedelta(minutes=11),
    )
    assert crawler_service.sales_analysis_sync_is_due(
        now - timedelta(hours=1),
        "running:stale-lease",
        now=now,
        state_updated_at=now - timedelta(minutes=11),
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
