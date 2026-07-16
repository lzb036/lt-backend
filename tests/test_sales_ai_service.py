from __future__ import annotations

import copy
import json
from contextlib import contextmanager
from datetime import date, datetime
from typing import Any

import pytest
from sqlalchemy import create_engine, event, select
from sqlalchemy.orm import Session, sessionmaker

from app.core.secure_storage import encrypt_text
from app.db.database import Base
from app.db.models import (
    SalesAnalysisConversationModel,
    SalesAnalysisMessageModel,
    StoreModel,
    UserAccountModel,
    UserAiTitleSettingsModel,
)
from app.services import sales_ai_service


CURRENT_DATE = date(2026, 7, 16)
RECENT_START = "2026-06-16"
RECENT_END = "2026-07-15"
OWNER = "tenant-secret-owner"
API_KEY = "credential-that-must-not-enter-messages"


@pytest.fixture()
def session_factory():
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)

    @event.listens_for(engine, "connect")
    def _enable_foreign_keys(dbapi_connection, _):
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
    try:
        yield factory
    finally:
        engine.dispose()


@pytest.fixture(autouse=True)
def local_dependencies(monkeypatch, session_factory):
    @contextmanager
    def _session_scope():
        with session_factory() as session:
            try:
                yield session
                session.commit()
            except Exception:
                session.rollback()
                raise

    monkeypatch.setattr(sales_ai_service, "session_scope", _session_scope)
    monkeypatch.setattr(
        sales_ai_service,
        "_current_shanghai_date",
        lambda: CURRENT_DATE,
    )


def _add_user(
    session: Session,
    username: str,
    *,
    with_ai_settings: bool = True,
) -> None:
    session.add(
        UserAccountModel(
            username=username,
            display_name="Private Owner Name",
            password_salt_b64="salt",
            password_hash_b64="hash",
            created_at=datetime(2026, 7, 16, 8, 0, 0),
            updated_at=datetime(2026, 7, 16, 8, 0, 0),
        )
    )
    if with_ai_settings:
        session.add(
            UserAiTitleSettingsModel(
                owner_username=username,
                provider="custom_openai",
                api_base_url="https://model.example.test/v1",
                api_key_encrypted=encrypt_text(API_KEY),
                model_name="test-model",
                verified_at=datetime(2026, 7, 16, 8, 30, 0),
                created_at=datetime(2026, 7, 16, 8, 0, 0),
                updated_at=datetime(2026, 7, 16, 8, 30, 0),
            )
        )
    session.flush()


def _add_store(
    session: Session,
    username: str,
    code: str,
    name: str,
) -> StoreModel:
    store = StoreModel(
        owner_username=username,
        store_code=code,
        store_name=name,
        contact_name="Private Contact",
        contact_phone="13912345678",
        rakuten_service_secret_encrypted="encrypted-service-secret",
        rakuten_license_key_encrypted="encrypted-license-key",
        created_at=datetime(2026, 7, 16, 8, 0, 0),
        updated_at=datetime(2026, 7, 16, 8, 0, 0),
    )
    session.add(store)
    session.flush()
    return store


def _seed_owner(
    session_factory,
    *,
    store_count: int = 1,
    with_ai_settings: bool = True,
) -> list[int]:
    with session_factory() as session:
        _add_user(session, OWNER, with_ai_settings=with_ai_settings)
        store_ids = [
            _add_store(
                session,
                OWNER,
                f"shop-{index}",
                f"Analysis Shop {index}",
            ).id
            for index in range(1, store_count + 1)
        ]
        session.commit()
        return store_ids


def _model_response(
    *,
    content: str | None = None,
    tool_calls: list[tuple[str, dict[str, Any]]] | None = None,
) -> dict[str, Any]:
    calls = [
        {
            "id": f"call-{index}",
            "type": "function",
            "function": {
                "name": name,
                "arguments": json.dumps(arguments, ensure_ascii=False),
            },
        }
        for index, (name, arguments) in enumerate(tool_calls or [], start=1)
    ]
    return {
        "choices": [
            {
                "message": {
                    "content": content,
                    "tool_calls": calls,
                }
            }
        ]
    }


def _compact_result(store_id: int) -> dict[str, Any]:
    return {
        "store": {"id": store_id, "name": "Analysis Shop 1"},
        "range": {"start": RECENT_START, "end": RECENT_END},
        "metric": "effectiveUnits",
        "dataUpdatedAt": "2026-07-16T14:30:00+08:00",
        "unresolvedAdjustmentCount": 2,
        "rows": [
            {
                "manageNumber": "MN-1",
                "itemName": "Safe Product",
                "effectiveUnits": 12,
                "effectiveSalesAmount": 3600.0,
            }
        ],
    }


def _create_conversation(owner_username: str = OWNER) -> dict[str, Any]:
    return sales_ai_service.create_conversation(owner_username)


def _serialized_messages(model_calls: list[dict[str, Any]]) -> str:
    return json.dumps(
        [call["messages"] for call in model_calls],
        ensure_ascii=False,
        sort_keys=True,
    )


def test_create_and_list_conversations_are_tenant_safe(session_factory):
    with session_factory() as session:
        _add_user(session, OWNER)
        _add_user(session, "other-owner")
        session.commit()

    first = sales_ai_service.create_conversation(OWNER, "  第一份分析  ")
    second = sales_ai_service.create_conversation(OWNER)
    other = sales_ai_service.create_conversation("other-owner", "Other")

    listed = sales_ai_service.list_conversations(OWNER)

    assert first["title"] == "第一份分析"
    assert second["title"] == "新分析"
    assert {row["id"] for row in listed} == {first["id"], second["id"]}
    assert other["id"] not in {row["id"] for row in listed}
    assert all(row["storeScope"] == [] for row in listed)


def test_stream_rejects_cross_tenant_conversation_before_model_call(
    monkeypatch,
    session_factory,
):
    with session_factory() as session:
        _add_user(session, OWNER)
        _add_user(session, "other-owner")
        _add_store(session, "other-owner", "other", "Other Shop")
        session.commit()
    conversation = _create_conversation()
    model_call = pytest.fail
    monkeypatch.setattr(sales_ai_service, "litellm_completion", model_call)

    with pytest.raises(LookupError, match="会话不存在"):
        list(
            sales_ai_service.stream_analysis(
                "other-owner",
                conversation["id"],
                "查看近期销量",
            )
        )


def test_one_store_auto_selects_expands_recent_dates_and_sanitizes_model_payloads(
    monkeypatch,
    session_factory,
):
    store_id = _seed_owner(session_factory)[0]
    conversation = _create_conversation()
    model_calls: list[dict[str, Any]] = []
    responses = iter(
        [
            _model_response(
                content=(
                    f"{OWNER} {API_KEY} Private Owner Name "
                    "Private Contact buyer@example.com "
                    "SELECT * FROM raw_orders"
                ),
                tool_calls=[
                    (
                        "get_product_sales_ranking",
                        {"metric": "effectiveUnits", "limit": 10},
                    )
                ]
            ),
            _model_response(content="Analysis Shop 1 的近期有效销量已完成分析。"),
        ]
    )

    def fake_completion(**kwargs):
        model_calls.append(copy.deepcopy(kwargs))
        return next(responses)

    executed: list[tuple[str, str, dict[str, Any]]] = []

    def fake_execute(owner_username, tool_name, arguments):
        executed.append((owner_username, tool_name, copy.deepcopy(arguments)))
        result = _compact_result(store_id)
        result.update(
            {
                "apiKey": API_KEY,
                "rawOrder": {
                    "buyerName": "Private Buyer",
                    "buyerEmail": "buyer@example.com",
                    "buyerPhone": "13912345678",
                    "buyerAddress": "Private Address",
                },
                "sql": "SELECT * FROM raw_orders",
            }
        )
        result["rows"][0]["buyerEmail"] = "buyer@example.com"
        return result

    monkeypatch.setattr(sales_ai_service, "litellm_completion", fake_completion)
    monkeypatch.setattr(sales_ai_service, "execute_sales_tool", fake_execute)

    events = list(
        sales_ai_service.stream_analysis(
            OWNER,
            conversation["id"],
            (
                "查看近期销量；owner_username=tenant-secret-owner；"
                f"apiKey={API_KEY}；buyerEmail=buyer@example.com；"
                "Private Owner Name；Private Contact；"
                "buyerPhone=13912345678；买家姓名=张三；"
                "收件地址=北京市朝阳区；"
                'OrderModelList=[{"OrderNumber":"R-1","ItemModelList":[]}];'
                "SELECT * FROM raw_orders"
            ),
        )
    )

    assert executed == [
        (
            OWNER,
            "get_product_sales_ranking",
            {
                "metric": "effectiveUnits",
                "limit": 10,
                "storeId": store_id,
                "startDate": RECENT_START,
                "endDate": RECENT_END,
            },
        )
    ]
    assert [event["type"] for event in events] == [
        "status",
        "tool_call",
        "tool_result",
        "delta",
        "completed",
    ]
    assert model_calls[0]["model"] == "openai/test-model"
    assert model_calls[0]["tools"] == sales_ai_service.SALES_ANALYSIS_TOOLS
    serialized_messages = _serialized_messages(model_calls)
    for forbidden in (
        API_KEY,
        OWNER,
        "Private Owner Name",
        "Private Contact",
        "Private Buyer",
        "buyer@example.com",
        "13912345678",
        "Private Address",
        "张三",
        "北京市朝阳区",
        "OrderModelList",
        "ItemModelList",
        "R-1",
        "rawOrder",
        "raw_orders",
        "SELECT *",
        '"sql"',
    ):
        assert forbidden not in serialized_messages
    assert RECENT_START in serialized_messages
    assert RECENT_END in serialized_messages

    tool_result_event = next(
        event for event in events if event["type"] == "tool_result"
    )
    persisted_result_text = json.dumps(
        tool_result_event["result"],
        ensure_ascii=False,
    )
    assert "Safe Product" in persisted_result_text
    assert "buyer@example.com" not in persisted_result_text
    assert "SELECT" not in persisted_result_text
    assert API_KEY not in persisted_result_text

    with session_factory() as session:
        row = session.scalar(
            select(SalesAnalysisMessageModel).where(
                SalesAnalysisMessageModel.conversation_id
                == conversation["id"]
            )
        )
        stored_conversation = session.get(
            SalesAnalysisConversationModel,
            conversation["id"],
        )

    assert row is not None
    assert row.answer_text == "Analysis Shop 1 的近期有效销量已完成分析。"
    assert row.model_name == "openai/test-model"
    assert json.loads(row.store_scope_json) == [store_id]
    assert json.loads(row.statistics_window_json) == {
        "start": RECENT_START,
        "end": RECENT_END,
    }
    stored_metadata = row.tool_arguments_json + row.result_summary_json
    assert "buyer@example.com" not in stored_metadata
    assert "SELECT" not in stored_metadata
    assert API_KEY not in stored_metadata
    assert stored_conversation is not None
    assert json.loads(stored_conversation.store_scope_json) == [store_id]
    assert stored_conversation.last_message_at is not None


def test_store_display_name_is_sanitized_before_entering_system_message(
    monkeypatch,
    session_factory,
):
    with session_factory() as session:
        _add_user(session, OWNER)
        _add_store(
            session,
            OWNER,
            "unsafe-name",
            f"{OWNER} SELECT * FROM raw_orders buyer@example.com",
        )
        session.commit()
    conversation = _create_conversation()
    model_calls: list[dict[str, Any]] = []

    def fake_completion(**kwargs):
        model_calls.append(copy.deepcopy(kwargs))
        return _model_response(content="已确认店铺范围。")

    monkeypatch.setattr(sales_ai_service, "litellm_completion", fake_completion)

    events = list(
        sales_ai_service.stream_analysis(
            OWNER,
            conversation["id"],
            "查看销量",
        )
    )

    assert events[-1]["type"] == "completed"
    serialized_messages = _serialized_messages(model_calls)
    assert OWNER not in serialized_messages
    assert "SELECT *" not in serialized_messages
    assert "raw_orders" not in serialized_messages
    assert "buyer@example.com" not in serialized_messages


def test_multiple_stores_require_clarification_without_calling_model(
    monkeypatch,
    session_factory,
):
    _seed_owner(session_factory, store_count=2)
    conversation = _create_conversation()
    monkeypatch.setattr(
        sales_ai_service,
        "litellm_completion",
        lambda **_: pytest.fail("model must not be called"),
    )
    monkeypatch.setattr(
        sales_ai_service,
        "execute_sales_tool",
        lambda *_: pytest.fail("analysis tool must not be called"),
    )

    events = list(
        sales_ai_service.stream_analysis(
            OWNER,
            conversation["id"],
            "查看近期销量",
        )
    )

    assert [event["type"] for event in events] == [
        "status",
        "delta",
        "completed",
    ]
    answer = events[-1]["message"]["answer"]
    assert "请选择一家店铺" in answer
    assert "Analysis Shop 1" in answer
    assert "Analysis Shop 2" in answer

    with session_factory() as session:
        row = session.scalar(
            select(SalesAnalysisMessageModel).where(
                SalesAnalysisMessageModel.conversation_id
                == conversation["id"]
            )
        )
    assert row is not None
    assert row.answer_text == answer
    assert json.loads(row.store_scope_json) == []


def test_unknown_model_tool_is_rejected_and_never_executed(
    monkeypatch,
    session_factory,
):
    _seed_owner(session_factory)
    conversation = _create_conversation()
    monkeypatch.setattr(
        sales_ai_service,
        "litellm_completion",
        lambda **_: _model_response(
            tool_calls=[("run_arbitrary_query", {"query": "unsafe"})]
        ),
    )
    monkeypatch.setattr(
        sales_ai_service,
        "execute_sales_tool",
        lambda *_: pytest.fail("unknown tool must not execute"),
    )

    events = list(
        sales_ai_service.stream_analysis(
            OWNER,
            conversation["id"],
            "查看销量",
        )
    )

    assert [event["type"] for event in events] == ["status", "error"]
    assert "不允许的分析工具" in events[-1]["message"]


def test_sensitive_values_in_allowed_tool_arguments_are_rejected_before_event(
    monkeypatch,
    session_factory,
):
    store_id = _seed_owner(session_factory)[0]
    conversation = _create_conversation()
    monkeypatch.setattr(
        sales_ai_service,
        "litellm_completion",
        lambda **_: _model_response(
            tool_calls=[
                (
                    "get_product_sales_trend",
                    {
                        "storeId": store_id,
                        "startDate": RECENT_START,
                        "endDate": RECENT_END,
                        "manageNumber": (
                            "SELECT * FROM raw_orders; buyer@example.com"
                        ),
                    },
                )
            ]
        ),
    )
    monkeypatch.setattr(
        sales_ai_service,
        "execute_sales_tool",
        lambda *_: pytest.fail("unsafe tool arguments must not execute"),
    )

    events = list(
        sales_ai_service.stream_analysis(
            OWNER,
            conversation["id"],
            "查看商品趋势",
        )
    )

    assert [event["type"] for event in events] == ["status", "error"]
    assert "参数不符合允许范围" in events[-1]["message"]
    serialized_events = json.dumps(events, ensure_ascii=False)
    assert "SELECT" not in serialized_events
    assert "buyer@example.com" not in serialized_events

    with session_factory() as session:
        row = session.scalar(
            select(SalesAnalysisMessageModel).where(
                SalesAnalysisMessageModel.conversation_id
                == conversation["id"]
            )
        )
    assert row is not None
    assert "SELECT" not in row.tool_arguments_json
    assert "buyer@example.com" not in row.tool_arguments_json


def test_turn_executes_at_most_four_model_tool_calls(
    monkeypatch,
    session_factory,
):
    store_id = _seed_owner(session_factory)[0]
    conversation = _create_conversation()
    calls = [
        (
            "get_store_sales_overview",
            {
                "storeId": store_id,
                "startDate": "2026-07-01",
                "endDate": "2026-07-15",
            },
        )
        for _ in range(5)
    ]
    monkeypatch.setattr(
        sales_ai_service,
        "litellm_completion",
        lambda **_: _model_response(tool_calls=calls),
    )
    executed: list[dict[str, Any]] = []

    def fake_execute(_owner, _tool_name, arguments):
        executed.append(copy.deepcopy(arguments))
        return _compact_result(store_id)

    monkeypatch.setattr(sales_ai_service, "execute_sales_tool", fake_execute)

    events = list(
        sales_ai_service.stream_analysis(
            OWNER,
            conversation["id"],
            "查看本月销量",
        )
    )

    assert len(executed) == 4
    assert sum(event["type"] == "tool_call" for event in events) == 4
    assert sum(event["type"] == "tool_result" for event in events) == 4
    assert events[-1]["type"] == "error"
    assert "最多调用 4 次" in events[-1]["message"]


def test_explanation_failure_after_tool_success_uses_deterministic_fallback(
    monkeypatch,
    session_factory,
):
    store_id = _seed_owner(session_factory)[0]
    conversation = _create_conversation()
    calls = 0

    def fake_completion(**_kwargs):
        nonlocal calls
        calls += 1
        if calls == 1:
            return _model_response(
                tool_calls=[
                    (
                        "get_product_sales_ranking",
                        {
                            "storeId": store_id,
                            "startDate": RECENT_START,
                            "endDate": RECENT_END,
                            "limit": 10,
                        },
                    )
                ]
            )
        raise RuntimeError("provider explanation failed")

    monkeypatch.setattr(sales_ai_service, "litellm_completion", fake_completion)
    monkeypatch.setattr(
        sales_ai_service,
        "execute_sales_tool",
        lambda *_: _compact_result(store_id),
    )

    events = list(
        sales_ai_service.stream_analysis(
            OWNER,
            conversation["id"],
            "查看近期销量排行",
        )
    )

    assert [event["type"] for event in events] == [
        "status",
        "tool_call",
        "tool_result",
        "delta",
        "completed",
    ]
    answer = events[-1]["message"]["answer"]
    for required in (
        "Analysis Shop 1",
        RECENT_START,
        RECENT_END,
        "有效销量 = 下单数量 - 取消数量 - 退款数量 - 退货数量",
        "2026-07-16T14:30:00+08:00",
        "未决调整数量：2",
        "MN-1",
        "Safe Product",
        "12",
        "3600.0",
    ):
        assert required in answer
    assert events[-1]["message"]["fallback"] is True

    with session_factory() as session:
        row = session.scalar(
            select(SalesAnalysisMessageModel).where(
                SalesAnalysisMessageModel.conversation_id
                == conversation["id"]
            )
        )
    assert row is not None
    assert row.answer_text == answer


def test_missing_verified_user_ai_settings_emits_error_without_model_call(
    monkeypatch,
    session_factory,
):
    _seed_owner(session_factory, with_ai_settings=False)
    conversation = _create_conversation()
    monkeypatch.setattr(
        sales_ai_service,
        "litellm_completion",
        lambda **_: pytest.fail("model must not be called"),
    )

    events = list(
        sales_ai_service.stream_analysis(
            OWNER,
            conversation["id"],
            "查看销量",
        )
    )

    assert [event["type"] for event in events] == ["status", "error"]
    assert "API Key" in events[-1]["message"]


def test_corrupt_ai_credentials_emit_sanitized_error_event(
    monkeypatch,
    session_factory,
):
    _seed_owner(session_factory)
    conversation = _create_conversation()

    def fail_decryption(_value):
        raise ValueError(f"cannot decrypt {API_KEY}")

    monkeypatch.setattr(sales_ai_service, "decrypt_text", fail_decryption)
    monkeypatch.setattr(
        sales_ai_service,
        "litellm_completion",
        lambda **_: pytest.fail("model must not be called"),
    )

    events = list(
        sales_ai_service.stream_analysis(
            OWNER,
            conversation["id"],
            "查看销量",
        )
    )

    assert [event["type"] for event in events] == ["status", "error"]
    assert "AI 配置不可用" in events[-1]["message"]
    assert API_KEY not in json.dumps(events, ensure_ascii=False)
