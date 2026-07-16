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
    responses = iter(
        [
            _model_response(
                tool_calls=[
                    (
                        "get_store_sales_overview",
                        {
                            "startDate": RECENT_START,
                            "endDate": RECENT_END,
                        },
                    )
                ]
            ),
            _model_response(content="已确认店铺范围。"),
        ]
    )

    def fake_completion(**kwargs):
        model_calls.append(copy.deepcopy(kwargs))
        return next(responses)

    monkeypatch.setattr(sales_ai_service, "litellm_completion", fake_completion)
    monkeypatch.setattr(
        sales_ai_service,
        "execute_sales_tool",
        lambda _owner, _tool_name, arguments: _compact_result(
            arguments["storeId"]
        ),
    )

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


def test_model_content_without_successful_tool_call_is_rejected_and_persisted(
    monkeypatch,
    session_factory,
):
    _seed_owner(session_factory)
    conversation = _create_conversation()
    fabricated = "本月销量为 999999，销售额为 888888。"
    monkeypatch.setattr(
        sales_ai_service,
        "litellm_completion",
        lambda **_: _model_response(content=fabricated),
    )
    monkeypatch.setattr(
        sales_ai_service,
        "execute_sales_tool",
        lambda *_: pytest.fail("no tool should execute"),
    )

    events = list(
        sales_ai_service.stream_analysis(
            OWNER,
            conversation["id"],
            "查看本月销量",
        )
    )

    assert [event["type"] for event in events] == ["status", "error"]
    assert "未调用销量分析工具" in events[-1]["message"]
    assert fabricated not in json.dumps(events, ensure_ascii=False)

    with session_factory() as session:
        row = session.scalar(
            select(SalesAnalysisMessageModel).where(
                SalesAnalysisMessageModel.conversation_id
                == conversation["id"]
            )
        )
    assert row is not None
    assert "未调用销量分析工具" in row.answer_text
    assert fabricated not in row.answer_text
    assert row.tool_name == ""


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


def test_explicit_owned_store_in_current_message_overrides_persisted_scope(
    monkeypatch,
    session_factory,
):
    first_store_id, second_store_id = _seed_owner(
        session_factory,
        store_count=2,
    )
    conversation = _create_conversation()
    with session_factory() as session:
        row = session.get(
            SalesAnalysisConversationModel,
            conversation["id"],
        )
        assert row is not None
        row.store_scope_json = json.dumps([first_store_id])
        session.commit()

    responses = iter(
        [
            _model_response(
                tool_calls=[
                    (
                        "get_product_sales_ranking",
                        {
                            "storeId": first_store_id,
                            "startDate": RECENT_START,
                            "endDate": RECENT_END,
                        },
                    )
                ]
            ),
            _model_response(content="第二家店铺分析完成。"),
        ]
    )
    monkeypatch.setattr(
        sales_ai_service,
        "litellm_completion",
        lambda **_: next(responses),
    )
    executed: list[dict[str, Any]] = []

    def fake_execute(_owner, _tool_name, arguments):
        executed.append(copy.deepcopy(arguments))
        result = _compact_result(second_store_id)
        result["store"]["name"] = "Analysis Shop 2"
        return result

    monkeypatch.setattr(sales_ai_service, "execute_sales_tool", fake_execute)

    events = list(
        sales_ai_service.stream_analysis(
            OWNER,
            conversation["id"],
            "请分析 shop-2 的近期销量",
        )
    )

    assert events[-1]["type"] == "completed"
    assert executed[0]["storeId"] == second_store_id
    with session_factory() as session:
        stored_conversation = session.get(
            SalesAnalysisConversationModel,
            conversation["id"],
        )
        message_row = session.scalar(
            select(SalesAnalysisMessageModel).where(
                SalesAnalysisMessageModel.conversation_id
                == conversation["id"]
            )
        )
    assert stored_conversation is not None
    assert json.loads(stored_conversation.store_scope_json) == [
        second_store_id
    ]
    assert message_row is not None
    assert json.loads(message_row.store_scope_json) == [second_store_id]


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


def test_task4_argument_schema_rejects_nested_alias_before_tool_event(
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
                    "get_product_sales_ranking",
                    {
                        "storeId": store_id,
                        "startDate": RECENT_START,
                        "endDate": RECENT_END,
                        "metric": {"买家姓名": "张三"},
                    },
                )
            ]
        ),
    )
    monkeypatch.setattr(
        sales_ai_service,
        "execute_sales_tool",
        lambda *_: pytest.fail("schema-invalid arguments must not execute"),
    )

    events = list(
        sales_ai_service.stream_analysis(
            OWNER,
            conversation["id"],
            "查看商品排行",
        )
    )

    assert [event["type"] for event in events] == ["status", "error"]
    serialized = json.dumps(events, ensure_ascii=False)
    assert "买家姓名" not in serialized
    assert "张三" not in serialized


def test_tool_result_uses_strict_recursive_schema_allowlist(
    monkeypatch,
    session_factory,
):
    store_id = _seed_owner(session_factory)[0]
    conversation = _create_conversation()
    model_calls: list[dict[str, Any]] = []
    responses = iter(
        [
            _model_response(
                tool_calls=[
                    (
                        "get_product_sales_ranking",
                        {
                            "storeId": store_id,
                            "startDate": RECENT_START,
                            "endDate": RECENT_END,
                            "includeSku": True,
                        },
                    )
                ]
            ),
            _model_response(content="已根据受控排行结果完成分析。"),
        ]
    )

    def fake_completion(**kwargs):
        model_calls.append(copy.deepcopy(kwargs))
        return next(responses)

    unsafe_result = {
        **_compact_result(store_id),
        "apiBase": "https://credential.example.test",
        "accessKeyId": "ACCESS-SECRET",
        "credentials": {"token": "TOKEN-SECRET"},
        "sqlMetadata": {"statement": "SELECT * FROM orders"},
        "买家姓名": "张三",
        "订单详情": {"订单号": "ORDER-SECRET"},
        "comparison": {"range": {"start": "1900-01-01"}},
    }
    unsafe_result["store"]["contactName"] = "Private Contact"
    unsafe_result["range"]["sql"] = "SELECT range"
    unsafe_result["rows"] = [
        {
            "manageNumber": "123456789012",
            "itemNumber": "000000000001",
            "itemName": "Safe Product 123456789012",
            "skuKey": "999999999999",
            "orderCount": 3,
            "orderedUnits": 14,
            "effectiveUnits": 12,
            "grossSalesAmount": 4200.0,
            "effectiveSalesAmount": 3600.0,
            "metricValue": 12,
            "canceledUnits": 99,
            "refundedUnits": 98,
            "returnedUnits": 97,
            "buyerName": "Private Buyer",
            "买家电话": "13912345678",
            "orderNumber": "ORDER-SECRET",
            "apiBase": "https://unsafe.example.test",
            "accessKeyId": "ROW-ACCESS-SECRET",
            "sqlMetadata": "DROP TABLE sales",
            "nested": {"buyerEmail": "buyer@example.com"},
        }
    ]

    monkeypatch.setattr(sales_ai_service, "litellm_completion", fake_completion)
    monkeypatch.setattr(
        sales_ai_service,
        "execute_sales_tool",
        lambda *_: copy.deepcopy(unsafe_result),
    )

    events = list(
        sales_ai_service.stream_analysis(
            OWNER,
            conversation["id"],
            "查看商品 SKU 排行",
        )
    )

    tool_result = next(
        event["result"] for event in events if event["type"] == "tool_result"
    )
    assert set(tool_result) == {
        "store",
        "range",
        "metric",
        "dataUpdatedAt",
        "unresolvedAdjustmentCount",
        "rows",
    }
    assert set(tool_result["store"]) == {"id", "name"}
    assert set(tool_result["range"]) == {"start", "end"}
    assert set(tool_result["rows"][0]) == {
        "manageNumber",
        "itemNumber",
        "itemName",
        "skuKey",
        "orderCount",
        "orderedUnits",
        "effectiveUnits",
        "grossSalesAmount",
        "effectiveSalesAmount",
        "metricValue",
    }
    assert tool_result["rows"][0]["manageNumber"] == "123456789012"
    assert tool_result["rows"][0]["itemNumber"] == "000000000001"
    assert tool_result["rows"][0]["skuKey"] == "999999999999"

    tool_message = next(
        message
        for message in model_calls[1]["messages"]
        if message["role"] == "tool"
    )
    assert json.loads(tool_message["content"]) == tool_result

    with session_factory() as session:
        row = session.scalar(
            select(SalesAnalysisMessageModel).where(
                SalesAnalysisMessageModel.conversation_id
                == conversation["id"]
            )
        )
    assert row is not None
    persisted_result = json.loads(row.result_summary_json)[0]["result"]
    assert persisted_result == tool_result

    serialized = json.dumps(
        {
            "event": tool_result,
            "messages": model_calls[1]["messages"],
            "storage": persisted_result,
        },
        ensure_ascii=False,
    )
    for forbidden in (
        "apiBase",
        "accessKeyId",
        "credentials",
        "TOKEN-SECRET",
        "sqlMetadata",
        "SELECT",
        "DROP TABLE",
        "买家姓名",
        "买家电话",
        "张三",
        "13912345678",
        "订单详情",
        "订单号",
        "ORDER-SECRET",
        "buyerName",
        "buyerEmail",
        "buyer@example.com",
        "contactName",
        "nested",
        "comparison",
    ):
        assert forbidden not in serialized


def test_numeric_12_digit_manage_number_is_valid_tool_argument(
    monkeypatch,
    session_factory,
):
    store_id = _seed_owner(session_factory)[0]
    conversation = _create_conversation()
    responses = iter(
        [
            _model_response(
                tool_calls=[
                    (
                        "get_product_sales_trend",
                        {
                            "storeId": store_id,
                            "startDate": RECENT_START,
                            "endDate": RECENT_END,
                            "manageNumber": "123456789012",
                            "grain": "day",
                        },
                    )
                ]
            ),
            _model_response(content="趋势分析完成。"),
        ]
    )
    monkeypatch.setattr(
        sales_ai_service,
        "litellm_completion",
        lambda **_: next(responses),
    )
    executed: list[dict[str, Any]] = []

    def fake_execute(_owner, _tool_name, arguments):
        executed.append(copy.deepcopy(arguments))
        result = _compact_result(store_id)
        result["manageNumber"] = "123456789012"
        result["grain"] = "day"
        result["rows"] = [
            {
                "period": RECENT_END,
                "orderedUnits": 2,
                "effectiveUnits": 2,
                "effectiveSalesAmount": 200.0,
            }
        ]
        return result

    monkeypatch.setattr(sales_ai_service, "execute_sales_tool", fake_execute)

    events = list(
        sales_ai_service.stream_analysis(
            OWNER,
            conversation["id"],
            "查看商品 123456789012 的趋势",
        )
    )

    assert events[-1]["type"] == "completed"
    assert executed[0]["manageNumber"] == "123456789012"


def test_successful_tool_audit_is_persisted_before_tool_result_yield(
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
                    "get_product_sales_ranking",
                    {
                        "storeId": store_id,
                        "startDate": RECENT_START,
                        "endDate": RECENT_END,
                    },
                )
            ]
        ),
    )
    monkeypatch.setattr(
        sales_ai_service,
        "execute_sales_tool",
        lambda *_: _compact_result(store_id),
    )

    stream = sales_ai_service.stream_analysis(
        OWNER,
        conversation["id"],
        "查看近期销量排行",
    )
    assert next(stream)["type"] == "status"
    assert next(stream)["type"] == "tool_call"
    assert next(stream)["type"] == "tool_result"
    stream.close()

    with session_factory() as session:
        row = session.scalar(
            select(SalesAnalysisMessageModel).where(
                SalesAnalysisMessageModel.conversation_id
                == conversation["id"]
            )
        )
    assert row is not None
    assert row.answer_text == ""
    assert row.tool_name == "get_product_sales_ranking"
    assert row.model_name == "openai/test-model"
    assert json.loads(row.store_scope_json) == [store_id]
    assert json.loads(row.statistics_window_json) == {
        "start": RECENT_START,
        "end": RECENT_END,
    }
    assert json.loads(row.tool_arguments_json) == [
        {
            "toolName": "get_product_sales_ranking",
            "arguments": {
                "storeId": store_id,
                "startDate": RECENT_START,
                "endDate": RECENT_END,
            },
        }
    ]
    assert json.loads(row.result_summary_json)[0]["result"]["rows"]


def test_model_context_is_bounded_and_keeps_newest_history(
    monkeypatch,
    session_factory,
):
    store_id = _seed_owner(session_factory)[0]
    conversation = _create_conversation()
    with session_factory() as session:
        for index in range(6):
            session.add(
                SalesAnalysisMessageModel(
                    conversation_id=conversation["id"],
                    owner_username=OWNER,
                    question_text=(
                        f"HISTORY-{index}-QUESTION-"
                        + ("Q" * 300_000)
                    ),
                    answer_text=(
                        f"HISTORY-{index}-ANSWER-"
                        + ("A" * 300_000)
                    ),
                    tool_name="",
                    tool_arguments_json="[]",
                    result_summary_json="[]",
                    model_name="test",
                    store_scope_json=json.dumps([store_id]),
                    statistics_window_json="{}",
                )
            )
            session.flush()
        session.commit()

    model_calls: list[dict[str, Any]] = []
    responses = iter(
        [
            _model_response(
                tool_calls=[
                    (
                        "get_product_sales_ranking",
                        {
                            "storeId": store_id,
                            "startDate": RECENT_START,
                            "endDate": RECENT_END,
                            "limit": 100,
                        },
                    )
                ]
            ),
            _model_response(content="受控上下文分析完成。"),
        ]
    )

    def fake_completion(**kwargs):
        model_calls.append(copy.deepcopy(kwargs))
        return next(responses)

    oversized_rows = [
        {
            "manageNumber": f"MN-{index:03d}",
            "itemNumber": f"ITEM-{index:03d}",
            "itemName": f"ROW-{index:03d}-" + ("X" * 10_000),
            "orderCount": 1,
            "orderedUnits": 1,
            "effectiveUnits": 1,
            "grossSalesAmount": 100.0,
            "effectiveSalesAmount": 100.0,
            "metricValue": 1,
        }
        for index in range(150)
    ]
    oversized_result = _compact_result(store_id)
    oversized_result["rows"] = oversized_rows
    monkeypatch.setattr(sales_ai_service, "litellm_completion", fake_completion)
    monkeypatch.setattr(
        sales_ai_service,
        "execute_sales_tool",
        lambda *_: copy.deepcopy(oversized_result),
    )
    current_question = "近期 CURRENT-QUESTION-" + ("C" * 2_000_000)

    events = list(
        sales_ai_service.stream_analysis(
            OWNER,
            conversation["id"],
            current_question,
        )
    )

    assert events[-1]["type"] == "completed"
    first_messages = model_calls[0]["messages"]
    current_message = first_messages[-1]["content"]
    history_messages = first_messages[1:-1]
    assert len(current_message) <= (
        sales_ai_service.MAX_MODEL_CURRENT_MESSAGE_CHARS
    )
    assert sum(len(item["content"]) for item in history_messages) <= (
        sales_ai_service.MAX_MODEL_HISTORY_CHARS
    )
    serialized_history = json.dumps(history_messages, ensure_ascii=False)
    assert "HISTORY-5-QUESTION" in serialized_history
    assert "HISTORY-5-ANSWER" in serialized_history
    assert "HISTORY-0-QUESTION" not in serialized_history

    tool_message = next(
        item
        for item in model_calls[1]["messages"]
        if item["role"] == "tool"
    )
    assert len(tool_message["content"]) <= (
        sales_ai_service.MAX_MODEL_TOOL_RESULT_CHARS
    )
    model_tool_result = json.loads(tool_message["content"])
    assert len(model_tool_result["rows"]) <= (
        sales_ai_service.MAX_MODEL_TOOL_RESULT_ROWS
    )
    assert model_tool_result["rows"][0]["manageNumber"] == "MN-000"
    assert all(
        row["manageNumber"] != "MN-099"
        for row in model_tool_result["rows"]
    )

    event_result = next(
        event["result"] for event in events if event["type"] == "tool_result"
    )
    assert len(event_result["rows"]) == sales_ai_service.MAX_RANKING_ROWS
    with session_factory() as session:
        row = session.scalars(
            select(SalesAnalysisMessageModel)
            .where(
                SalesAnalysisMessageModel.conversation_id
                == conversation["id"],
                SalesAnalysisMessageModel.question_text.like(
                    "近期 CURRENT-QUESTION-%"
                ),
            )
            .order_by(SalesAnalysisMessageModel.id.desc())
        ).first()
    assert row is not None
    assert len(row.question_text) <= (
        sales_ai_service.MAX_PERSISTED_QUESTION_CHARS
    )
    persisted_result = json.loads(row.result_summary_json)[0]["result"]
    assert len(persisted_result["rows"]) == (
        sales_ai_service.MAX_RANKING_ROWS
    )


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
