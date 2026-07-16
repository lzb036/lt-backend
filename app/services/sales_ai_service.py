from __future__ import annotations

import json
import math
import re
from collections.abc import Iterator
from datetime import date, datetime, timedelta, timezone
from typing import Any

from sqlalchemy import func, select

from app.core.secure_storage import decrypt_text
from app.db.database import session_scope
from app.db.models import (
    SalesAnalysisConversationModel,
    SalesAnalysisMessageModel,
    StoreModel,
    UserAccountModel,
)
from app.services.ai_title_service import (
    ensure_user_settings,
    litellm_completion,
    resolved_model_name,
)
from app.services import sales_analysis_service
from app.services.sales_analysis_service import (
    SALES_ANALYSIS_TOOLS,
    execute_sales_tool,
)


MAX_TOOL_CALLS_PER_TURN = 4
MAX_PERSISTED_QUESTION_CHARS = 12_000
MAX_PERSISTED_ANSWER_CHARS = 24_000
MAX_MODEL_CURRENT_MESSAGE_CHARS = 4_000
MAX_MODEL_HISTORY_CHARS = 12_000
MAX_MODEL_HISTORY_ENTRY_CHARS = 2_000
MAX_HISTORY_ROWS_SCAN = 50
MAX_HISTORY_SOURCE_CHARS = 4_000
MAX_MODEL_TOOL_RESULT_CHARS = 16_000
MAX_MODEL_TOOL_RESULT_ROWS = 40
TRUNCATION_MARKER = "…[已截断]"
SHANGHAI_TIMEZONE = timezone(timedelta(hours=8))
DEFAULT_CONVERSATION_TITLE = "新分析"
EFFECTIVE_SALES_DEFINITION = (
    "有效销量 = 下单数量 - 取消数量 - 退款数量 - 退货数量"
)
STORE_SCOPED_TOOLS = {
    "get_store_sales_overview",
    "get_product_sales_ranking",
    "get_product_sales_trend",
    "compare_product_sales",
    "get_sku_sales_breakdown",
    "get_slow_moving_products",
    "get_sales_adjustment_summary",
}
DATE_SCOPED_TOOLS = set(STORE_SCOPED_TOOLS)
ALLOWED_TOOL_NAMES = {
    tool["function"]["name"] for tool in SALES_ANALYSIS_TOOLS
}
TOOL_LABELS = {
    "list_owned_stores": "店铺列表",
    "get_store_sales_overview": "店铺销量概览",
    "get_product_sales_ranking": "商品销量排行",
    "get_product_sales_trend": "商品销量趋势",
    "compare_product_sales": "商品销量对比",
    "get_sku_sales_breakdown": "SKU 销量明细",
    "get_slow_moving_products": "低销量商品",
    "get_sales_adjustment_summary": "销量调整汇总",
}

MAX_STORE_ROWS = 500
MAX_RANKING_ROWS = 100
MAX_TREND_ROWS = 366
MAX_COMPARISON_ROWS = 20
MAX_COMPARISON_SERIES_ROWS = 7_500
MAX_SKU_ROWS = 100
MAX_SLOW_MOVING_ROWS = 100
MAX_ADJUSTMENT_ROWS = 16


def _schema_object(fields: dict[str, Any]) -> tuple[str, dict[str, Any]]:
    return ("object", fields)


def _schema_list(
    item: Any,
    max_items: int,
) -> tuple[str, Any, int]:
    return ("list", item, max_items)


def _schema_text(
    max_chars: int,
    *,
    sensitive: bool = False,
    nullable: bool = False,
) -> tuple[str, int, bool, bool]:
    return ("text", max_chars, sensitive, nullable)


INTEGER_SCHEMA = ("integer",)
NUMBER_SCHEMA = ("number", False)
NULLABLE_NUMBER_SCHEMA = ("number", True)
BOOLEAN_SCHEMA = ("boolean",)
STORE_SCHEMA = _schema_object(
    {
        "id": INTEGER_SCHEMA,
        "name": _schema_text(255, sensitive=True),
    }
)
RANGE_SCHEMA = _schema_object(
    {
        "start": _schema_text(10),
        "end": _schema_text(10),
    }
)
COMMON_RESULT_FIELDS = {
    "store": STORE_SCHEMA,
    "range": RANGE_SCHEMA,
    "metric": _schema_text(64),
    "dataUpdatedAt": _schema_text(40, nullable=True),
    "unresolvedAdjustmentCount": INTEGER_SCHEMA,
}
SALES_TOTAL_FIELDS = {
    "orderCount": INTEGER_SCHEMA,
    "orderedUnits": INTEGER_SCHEMA,
    "effectiveUnits": INTEGER_SCHEMA,
    "grossSalesAmount": NUMBER_SCHEMA,
    "effectiveSalesAmount": NUMBER_SCHEMA,
    "canceledUnits": INTEGER_SCHEMA,
    "refundedUnits": INTEGER_SCHEMA,
    "returnedUnits": INTEGER_SCHEMA,
}
RANKING_ROW_SCHEMA = _schema_object(
    {
        "manageNumber": _schema_text(255, sensitive=True),
        "itemNumber": _schema_text(255, sensitive=True),
        "itemName": _schema_text(500, sensitive=True),
        "skuKey": _schema_text(255, sensitive=True),
        "orderCount": INTEGER_SCHEMA,
        "orderedUnits": INTEGER_SCHEMA,
        "effectiveUnits": INTEGER_SCHEMA,
        "grossSalesAmount": NUMBER_SCHEMA,
        "effectiveSalesAmount": NUMBER_SCHEMA,
        "metricValue": NUMBER_SCHEMA,
    }
)
TREND_ROW_SCHEMA = _schema_object(
    {
        "period": _schema_text(10),
        "orderedUnits": INTEGER_SCHEMA,
        "effectiveUnits": INTEGER_SCHEMA,
        "effectiveSalesAmount": NUMBER_SCHEMA,
    }
)
COMPARISON_SUMMARY_ROW_SCHEMA = _schema_object(
    {
        "manageNumber": _schema_text(255, sensitive=True),
        "itemName": _schema_text(500, sensitive=True),
        "orderedUnits": INTEGER_SCHEMA,
        "effectiveUnits": INTEGER_SCHEMA,
        "effectiveSalesAmount": NUMBER_SCHEMA,
        "adjustmentRate": NUMBER_SCHEMA,
    }
)
COMPARISON_SERIES_ROW_SCHEMA = _schema_object(
    {
        "manageNumber": _schema_text(255, sensitive=True),
        **TREND_ROW_SCHEMA[1],
    }
)
TOOL_RESULT_SCHEMAS = {
    "list_owned_stores": _schema_object(
        {
            "dataUpdatedAt": _schema_text(40, nullable=True),
            "rows": _schema_list(
                _schema_object(
                    {
                        "id": INTEGER_SCHEMA,
                        "name": _schema_text(255, sensitive=True),
                        "code": _schema_text(120, sensitive=True),
                        "enabled": BOOLEAN_SCHEMA,
                    }
                ),
                MAX_STORE_ROWS,
            ),
        }
    ),
    "get_store_sales_overview": _schema_object(
        {
            **COMMON_RESULT_FIELDS,
            "rows": _schema_list(
                _schema_object(SALES_TOTAL_FIELDS),
                1,
            ),
            "comparison": _schema_object(
                {
                    "range": RANGE_SCHEMA,
                    **SALES_TOTAL_FIELDS,
                    "changes": _schema_object(
                        {
                            "orderCount": NULLABLE_NUMBER_SCHEMA,
                            "effectiveUnits": NULLABLE_NUMBER_SCHEMA,
                            "effectiveSalesAmount": NULLABLE_NUMBER_SCHEMA,
                        }
                    ),
                }
            ),
        }
    ),
    "get_product_sales_ranking": _schema_object(
        {
            **COMMON_RESULT_FIELDS,
            "rows": _schema_list(
                RANKING_ROW_SCHEMA,
                MAX_RANKING_ROWS,
            ),
        }
    ),
    "get_product_sales_trend": _schema_object(
        {
            **COMMON_RESULT_FIELDS,
            "manageNumber": _schema_text(255, sensitive=True),
            "grain": _schema_text(16),
            "rows": _schema_list(TREND_ROW_SCHEMA, MAX_TREND_ROWS),
        }
    ),
    "compare_product_sales": _schema_object(
        {
            **COMMON_RESULT_FIELDS,
            "grain": _schema_text(16),
            "rows": _schema_list(
                COMPARISON_SUMMARY_ROW_SCHEMA,
                MAX_COMPARISON_ROWS,
            ),
            "series": _schema_list(
                COMPARISON_SERIES_ROW_SCHEMA,
                MAX_COMPARISON_SERIES_ROWS,
            ),
        }
    ),
    "get_sku_sales_breakdown": _schema_object(
        {
            **COMMON_RESULT_FIELDS,
            "manageNumber": _schema_text(255, sensitive=True),
            "rows": _schema_list(
                _schema_object(
                    {
                        "skuKey": _schema_text(255, sensitive=True),
                        "itemName": _schema_text(500, sensitive=True),
                        "orderedUnits": INTEGER_SCHEMA,
                        "effectiveUnits": INTEGER_SCHEMA,
                        "effectiveSalesAmount": NUMBER_SCHEMA,
                        "unitShare": NUMBER_SCHEMA,
                        "salesShare": NUMBER_SCHEMA,
                    }
                ),
                MAX_SKU_ROWS,
            ),
        }
    ),
    "get_slow_moving_products": _schema_object(
        {
            **COMMON_RESULT_FIELDS,
            "threshold": _schema_object(
                {
                    "minListedDays": INTEGER_SCHEMA,
                    "maxEffectiveUnits": INTEGER_SCHEMA,
                }
            ),
            "rows": _schema_list(
                _schema_object(
                    {
                        "manageNumber": _schema_text(
                            255,
                            sensitive=True,
                        ),
                        "itemName": _schema_text(500, sensitive=True),
                        "listedAt": _schema_text(10),
                        "listedDays": INTEGER_SCHEMA,
                        "effectiveUnits": INTEGER_SCHEMA,
                        "effectiveSalesAmount": NUMBER_SCHEMA,
                    }
                ),
                MAX_SLOW_MOVING_ROWS,
            ),
        }
    ),
    "get_sales_adjustment_summary": _schema_object(
        {
            **COMMON_RESULT_FIELDS,
            "rows": _schema_list(
                _schema_object(
                    {
                        "adjustmentType": _schema_text(64),
                        "status": _schema_text(64),
                        "adjustmentCount": INTEGER_SCHEMA,
                        "units": INTEGER_SCHEMA,
                        "amount": NUMBER_SCHEMA,
                    }
                ),
                MAX_ADJUSTMENT_ROWS,
            ),
        }
    ),
}
_SENSITIVE_ASSIGNMENT_RE = re.compile(
    r"(?i)\b(?:"
    r"owner(?:_?username|_?name)?|"
    r"api(?:_?key|_?secret|_?token)?|"
    r"license(?:_?key)?|service(?:_?secret)?|"
    r"buyer(?:_?name|_?email|_?phone|_?address)?|"
    r"customer(?:_?name|_?email|_?phone|_?address)?|"
    r"recipient(?:_?name|_?email|_?phone|_?address)?|"
    r"contact(?:_?name|_?email|_?phone|_?address)?|"
    r"password|authorization|raw(?:_?order|_?payload)"
    r")\s*[:=：]\s*[^,，;；\n]+"
)
_CHINESE_SENSITIVE_ASSIGNMENT_RE = re.compile(
    r"(?:"
    r"买家(?:姓名|名称|邮箱|电话|手机|地址)?|"
    r"购买者(?:姓名|名称|邮箱|电话|手机|地址)?|"
    r"客户(?:姓名|名称|邮箱|电话|手机|地址)?|"
    r"收件人(?:姓名|名称|邮箱|电话|手机|地址)?|"
    r"收件地址|联系地址|联系电话|联系手机|手机号码|电话号码|"
    r"邮箱地址"
    r")\s*[:=：]\s*[^,，;；\n]+"
)
_EMAIL_RE = re.compile(
    r"(?i)\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b"
)
_PHONE_CANDIDATE_RE = re.compile(
    r"(?<![\w-])(?:\+?\d[\d ()-]{7,}\d)(?!\w)"
)
_RAW_ORDER_SEGMENT_RE = re.compile(
    r"(?is)[\"']?(?:"
    r"raw[_ -]?order|orderModelList|packageModelList|itemModelList|"
    r"orderNumber"
    r")[\"']?\s*[:=：]\s*.*?(?=(?:[;；\n]|$))"
)
_RAW_ORDER_MARKER_RE = re.compile(
    r"(?i)\b(?:"
    r"raw[_ -]?order|orderModelList|packageModelList|itemModelList|"
    r"orderNumber"
    r")\b"
)
_DATABASE_STATEMENT_RE = re.compile(
    r"(?is)\b(?:select|insert|update|delete|drop|alter|create|replace|"
    r"truncate|merge)\b.*?(?=(?:[,;；\n]|$))"
)


def _current_shanghai_date() -> date:
    return datetime.now(SHANGHAI_TIMEZONE).date()


def _now_local_naive() -> datetime:
    return datetime.now(SHANGHAI_TIMEZONE).replace(tzinfo=None)


def _json_load(value: str, default: Any) -> Any:
    try:
        parsed = json.loads(value or "")
    except (TypeError, ValueError):
        return default
    return parsed


def _json_dump(value: Any) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )


def _truncate_text(value: Any, max_chars: int) -> str:
    text = str(value or "")
    if len(text) <= max_chars:
        return text
    if max_chars <= len(TRUNCATION_MARKER):
        return TRUNCATION_MARKER[:max_chars]
    return text[: max_chars - len(TRUNCATION_MARKER)] + TRUNCATION_MARKER


def _timestamp_to_public(value: datetime | None) -> str | None:
    return value.isoformat(timespec="seconds") if value is not None else None


def _conversation_to_public(
    row: SalesAnalysisConversationModel,
) -> dict[str, Any]:
    scope = _json_load(row.store_scope_json, [])
    return {
        "id": row.id,
        "title": row.title,
        "storeScope": scope if isinstance(scope, list) else [],
        "lastMessageAt": _timestamp_to_public(row.last_message_at),
        "createdAt": _timestamp_to_public(row.created_at),
        "updatedAt": _timestamp_to_public(row.updated_at),
    }


def _message_to_public(
    row: SalesAnalysisMessageModel,
    *,
    fallback: bool = False,
) -> dict[str, Any]:
    tool_arguments = _json_load(row.tool_arguments_json, [])
    result_summary = _json_load(row.result_summary_json, [])
    store_scope = _json_load(row.store_scope_json, [])
    statistics_window = _json_load(row.statistics_window_json, {})
    return {
        "id": row.id,
        "conversationId": row.conversation_id,
        "question": row.question_text,
        "answer": row.answer_text,
        "toolName": row.tool_name,
        "toolArguments": tool_arguments,
        "resultSummary": result_summary,
        "modelName": row.model_name,
        "storeScope": store_scope,
        "statisticsWindow": statistics_window,
        "fallback": fallback,
        "createdAt": _timestamp_to_public(row.created_at),
        "updatedAt": _timestamp_to_public(row.updated_at),
    }


def list_conversations(owner_username: str) -> list[dict[str, Any]]:
    normalized_owner = str(owner_username or "").strip()
    if not normalized_owner:
        return []
    with session_scope() as session:
        rows = session.scalars(
            select(SalesAnalysisConversationModel)
            .where(
                SalesAnalysisConversationModel.owner_username
                == normalized_owner
            )
            .order_by(
                SalesAnalysisConversationModel.last_message_at.desc(),
                SalesAnalysisConversationModel.id.desc(),
            )
        ).all()
        return [_conversation_to_public(row) for row in rows]


def create_conversation(
    owner_username: str,
    title: str = DEFAULT_CONVERSATION_TITLE,
) -> dict[str, Any]:
    normalized_owner = str(owner_username or "").strip()
    if not normalized_owner:
        raise ValueError("用户不能为空。")
    normalized_title = str(title or "").strip() or DEFAULT_CONVERSATION_TITLE
    if len(normalized_title) > 255:
        normalized_title = normalized_title[:255]
    with session_scope() as session:
        row = SalesAnalysisConversationModel(
            owner_username=normalized_owner,
            title=normalized_title,
            store_scope_json="[]",
        )
        session.add(row)
        session.flush()
        return _conversation_to_public(row)


def _owned_stores(
    session: Any,
    owner_username: str,
) -> list[StoreModel]:
    return list(
        session.scalars(
            select(StoreModel)
            .where(StoreModel.owner_username == owner_username)
            .order_by(StoreModel.id.asc())
        ).all()
    )


def _explicit_store_matches(
    message: str,
    stores: list[StoreModel],
) -> list[StoreModel]:
    normalized_message = str(message or "").casefold()
    matches: list[StoreModel] = []
    for store in stores:
        text_candidates = {
            str(store.store_name or "").strip().casefold(),
            str(store.store_code or "").strip().casefold(),
            str(store.alias_name or "").strip().casefold(),
        }
        text_match = any(
            candidate and candidate in normalized_message
            for candidate in text_candidates
        )
        id_match = bool(
            re.search(
                rf"(?i)(?:store\s*id|storeId|店铺(?:编号|ID)?)\s*[:=#]?\s*{store.id}\b",
                str(message or ""),
            )
        )
        if text_match or id_match:
            matches.append(store)
    return matches


def _select_store(
    message: str,
    stores: list[StoreModel],
    stored_scope: list[Any],
) -> StoreModel | None:
    stores_by_id = {store.id: store for store in stores}
    explicit = _explicit_store_matches(message, stores)
    if len(explicit) == 1:
        return explicit[0]
    scoped = [
        stores_by_id[store_id]
        for store_id in stored_scope
        if isinstance(store_id, int) and store_id in stores_by_id
    ]
    if len(scoped) == 1:
        return scoped[0]
    if len(stores) == 1:
        return stores[0]
    return None


def _start_turn(
    owner_username: str,
    conversation_id: int,
    question: str,
) -> dict[str, Any]:
    with session_scope() as session:
        conversation = session.scalar(
            select(SalesAnalysisConversationModel).where(
                SalesAnalysisConversationModel.id == conversation_id,
                SalesAnalysisConversationModel.owner_username
                == owner_username,
            )
        )
        if conversation is None:
            raise LookupError("会话不存在或无权访问。")
        history_rows = session.execute(
            select(
                func.substr(
                    SalesAnalysisMessageModel.question_text,
                    1,
                    MAX_HISTORY_SOURCE_CHARS,
                ).label("question_text"),
                func.substr(
                    SalesAnalysisMessageModel.answer_text,
                    1,
                    MAX_HISTORY_SOURCE_CHARS,
                ).label("answer_text"),
            )
            .where(
                SalesAnalysisMessageModel.conversation_id
                == conversation_id,
                SalesAnalysisMessageModel.owner_username == owner_username,
            )
            .order_by(SalesAnalysisMessageModel.id.desc())
            .limit(MAX_HISTORY_ROWS_SCAN)
        ).all()
        stores = _owned_stores(session, owner_username)
        account = session.scalar(
            select(UserAccountModel).where(
                UserAccountModel.username == owner_username
            )
        )
        stored_scope = _json_load(conversation.store_scope_json, [])
        if not isinstance(stored_scope, list):
            stored_scope = []
        selected_store = _select_store(question, stores, stored_scope)
        store_scope = [selected_store.id] if selected_store is not None else []
        if selected_store is not None:
            conversation.store_scope_json = _json_dump(store_scope)
        now = _now_local_naive()
        conversation.last_message_at = now
        message_row = SalesAnalysisMessageModel(
            conversation_id=conversation.id,
            owner_username=owner_username,
            question_text=question,
            answer_text="",
            tool_name="",
            tool_arguments_json="[]",
            result_summary_json="[]",
            model_name="",
            store_scope_json=_json_dump(store_scope),
            statistics_window_json="{}",
        )
        session.add(message_row)
        session.flush()
        return {
            "messageId": message_row.id,
            "stores": [
                {
                    "id": store.id,
                    "name": store.store_name,
                    "code": store.store_code,
                }
                for store in stores
            ],
            "selectedStore": (
                {
                    "id": selected_store.id,
                    "name": selected_store.store_name,
                    "code": selected_store.store_code,
                }
                if selected_store is not None
                else None
            ),
            "history": [
                {
                    "question": row.question_text,
                    "answer": row.answer_text,
                }
                for row in reversed(history_rows)
            ],
            "sensitiveValues": list(
                dict.fromkeys(
                    value
                    for value in (
                        account.display_name if account is not None else "",
                        *(
                            contact_value
                            for store in stores
                            for contact_value in (
                                store.contact_name,
                                store.contact_phone,
                            )
                        ),
                    )
                    if isinstance(value, str) and value.strip()
                )
            ),
        }


def _load_model_configuration(owner_username: str) -> dict[str, str]:
    with session_scope() as session:
        settings = ensure_user_settings(session, owner_username)
        api_key = decrypt_text(settings.api_key_encrypted)
        if not api_key:
            raise RuntimeError(
                "请先在 AI 管理中配置你自己的 API Key。"
            )
        if not settings.verified_at or settings.last_error:
            raise RuntimeError(
                "请先在 AI 管理中保存配置并通过连接测试。"
            )
        model_name = resolved_model_name(settings)
        if not model_name:
            raise RuntimeError("请先在 AI 管理中配置模型名称。")
        return {
            "apiKey": api_key,
            "apiBase": settings.api_base_url or "",
            "modelName": model_name,
        }


def _relative_date_window(message: str) -> dict[str, str] | None:
    normalized = str(message or "")
    if not any(
        marker in normalized
        for marker in ("近期", "最近30天", "最近 30 天", "近30天", "近 30 天")
    ):
        return None
    end_date = _current_shanghai_date() - timedelta(days=1)
    start_date = end_date - timedelta(days=29)
    return {
        "start": start_date.isoformat(),
        "end": end_date.isoformat(),
    }


def _expand_relative_dates(
    message: str,
    relative_window: dict[str, str] | None,
) -> str:
    if relative_window is None:
        return str(message or "")
    replacement = (
        f"{relative_window['start']} 至 {relative_window['end']}"
        "（最近 30 个完整自然日）"
    )
    expanded = str(message or "")
    for marker in (
        "最近 30 天",
        "最近30天",
        "近 30 天",
        "近30天",
        "近期",
    ):
        expanded = expanded.replace(marker, replacement)
    return expanded


def _sanitize_text(
    value: Any,
    *,
    owner_username: str,
    secrets: tuple[str, ...] = (),
) -> str:
    text = _sanitize_non_phone_sensitive_text(
        value,
        owner_username=owner_username,
        secrets=secrets,
    )
    return _PHONE_CANDIDATE_RE.sub(
        lambda match: (
            "[敏感信息已省略]"
            if 10
            <= sum(character.isdigit() for character in match.group())
            <= 15
            else match.group()
        ),
        text,
    )


def _sanitize_non_phone_sensitive_text(
    value: Any,
    *,
    owner_username: str,
    secrets: tuple[str, ...] = (),
) -> str:
    text = str(value or "")
    for secret in (owner_username, *secrets):
        if secret:
            text = text.replace(secret, "[敏感信息已省略]")
    text = _SENSITIVE_ASSIGNMENT_RE.sub("[敏感信息已省略]", text)
    text = _CHINESE_SENSITIVE_ASSIGNMENT_RE.sub(
        "[敏感信息已省略]",
        text,
    )
    text = _RAW_ORDER_SEGMENT_RE.sub("[完整订单已省略]", text)
    text = _RAW_ORDER_MARKER_RE.sub("[完整订单已省略]", text)
    text = _EMAIL_RE.sub("[敏感信息已省略]", text)
    text = _DATABASE_STATEMENT_RE.sub("[数据库语句已省略]", text)
    return text


_DROP_SCHEMA_VALUE = object()


def _sanitize_schema_value(
    value: Any,
    schema: Any,
    *,
    owner_username: str,
    secrets: tuple[str, ...],
) -> Any:
    kind = schema[0]
    if kind == "object":
        if not isinstance(value, dict):
            return _DROP_SCHEMA_VALUE
        sanitized: dict[str, Any] = {}
        for key, child_schema in schema[1].items():
            if key not in value:
                continue
            child_value = _sanitize_schema_value(
                value[key],
                child_schema,
                owner_username=owner_username,
                secrets=secrets,
            )
            if child_value is not _DROP_SCHEMA_VALUE:
                sanitized[key] = child_value
        return sanitized
    if kind == "list":
        if not isinstance(value, list):
            return _DROP_SCHEMA_VALUE
        sanitized_items: list[Any] = []
        for item in value[: schema[2]]:
            sanitized_item = _sanitize_schema_value(
                item,
                schema[1],
                owner_username=owner_username,
                secrets=secrets,
            )
            if sanitized_item is not _DROP_SCHEMA_VALUE:
                sanitized_items.append(sanitized_item)
        return sanitized_items
    if kind == "text":
        max_chars, sensitive, nullable = schema[1:]
        if value is None and nullable:
            return None
        if not isinstance(value, str):
            return _DROP_SCHEMA_VALUE
        text = (
            _sanitize_non_phone_sensitive_text(
                value,
                owner_username=owner_username,
                secrets=secrets,
            )
            if sensitive
            else value
        )
        return text[:max_chars]
    if kind == "integer":
        if isinstance(value, int) and not isinstance(value, bool):
            return value
        return _DROP_SCHEMA_VALUE
    if kind == "number":
        if value is None and schema[1]:
            return None
        if (
            isinstance(value, (int, float))
            and not isinstance(value, bool)
            and math.isfinite(float(value))
        ):
            return value
        return _DROP_SCHEMA_VALUE
    if kind == "boolean" and isinstance(value, bool):
        return value
    return _DROP_SCHEMA_VALUE


def _sanitize_tool_result(
    tool_name: str,
    value: Any,
    *,
    owner_username: str,
    secrets: tuple[str, ...],
) -> dict[str, Any]:
    schema = TOOL_RESULT_SCHEMAS[tool_name]
    sanitized = _sanitize_schema_value(
        value,
        schema,
        owner_username=owner_username,
        secrets=secrets,
    )
    return sanitized if isinstance(sanitized, dict) else {}


def _bounded_tool_result_for_model(
    result: dict[str, Any],
) -> dict[str, Any]:
    bounded = json.loads(_json_dump(result))
    for key in ("rows", "series"):
        value = bounded.get(key)
        if isinstance(value, list):
            bounded[key] = value[:MAX_MODEL_TOOL_RESULT_ROWS]
    while len(_json_dump(bounded)) > MAX_MODEL_TOOL_RESULT_CHARS:
        candidates = [
            (key, bounded[key])
            for key in ("series", "rows")
            if isinstance(bounded.get(key), list) and bounded[key]
        ]
        if not candidates:
            break
        _, longest = max(
            candidates,
            key=lambda item: (
                len(item[1]),
                1 if item[0] == "series" else 0,
            ),
        )
        longest.pop()
    return bounded


def _tool_call_field(value: Any, field: str, default: Any = None) -> Any:
    if isinstance(value, dict):
        return value.get(field, default)
    return getattr(value, field, default)


def _response_message(response: Any) -> Any:
    choices = _tool_call_field(response, "choices", [])
    if not isinstance(choices, (list, tuple)) or not choices:
        raise RuntimeError("模型未返回有效响应。")
    return _tool_call_field(choices[0], "message")


def _normalized_tool_calls(message: Any) -> list[dict[str, Any]]:
    raw_calls = _tool_call_field(message, "tool_calls", []) or []
    normalized: list[dict[str, Any]] = []
    for index, raw_call in enumerate(raw_calls, start=1):
        function = _tool_call_field(raw_call, "function")
        name = str(_tool_call_field(function, "name", "") or "").strip()
        raw_arguments = _tool_call_field(function, "arguments", "{}")
        if isinstance(raw_arguments, str):
            try:
                arguments = json.loads(raw_arguments or "{}")
            except ValueError as exc:
                raise ValueError("分析工具参数格式不正确。") from exc
        elif isinstance(raw_arguments, dict):
            arguments = dict(raw_arguments)
        else:
            raise ValueError("分析工具参数格式不正确。")
        if not isinstance(arguments, dict):
            raise ValueError("分析工具参数必须是对象。")
        normalized.append(
            {
                "id": str(
                    _tool_call_field(raw_call, "id", f"call-{index}")
                    or f"call-{index}"
                ),
                "name": name,
                "arguments": arguments,
            }
        )
    return normalized


def _assistant_message_for_history(
    content: str,
    tool_calls: list[dict[str, Any]],
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "role": "assistant",
        "content": content,
    }
    if tool_calls:
        payload["tool_calls"] = [
            {
                "id": call["id"],
                "type": "function",
                "function": {
                    "name": call["name"],
                    "arguments": _json_dump(call["arguments"]),
                },
            }
            for call in tool_calls
        ]
    return payload


def _prepare_tool_arguments(
    tool_name: str,
    raw_arguments: dict[str, Any],
    *,
    selected_store_id: int,
    relative_window: dict[str, str] | None,
    owner_username: str,
    secrets: tuple[str, ...],
) -> dict[str, Any]:
    arguments = copy_json_value(raw_arguments)
    if tool_name in STORE_SCOPED_TOOLS:
        arguments["storeId"] = selected_store_id
    if tool_name in DATE_SCOPED_TOOLS and relative_window is not None:
        arguments["startDate"] = relative_window["start"]
        arguments["endDate"] = relative_window["end"]
    argument_model = sales_analysis_service._TOOL_MODELS[tool_name]
    validated = argument_model.model_validate(arguments)
    validated_arguments = validated.model_dump(
        mode="json",
        by_alias=True,
        exclude_none=True,
        exclude_unset=True,
    )
    if not _tool_argument_value_is_safe(
        validated_arguments,
        owner_username=owner_username,
        secrets=secrets,
    ):
        raise ValueError("分析工具参数包含敏感内容。")
    return validated_arguments


def copy_json_value(value: Any) -> Any:
    return json.loads(json.dumps(value, ensure_ascii=False))


def _tool_argument_value_is_safe(
    value: Any,
    *,
    owner_username: str,
    secrets: tuple[str, ...],
) -> bool:
    if isinstance(value, dict):
        return all(
            _tool_argument_value_is_safe(
                item,
                owner_username=owner_username,
                secrets=secrets,
            )
            for item in value.values()
        )
    if isinstance(value, list):
        return all(
            _tool_argument_value_is_safe(
                item,
                owner_username=owner_username,
                secrets=secrets,
            )
            for item in value
        )
    if isinstance(value, str):
        return (
            _sanitize_non_phone_sensitive_text(
                value,
                owner_username=owner_username,
                secrets=secrets,
            )
            == value
        )
    return value is None or isinstance(value, (bool, int, float))


def _statistics_window(
    relative_window: dict[str, str] | None,
    tool_records: list[dict[str, Any]],
) -> dict[str, str]:
    for record in reversed(tool_records):
        result_range = record["result"].get("range")
        if (
            isinstance(result_range, dict)
            and isinstance(result_range.get("start"), str)
            and isinstance(result_range.get("end"), str)
        ):
            return {
                "start": result_range["start"],
                "end": result_range["end"],
            }
    return dict(relative_window or {})


def _persist_message_state(
    *,
    owner_username: str,
    conversation_id: int,
    message_id: int,
    answer: str | None,
    model_name: str,
    selected_store_id: int | None,
    relative_window: dict[str, str] | None,
    tool_records: list[dict[str, Any]],
    fallback: bool = False,
) -> dict[str, Any]:
    with session_scope() as session:
        row = session.scalar(
            select(SalesAnalysisMessageModel).where(
                SalesAnalysisMessageModel.id == message_id,
                SalesAnalysisMessageModel.conversation_id
                == conversation_id,
                SalesAnalysisMessageModel.owner_username == owner_username,
            )
        )
        conversation = session.scalar(
            select(SalesAnalysisConversationModel).where(
                SalesAnalysisConversationModel.id == conversation_id,
                SalesAnalysisConversationModel.owner_username
                == owner_username,
            )
        )
        if row is None or conversation is None:
            raise LookupError("会话消息不存在或无权访问。")
        tool_names = [record["toolName"] for record in tool_records]
        if answer is not None:
            row.answer_text = _truncate_text(
                answer,
                MAX_PERSISTED_ANSWER_CHARS,
            )
        row.tool_name = ",".join(tool_names)[:128]
        row.tool_arguments_json = _json_dump(
            [
                {
                    "toolName": record["toolName"],
                    "arguments": record["arguments"],
                }
                for record in tool_records
            ]
        )
        row.result_summary_json = _json_dump(
            [
                {
                    "toolName": record["toolName"],
                    "result": record["result"],
                }
                for record in tool_records
            ]
        )
        row.model_name = model_name
        store_scope = (
            [selected_store_id] if selected_store_id is not None else []
        )
        row.store_scope_json = _json_dump(store_scope)
        row.statistics_window_json = _json_dump(
            _statistics_window(relative_window, tool_records)
        )
        conversation.store_scope_json = _json_dump(store_scope)
        conversation.last_message_at = _now_local_naive()
        session.flush()
        return _message_to_public(row, fallback=fallback)


def _persist_tool_audit(
    *,
    owner_username: str,
    conversation_id: int,
    message_id: int,
    model_name: str,
    selected_store_id: int,
    relative_window: dict[str, str] | None,
    tool_records: list[dict[str, Any]],
) -> None:
    _persist_message_state(
        owner_username=owner_username,
        conversation_id=conversation_id,
        message_id=message_id,
        answer=None,
        model_name=model_name,
        selected_store_id=selected_store_id,
        relative_window=relative_window,
        tool_records=tool_records,
    )


def _finalize_message(
    *,
    owner_username: str,
    conversation_id: int,
    message_id: int,
    answer: str,
    model_name: str,
    selected_store_id: int | None,
    relative_window: dict[str, str] | None,
    tool_records: list[dict[str, Any]],
    fallback: bool = False,
) -> dict[str, Any]:
    return _persist_message_state(
        owner_username=owner_username,
        conversation_id=conversation_id,
        message_id=message_id,
        answer=answer,
        model_name=model_name,
        selected_store_id=selected_store_id,
        relative_window=relative_window,
        tool_records=tool_records,
        fallback=fallback,
    )


def _clarification_answer(stores: list[dict[str, Any]]) -> str:
    if not stores:
        return "当前没有可用于销量分析的店铺。"
    options = "；".join(
        f"{store['name']}（店铺编号 {store['id']}）"
        for store in stores
    )
    return f"你有多家店铺，请选择一家店铺后再分析：{options}。"


def _system_prompt(
    selected_store: dict[str, Any],
    relative_window: dict[str, str] | None,
) -> str:
    date_rule = (
        f"本次“近期”固定为 {relative_window['start']} 至 "
        f"{relative_window['end']}。"
        if relative_window is not None
        else "所有工具必须使用明确的 YYYY-MM-DD 起止日期。"
    )
    return (
        "你是商品销量分析助手。只能使用提供的只读分析工具，并且只能依据工具返回的结构化结果回答。"
        "不得请求、推断或输出店铺访问凭证、完整订单、买家资料、用户身份或数据库查询语句。"
        f"当前店铺为 {selected_store['name']}，店铺编号 {selected_store['id']}。"
        f"当前上海日期为 {_current_shanghai_date().isoformat()}。"
        f"{date_rule}"
        f"“销量”默认指有效销量，定义为：{EFFECTIVE_SALES_DEFINITION}。"
        "回答必须显示店铺、具体起止日期、有效销量口径、数据最后更新时间和未决调整数量。"
    )


def _bounded_history_model_messages(
    history: list[dict[str, str]],
    *,
    owner_username: str,
    secrets: tuple[str, ...],
) -> list[dict[str, str]]:
    turns: list[list[dict[str, str]]] = []
    for previous in history:
        question = _truncate_text(
            _sanitize_text(
                previous.get("question", ""),
                owner_username=owner_username,
                secrets=secrets,
            ),
            MAX_MODEL_HISTORY_ENTRY_CHARS,
        )
        answer = _truncate_text(
            _sanitize_text(
                previous.get("answer", ""),
                owner_username=owner_username,
                secrets=secrets,
            ),
            MAX_MODEL_HISTORY_ENTRY_CHARS,
        )
        turn: list[dict[str, str]] = []
        if question:
            turn.append({"role": "user", "content": question})
        if answer:
            turn.append({"role": "assistant", "content": answer})
        if turn:
            turns.append(turn)

    remaining = MAX_MODEL_HISTORY_CHARS
    selected_reversed: list[list[dict[str, str]]] = []
    for turn in reversed(turns):
        turn_chars = sum(len(message["content"]) for message in turn)
        if turn_chars > remaining:
            continue
        selected_reversed.append(turn)
        remaining -= turn_chars
    selected: list[dict[str, str]] = []
    for turn in reversed(selected_reversed):
        selected.extend(turn)
    return selected


def _model_messages(
    *,
    owner_username: str,
    secrets: tuple[str, ...],
    selected_store: dict[str, Any],
    question: str,
    history: list[dict[str, str]],
    relative_window: dict[str, str] | None,
) -> list[dict[str, Any]]:
    sanitized_store = {
        **selected_store,
        "name": _sanitize_text(
            selected_store.get("name", ""),
            owner_username=owner_username,
            secrets=secrets,
        ),
        "code": _sanitize_text(
            selected_store.get("code", ""),
            owner_username=owner_username,
            secrets=secrets,
        ),
    }
    messages: list[dict[str, Any]] = [
        {
            "role": "system",
            "content": _system_prompt(sanitized_store, relative_window),
        }
    ]
    messages.extend(
        _bounded_history_model_messages(
            history,
            owner_username=owner_username,
            secrets=secrets,
        )
    )
    expanded_question = _expand_relative_dates(question, relative_window)
    messages.append(
        {
            "role": "user",
            "content": _truncate_text(
                _sanitize_text(
                    expanded_question,
                    owner_username=owner_username,
                    secrets=secrets,
                ),
                MAX_MODEL_CURRENT_MESSAGE_CHARS,
            ),
        }
    )
    return messages


def _completion_kwargs(
    model_configuration: dict[str, str],
    messages: list[dict[str, Any]],
) -> dict[str, Any]:
    return {
        "model": model_configuration["modelName"],
        "messages": messages,
        "tools": SALES_ANALYSIS_TOOLS,
        "tool_choice": "auto",
        "api_key": model_configuration["apiKey"],
        "api_base": model_configuration["apiBase"] or None,
        "stream": False,
        "temperature": 0.1,
        "max_tokens": 1600,
        "timeout": 120,
        "drop_params": True,
    }


def _fallback_answer(
    selected_store: dict[str, Any],
    relative_window: dict[str, str] | None,
    tool_records: list[dict[str, Any]],
) -> str:
    statistics_window = _statistics_window(
        relative_window,
        tool_records,
    )
    start_date = statistics_window.get("start", "未提供")
    end_date = statistics_window.get("end", "未提供")
    updated_at: str | None = None
    unresolved_count = 0
    row_lines: list[str] = []
    for record in tool_records:
        result = record["result"]
        if isinstance(result.get("dataUpdatedAt"), str):
            updated_at = result["dataUpdatedAt"]
        raw_unresolved = result.get("unresolvedAdjustmentCount")
        if isinstance(raw_unresolved, int):
            unresolved_count = max(unresolved_count, raw_unresolved)
        rows = result.get("rows")
        if isinstance(rows, list):
            row_lines.extend(
                _json_dump(row)
                for row in rows
                if isinstance(row, dict)
            )
    if not row_lines:
        row_lines.append("（无返回行）")
    return "\n".join(
        [
            f"店铺：{selected_store['name']}",
            f"日期：{start_date} 至 {end_date}",
            f"口径：{EFFECTIVE_SALES_DEFINITION}",
            f"数据最后更新时间：{updated_at or '暂无'}",
            f"未决调整数量：{unresolved_count}",
            "返回表格行：",
            *row_lines,
        ]
    )


def stream_analysis(
    owner_username: str,
    conversation_id: int,
    message: str,
) -> Iterator[dict[str, Any]]:
    normalized_owner = str(owner_username or "").strip()
    normalized_message = str(message or "").strip()
    if not normalized_owner:
        raise ValueError("用户不能为空。")
    if not normalized_message:
        raise ValueError("分析问题不能为空。")
    persisted_message = _truncate_text(
        normalized_message,
        MAX_PERSISTED_QUESTION_CHARS,
    )
    turn = _start_turn(
        normalized_owner,
        int(conversation_id),
        persisted_message,
    )
    message_id = int(turn["messageId"])
    selected_store = turn["selectedStore"]
    relative_window = _relative_date_window(persisted_message)
    tool_records: list[dict[str, Any]] = []
    model_name = ""
    yield {"type": "status", "message": "正在准备销量分析。"}

    if selected_store is None:
        answer = _clarification_answer(turn["stores"])
        persisted = _finalize_message(
            owner_username=normalized_owner,
            conversation_id=conversation_id,
            message_id=message_id,
            answer=answer,
            model_name="",
            selected_store_id=None,
            relative_window=relative_window,
            tool_records=tool_records,
        )
        yield {"type": "delta", "content": persisted["answer"]}
        yield {"type": "completed", "message": persisted}
        return

    try:
        model_configuration = _load_model_configuration(normalized_owner)
        model_name = model_configuration["modelName"]
    except Exception as exc:
        answer = (
            str(exc)
            if isinstance(exc, RuntimeError)
            else "AI 配置不可用，请重新保存并验证配置。"
        )
        _finalize_message(
            owner_username=normalized_owner,
            conversation_id=conversation_id,
            message_id=message_id,
            answer=answer,
            model_name="",
            selected_store_id=selected_store["id"],
            relative_window=relative_window,
            tool_records=tool_records,
        )
        yield {"type": "error", "message": answer}
        return

    messages = _model_messages(
        owner_username=normalized_owner,
        secrets=tuple(
            value
            for value in (
                model_configuration["apiKey"],
                *turn["sensitiveValues"],
            )
            if value
        ),
        selected_store=selected_store,
        question=persisted_message,
        history=turn["history"],
        relative_window=relative_window,
    )
    redaction_secrets = tuple(
        value
        for value in (
            model_configuration["apiKey"],
            *turn["sensitiveValues"],
        )
        if value
    )
    executed_tool_calls = 0

    while True:
        try:
            response = litellm_completion(
                **_completion_kwargs(model_configuration, messages)
            )
            response_message = _response_message(response)
            raw_content = _tool_call_field(
                response_message,
                "content",
                "",
            )
            content = (
                raw_content
                if isinstance(raw_content, str)
                else ""
            )
            tool_calls = _normalized_tool_calls(response_message)
        except Exception:
            if tool_records:
                fallback_answer = _fallback_answer(
                    selected_store,
                    relative_window,
                    tool_records,
                )
                persisted = _finalize_message(
                    owner_username=normalized_owner,
                    conversation_id=conversation_id,
                    message_id=message_id,
                    answer=fallback_answer,
                    model_name=model_name,
                    selected_store_id=selected_store["id"],
                    relative_window=relative_window,
                    tool_records=tool_records,
                    fallback=True,
                )
                yield {
                    "type": "delta",
                    "content": persisted["answer"],
                }
                yield {"type": "completed", "message": persisted}
                return
            answer = "AI 分析失败，请稍后重试。"
            _finalize_message(
                owner_username=normalized_owner,
                conversation_id=conversation_id,
                message_id=message_id,
                answer=answer,
                model_name=model_name,
                selected_store_id=selected_store["id"],
                relative_window=relative_window,
                tool_records=tool_records,
            )
            yield {"type": "error", "message": answer}
            return

        if not tool_calls:
            if not tool_records:
                answer = (
                    "模型未调用销量分析工具，无法生成可信的销量结论。"
                )
                _finalize_message(
                    owner_username=normalized_owner,
                    conversation_id=conversation_id,
                    message_id=message_id,
                    answer=answer,
                    model_name=model_name,
                    selected_store_id=selected_store["id"],
                    relative_window=relative_window,
                    tool_records=tool_records,
                )
                yield {"type": "error", "message": answer}
                return
            sanitized_answer = _sanitize_text(
                content,
                owner_username=normalized_owner,
                secrets=redaction_secrets,
            ).strip()
            if not sanitized_answer:
                if tool_records:
                    sanitized_answer = _fallback_answer(
                        selected_store,
                        relative_window,
                        tool_records,
                    )
                    fallback = True
                else:
                    answer = "模型未返回可用的分析结果。"
                    _finalize_message(
                        owner_username=normalized_owner,
                        conversation_id=conversation_id,
                        message_id=message_id,
                        answer=answer,
                        model_name=model_name,
                        selected_store_id=selected_store["id"],
                        relative_window=relative_window,
                        tool_records=tool_records,
                    )
                    yield {"type": "error", "message": answer}
                    return
            else:
                fallback = False
            persisted = _finalize_message(
                owner_username=normalized_owner,
                conversation_id=conversation_id,
                message_id=message_id,
                answer=sanitized_answer,
                model_name=model_name,
                selected_store_id=selected_store["id"],
                relative_window=relative_window,
                tool_records=tool_records,
                fallback=fallback,
            )
            yield {"type": "delta", "content": persisted["answer"]}
            yield {"type": "completed", "message": persisted}
            return

        messages.append(
            _assistant_message_for_history(
                _sanitize_text(
                    content,
                    owner_username=normalized_owner,
                    secrets=redaction_secrets,
                ),
                tool_calls,
            )
        )
        for tool_call in tool_calls:
            if executed_tool_calls >= MAX_TOOL_CALLS_PER_TURN:
                answer = (
                    f"单次分析最多调用 {MAX_TOOL_CALLS_PER_TURN} 次工具，"
                    "请缩小问题范围后重试。"
                )
                _finalize_message(
                    owner_username=normalized_owner,
                    conversation_id=conversation_id,
                    message_id=message_id,
                    answer=answer,
                    model_name=model_name,
                    selected_store_id=selected_store["id"],
                    relative_window=relative_window,
                    tool_records=tool_records,
                )
                yield {"type": "error", "message": answer}
                return
            tool_name = tool_call["name"]
            if tool_name not in ALLOWED_TOOL_NAMES:
                answer = "模型请求了不允许的分析工具。"
                _finalize_message(
                    owner_username=normalized_owner,
                    conversation_id=conversation_id,
                    message_id=message_id,
                    answer=answer,
                    model_name=model_name,
                    selected_store_id=selected_store["id"],
                    relative_window=relative_window,
                    tool_records=tool_records,
                )
                yield {"type": "error", "message": answer}
                return
            try:
                arguments = _prepare_tool_arguments(
                    tool_name,
                    tool_call["arguments"],
                    selected_store_id=selected_store["id"],
                    relative_window=relative_window,
                    owner_username=normalized_owner,
                    secrets=redaction_secrets,
                )
            except (TypeError, ValueError):
                answer = "模型提供的分析工具参数不符合允许范围。"
                _finalize_message(
                    owner_username=normalized_owner,
                    conversation_id=conversation_id,
                    message_id=message_id,
                    answer=answer,
                    model_name=model_name,
                    selected_store_id=selected_store["id"],
                    relative_window=relative_window,
                    tool_records=tool_records,
                )
                yield {"type": "error", "message": answer}
                return
            executed_tool_calls += 1
            yield {
                "type": "tool_call",
                "toolName": tool_name,
                "label": TOOL_LABELS[tool_name],
                "arguments": arguments,
            }
            try:
                raw_result = execute_sales_tool(
                    normalized_owner,
                    tool_name,
                    arguments,
                )
            except Exception:
                answer = "销量分析工具执行失败，请检查查询条件。"
                _finalize_message(
                    owner_username=normalized_owner,
                    conversation_id=conversation_id,
                    message_id=message_id,
                    answer=answer,
                    model_name=model_name,
                    selected_store_id=selected_store["id"],
                    relative_window=relative_window,
                    tool_records=tool_records,
                )
                yield {"type": "error", "message": answer}
                return
            sanitized_result = _sanitize_tool_result(
                tool_name,
                raw_result,
                owner_username=normalized_owner,
                secrets=redaction_secrets,
            )
            record = {
                "toolName": tool_name,
                "arguments": arguments,
                "result": sanitized_result,
            }
            tool_records.append(record)
            _persist_tool_audit(
                owner_username=normalized_owner,
                conversation_id=conversation_id,
                message_id=message_id,
                model_name=model_name,
                selected_store_id=selected_store["id"],
                relative_window=relative_window,
                tool_records=tool_records,
            )
            yield {
                "type": "tool_result",
                "toolName": tool_name,
                "label": TOOL_LABELS[tool_name],
                "result": sanitized_result,
            }
            messages.append(
                {
                    "role": "tool",
                    "tool_call_id": tool_call["id"],
                    "name": tool_name,
                    "content": _json_dump(
                        _bounded_tool_result_for_model(
                            sanitized_result
                        )
                    ),
                }
            )
