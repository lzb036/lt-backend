from __future__ import annotations

from copy import deepcopy
from typing import Any

from app.db.database import session_scope
from app.db.models import UserSalesAnalysisSettingsModel


DEFAULT_PERIOD_DAYS = 30
DEFAULT_RANKING_LIMIT = 10
DEFAULT_METRIC = "effectiveUnits"
DEFAULT_GRAIN = "day"
DEFAULT_ANSWER_DETAIL_LEVEL = "standard"
DEFAULT_SETTINGS = {
    "defaultPeriodDays": DEFAULT_PERIOD_DAYS,
    "defaultRankingLimit": DEFAULT_RANKING_LIMIT,
    "defaultMetric": DEFAULT_METRIC,
    "defaultGrain": DEFAULT_GRAIN,
    "answerDetailLevel": DEFAULT_ANSWER_DETAIL_LEVEL,
    "prioritizeAdjustmentRisk": True,
    "showDataUpdatedAt": True,
    "showMetricDefinition": True,
    "customBusinessInstructions": "",
}

PERIOD_DAYS = {7, 30, 60, 90}
METRICS = {
    "effectiveUnits",
    "orderedUnits",
    "effectiveSalesAmount",
    "orderCount",
}
GRAINS = {"day", "week", "month"}
ANSWER_DETAIL_LEVELS = {"concise", "standard", "detailed"}

CAPABILITIES = [
    {
        "key": "storeOverview",
        "title": "店铺销量概览",
        "description": "查看指定周期内店铺的销量、订单和调整汇总。",
        "example": "最近 30 天店铺销量表现如何？",
        "metrics": ["有效销量", "下单件数", "预估有效销售额", "订单数"],
    },
    {
        "key": "productRanking",
        "title": "商品销量排行",
        "description": "按受支持指标查看商品排行。",
        "example": "列出最近 30 天销量前 10 的商品。",
        "metrics": ["有效销量", "下单件数", "预估有效销售额", "订单数"],
    },
    {
        "key": "salesTrend",
        "title": "商品销量趋势",
        "description": "按日、周或月查看销量变化。",
        "example": "查看这个商品最近 60 天的周销量趋势。",
        "metrics": ["有效销量", "下单件数", "预估有效销售额"],
    },
    {
        "key": "productComparison",
        "title": "多商品销量对比",
        "description": "在同一店铺和统计周期内对比多个商品。",
        "example": "对比这三个商品最近 30 天的有效销量。",
        "metrics": ["有效销量", "预估有效销售额", "订单数"],
    },
    {
        "key": "skuBreakdown",
        "title": "SKU 销量明细和占比",
        "description": "查看商品下各 SKU 的销量明细和占比。",
        "example": "这个商品哪个 SKU 卖得最多？",
        "metrics": ["有效销量", "销量占比"],
    },
    {
        "key": "slowMovingProducts",
        "title": "滞销及零销量商品",
        "description": "识别指定周期内低销量或零销量商品。",
        "example": "找出最近 30 天没有销量的商品。",
        "metrics": ["有效销量", "最近订单时间"],
    },
    {
        "key": "adjustments",
        "title": "取消、退款、退货和未归属调整",
        "description": "查看影响有效销量的数据调整和未归属风险。",
        "example": "最近退款退货对销量影响多大？",
        "metrics": ["取消数量", "退款数量", "退货数量", "未归属调整"],
    },
    {
        "key": "dataSync",
        "title": "订单增量同步和数据更新时间",
        "description": "查看同步状态并按权限触发店铺订单增量同步。",
        "example": "当前销量数据更新到什么时候？",
        "metrics": ["同步状态", "数据更新时间"],
        "facts": [
            "首次同步默认覆盖最近 90 天。",
            "自动同步间隔约为 30 分钟。",
        ],
    },
    {
        "key": "aiConversation",
        "title": "AI 自然语言提问和历史会话",
        "description": "使用自然语言提问并保留当前用户的分析历史。",
        "example": "总结近期销量变化并列出主要风险。",
        "metrics": ["工具查询结果", "历史消息"],
    },
]

CONSTRAINTS = [
    {
        "key": "dataPermissions",
        "title": "数据权限",
        "items": [
            "只能同步和分析当前用户拥有的店铺。",
            "超级管理员不能自动分析其他用户店铺。",
            "一个会话绑定一个店铺，绑定后不可切换。",
        ],
    },
    {
        "key": "aiAndSecrets",
        "title": "AI 与密钥",
        "items": [
            "店铺密钥和 AI API Key 不发送到前端页面或模型上下文。",
            "AI 不得直接访问数据库或生成、执行任意 SQL。",
            "AI 只能调用预定义只读工具。",
        ],
    },
    {
        "key": "analysisScope",
        "title": "分析范围",
        "items": [
            "单次最多调用 4 次分析工具。",
            "工具参数、时间范围、商品数量和返回记录数有固定上限。",
            "用户偏好只能影响默认值和回答方式，不能扩大硬上限。",
        ],
    },
    {
        "key": "metricDefinitions",
        "title": "统计口径",
        "items": [
            "有效销量 = 下单数量 - 取消数量 - 已确认退款数量 - 已确认退货数量。",
            "无法归属商品的部分退款和退货不得猜测分摊。",
            "预估有效销售额不包含优惠券、折扣、退款分摊和税费分摊。",
        ],
    },
    {
        "key": "errorHandling",
        "title": "异常处理",
        "items": [
            "AI 服务异常或回答未通过事实校验时直接显示异常。",
            "不返回兜底答案，不把失败结果伪装成正常分析。",
        ],
    },
]


def _settings_to_public(
    row: UserSalesAnalysisSettingsModel,
) -> dict[str, Any]:
    instructions = str(row.custom_business_instructions or "").strip()
    return {
        "defaultPeriodDays": (
            row.default_period_days
            if row.default_period_days in PERIOD_DAYS
            else DEFAULT_PERIOD_DAYS
        ),
        "defaultRankingLimit": (
            row.default_ranking_limit
            if isinstance(row.default_ranking_limit, int)
            and not isinstance(row.default_ranking_limit, bool)
            and 5 <= row.default_ranking_limit <= 100
            else DEFAULT_RANKING_LIMIT
        ),
        "defaultMetric": (
            row.default_metric
            if row.default_metric in METRICS
            else DEFAULT_METRIC
        ),
        "defaultGrain": (
            row.default_grain
            if row.default_grain in GRAINS
            else DEFAULT_GRAIN
        ),
        "answerDetailLevel": (
            row.answer_detail_level
            if row.answer_detail_level in ANSWER_DETAIL_LEVELS
            else DEFAULT_ANSWER_DETAIL_LEVEL
        ),
        "prioritizeAdjustmentRisk": bool(
            row.prioritize_adjustment_risk
        ),
        "showDataUpdatedAt": bool(row.show_data_updated_at),
        "showMetricDefinition": bool(row.show_metric_definition),
        "customBusinessInstructions": (
            instructions if len(instructions) <= 4000 else ""
        ),
    }


def _ensure_settings(
    session: Any,
    owner_username: str,
) -> UserSalesAnalysisSettingsModel:
    row = session.get(UserSalesAnalysisSettingsModel, owner_username)
    if row is None:
        row = UserSalesAnalysisSettingsModel(owner_username=owner_username)
        session.add(row)
        session.flush()
    return row


def _enum_value(payload: Any, field: str, allowed: set[str]) -> str:
    value = str(getattr(payload, field, "") or "").strip()
    normalized = value.lower()
    allowed_by_lower = {item.lower(): item for item in allowed}
    if normalized not in allowed_by_lower:
        raise ValueError(f"{field} 的值不受支持。")
    return allowed_by_lower[normalized]


def _boolean_value(payload: Any, field: str) -> bool:
    value = getattr(payload, field)
    if isinstance(value, bool):
        return value
    if value in (0, 1):
        return bool(value)
    raise ValueError(f"{field} 必须是布尔值。")


def get_settings(owner_username: str) -> dict[str, Any]:
    with session_scope() as session:
        row = session.get(UserSalesAnalysisSettingsModel, owner_username)
        if row is None:
            return dict(DEFAULT_SETTINGS)
        return _settings_to_public(row)


def update_settings(
    owner_username: str,
    payload: Any,
) -> dict[str, Any]:
    period_days = int(payload.defaultPeriodDays)
    if period_days not in PERIOD_DAYS:
        raise ValueError("默认分析周期不受支持。")
    ranking_limit = int(payload.defaultRankingLimit)
    if ranking_limit < 5 or ranking_limit > 100:
        raise ValueError("默认排行数量必须在 5 至 100 之间。")
    instructions = str(
        payload.customBusinessInstructions or ""
    ).strip()
    if len(instructions) > 4000:
        raise ValueError("自定义业务要求不能超过 4000 字。")

    with session_scope() as session:
        row = _ensure_settings(session, owner_username)
        row.default_period_days = period_days
        row.default_ranking_limit = ranking_limit
        row.default_metric = _enum_value(
            payload,
            "defaultMetric",
            METRICS,
        )
        row.default_grain = _enum_value(payload, "defaultGrain", GRAINS)
        row.answer_detail_level = _enum_value(
            payload,
            "answerDetailLevel",
            ANSWER_DETAIL_LEVELS,
        )
        row.prioritize_adjustment_risk = _boolean_value(
            payload,
            "prioritizeAdjustmentRisk",
        )
        row.show_data_updated_at = _boolean_value(
            payload,
            "showDataUpdatedAt",
        )
        row.show_metric_definition = _boolean_value(
            payload,
            "showMetricDefinition",
        )
        row.custom_business_instructions = instructions
        session.flush()
        return _settings_to_public(row)


def capability_catalog() -> list[dict[str, Any]]:
    return deepcopy(CAPABILITIES)


def constraint_catalog() -> list[dict[str, Any]]:
    return deepcopy(CONSTRAINTS)
