from __future__ import annotations

from collections.abc import Callable
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from typing import Annotated, Any, Literal, cast

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    StringConstraints,
    field_validator,
    model_validator,
)
from sqlalchemy import (
    Integer,
    and_,
    cast as sql_cast,
    exists,
    func,
    or_,
    select,
)
from sqlalchemy.orm import Session

from app.db.database import session_scope
from app.db.models import (
    ProductModel,
    ProductSalesDailyModel,
    SalesItemAdjustmentModel,
    SalesOrderItemModel,
    SalesOrderModel,
    SalesSyncStateModel,
    StoreModel,
)
from app.services import sales_sync_service
from app.services.sales_time import (
    as_sales_datetime,
    iso_sales_datetime,
)


MAX_RANGE_DAYS = 366
MAX_RANKING_LIMIT = 100
MAX_COMPARISON_PRODUCTS = 20
MAX_RANKING_FACT_ROWS = 50_000
EFFECTIVE_SALES_AMOUNT_DEFINITION = (
    "估算商品金额：按商品行单价 × 有效销量计算；"
    "未分摊优惠券、折扣、退款金额或税额，"
    "仅反映数量扣减后的商品行估算，不代表权威净收入。"
)

ManageNumber = Annotated[
    str,
    StringConstraints(
        strict=True,
        strip_whitespace=True,
        min_length=1,
        max_length=255,
    ),
]
Grain = Literal["day", "week", "month"]
RankingMetric = Literal[
    "effectiveUnits",
    "effectiveSalesAmount",
    "orderedUnits",
    "grossSalesAmount",
    "orderCount",
]


def _parse_iso_date(value: Any) -> date:
    if not isinstance(value, str):
        raise ValueError("日期必须是 YYYY-MM-DD 格式的字符串。")
    if len(value) != 10 or value[4] != "-" or value[7] != "-":
        raise ValueError("日期必须是 YYYY-MM-DD 格式的字符串。")
    try:
        parsed = date.fromisoformat(value)
    except ValueError as exc:
        raise ValueError("日期必须是有效的 YYYY-MM-DD。") from exc
    if parsed.isoformat() != value:
        raise ValueError("日期必须是标准 YYYY-MM-DD 格式。")
    return parsed


class StrictToolArguments(BaseModel):
    model_config = ConfigDict(extra="forbid", populate_by_name=False)


class DateRangeArguments(StrictToolArguments):
    start_date: date = Field(alias="startDate")
    end_date: date = Field(alias="endDate")

    @field_validator("start_date", "end_date", mode="before")
    @classmethod
    def validate_iso_date(cls, value: Any) -> date:
        return _parse_iso_date(value)

    @model_validator(mode="after")
    def validate_date_range(self) -> DateRangeArguments:
        if self.start_date > self.end_date:
            raise ValueError("startDate 不能晚于 endDate。")
        if self.end_date == date.max:
            raise ValueError("endDate 超出可查询范围。")
        inclusive_days = (self.end_date - self.start_date).days + 1
        if inclusive_days > MAX_RANGE_DAYS:
            raise ValueError(f"时间范围不能超过 {MAX_RANGE_DAYS} 天。")
        return self


class StoreDateRangeArguments(DateRangeArguments):
    store_id: int = Field(alias="storeId", strict=True, gt=0)


class ListOwnedStoresArguments(StrictToolArguments):
    pass


class StoreSalesOverviewArguments(StoreDateRangeArguments):
    compare_start_date: date | None = Field(default=None, alias="compareStartDate")
    compare_end_date: date | None = Field(default=None, alias="compareEndDate")

    @field_validator("compare_start_date", "compare_end_date", mode="before")
    @classmethod
    def validate_optional_iso_date(cls, value: Any) -> date | None:
        return None if value is None else _parse_iso_date(value)

    @model_validator(mode="after")
    def validate_comparison_range(self) -> StoreSalesOverviewArguments:
        if (self.compare_start_date is None) != (self.compare_end_date is None):
            raise ValueError(
                "compareStartDate 和 compareEndDate 必须同时提供。"
            )
        if self.compare_start_date is not None and self.compare_end_date is not None:
            if self.compare_start_date > self.compare_end_date:
                raise ValueError(
                    "compareStartDate 不能晚于 compareEndDate。"
                )
            if self.compare_end_date == date.max:
                raise ValueError("compareEndDate 超出可查询范围。")
            inclusive_days = (
                self.compare_end_date - self.compare_start_date
            ).days + 1
            if inclusive_days > MAX_RANGE_DAYS:
                raise ValueError(
                    f"对比时间范围不能超过 {MAX_RANGE_DAYS} 天。"
                )
        return self


class ProductSalesRankingArguments(StoreDateRangeArguments):
    metric: RankingMetric = "effectiveUnits"
    limit: int = Field(default=10, strict=True, ge=1, le=MAX_RANKING_LIMIT)
    include_sku: bool = Field(default=False, alias="includeSku", strict=True)


class ProductSalesTrendArguments(StoreDateRangeArguments):
    manage_number: ManageNumber = Field(alias="manageNumber")
    grain: Grain = "day"


class CompareProductSalesArguments(StoreDateRangeArguments):
    manage_numbers: list[ManageNumber] = Field(
        alias="manageNumbers",
        min_length=2,
        max_length=MAX_COMPARISON_PRODUCTS,
    )
    grain: Grain = "day"

    @field_validator("manage_numbers")
    @classmethod
    def validate_unique_products(cls, value: list[str]) -> list[str]:
        if len(set(value)) != len(value):
            raise ValueError("manageNumbers 不能包含重复商品。")
        return value


class SkuSalesBreakdownArguments(StoreDateRangeArguments):
    manage_number: ManageNumber = Field(alias="manageNumber")
    limit: int = Field(
        default=MAX_RANKING_LIMIT,
        strict=True,
        ge=1,
        le=MAX_RANKING_LIMIT,
    )


class SlowMovingProductsArguments(StoreDateRangeArguments):
    min_listed_days: int = Field(
        default=30,
        alias="minListedDays",
        strict=True,
        ge=1,
        le=3650,
    )
    max_effective_units: int = Field(
        default=0,
        alias="maxEffectiveUnits",
        strict=True,
        ge=0,
        le=1_000_000,
    )
    limit: int = Field(default=20, strict=True, ge=1, le=MAX_RANKING_LIMIT)

    @model_validator(mode="after")
    def validate_listed_cutoff(self) -> SlowMovingProductsArguments:
        if self.end_date.toordinal() < self.min_listed_days:
            raise ValueError("endDate 与 minListedDays 超出可查询范围。")
        return self


class SalesAdjustmentSummaryArguments(StoreDateRangeArguments):
    pass


ArgumentModel = type[StrictToolArguments]
ToolHandler = Callable[[Session, str, StrictToolArguments], dict[str, Any]]


def _tool_definition(
    name: str,
    description: str,
    argument_model: ArgumentModel,
) -> dict[str, Any]:
    return {
        "type": "function",
        "function": {
            "name": name,
            "description": description,
            "parameters": argument_model.model_json_schema(by_alias=True),
        },
    }


_TOOL_MODELS: dict[str, ArgumentModel] = {
    "list_owned_stores": ListOwnedStoresArguments,
    "get_store_sales_overview": StoreSalesOverviewArguments,
    "get_product_sales_ranking": ProductSalesRankingArguments,
    "get_product_sales_trend": ProductSalesTrendArguments,
    "compare_product_sales": CompareProductSalesArguments,
    "get_sku_sales_breakdown": SkuSalesBreakdownArguments,
    "get_slow_moving_products": SlowMovingProductsArguments,
    "get_sales_adjustment_summary": SalesAdjustmentSummaryArguments,
}

SALES_ANALYSIS_TOOLS = [
    _tool_definition(
        "list_owned_stores",
        "列出当前用户拥有且可用于销量分析的店铺。",
        ListOwnedStoresArguments,
    ),
    _tool_definition(
        "get_store_sales_overview",
        "返回一个自有店铺在具体日期范围内的销量、估算商品金额和调整概览。",
        StoreSalesOverviewArguments,
    ),
    _tool_definition(
        "get_product_sales_ranking",
        "按受支持指标返回一个自有店铺的商品或 SKU 排行。",
        ProductSalesRankingArguments,
    ),
    _tool_definition(
        "get_product_sales_trend",
        "返回一个商品按日、周或月聚合的有效销量和估算商品金额趋势。",
        ProductSalesTrendArguments,
    ),
    _tool_definition(
        "compare_product_sales",
        "比较 2 至 20 个商品的销量、估算商品金额、调整率和趋势。",
        CompareProductSalesArguments,
    ),
    _tool_definition(
        "get_sku_sales_breakdown",
        "返回一个商品在具体日期范围内的 SKU 销量、估算商品金额和占比。",
        SkuSalesBreakdownArguments,
    ),
    _tool_definition(
        "get_slow_moving_products",
        "返回达到最低上架天数且有效销量不高于阈值的商品。",
        SlowMovingProductsArguments,
    ),
    _tool_definition(
        "get_sales_adjustment_summary",
        "返回取消、退款、退货和未决调整的受控汇总。",
        SalesAdjustmentSummaryArguments,
    ),
]


def _range_datetimes(start_date: date, end_date: date) -> tuple[datetime, datetime]:
    return (
        datetime.combine(start_date, time.min),
        datetime.combine(end_date + timedelta(days=1), time.min),
    )


def _decimal_number(value: Decimal | int | float | None) -> float:
    return float(value or 0)


def _integer(value: Any) -> int:
    return int(value or 0)


def _latest_datetime(*values: datetime | None) -> datetime | None:
    present = [value for value in values if value is not None]
    return (
        max(present, key=lambda value: _as_shanghai_datetime(value).timestamp())
        if present
        else None
    )


def _as_shanghai_datetime(value: datetime) -> datetime:
    return as_sales_datetime(value)


def _iso_updated_at(value: datetime | None) -> str | None:
    return iso_sales_datetime(value)


def _require_owned_store(
    session: Session,
    owner_username: str,
    store_id: int,
) -> StoreModel:
    store = session.scalar(
        select(StoreModel).where(
            StoreModel.id == store_id,
            StoreModel.owner_username == owner_username,
        )
    )
    if store is None:
        raise LookupError("店铺不存在或无权访问。")
    return store


def _daily_filters(
    owner_username: str,
    store_id: int,
    start_date: date,
    end_date: date,
) -> tuple[Any, ...]:
    return (
        ProductSalesDailyModel.owner_username == owner_username,
        ProductSalesDailyModel.store_id == store_id,
        ProductSalesDailyModel.sales_date >= start_date,
        ProductSalesDailyModel.sales_date <= end_date,
    )


def _item_adjustment_filters(
    owner_username: str,
    store_id: int,
    start_date: date,
    end_date: date,
) -> tuple[Any, ...]:
    start_at, end_at = _range_datetimes(start_date, end_date)
    return (
        SalesItemAdjustmentModel.owner_username == owner_username,
        SalesItemAdjustmentModel.store_id == store_id,
        SalesOrderItemModel.owner_username == owner_username,
        SalesOrderItemModel.store_id == store_id,
        SalesOrderItemModel.ordered_at >= start_at,
        SalesOrderItemModel.ordered_at < end_at,
    )


def _order_item_filters(
    owner_username: str,
    store_id: int,
    start_date: date,
    end_date: date,
) -> tuple[Any, ...]:
    start_at, end_at = _range_datetimes(start_date, end_date)
    return (
        SalesOrderItemModel.owner_username == owner_username,
        SalesOrderItemModel.store_id == store_id,
        SalesOrderItemModel.ordered_at >= start_at,
        SalesOrderItemModel.ordered_at < end_at,
    )


def _data_updated_at(
    session: Session,
    owner_username: str,
    store_id: int,
    start_date: date,
    end_date: date,
) -> datetime | None:
    last_successful_sync_at = session.scalar(
        select(func.max(SalesSyncStateModel.last_successful_sync_at)).where(
            SalesSyncStateModel.owner_username == owner_username,
            SalesSyncStateModel.store_id == store_id,
        )
    )
    if last_successful_sync_at is not None:
        return last_successful_sync_at

    start_at, end_at = _range_datetimes(start_date, end_date)
    daily_updated_at = session.scalar(
        select(func.max(ProductSalesDailyModel.updated_at)).where(
            *_daily_filters(
                owner_username,
                store_id,
                start_date,
                end_date,
            )
        )
    )
    order_values = session.execute(
        select(
            func.max(SalesOrderModel.updated_at),
            func.max(SalesOrderModel.last_synced_at),
        ).where(
            SalesOrderModel.owner_username == owner_username,
            SalesOrderModel.store_id == store_id,
            SalesOrderModel.ordered_at >= start_at,
            SalesOrderModel.ordered_at < end_at,
        )
    ).one()
    item_updated_at = session.scalar(
        select(func.max(SalesOrderItemModel.updated_at)).where(
            *_order_item_filters(
                owner_username,
                store_id,
                start_date,
                end_date,
            )
        )
    )
    adjustment_values = session.execute(
        select(func.max(SalesItemAdjustmentModel.updated_at))
        .join(
            SalesOrderItemModel,
            SalesOrderItemModel.id
            == SalesItemAdjustmentModel.sales_order_item_id,
        )
        .where(
            *_item_adjustment_filters(
                owner_username,
                store_id,
                start_date,
                end_date,
            )
        )
    ).one()
    product_updated_at = session.scalar(
        select(func.max(ProductModel.updated_at)).where(
            ProductModel.owner_username == owner_username,
            ProductModel.store_id == store_id,
        )
    )
    store_values = session.execute(
        select(
            func.max(StoreModel.updated_at),
            func.max(StoreModel.last_checked_at),
            func.max(StoreModel.last_product_synced_at),
            func.max(StoreModel.last_synced_at),
        ).where(
            StoreModel.owner_username == owner_username,
            StoreModel.id == store_id,
        )
    ).one()
    sync_values = session.execute(
        select(func.max(SalesSyncStateModel.updated_at)).where(
            SalesSyncStateModel.owner_username == owner_username,
            SalesSyncStateModel.store_id == store_id,
        )
    ).one()
    return _latest_datetime(
        daily_updated_at,
        *order_values,
        item_updated_at,
        *adjustment_values,
        product_updated_at,
        *store_values,
        *sync_values,
    )


def _owner_data_updated_at(
    session: Session,
    owner_username: str,
) -> datetime | None:
    last_successful_sync_at = session.scalar(
        select(func.max(SalesSyncStateModel.last_successful_sync_at)).where(
            SalesSyncStateModel.owner_username == owner_username,
            SalesSyncStateModel.last_successful_sync_at.is_not(None),
        )
    )
    if last_successful_sync_at is not None:
        return last_successful_sync_at

    store_values = session.execute(
        select(
            func.max(StoreModel.updated_at),
            func.max(StoreModel.last_checked_at),
            func.max(StoreModel.last_product_synced_at),
            func.max(StoreModel.last_synced_at),
        ).where(StoreModel.owner_username == owner_username)
    ).one()
    sync_values = session.execute(
        select(func.max(SalesSyncStateModel.updated_at)).where(
            SalesSyncStateModel.owner_username == owner_username
        )
    ).one()
    order_values = session.execute(
        select(
            func.max(SalesOrderModel.updated_at),
            func.max(SalesOrderModel.last_synced_at),
        ).where(SalesOrderModel.owner_username == owner_username)
    ).one()
    item_updated_at = session.scalar(
        select(func.max(SalesOrderItemModel.updated_at)).where(
            SalesOrderItemModel.owner_username == owner_username
        )
    )
    daily_updated_at = session.scalar(
        select(func.max(ProductSalesDailyModel.updated_at)).where(
            ProductSalesDailyModel.owner_username == owner_username
        )
    )
    adjustment_values = session.execute(
        select(func.max(SalesItemAdjustmentModel.updated_at)).where(
            SalesItemAdjustmentModel.owner_username == owner_username
        )
    ).one()
    product_updated_at = session.scalar(
        select(func.max(ProductModel.updated_at)).where(
            ProductModel.owner_username == owner_username
        )
    )
    return _latest_datetime(
        *store_values,
        *sync_values,
        *order_values,
        item_updated_at,
        daily_updated_at,
        *adjustment_values,
        product_updated_at,
    )


def _unresolved_adjustment_exists(
    owner_username: str,
    store_id: int,
) -> Any:
    return exists(
        select(SalesItemAdjustmentModel.id)
        .join(
            SalesOrderItemModel,
            SalesOrderItemModel.id
            == SalesItemAdjustmentModel.sales_order_item_id,
        )
        .where(
            SalesOrderItemModel.sales_order_id == SalesOrderModel.id,
            SalesOrderItemModel.owner_username == owner_username,
            SalesOrderItemModel.store_id == store_id,
            SalesItemAdjustmentModel.owner_username == owner_username,
            SalesItemAdjustmentModel.store_id == store_id,
            SalesItemAdjustmentModel.status == "unresolved",
        )
    )


def _unresolved_order_filters(
    owner_username: str,
    store_id: int,
    start_date: date,
    end_date: date,
) -> tuple[Any, ...]:
    start_at, end_at = _range_datetimes(start_date, end_date)
    return (
        SalesOrderModel.owner_username == owner_username,
        SalesOrderModel.store_id == store_id,
        SalesOrderModel.ordered_at >= start_at,
        SalesOrderModel.ordered_at < end_at,
        or_(
            SalesOrderModel.has_unresolved_adjustment.is_(True),
            _unresolved_adjustment_exists(owner_username, store_id),
        ),
    )


def _unresolved_adjustment_count(
    session: Session,
    owner_username: str,
    store_id: int,
    start_date: date,
    end_date: date,
) -> int:
    return _integer(
        session.scalar(
            select(func.count(SalesOrderModel.id))
            .where(
                *_unresolved_order_filters(
                    owner_username,
                    store_id,
                    start_date,
                    end_date,
                )
            )
        )
    )


def _initial_sync_completed(
    session: Session,
    owner_username: str,
    store_id: int,
) -> bool:
    return bool(
        session.scalar(
            select(
                SalesSyncStateModel.initial_sync_completed
            ).where(
                SalesSyncStateModel.owner_username
                == owner_username,
                SalesSyncStateModel.store_id == store_id,
            )
        )
    )


def _result_metadata(
    session: Session,
    *,
    owner_username: str,
    store: StoreModel,
    start_date: date,
    end_date: date,
    metric: str,
) -> dict[str, Any]:
    unresolved_count = _unresolved_adjustment_count(
        session,
        owner_username,
        store.id,
        start_date,
        end_date,
    )
    initial_sync_completed = _initial_sync_completed(
        session,
        owner_username,
        store.id,
    )
    return {
        "store": {"id": store.id, "name": store.store_name},
        "range": {
            "start": start_date.isoformat(),
            "end": end_date.isoformat(),
        },
        "metric": metric,
        "dataUpdatedAt": _iso_updated_at(
            _data_updated_at(
                session,
                owner_username,
                store.id,
                start_date,
                end_date,
            )
        ),
        "unresolvedAdjustmentCount": unresolved_count,
        "initialSyncCompleted": initial_sync_completed,
        "dataIncomplete": not initial_sync_completed,
        "effectiveSalesAmountDefinition": (
            EFFECTIVE_SALES_AMOUNT_DEFINITION
        ),
    }


def _sales_totals(
    session: Session,
    owner_username: str,
    store_id: int,
    start_date: date,
    end_date: date,
) -> dict[str, int | float]:
    row = session.execute(
        select(
            func.coalesce(func.sum(ProductSalesDailyModel.ordered_units), 0),
            func.coalesce(func.sum(ProductSalesDailyModel.effective_units), 0),
            func.coalesce(
                func.sum(ProductSalesDailyModel.gross_sales_amount),
                0,
            ),
            func.coalesce(
                func.sum(ProductSalesDailyModel.effective_sales_amount),
                0,
            ),
            func.coalesce(func.sum(ProductSalesDailyModel.canceled_units), 0),
            func.coalesce(func.sum(ProductSalesDailyModel.refunded_units), 0),
            func.coalesce(func.sum(ProductSalesDailyModel.returned_units), 0),
        ).where(
            *_daily_filters(
                owner_username,
                store_id,
                start_date,
                end_date,
            )
        )
    ).one()
    start_at, end_at = _range_datetimes(start_date, end_date)
    order_count = _integer(
        session.scalar(
            select(func.count(SalesOrderModel.id)).where(
                SalesOrderModel.owner_username == owner_username,
                SalesOrderModel.store_id == store_id,
                SalesOrderModel.ordered_at >= start_at,
                SalesOrderModel.ordered_at < end_at,
            )
        )
    )
    return {
        "orderCount": order_count,
        "orderedUnits": _integer(row[0]),
        "effectiveUnits": _integer(row[1]),
        "grossSalesAmount": _decimal_number(row[2]),
        "effectiveSalesAmount": _decimal_number(row[3]),
        "canceledUnits": _integer(row[4]),
        "refundedUnits": _integer(row[5]),
        "returnedUnits": _integer(row[6]),
    }


def _change_rate(current: int | float, previous: int | float) -> float | None:
    if previous == 0:
        return 0.0 if current == 0 else None
    return (float(current) - float(previous)) / abs(float(previous))


def _list_owned_stores(
    session: Session,
    owner_username: str,
    _: StrictToolArguments,
) -> dict[str, Any]:
    store_rows = session.execute(
        select(
            StoreModel,
            SalesSyncStateModel.initial_sync_completed,
        )
        .outerjoin(
            SalesSyncStateModel,
            and_(
                SalesSyncStateModel.store_id == StoreModel.id,
                SalesSyncStateModel.owner_username
                == StoreModel.owner_username,
            ),
        )
        .where(StoreModel.owner_username == owner_username)
        .order_by(StoreModel.id.asc())
    ).all()
    completion_flags = [
        bool(initial_sync_completed)
        for _store, initial_sync_completed in store_rows
    ]
    all_completed = bool(completion_flags) and all(
        completion_flags
    )
    return {
        "dataUpdatedAt": _iso_updated_at(
            _owner_data_updated_at(session, owner_username)
        ),
        "initialSyncCompleted": all_completed,
        "dataIncomplete": bool(completion_flags) and not all_completed,
        "effectiveSalesAmountDefinition": (
            EFFECTIVE_SALES_AMOUNT_DEFINITION
        ),
        "rows": [
            {
                "id": store.id,
                "name": store.store_name,
                "code": store.store_code,
                "enabled": bool(store.enabled),
                "initialSyncCompleted": bool(
                    initial_sync_completed
                ),
                "dataIncomplete": not bool(
                    initial_sync_completed
                ),
            }
            for store, initial_sync_completed in store_rows
        ],
    }


def _store_sales_overview(
    session: Session,
    owner_username: str,
    arguments: StrictToolArguments,
) -> dict[str, Any]:
    args = cast(StoreSalesOverviewArguments, arguments)
    store = _require_owned_store(session, owner_username, args.store_id)
    result = _result_metadata(
        session,
        owner_username=owner_username,
        store=store,
        start_date=args.start_date,
        end_date=args.end_date,
        metric="effectiveUnits",
    )
    current = _sales_totals(
        session,
        owner_username,
        store.id,
        args.start_date,
        args.end_date,
    )
    result["rows"] = [current]
    if args.compare_start_date is not None and args.compare_end_date is not None:
        previous = _sales_totals(
            session,
            owner_username,
            store.id,
            args.compare_start_date,
            args.compare_end_date,
        )
        result["comparison"] = {
            "range": {
                "start": args.compare_start_date.isoformat(),
                "end": args.compare_end_date.isoformat(),
            },
            **previous,
            "changes": {
                key: _change_rate(current[key], previous[key])
                for key in (
                    "orderCount",
                    "effectiveUnits",
                    "effectiveSalesAmount",
                )
            },
        }
    return result


def _ranking_daily_query_parts(
    owner_username: str,
    args: ProductSalesRankingArguments,
) -> tuple[Any, dict[str, Any]]:
    daily_group_columns = [ProductSalesDailyModel.manage_number]
    if args.include_sku:
        daily_group_columns.append(ProductSalesDailyModel.sku_key)
    ordered_units = func.coalesce(
        func.sum(ProductSalesDailyModel.ordered_units),
        0,
    ).label("ordered_units")
    effective_units = func.coalesce(
        func.sum(ProductSalesDailyModel.effective_units),
        0,
    ).label("effective_units")
    gross_sales_amount = func.coalesce(
        func.sum(ProductSalesDailyModel.gross_sales_amount),
        0,
    ).label("gross_sales_amount")
    effective_sales_amount = func.coalesce(
        func.sum(ProductSalesDailyModel.effective_sales_amount),
        0,
    ).label("effective_sales_amount")
    metric_expressions = {
        "orderedUnits": ordered_units,
        "effectiveUnits": effective_units,
        "grossSalesAmount": gross_sales_amount,
        "effectiveSalesAmount": effective_sales_amount,
    }
    selected_columns = [
        ProductSalesDailyModel.manage_number.label("manage_number"),
    ]
    if args.include_sku:
        selected_columns.append(
            ProductSalesDailyModel.sku_key.label("sku_key")
        )
    selected_columns.extend(
        [
            func.max(ProductSalesDailyModel.item_number).label(
                "item_number"
            ),
            func.max(ProductSalesDailyModel.item_name_snapshot).label(
                "item_name"
            ),
            ordered_units,
            effective_units,
            gross_sales_amount,
            effective_sales_amount,
        ]
    )
    return (
        select(*selected_columns)
        .where(
            *_daily_filters(
                owner_username,
                args.store_id,
                args.start_date,
                args.end_date,
            )
        )
        .group_by(*daily_group_columns),
        metric_expressions,
    )


def _ranking_fact_identity_filter(
    product_key: str,
    item_number: str,
) -> Any:
    blank_manage = (
        func.trim(SalesOrderItemModel.manage_number) == ""
    )
    if (
        item_number
        and product_key
        == sales_sync_service._bounded_fallback_product_key(
            "item-number",
            item_number,
        )
    ):
        return and_(
            blank_manage,
            SalesOrderItemModel.item_number == item_number,
        )
    if product_key.startswith("item-id:"):
        return and_(
            blank_manage,
            func.trim(SalesOrderItemModel.item_number) == "",
            SalesOrderItemModel.item_id
            == product_key.removeprefix("item-id:"),
        )
    if product_key.startswith("item-detail:"):
        return and_(
            blank_manage,
            func.trim(SalesOrderItemModel.item_number) == "",
            func.trim(SalesOrderItemModel.item_id) == "",
            SalesOrderItemModel.item_detail_id
            == product_key.removeprefix("item-detail:"),
        )
    if product_key.startswith(
        ("v1:item-id:", "v1:item-detail:")
    ):
        return and_(
            blank_manage,
            func.trim(SalesOrderItemModel.item_number) == "",
        )
    return SalesOrderItemModel.manage_number == product_key


def _bounded_ranking_fact_rows(
    session: Session,
    owner_username: str,
    args: ProductSalesRankingArguments,
    candidate_rows: list[dict[str, Any]] | None = None,
) -> list[Any]:
    query = select(
        SalesOrderItemModel.manage_number,
        SalesOrderItemModel.item_number,
        SalesOrderItemModel.item_id,
        SalesOrderItemModel.item_detail_id,
        SalesOrderItemModel.sku_key,
        SalesOrderItemModel.order_number,
    ).where(
        *_order_item_filters(
            owner_username,
            args.store_id,
            args.start_date,
            args.end_date,
        )
    )
    if candidate_rows is not None:
        if not candidate_rows:
            return []
        candidate_filters = []
        for candidate in candidate_rows:
            identity_filter = _ranking_fact_identity_filter(
                str(candidate["manage_number"] or ""),
                str(candidate["item_number"] or ""),
            )
            if args.include_sku:
                identity_filter = and_(
                    identity_filter,
                    SalesOrderItemModel.sku_key
                    == str(candidate["sku_key"] or ""),
                )
            candidate_filters.append(identity_filter)
        query = query.where(or_(*candidate_filters))
    rows = session.execute(
        query.distinct().limit(MAX_RANKING_FACT_ROWS + 1)
    ).all()
    if len(rows) > MAX_RANKING_FACT_ROWS:
        raise ValueError(
            "订单数排行数据量过大，请缩小日期范围后重试。"
        )
    return rows


def _ranking_fact_order_counts(
    fact_rows: list[Any],
    *,
    include_sku: bool,
) -> dict[tuple[str, str | None], int]:
    order_numbers: dict[tuple[str, str | None], set[str]] = {}
    for fact_row in fact_rows:
        product_key = sales_sync_service._daily_product_key(
            fact_row
        )
        count_key = (
            product_key,
            fact_row.sku_key if include_sku else None,
        )
        order_numbers.setdefault(count_key, set()).add(
            fact_row.order_number
        )
    return {
        key: len(values)
        for key, values in order_numbers.items()
    }


def _ranking_daily_candidate_filter(
    candidate_keys: list[tuple[str, str | None]],
    *,
    include_sku: bool,
) -> Any:
    if include_sku:
        return or_(
            *(
                and_(
                    ProductSalesDailyModel.manage_number
                    == manage_number,
                    ProductSalesDailyModel.sku_key == str(sku_key or ""),
                )
                for manage_number, sku_key in candidate_keys
            )
        )
    return ProductSalesDailyModel.manage_number.in_(
        [manage_number for manage_number, _sku_key in candidate_keys]
    )


def _ranking_public_rows(
    daily_rows: list[dict[str, Any]],
    fact_order_counts: dict[tuple[str, str | None], int],
    args: ProductSalesRankingArguments,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for raw in daily_rows:
        count_key = (
            raw["manage_number"],
            raw["sku_key"] if args.include_sku else None,
        )
        row = {
            "manageNumber": raw["manage_number"],
            "itemNumber": raw["item_number"],
            "itemName": raw["item_name"],
            "orderCount": fact_order_counts.get(count_key, 0),
            "orderedUnits": _integer(raw["ordered_units"]),
            "effectiveUnits": _integer(raw["effective_units"]),
            "grossSalesAmount": _decimal_number(
                raw["gross_sales_amount"]
            ),
            "effectiveSalesAmount": _decimal_number(
                raw["effective_sales_amount"]
            ),
        }
        if args.include_sku:
            row["skuKey"] = raw["sku_key"]
        row["metricValue"] = row[args.metric]
        rows.append(row)
    return rows


def _ranking_rows(
    session: Session,
    owner_username: str,
    args: ProductSalesRankingArguments,
) -> list[dict[str, Any]]:
    daily_query, metric_expressions = _ranking_daily_query_parts(
        owner_username,
        args
    )
    if args.metric != "orderCount":
        daily_rows = list(
            session.execute(
                daily_query.order_by(
                    metric_expressions[args.metric].desc(),
                    ProductSalesDailyModel.manage_number.asc(),
                    *(
                        (ProductSalesDailyModel.sku_key.asc(),)
                        if args.include_sku
                        else ()
                    ),
                ).limit(args.limit)
            ).mappings()
        )
        fact_order_counts = _ranking_fact_order_counts(
            _bounded_ranking_fact_rows(
                session,
                owner_username,
                args,
                daily_rows,
            ),
            include_sku=args.include_sku,
        )
        return _ranking_public_rows(
            daily_rows,
            fact_order_counts,
            args,
        )

    fact_order_counts = _ranking_fact_order_counts(
        _bounded_ranking_fact_rows(
            session,
            owner_username,
            args,
        ),
        include_sku=args.include_sku,
    )
    candidate_keys = sorted(
        fact_order_counts,
        key=lambda key: (
            -fact_order_counts[key],
            key[0],
            str(key[1] or ""),
        ),
    )[: args.limit]
    if not candidate_keys:
        return []
    daily_rows = list(
        session.execute(
            daily_query.where(
                _ranking_daily_candidate_filter(
                    candidate_keys,
                    include_sku=args.include_sku,
                )
            )
            .order_by(
                ProductSalesDailyModel.manage_number.asc(),
                *(
                    (ProductSalesDailyModel.sku_key.asc(),)
                    if args.include_sku
                    else ()
                ),
            )
            .limit(len(candidate_keys))
        ).mappings()
    )
    rows = _ranking_public_rows(
        daily_rows,
        fact_order_counts,
        args,
    )
    if args.metric == "orderCount":
        rows.sort(
            key=lambda row: (
                -row["orderCount"],
                row["manageNumber"],
                row.get("skuKey", ""),
            )
        )
    return rows


def _product_sales_ranking(
    session: Session,
    owner_username: str,
    arguments: StrictToolArguments,
) -> dict[str, Any]:
    args = cast(ProductSalesRankingArguments, arguments)
    store = _require_owned_store(session, owner_username, args.store_id)
    result = _result_metadata(
        session,
        owner_username=owner_username,
        store=store,
        start_date=args.start_date,
        end_date=args.end_date,
        metric=args.metric,
    )
    result["rows"] = _ranking_rows(session, owner_username, args)
    return result


def _period_key(value: date, grain: Grain) -> str:
    if grain == "day":
        return value.isoformat()
    if grain == "month":
        return value.strftime("%Y-%m")
    monday = value - timedelta(days=value.weekday())
    return monday.isoformat()


def _period_group_expression(session: Session, grain: Grain) -> Any:
    sales_date = ProductSalesDailyModel.sales_date
    if grain == "day":
        return sales_date

    dialect_name = session.get_bind().dialect.name
    if grain == "month":
        if dialect_name == "mysql":
            return func.date_format(sales_date, "%Y-%m")
        if dialect_name == "sqlite":
            return func.strftime("%Y-%m", sales_date)
        return func.date_trunc("month", sales_date)

    if dialect_name == "mysql":
        return func.yearweek(sales_date, 3)
    if dialect_name == "sqlite":
        weekday = sql_cast(func.strftime("%w", sales_date), Integer)
        return func.date(
            sales_date,
            func.printf("-%d days", (weekday + 6) % 7),
        )
    return func.date_trunc("week", sales_date)


def _date_value(value: Any) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value)[:10])


def _grouped_trend_rows(
    session: Session,
    *,
    owner_username: str,
    store_id: int,
    start_date: date,
    end_date: date,
    manage_numbers: list[str],
    grain: Grain,
) -> list[dict[str, Any]]:
    period_expression = _period_group_expression(session, grain)
    raw_rows = session.execute(
        select(
            ProductSalesDailyModel.manage_number.label("manage_number"),
            func.min(ProductSalesDailyModel.sales_date).label(
                "sample_date"
            ),
            func.coalesce(
                func.sum(ProductSalesDailyModel.ordered_units),
                0,
            ).label("ordered_units"),
            func.coalesce(
                func.sum(ProductSalesDailyModel.effective_units),
                0,
            ).label("effective_units"),
            func.coalesce(
                func.sum(ProductSalesDailyModel.effective_sales_amount),
                0,
            ).label("effective_sales_amount"),
        )
        .where(
            *_daily_filters(
                owner_username,
                store_id,
                start_date,
                end_date,
            ),
            ProductSalesDailyModel.manage_number.in_(manage_numbers),
        )
        .group_by(
            ProductSalesDailyModel.manage_number,
            period_expression,
        )
        .order_by(
            ProductSalesDailyModel.manage_number.asc(),
            period_expression.asc(),
        )
    ).mappings()
    return [
        {
            "manageNumber": raw["manage_number"],
            "period": _period_key(
                _date_value(raw["sample_date"]),
                grain,
            ),
            "orderedUnits": _integer(raw["ordered_units"]),
            "effectiveUnits": _integer(raw["effective_units"]),
            "effectiveSalesAmount": _decimal_number(
                raw["effective_sales_amount"]
            ),
        }
        for raw in raw_rows
    ]


def _product_sales_trend(
    session: Session,
    owner_username: str,
    arguments: StrictToolArguments,
) -> dict[str, Any]:
    args = cast(ProductSalesTrendArguments, arguments)
    store = _require_owned_store(session, owner_username, args.store_id)
    grouped_rows = _grouped_trend_rows(
        session,
        owner_username=owner_username,
        store_id=store.id,
        start_date=args.start_date,
        end_date=args.end_date,
        manage_numbers=[args.manage_number],
        grain=args.grain,
    )
    result = _result_metadata(
        session,
        owner_username=owner_username,
        store=store,
        start_date=args.start_date,
        end_date=args.end_date,
        metric="effectiveUnits",
    )
    result["manageNumber"] = args.manage_number
    result["grain"] = args.grain
    result["rows"] = [
        {
            key: value
            for key, value in row.items()
            if key != "manageNumber"
        }
        for row in grouped_rows
    ]
    return result


def _compare_product_sales(
    session: Session,
    owner_username: str,
    arguments: StrictToolArguments,
) -> dict[str, Any]:
    args = cast(CompareProductSalesArguments, arguments)
    store = _require_owned_store(session, owner_username, args.store_id)
    summary_rows = session.execute(
        select(
            ProductSalesDailyModel.manage_number.label("manage_number"),
            func.max(ProductSalesDailyModel.item_name_snapshot).label(
                "item_name"
            ),
            func.coalesce(
                func.sum(ProductSalesDailyModel.ordered_units),
                0,
            ).label("ordered_units"),
            func.coalesce(
                func.sum(ProductSalesDailyModel.effective_units),
                0,
            ).label("effective_units"),
            func.coalesce(
                func.sum(ProductSalesDailyModel.effective_sales_amount),
                0,
            ).label("effective_sales_amount"),
            (
                func.coalesce(
                    func.sum(ProductSalesDailyModel.canceled_units),
                    0,
                )
                + func.coalesce(
                    func.sum(ProductSalesDailyModel.refunded_units),
                    0,
                )
                + func.coalesce(
                    func.sum(ProductSalesDailyModel.returned_units),
                    0,
                )
            ).label("adjusted_units"),
        )
        .where(
            *_daily_filters(
                owner_username,
                store.id,
                args.start_date,
                args.end_date,
            ),
            ProductSalesDailyModel.manage_number.in_(args.manage_numbers),
        )
        .group_by(ProductSalesDailyModel.manage_number)
    ).mappings()
    summary_by_product = {
        raw["manage_number"]: raw for raw in summary_rows
    }
    grouped_series = _grouped_trend_rows(
        session,
        owner_username=owner_username,
        store_id=store.id,
        start_date=args.start_date,
        end_date=args.end_date,
        manage_numbers=args.manage_numbers,
        grain=args.grain,
    )
    series_by_product: dict[str, list[dict[str, Any]]] = {
        manage_number: [] for manage_number in args.manage_numbers
    }
    for row in grouped_series:
        series_by_product[row["manageNumber"]].append(row)

    summaries: list[dict[str, Any]] = []
    series: list[dict[str, Any]] = []
    for manage_number in args.manage_numbers:
        raw = summary_by_product.get(manage_number)
        ordered_units = _integer(
            raw["ordered_units"] if raw is not None else 0
        )
        effective_units = _integer(
            raw["effective_units"] if raw is not None else 0
        )
        adjusted_units = _integer(
            raw["adjusted_units"] if raw is not None else 0
        )
        summaries.append(
            {
                "manageNumber": manage_number,
                "itemName": raw["item_name"] if raw is not None else "",
                "orderedUnits": ordered_units,
                "effectiveUnits": effective_units,
                "effectiveSalesAmount": _decimal_number(
                    raw["effective_sales_amount"]
                    if raw is not None
                    else 0
                ),
                "adjustmentRate": (
                    adjusted_units / ordered_units if ordered_units else 0.0
                ),
            }
        )
        series.extend(series_by_product[manage_number])
    result = _result_metadata(
        session,
        owner_username=owner_username,
        store=store,
        start_date=args.start_date,
        end_date=args.end_date,
        metric="effectiveUnits",
    )
    result["grain"] = args.grain
    result["rows"] = summaries
    result["series"] = series
    return result


def _sku_sales_breakdown(
    session: Session,
    owner_username: str,
    arguments: StrictToolArguments,
) -> dict[str, Any]:
    args = cast(SkuSalesBreakdownArguments, arguments)
    store = _require_owned_store(session, owner_username, args.store_id)
    filters = (
        *_daily_filters(
            owner_username,
            store.id,
            args.start_date,
            args.end_date,
        ),
        ProductSalesDailyModel.manage_number == args.manage_number,
    )
    totals = session.execute(
        select(
            func.coalesce(
                func.sum(ProductSalesDailyModel.effective_units),
                0,
            ),
            func.coalesce(
                func.sum(ProductSalesDailyModel.effective_sales_amount),
                0,
            ),
        ).where(*filters)
    ).one()
    effective_units = func.coalesce(
        func.sum(ProductSalesDailyModel.effective_units),
        0,
    ).label("effective_units")
    raw_rows = session.execute(
        select(
            ProductSalesDailyModel.sku_key,
            func.max(ProductSalesDailyModel.item_name_snapshot),
            func.coalesce(func.sum(ProductSalesDailyModel.ordered_units), 0),
            effective_units,
            func.coalesce(
                func.sum(ProductSalesDailyModel.effective_sales_amount),
                0,
            ),
        )
        .where(*filters)
        .group_by(ProductSalesDailyModel.sku_key)
        .order_by(
            effective_units.desc(),
            ProductSalesDailyModel.sku_key.asc(),
        )
        .limit(args.limit)
    ).all()
    total_units = _integer(totals[0])
    total_sales = _decimal_number(totals[1])
    rows = [
        {
            "skuKey": raw[0],
            "itemName": raw[1],
            "orderedUnits": _integer(raw[2]),
            "effectiveUnits": _integer(raw[3]),
            "effectiveSalesAmount": _decimal_number(raw[4]),
            "unitShare": (
                _integer(raw[3]) / total_units if total_units else 0.0
            ),
            "salesShare": (
                _decimal_number(raw[4]) / total_sales
                if total_sales
                else 0.0
            ),
        }
        for raw in raw_rows
    ]
    result = _result_metadata(
        session,
        owner_username=owner_username,
        store=store,
        start_date=args.start_date,
        end_date=args.end_date,
        metric="effectiveUnits",
    )
    result["manageNumber"] = args.manage_number
    result["rows"] = rows
    return result


def _slow_moving_products(
    session: Session,
    owner_username: str,
    arguments: StrictToolArguments,
) -> dict[str, Any]:
    args = cast(SlowMovingProductsArguments, arguments)
    store = _require_owned_store(session, owner_username, args.store_id)
    sales = (
        select(
            ProductSalesDailyModel.manage_number.label("manage_number"),
            func.coalesce(
                func.sum(ProductSalesDailyModel.effective_units),
                0,
            ).label("effective_units"),
            func.coalesce(
                func.sum(ProductSalesDailyModel.effective_sales_amount),
                0,
            ).label("effective_sales_amount"),
        )
        .where(
            *_daily_filters(
                owner_username,
                store.id,
                args.start_date,
                args.end_date,
            )
        )
        .group_by(ProductSalesDailyModel.manage_number)
        .subquery()
    )
    listed_cutoff = datetime.combine(
        args.end_date - timedelta(days=args.min_listed_days - 1),
        time.max,
    )
    raw_rows = session.execute(
        select(
            ProductModel.rakuten_manage_number,
            ProductModel.title,
            ProductModel.listed_at,
            func.coalesce(sales.c.effective_units, 0),
            func.coalesce(sales.c.effective_sales_amount, 0),
        )
        .outerjoin(
            sales,
            sales.c.manage_number == ProductModel.rakuten_manage_number,
        )
        .where(
            ProductModel.owner_username == owner_username,
            ProductModel.store_id == store.id,
            ProductModel.review_status == "listed",
            ProductModel.rakuten_listing_status != "unlisted",
            ProductModel.rakuten_manage_number.is_not(None),
            ProductModel.rakuten_manage_number != "",
            ProductModel.listed_at.is_not(None),
            ProductModel.listed_at <= listed_cutoff,
            func.coalesce(sales.c.effective_units, 0)
            <= args.max_effective_units,
        )
        .order_by(
            func.coalesce(sales.c.effective_units, 0).asc(),
            ProductModel.listed_at.asc(),
            ProductModel.rakuten_manage_number.asc(),
        )
        .limit(args.limit)
    ).all()
    rows = [
        {
            "manageNumber": raw[0],
            "itemName": raw[1],
            "listedAt": raw[2].date().isoformat(),
            "listedDays": (args.end_date - raw[2].date()).days + 1,
            "effectiveUnits": _integer(raw[3]),
            "effectiveSalesAmount": _decimal_number(raw[4]),
        }
        for raw in raw_rows
    ]
    result = _result_metadata(
        session,
        owner_username=owner_username,
        store=store,
        start_date=args.start_date,
        end_date=args.end_date,
        metric="effectiveUnits",
    )
    result["threshold"] = {
        "minListedDays": args.min_listed_days,
        "maxEffectiveUnits": args.max_effective_units,
    }
    result["rows"] = rows
    return result


def _sales_adjustment_summary(
    session: Session,
    owner_username: str,
    arguments: StrictToolArguments,
) -> dict[str, Any]:
    args = cast(SalesAdjustmentSummaryArguments, arguments)
    store = _require_owned_store(session, owner_username, args.store_id)
    raw_rows = session.execute(
        select(
            SalesItemAdjustmentModel.adjustment_type,
            SalesItemAdjustmentModel.status,
            func.count(
                func.distinct(SalesOrderItemModel.order_number)
            ),
            func.coalesce(func.sum(SalesItemAdjustmentModel.units), 0),
            func.coalesce(func.sum(SalesItemAdjustmentModel.amount), 0),
        )
        .join(
            SalesOrderItemModel,
            SalesOrderItemModel.id
            == SalesItemAdjustmentModel.sales_order_item_id,
        )
        .where(
            *_item_adjustment_filters(
                owner_username,
                store.id,
                args.start_date,
                args.end_date,
            ),
            SalesItemAdjustmentModel.status.in_(
                ("confirmed", "unresolved")
            ),
        )
        .group_by(
            SalesItemAdjustmentModel.adjustment_type,
            SalesItemAdjustmentModel.status,
        )
        .order_by(
            SalesItemAdjustmentModel.adjustment_type.asc(),
            SalesItemAdjustmentModel.status.asc(),
        )
    ).all()
    start_at, end_at = _range_datetimes(args.start_date, args.end_date)
    unresolved_exists = _unresolved_adjustment_exists(
        owner_username,
        store.id,
    )
    unattributed_unresolved_count = _integer(
        session.scalar(
            select(func.count(SalesOrderModel.id)).where(
                SalesOrderModel.owner_username == owner_username,
                SalesOrderModel.store_id == store.id,
                SalesOrderModel.ordered_at >= start_at,
                SalesOrderModel.ordered_at < end_at,
                SalesOrderModel.has_unresolved_adjustment.is_(True),
                ~unresolved_exists,
            )
        )
    )
    result = _result_metadata(
        session,
        owner_username=owner_username,
        store=store,
        start_date=args.start_date,
        end_date=args.end_date,
        metric="adjustmentUnits",
    )
    rows = [
        {
            "adjustmentType": row[0],
            "status": row[1],
            "adjustmentCount": _integer(row[2]),
            "units": _integer(row[3]),
            "amount": _decimal_number(row[4]),
        }
        for row in raw_rows
    ]
    if unattributed_unresolved_count:
        rows.append(
            {
                "adjustmentType": "unattributed",
                "status": "unresolved",
                "adjustmentCount": unattributed_unresolved_count,
                "units": 0,
                "amount": 0.0,
            }
        )
    result["rows"] = rows
    return result


_TOOL_HANDLERS: dict[str, ToolHandler] = {
    "list_owned_stores": _list_owned_stores,
    "get_store_sales_overview": _store_sales_overview,
    "get_product_sales_ranking": _product_sales_ranking,
    "get_product_sales_trend": _product_sales_trend,
    "compare_product_sales": _compare_product_sales,
    "get_sku_sales_breakdown": _sku_sales_breakdown,
    "get_slow_moving_products": _slow_moving_products,
    "get_sales_adjustment_summary": _sales_adjustment_summary,
}


def execute_sales_tool(
    owner_username: str,
    tool_name: str,
    arguments: dict[str, Any],
) -> dict[str, Any]:
    argument_model = _TOOL_MODELS.get(tool_name)
    handler = _TOOL_HANDLERS.get(tool_name)
    if argument_model is None or handler is None:
        raise ValueError(f"未知的销量分析工具：{tool_name}")
    validated = argument_model.model_validate(arguments)
    with session_scope() as session:
        return handler(session, owner_username, validated)
