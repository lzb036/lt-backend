from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from decimal import Decimal, InvalidOperation
import hashlib
import json
from typing import Any, Iterable

from sqlalchemy import delete, select
from sqlalchemy.orm import Session

from app.core.secure_storage import decrypt_text
from app.db.database import session_scope
from app.db.models import (
    ProductSalesDailyModel,
    SalesItemAdjustmentModel,
    SalesOrderItemModel,
    SalesOrderModel,
    SalesSyncStateModel,
    StoreModel,
)
from app.services import rakuten_order_service


INCREMENTAL_RECHECK_DAYS = 7
RAKUTEN_ORDER_STATUSES = [100, 200, 300, 400, 500, 600, 700, 800, 900]
SNAPSHOT_ADJUSTMENT_SOURCE_PREFIX = "sales_sync:"


@dataclass(frozen=True)
class AdjustmentDerivation:
    canceled_units: int
    refunded_units: int
    returned_units: int
    unresolved_refund_units: int
    effective_units: int


def calculate_effective_units(
    ordered: int,
    canceled: int,
    refunded: int,
    returned: int,
) -> int:
    return max(
        0,
        _non_negative_int(ordered)
        - _non_negative_int(canceled)
        - _non_negative_int(refunded)
        - _non_negative_int(returned),
    )


def derive_adjustments(
    *,
    ordered_units: int,
    latest_units: int | None = None,
    order_canceled: bool = False,
    delete_item: bool = False,
    canceled_units: int = 0,
    refund_units: int = 0,
    return_units: int = 0,
    return_refund: bool = False,
    unresolved_refund_units: int = 0,
) -> AdjustmentDerivation:
    ordered = _non_negative_int(ordered_units)
    latest = ordered if latest_units is None else _non_negative_int(latest_units)
    explicit_canceled = _non_negative_int(canceled_units)
    refunded = _non_negative_int(refund_units)
    returned = _non_negative_int(return_units)
    unresolved = _non_negative_int(unresolved_refund_units)

    if return_refund:
        refunded -= min(refunded, returned)

    if order_canceled or delete_item:
        canceled = ordered
    else:
        quantity_reduction = max(0, ordered - latest)
        attributed_reduction = min(
            quantity_reduction,
            refunded + returned,
        )
        implicit_canceled = quantity_reduction - attributed_reduction
        canceled = min(
            ordered,
            max(explicit_canceled, implicit_canceled),
        )

    remaining = max(0, ordered - canceled)
    returned = min(returned, remaining)
    remaining -= returned
    refunded = min(refunded, remaining)

    return AdjustmentDerivation(
        canceled_units=canceled,
        refunded_units=refunded,
        returned_units=returned,
        unresolved_refund_units=unresolved,
        effective_units=calculate_effective_units(
            ordered,
            canceled,
            refunded,
            returned,
        ),
    )


def sync_owned_store(
    owner_username: str,
    store_id: int,
    *,
    initial_days: int = 90,
) -> dict[str, Any]:
    normalized_owner = _text(owner_username)
    normalized_store_id = int(store_id)
    normalized_initial_days = max(1, int(initial_days))

    with session_scope() as session:
        store = session.scalar(
            select(StoreModel)
            .where(
                StoreModel.id == normalized_store_id,
                StoreModel.owner_username == normalized_owner,
            )
            .with_for_update()
        )
        if store is None:
            raise LookupError("店铺不存在或无权访问。")

        state = session.scalar(
            select(SalesSyncStateModel)
            .where(
                SalesSyncStateModel.store_id == normalized_store_id,
                SalesSyncStateModel.owner_username == normalized_owner,
            )
            .with_for_update()
        )
        if state is None:
            state = SalesSyncStateModel(
                owner_username=normalized_owner,
                store_id=normalized_store_id,
                initial_sync_completed=False,
                sync_status="idle",
            )
            session.add(state)
            session.flush()
        if state.sync_status == "running":
            return _sync_state_payload(state, already_running=True)

        initial_sync_completed = bool(state.initial_sync_completed)
        state.sync_status = "running"
        state.progress_current = 0
        state.progress_total = 0
        state.last_error = None
        service_secret = decrypt_text(store.rakuten_service_secret_encrypted)
        license_key = decrypt_text(store.rakuten_license_key_encrypted)

    now = datetime.now()
    lookback_days = (
        INCREMENTAL_RECHECK_DAYS
        if initial_sync_completed
        else normalized_initial_days
    )
    start_at = now - timedelta(days=lookback_days)

    try:
        order_numbers = rakuten_order_service.search_order_numbers(
            service_secret,
            license_key,
            start_at,
            now,
            RAKUTEN_ORDER_STATUSES,
        )
        orders = rakuten_order_service.get_orders(
            service_secret,
            license_key,
            order_numbers,
        )

        with session_scope() as session:
            store = session.scalar(
                select(StoreModel).where(
                    StoreModel.id == normalized_store_id,
                    StoreModel.owner_username == normalized_owner,
                )
            )
            if store is None:
                raise LookupError("店铺不存在或无权访问。")
            state = session.scalar(
                select(SalesSyncStateModel)
                .where(
                    SalesSyncStateModel.store_id == normalized_store_id,
                    SalesSyncStateModel.owner_username == normalized_owner,
                )
                .with_for_update()
            )
            if state is None:
                raise RuntimeError("店铺销量同步状态不存在。")

            state.progress_total = len(orders)
            affected_dates: set[date] = set()
            last_remote_updated_at = state.last_remote_updated_at
            for index, order_payload in enumerate(orders, start=1):
                order_dates, remote_updated_at = _reconcile_order_snapshot(
                    session,
                    store,
                    order_payload,
                    synced_at=now,
                )
                affected_dates.update(order_dates)
                if (
                    remote_updated_at is not None
                    and (
                        last_remote_updated_at is None
                        or remote_updated_at > last_remote_updated_at
                    )
                ):
                    last_remote_updated_at = remote_updated_at
                state.progress_current = index

            if affected_dates:
                rebuild_daily_sales(
                    session,
                    normalized_store_id,
                    min(affected_dates),
                    max(affected_dates),
                )

            state.initial_sync_completed = True
            state.last_successful_sync_at = now
            state.last_remote_updated_at = last_remote_updated_at
            state.sync_status = "idle"
            state.last_error = None
            result = _sync_state_payload(state, already_running=False)
            result.update(
                {
                    "status": "completed",
                    "orderCount": len(orders),
                    "affectedDateCount": len(affected_dates),
                }
            )
            return result
    except Exception:
        with session_scope() as session:
            state = session.scalar(
                select(SalesSyncStateModel)
                .where(
                    SalesSyncStateModel.store_id == normalized_store_id,
                    SalesSyncStateModel.owner_username == normalized_owner,
                )
                .with_for_update()
            )
            if state is not None:
                state.sync_status = "error"
                state.last_error = "销量同步失败，请稍后重试。"
        raise


def rebuild_daily_sales(
    session: Session,
    store_id: int,
    start_date: date,
    end_date: date,
) -> None:
    if start_date > end_date:
        raise ValueError("start_date must not be after end_date")

    normalized_store_id = int(store_id)
    store = session.scalar(
        select(StoreModel).where(StoreModel.id == normalized_store_id)
    )
    if store is None:
        raise LookupError("店铺不存在。")

    session.execute(
        delete(ProductSalesDailyModel).where(
            ProductSalesDailyModel.store_id == normalized_store_id,
            ProductSalesDailyModel.sales_date >= start_date,
            ProductSalesDailyModel.sales_date <= end_date,
        )
    )
    session.flush()

    start_at = datetime.combine(start_date, time.min)
    end_at = datetime.combine(end_date + timedelta(days=1), time.min)
    items = session.scalars(
        select(SalesOrderItemModel)
        .where(
            SalesOrderItemModel.store_id == normalized_store_id,
            SalesOrderItemModel.owner_username == store.owner_username,
            SalesOrderItemModel.ordered_at >= start_at,
            SalesOrderItemModel.ordered_at < end_at,
        )
        .order_by(
            SalesOrderItemModel.ordered_at.asc(),
            SalesOrderItemModel.id.asc(),
        )
    ).all()

    grouped: dict[tuple[date, str, str], dict[str, Any]] = {}
    for item in items:
        key = (
            item.ordered_at.date(),
            item.manage_number,
            item.sku_key,
        )
        aggregate = grouped.setdefault(
            key,
            {
                "item_number": item.item_number,
                "item_name": item.item_name,
                "order_numbers": set(),
                "ordered_units": 0,
                "canceled_units": 0,
                "refunded_units": 0,
                "returned_units": 0,
                "effective_units": 0,
                "gross_sales_amount": Decimal("0"),
                "effective_sales_amount": Decimal("0"),
            },
        )
        aggregate["item_number"] = item.item_number
        aggregate["item_name"] = item.item_name
        aggregate["order_numbers"].add(item.order_number)
        aggregate["ordered_units"] += item.ordered_units
        aggregate["canceled_units"] += item.canceled_units
        aggregate["refunded_units"] += item.refunded_units
        aggregate["returned_units"] += item.returned_units
        aggregate["effective_units"] += item.effective_units
        aggregate["gross_sales_amount"] += item.unit_price * item.ordered_units
        aggregate["effective_sales_amount"] += item.effective_amount

    session.add_all(
        [
            ProductSalesDailyModel(
                owner_username=store.owner_username,
                store_id=normalized_store_id,
                sales_date=sales_date,
                manage_number=manage_number,
                item_number=aggregate["item_number"],
                sku_key=sku_key,
                item_name_snapshot=aggregate["item_name"],
                order_count=len(aggregate["order_numbers"]),
                ordered_units=aggregate["ordered_units"],
                canceled_units=aggregate["canceled_units"],
                refunded_units=aggregate["refunded_units"],
                returned_units=aggregate["returned_units"],
                effective_units=aggregate["effective_units"],
                gross_sales_amount=aggregate["gross_sales_amount"],
                effective_sales_amount=aggregate[
                    "effective_sales_amount"
                ],
            )
            for (
                sales_date,
                manage_number,
                sku_key,
            ), aggregate in grouped.items()
        ]
    )
    session.flush()


def _reconcile_order_snapshot(
    session: Session,
    store: StoreModel,
    payload: dict[str, Any],
    *,
    synced_at: datetime,
) -> tuple[set[date], datetime | None]:
    order_number = _first_text(payload, ("orderNumber",))
    if not order_number:
        raise ValueError("订单号缺失。")

    ordered_at = _first_datetime(
        payload,
        (
            "orderDatetime",
            "orderDateTime",
            "orderDate",
            "orderTimestamp",
        ),
    ) or synced_at
    remote_updated_at = _first_datetime(
        payload,
        (
            "updateDatetime",
            "updatedAt",
            "orderUpdateDatetime",
        ),
    )
    order = session.scalar(
        select(SalesOrderModel).where(
            SalesOrderModel.store_id == store.id,
            SalesOrderModel.order_number == order_number,
        )
    )
    affected_dates = {ordered_at.date()}
    if order is None:
        order = SalesOrderModel(
            owner_username=store.owner_username,
            store_id=store.id,
            order_number=order_number,
            ordered_at=ordered_at,
            raw_order_json="{}",
            last_synced_at=synced_at,
        )
        session.add(order)
        session.flush()
    else:
        affected_dates.add(order.ordered_at.date())

    order.order_progress = _first_text(payload, ("orderProgress",))
    order.order_status = _first_text(
        payload,
        ("orderStatus", "status", "orderProgressName"),
    )
    order.ordered_at = ordered_at
    order.updated_at_remote = remote_updated_at
    order.total_amount = _first_decimal(
        payload,
        ("totalPrice", "totalAmount", "settlementAmount"),
    )
    order.currency = (
        _first_text(payload, ("currencyCode", "currency")) or "JPY"
    )
    order.is_canceled = _order_is_canceled(payload)
    order.raw_order_json = _json(payload)
    order.last_synced_at = synced_at

    existing_items = {
        item.item_detail_id: item
        for item in session.scalars(
            select(SalesOrderItemModel).where(
                SalesOrderItemModel.sales_order_id == order.id,
                SalesOrderItemModel.owner_username == store.owner_username,
                SalesOrderItemModel.store_id == store.id,
                SalesOrderItemModel.order_number == order_number,
            )
        ).all()
    }
    item_pairs = list(_order_item_pairs(payload))
    full_refund = _order_is_full_refund(payload)
    has_item_refund_attribution = any(
        _item_refund_units(raw_item) > 0
        or _item_return_units(raw_item) > 0
        for _, raw_item in item_pairs
    )
    unresolved_units = _order_unresolved_refund_units(payload)
    unresolved_amount = _order_unresolved_refund_amount(payload)
    unresolved = _order_has_unresolved_refund(payload) and not (
        full_refund or has_item_refund_attribution
    )
    seen_item_ids: set[str] = set()

    for position, (normalized_item, raw_item) in enumerate(
        item_pairs,
        start=1,
    ):
        item_detail_id = _text(normalized_item.get("itemDetailId"))
        if not item_detail_id:
            item_detail_id = _text(normalized_item.get("lineFingerprint"))
        if not item_detail_id:
            item_detail_id = _fallback_line_id(
                order_number,
                normalized_item,
                position,
            )
        seen_item_ids.add(item_detail_id)

        current_units = _non_negative_int(normalized_item.get("units"))
        item = existing_items.get(item_detail_id)
        ordered_units = (
            current_units if item is None else item.ordered_units
        )
        refund_units = _item_refund_units(raw_item)
        return_units = _item_return_units(raw_item)
        if full_refund and not has_item_refund_attribution:
            refund_units = ordered_units
        item_unresolved_units = (
            unresolved_units if unresolved and position == 1 else 0
        )
        item_unresolved_amount = (
            unresolved_amount if unresolved and position == 1 else Decimal("0")
        )
        derivation = derive_adjustments(
            ordered_units=ordered_units,
            latest_units=current_units,
            order_canceled=order.is_canceled,
            delete_item=bool(normalized_item.get("deleteItemFlag")),
            canceled_units=_item_canceled_units(raw_item),
            refund_units=refund_units,
            return_units=return_units,
            return_refund=_item_is_return_refund(raw_item),
            unresolved_refund_units=item_unresolved_units,
        )
        sku_models = normalized_item.get("SkuModelList")
        sku_json = _json(sku_models if isinstance(sku_models, list) else [])
        sku_key = _sku_key(sku_json)
        unit_price = _first_decimal(raw_item, ("price", "priceTaxIncl"))

        if item is None:
            item = SalesOrderItemModel.from_service_payload(
                owner_username=store.owner_username,
                store_id=store.id,
                sales_order_id=order.id,
                order_number=order_number,
                item_detail_id=item_detail_id,
                manage_number=_text(normalized_item.get("manageNumber")),
                item_number=_text(normalized_item.get("itemNumber")),
                item_id=_text(normalized_item.get("itemId")),
                sku_key=sku_key,
                sku_json=sku_json,
                item_name=_first_text(raw_item, ("itemName", "name")),
                unit_price=unit_price,
                ordered_units=ordered_units,
                latest_units=current_units,
                canceled_units=derivation.canceled_units,
                refunded_units=derivation.refunded_units,
                returned_units=derivation.returned_units,
                unresolved_refunded_units=(
                    derivation.unresolved_refund_units
                ),
                delete_item_flag=bool(
                    normalized_item.get("deleteItemFlag")
                ),
                restore_inventory_flag=bool(
                    normalized_item.get("restoreInventoryFlag")
                ),
                ordered_at=ordered_at,
            )
            session.add(item)
            session.flush()
        else:
            item.manage_number = _text(
                normalized_item.get("manageNumber")
            )
            item.item_number = _text(normalized_item.get("itemNumber"))
            item.item_id = _text(normalized_item.get("itemId"))
            item.sku_key = sku_key
            item.sku_json = sku_json
            item.item_name = _first_text(raw_item, ("itemName", "name"))
            item.unit_price = unit_price
            item.latest_units = current_units
            item.canceled_units = derivation.canceled_units
            item.refunded_units = derivation.refunded_units
            item.returned_units = derivation.returned_units
            item.effective_units = derivation.effective_units
            item.effective_amount = unit_price * derivation.effective_units
            item.delete_item_flag = bool(
                normalized_item.get("deleteItemFlag")
            )
            item.restore_inventory_flag = bool(
                normalized_item.get("restoreInventoryFlag")
            )
            item.ordered_at = ordered_at

        _reconcile_item_adjustments(
            session,
            item,
            derivation,
            raw_payload=raw_item,
            remote_updated_at=remote_updated_at,
            unresolved_amount=item_unresolved_amount,
        )

    for item_detail_id, item in existing_items.items():
        if item_detail_id in seen_item_ids:
            continue
        derivation = derive_adjustments(
            ordered_units=item.ordered_units,
            latest_units=0,
            order_canceled=order.is_canceled,
            delete_item=True,
        )
        item.latest_units = 0
        item.canceled_units = derivation.canceled_units
        item.refunded_units = 0
        item.returned_units = 0
        item.effective_units = derivation.effective_units
        item.effective_amount = item.unit_price * derivation.effective_units
        item.delete_item_flag = True
        _reconcile_item_adjustments(
            session,
            item,
            derivation,
            raw_payload={"missingFromLatestSnapshot": True},
            remote_updated_at=remote_updated_at,
            unresolved_amount=Decimal("0"),
        )

    order.has_unresolved_adjustment = unresolved
    return affected_dates, remote_updated_at


def _reconcile_item_adjustments(
    session: Session,
    item: SalesOrderItemModel,
    derivation: AdjustmentDerivation,
    *,
    raw_payload: dict[str, Any],
    remote_updated_at: datetime | None,
    unresolved_amount: Decimal,
) -> None:
    expected: dict[str, dict[str, Any]] = {}
    if derivation.canceled_units:
        expected[f"{SNAPSHOT_ADJUSTMENT_SOURCE_PREFIX}cancel"] = {
            "adjustment_type": "cancel",
            "units": derivation.canceled_units,
            "amount": item.unit_price * derivation.canceled_units,
            "status": "confirmed",
            "reason": "订单或商品数量已取消",
        }
    if derivation.refunded_units:
        expected[f"{SNAPSHOT_ADJUSTMENT_SOURCE_PREFIX}refund"] = {
            "adjustment_type": "refund",
            "units": derivation.refunded_units,
            "amount": item.unit_price * derivation.refunded_units,
            "status": "confirmed",
            "reason": "商品退款已确认",
        }
    if derivation.returned_units:
        expected[f"{SNAPSHOT_ADJUSTMENT_SOURCE_PREFIX}return"] = {
            "adjustment_type": "return",
            "units": derivation.returned_units,
            "amount": item.unit_price * derivation.returned_units,
            "status": "confirmed",
            "reason": "商品退货已确认",
        }
    if derivation.unresolved_refund_units or unresolved_amount:
        expected[f"{SNAPSHOT_ADJUSTMENT_SOURCE_PREFIX}refund_unresolved"] = {
            "adjustment_type": "refund",
            "units": derivation.unresolved_refund_units,
            "amount": unresolved_amount,
            "status": "unresolved",
            "reason": "部分退款无法定位到具体商品",
        }

    existing_rows = session.scalars(
        select(SalesItemAdjustmentModel)
        .where(
            SalesItemAdjustmentModel.sales_order_item_id == item.id,
            SalesItemAdjustmentModel.owner_username
            == item.owner_username,
            SalesItemAdjustmentModel.store_id == item.store_id,
            SalesItemAdjustmentModel.source.like(
                f"{SNAPSHOT_ADJUSTMENT_SOURCE_PREFIX}%"
            ),
        )
        .order_by(SalesItemAdjustmentModel.id.asc())
    ).all()
    existing_by_source: dict[str, SalesItemAdjustmentModel] = {}
    for row in existing_rows:
        if row.source not in existing_by_source:
            existing_by_source[row.source] = row
        else:
            row.status = "reverted"

    raw_payload_json = _json(raw_payload)
    for source, values in expected.items():
        row = existing_by_source.get(source)
        if row is None:
            row = SalesItemAdjustmentModel(
                owner_username=item.owner_username,
                store_id=item.store_id,
                sales_order_item_id=item.id,
                source=source,
                raw_payload_json=raw_payload_json,
            )
            session.add(row)
        row.adjustment_type = values["adjustment_type"]
        row.units = values["units"]
        row.amount = values["amount"]
        row.status = values["status"]
        row.reason = values["reason"]
        row.remote_updated_at = remote_updated_at
        row.raw_payload_json = raw_payload_json

    for source, row in existing_by_source.items():
        if source not in expected:
            row.status = "reverted"
            row.remote_updated_at = remote_updated_at
            row.raw_payload_json = raw_payload_json


def _sync_state_payload(
    state: SalesSyncStateModel,
    *,
    already_running: bool,
) -> dict[str, Any]:
    payload = {
        "storeId": state.store_id,
        "ownerUsername": state.owner_username,
        "status": state.sync_status,
        "alreadyRunning": already_running,
        "initialSyncCompleted": bool(state.initial_sync_completed),
        "progressCurrent": state.progress_current,
        "progressTotal": state.progress_total,
        "lastSuccessfulSyncAt": _iso(state.last_successful_sync_at),
        "lastRemoteUpdatedAt": _iso(state.last_remote_updated_at),
        "lastError": state.last_error or "",
    }
    if already_running:
        payload["activeTask"] = {
            "storeId": state.store_id,
            "status": state.sync_status,
            "progressCurrent": state.progress_current,
            "progressTotal": state.progress_total,
        }
    return payload


def _order_item_pairs(
    order: dict[str, Any],
) -> Iterable[tuple[dict[str, Any], dict[str, Any]]]:
    normalized_items = list(rakuten_order_service.iter_order_items(order))
    raw_items = list(_raw_order_items(order))
    for index, normalized in enumerate(normalized_items):
        raw = raw_items[index] if index < len(raw_items) else {}
        yield normalized, raw


def _raw_order_items(order: dict[str, Any]) -> Iterable[dict[str, Any]]:
    packages = order.get("PackageModelList")
    if not isinstance(packages, list):
        return
    for package in packages:
        if not isinstance(package, dict):
            continue
        items = package.get("ItemModelList")
        if not isinstance(items, list):
            continue
        for item in items:
            if isinstance(item, dict):
                yield item


def _order_is_canceled(payload: dict[str, Any]) -> bool:
    if _first_bool(
        payload,
        ("isCanceled", "isCancelled", "cancelFlag", "orderCancelFlag"),
    ):
        return True
    status = " ".join(
        [
            _first_text(payload, ("orderStatus",)),
            _first_text(payload, ("orderProgressName",)),
        ]
    ).lower()
    return any(
        token in status
        for token in ("cancel", "cancelled", "canceled", "取消", "キャンセル")
    )


def _order_is_full_refund(payload: dict[str, Any]) -> bool:
    if _first_bool(payload, ("isFullRefund", "fullRefund")):
        return True
    status = _first_text(
        payload,
        ("refundStatus", "orderStatus", "orderProgressName"),
    ).lower()
    if status in {"full", "fully_refunded", "refunded"}:
        return True
    return any(
        token in status
        for token in ("full refund", "全额退款", "全額返金")
    )


def _order_has_unresolved_refund(payload: dict[str, Any]) -> bool:
    if _first_bool(
        payload,
        ("partialRefund", "hasPartialRefund", "unresolvedRefund"),
    ):
        return True
    return (
        _order_unresolved_refund_units(payload) > 0
        or _order_unresolved_refund_amount(payload) > 0
    )


def _order_unresolved_refund_units(payload: dict[str, Any]) -> int:
    return _first_int(
        payload,
        (
            "unresolvedRefundUnits",
            "partialRefundUnits",
            "refundUnits",
        ),
    )


def _order_unresolved_refund_amount(payload: dict[str, Any]) -> Decimal:
    return _first_decimal(
        payload,
        (
            "unresolvedRefundAmount",
            "partialRefundAmount",
            "refundAmount",
        ),
    )


def _item_canceled_units(payload: dict[str, Any]) -> int:
    return _first_int(payload, ("canceledUnits", "cancelledUnits", "cancelUnits"))


def _item_refund_units(payload: dict[str, Any]) -> int:
    return _first_int(payload, ("refundedUnits", "refundUnits"))


def _item_return_units(payload: dict[str, Any]) -> int:
    return _first_int(payload, ("returnedUnits", "returnUnits"))


def _item_is_return_refund(payload: dict[str, Any]) -> bool:
    return _first_bool(
        payload,
        (
            "returnRefund",
            "returnRefundFlag",
            "isReturnRefund",
        ),
    )


def _sku_key(sku_json: str) -> str:
    if sku_json == "[]":
        return ""
    digest = hashlib.sha256(sku_json.encode("utf-8")).hexdigest()
    return f"v1:{digest}"


def _fallback_line_id(
    order_number: str,
    normalized_item: dict[str, Any],
    position: int,
) -> str:
    source = {
        "orderNumber": order_number,
        "position": position,
        "itemId": _text(normalized_item.get("itemId")),
        "itemNumber": _text(normalized_item.get("itemNumber")),
        "manageNumber": _text(normalized_item.get("manageNumber")),
        "sku": normalized_item.get("SkuModelList"),
    }
    digest = hashlib.sha256(_json(source).encode("utf-8")).hexdigest()
    return f"v1:{digest}"


def _first_text(payload: dict[str, Any], keys: Iterable[str]) -> str:
    for key in keys:
        value = _text(payload.get(key))
        if value:
            return value
    return ""


def _first_int(payload: dict[str, Any], keys: Iterable[str]) -> int:
    for key in keys:
        if key in payload:
            return _non_negative_int(payload.get(key))
    return 0


def _first_decimal(
    payload: dict[str, Any],
    keys: Iterable[str],
) -> Decimal:
    for key in keys:
        if key not in payload:
            continue
        try:
            return Decimal(str(payload.get(key) or 0))
        except (InvalidOperation, TypeError, ValueError):
            continue
    return Decimal("0")


def _first_bool(payload: dict[str, Any], keys: Iterable[str]) -> bool:
    for key in keys:
        if key in payload:
            return _bool(payload.get(key))
    return False


def _first_datetime(
    payload: dict[str, Any],
    keys: Iterable[str],
) -> datetime | None:
    for key in keys:
        parsed = _datetime(payload.get(key))
        if parsed is not None:
            return parsed
    return None


def _datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value.replace(tzinfo=None)
    text = _text(value)
    if not text:
        return None
    normalized = text.replace("Z", "+00:00")
    for parser in (
        datetime.fromisoformat,
        lambda item: datetime.strptime(item, "%Y-%m-%d %H:%M:%S"),
        lambda item: datetime.strptime(item, "%Y-%m-%d"),
    ):
        try:
            parsed = parser(normalized)
            return parsed.replace(tzinfo=None)
        except ValueError:
            continue
    return None


def _non_negative_int(value: Any) -> int:
    try:
        return max(0, int(value))
    except (TypeError, ValueError):
        return 0


def _bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    text = _text(value).lower()
    if text in {"1", "true", "yes", "on"}:
        return True
    if text in {"0", "false", "no", "off", ""}:
        return False
    try:
        return bool(int(text))
    except (TypeError, ValueError):
        return bool(value)


def _text(value: Any) -> str:
    return str(value or "").strip()


def _json(value: Any) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )


def _iso(value: datetime | None) -> str:
    return value.isoformat() if value is not None else ""
