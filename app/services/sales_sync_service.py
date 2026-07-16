from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal, InvalidOperation
import hashlib
import json
from threading import Event, Lock, Thread
from typing import Any, Iterable
import uuid

from sqlalchemy import and_, delete, or_, select, update
from sqlalchemy.exc import IntegrityError
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
INCOMPLETE_ADJUSTED_RECHECK_DAYS = 30
COMPLETED_RECHECK_MAX_DAYS = 90
COMPLETED_RECHECK_INTERVAL = timedelta(days=1)
DAILY_PRODUCT_KEY_MAX_LENGTH = 255
RAKUTEN_ORDER_STATUSES = [100, 200, 300, 400, 500, 600, 700, 800, 900]
SNAPSHOT_ADJUSTMENT_SOURCE_PREFIX = "sales_sync:"
SALES_SYNC_LEASE_TIMEOUT = timedelta(minutes=10)
SALES_SYNC_HEARTBEAT_INTERVAL_SECONDS = (
    SALES_SYNC_LEASE_TIMEOUT.total_seconds() / 3
)
RUNNING_SYNC_STATUS_PREFIX = "running:"


@dataclass(frozen=True)
class AdjustmentDerivation:
    canceled_units: int
    refunded_units: int
    returned_units: int
    unresolved_refund_units: int
    unresolved_return_units: int
    has_unresolved_refund: bool
    has_unresolved_return: bool
    effective_units: int


@dataclass(frozen=True)
class ReconcileOutcome:
    affected_dates: set[date]
    remote_updated_at: datetime | None
    disposition: str


@dataclass(frozen=True)
class PreparedItemSnapshot:
    position: int
    normalized_item: dict[str, Any]
    raw_item: dict[str, Any]
    item_detail_id: str
    current_units: int
    ordered_units: int
    unit_price: Decimal
    derivation: AdjustmentDerivation


class IncompleteSnapshotError(ValueError):
    pass


class SalesSyncLeaseLostError(RuntimeError):
    pass


class _PeriodicLeaseHeartbeat:
    def __init__(
        self,
        owner_username: str,
        store_id: int,
        lease_status: str,
        *,
        interval_seconds: float | None = None,
    ) -> None:
        self.owner_username = owner_username
        self.store_id = store_id
        self.lease_status = lease_status
        self.interval_seconds = max(
            0.001,
            float(
                SALES_SYNC_HEARTBEAT_INTERVAL_SECONDS
                if interval_seconds is None
                else interval_seconds
            ),
        )
        self._stop_event = Event()
        self._lock = Lock()
        self._thread: Thread | None = None
        self._failure: BaseException | None = None
        self._progress_current: int | None = None
        self._progress_total: int | None = None

    def __enter__(self) -> _PeriodicLeaseHeartbeat:
        self.start()
        return self

    def __exit__(self, exc_type, _exc, _traceback) -> bool:
        self.stop(raise_failure=exc_type is None)
        return False

    def start(self) -> None:
        with self._lock:
            if self._thread is not None:
                raise RuntimeError("销量同步心跳已经启动。")
            self._stop_event.clear()
            self._failure = None
        self.pulse()
        thread = Thread(
            target=self._run,
            name=f"sales-sync-heartbeat-{self.store_id}",
            daemon=True,
        )
        with self._lock:
            self._thread = thread
        thread.start()

    def stop(self, *, raise_failure: bool = True) -> None:
        with self._lock:
            thread = self._thread
            self._thread = None
        if thread is not None:
            self._stop_event.set()
            thread.join()
        if raise_failure:
            self.raise_if_failed()

    def set_progress(
        self,
        progress_current: int | None,
        progress_total: int | None,
    ) -> None:
        with self._lock:
            self._progress_current = progress_current
            self._progress_total = progress_total

    def pulse(self) -> None:
        with self._lock:
            progress_current = self._progress_current
            progress_total = self._progress_total
        _heartbeat_lease_in_new_transaction(
            self.owner_username,
            self.store_id,
            self.lease_status,
            progress_current=progress_current,
            progress_total=progress_total,
        )

    def raise_if_failed(self) -> None:
        with self._lock:
            failure = self._failure
        if failure is not None:
            raise failure

    def _run(self) -> None:
        while not self._stop_event.wait(self.interval_seconds):
            try:
                self.pulse()
            except BaseException as exc:
                with self._lock:
                    if self._failure is None:
                        self._failure = exc
                self._stop_event.set()
                return


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
    unresolved_return_units: int = 0,
    has_unresolved_refund: bool = False,
    has_unresolved_return: bool = False,
) -> AdjustmentDerivation:
    ordered = _non_negative_int(ordered_units)
    latest = ordered if latest_units is None else _non_negative_int(latest_units)
    explicit_canceled = _non_negative_int(canceled_units)
    refunded = _non_negative_int(refund_units)
    returned = _non_negative_int(return_units)
    unresolved = _non_negative_int(unresolved_refund_units)
    unresolved_returned = _non_negative_int(unresolved_return_units)

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
        unresolved_return_units=unresolved_returned,
        has_unresolved_refund=bool(has_unresolved_refund),
        has_unresolved_return=bool(has_unresolved_return),
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
    lease_status = _new_lease_status()
    acquired_at = _now()

    with session_scope() as session:
        store = session.scalar(
            select(StoreModel).where(
                StoreModel.id == normalized_store_id,
                StoreModel.owner_username == normalized_owner,
            )
        )
        if store is None:
            raise LookupError("店铺不存在或无权访问。")

        _ensure_sync_state(
            session,
            owner_username=normalized_owner,
            store_id=normalized_store_id,
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
        acquired = _acquire_sync_lease(
            session,
            owner_username=normalized_owner,
            store_id=normalized_store_id,
            lease_status=lease_status,
            now=acquired_at,
        )
        if acquired:
            state = session.scalar(
                select(SalesSyncStateModel).where(
                    SalesSyncStateModel.store_id == normalized_store_id,
                    SalesSyncStateModel.owner_username == normalized_owner,
                )
            )
            if state is None:
                raise RuntimeError("店铺销量同步状态不存在。")
            initial_sync_completed = bool(state.initial_sync_completed)
            encrypted_service_secret = (
                store.rakuten_service_secret_encrypted
            )
            encrypted_license_key = store.rakuten_license_key_encrypted

    if not acquired:
        with session_scope() as session:
            state = session.scalar(
                _active_sync_state_statement(
                    normalized_owner,
                    normalized_store_id,
                )
            )
            if state is None:
                raise RuntimeError("店铺销量同步状态不存在。")
            return _sync_state_payload(state, already_running=True)

    now = _now()
    search_days = (
        INCREMENTAL_RECHECK_DAYS
        if initial_sync_completed
        else normalized_initial_days
    )
    start_at = now - timedelta(days=search_days)

    try:
        with _PeriodicLeaseHeartbeat(
            normalized_owner,
            normalized_store_id,
            lease_status,
        ) as heartbeat:
            service_secret = decrypt_text(encrypted_service_secret)
            license_key = decrypt_text(encrypted_license_key)
            heartbeat.raise_if_failed()
            recent_order_numbers = (
                rakuten_order_service.search_order_numbers(
                    service_secret,
                    license_key,
                    start_at,
                    now,
                    RAKUTEN_ORDER_STATUSES,
                )
            )
            heartbeat.raise_if_failed()
            local_order_numbers: list[str] = []
            if initial_sync_completed:
                with session_scope() as session:
                    local_order_numbers = _local_recheck_order_numbers(
                        session,
                        owner_username=normalized_owner,
                        store_id=normalized_store_id,
                        now=now,
                    )
            order_numbers = _dedupe_order_numbers(
                recent_order_numbers,
                local_order_numbers,
            )
            heartbeat.set_progress(0, len(order_numbers))
            heartbeat.pulse()
            orders = rakuten_order_service.get_orders(
                service_secret,
                license_key,
                order_numbers,
            )
            heartbeat.raise_if_failed()
            heartbeat.set_progress(0, len(orders))
            heartbeat.pulse()

            with session_scope() as session:
                store = session.scalar(
                    select(StoreModel).where(
                        StoreModel.id == normalized_store_id,
                        StoreModel.owner_username == normalized_owner,
                    )
                )
                if store is None:
                    raise LookupError("店铺不存在或无权访问。")
                affected_dates: set[date] = set()
                state = session.scalar(
                    select(SalesSyncStateModel).where(
                        SalesSyncStateModel.store_id
                        == normalized_store_id,
                        SalesSyncStateModel.owner_username
                        == normalized_owner,
                    )
                )
                if state is None:
                    raise RuntimeError("店铺销量同步状态不存在。")
                if state.sync_status != lease_status:
                    raise SalesSyncLeaseLostError("销量同步租约已失效。")
                last_remote_updated_at = state.last_remote_updated_at
                stale_order_count = 0
                incomplete_order_count = 0
                for index, order_payload in enumerate(orders, start=1):
                    heartbeat.raise_if_failed()
                    try:
                        outcome = _reconcile_order_snapshot(
                            session,
                            store,
                            order_payload,
                            synced_at=now,
                        )
                    except IncompleteSnapshotError:
                        incomplete_order_count += 1
                    else:
                        affected_dates.update(outcome.affected_dates)
                        if outcome.disposition == "stale":
                            stale_order_count += 1
                        remote_updated_at = outcome.remote_updated_at
                        if (
                            remote_updated_at is not None
                            and (
                                last_remote_updated_at is None
                                or remote_updated_at
                                > last_remote_updated_at
                            )
                        ):
                            last_remote_updated_at = remote_updated_at
                    heartbeat.set_progress(index, len(orders))

                heartbeat.raise_if_failed()
                if affected_dates:
                    rebuild_daily_sales(
                        session,
                        normalized_store_id,
                        min(affected_dates),
                        max(affected_dates),
                    )
                heartbeat.raise_if_failed()
                heartbeat.stop()

                completed_at = _now()
                completed = session.execute(
                    update(SalesSyncStateModel)
                    .where(
                        SalesSyncStateModel.store_id
                        == normalized_store_id,
                        SalesSyncStateModel.owner_username
                        == normalized_owner,
                        SalesSyncStateModel.sync_status == lease_status,
                    )
                    .values(
                        initial_sync_completed=True,
                        last_successful_sync_at=completed_at,
                        last_remote_updated_at=last_remote_updated_at,
                        sync_status="idle",
                        progress_current=len(orders),
                        progress_total=len(orders),
                        last_error=None,
                        updated_at=completed_at,
                    )
                ).rowcount
                if completed != 1:
                    raise SalesSyncLeaseLostError("销量同步租约已失效。")
                state = session.scalar(
                    select(SalesSyncStateModel).where(
                        SalesSyncStateModel.store_id
                        == normalized_store_id,
                        SalesSyncStateModel.owner_username
                        == normalized_owner,
                    )
                )
                if state is None:
                    raise RuntimeError("店铺销量同步状态不存在。")
                result = _sync_state_payload(
                    state,
                    already_running=False,
                )
                result.update(
                    {
                        "status": "completed",
                        "orderCount": len(orders),
                        "affectedDateCount": len(affected_dates),
                        "staleOrderCount": stale_order_count,
                        "incompleteOrderCount": incomplete_order_count,
                    }
                )
                return result
    except Exception:
        with session_scope() as session:
            failed_at = _now()
            session.execute(
                update(SalesSyncStateModel)
                .where(
                    SalesSyncStateModel.store_id == normalized_store_id,
                    SalesSyncStateModel.owner_username == normalized_owner,
                    SalesSyncStateModel.sync_status == lease_status,
                )
                .values(
                    sync_status="error",
                    last_error="销量同步失败，请稍后重试。",
                    updated_at=failed_at,
                )
            )
        raise


def _now() -> datetime:
    return datetime.now()


def _new_lease_status() -> str:
    return f"{RUNNING_SYNC_STATUS_PREFIX}{uuid.uuid4().hex[:20]}"


def _ensure_sync_state(
    session: Session,
    *,
    owner_username: str,
    store_id: int,
) -> None:
    existing = session.scalar(
        select(SalesSyncStateModel.store_id).where(
            SalesSyncStateModel.store_id == store_id,
            SalesSyncStateModel.owner_username == owner_username,
        )
    )
    if existing is not None:
        return
    try:
        with session.begin_nested():
            session.add(
                SalesSyncStateModel(
                    owner_username=owner_username,
                    store_id=store_id,
                    initial_sync_completed=False,
                    sync_status="idle",
                )
            )
            session.flush()
    except IntegrityError:
        session.expire_all()


def _lease_acquisition_statement(
    owner_username: str,
    store_id: int,
    lease_status: str,
    now: datetime,
):
    stale_before = now - SALES_SYNC_LEASE_TIMEOUT
    running_condition = SalesSyncStateModel.sync_status.like("running%")
    return (
        update(SalesSyncStateModel)
        .where(
            SalesSyncStateModel.store_id == store_id,
            SalesSyncStateModel.owner_username == owner_username,
            or_(
                SalesSyncStateModel.sync_status.not_like("running%"),
                and_(
                    running_condition,
                    SalesSyncStateModel.updated_at < stale_before,
                ),
            ),
        )
        .values(
            sync_status=lease_status,
            progress_current=0,
            progress_total=0,
            last_error=None,
            updated_at=now,
        )
    )


def _active_sync_state_statement(
    owner_username: str,
    store_id: int,
):
    return (
        select(SalesSyncStateModel)
        .where(
            SalesSyncStateModel.store_id == store_id,
            SalesSyncStateModel.owner_username == owner_username,
        )
        .with_for_update()
    )


def _local_recheck_order_numbers(
    session: Session,
    *,
    owner_username: str,
    store_id: int,
    now: datetime,
) -> list[str]:
    completed_cutoff = now - timedelta(days=COMPLETED_RECHECK_MAX_DAYS)
    incomplete_adjusted_cutoff = now - timedelta(
        days=INCOMPLETE_ADJUSTED_RECHECK_DAYS
    )
    orders = session.scalars(
        select(SalesOrderModel)
        .where(
            SalesOrderModel.owner_username == owner_username,
            SalesOrderModel.store_id == store_id,
            SalesOrderModel.ordered_at >= completed_cutoff,
        )
        .order_by(
            SalesOrderModel.ordered_at.desc(),
            SalesOrderModel.order_number.asc(),
        )
    ).all()
    adjusted_order_ids = set(
        session.scalars(
            select(SalesOrderItemModel.sales_order_id)
            .join(
                SalesItemAdjustmentModel,
                SalesItemAdjustmentModel.sales_order_item_id
                == SalesOrderItemModel.id,
            )
            .where(
                SalesOrderItemModel.owner_username == owner_username,
                SalesOrderItemModel.store_id == store_id,
                SalesItemAdjustmentModel.status.in_(
                    ("confirmed", "unresolved")
                ),
            )
        ).all()
    )

    result: list[str] = []
    for order in orders:
        completed = _stored_order_is_completed(order)
        adjusted = bool(
            order.id in adjusted_order_ids
            or order.has_unresolved_adjustment
            or order.is_canceled
        )
        if (
            order.ordered_at >= incomplete_adjusted_cutoff
            and (not completed or adjusted)
        ):
            result.append(order.order_number)
            continue
        if completed and _completed_order_recheck_due(order, now):
            result.append(order.order_number)
    return result


def _stored_order_is_completed(order: SalesOrderModel) -> bool:
    progress = _non_negative_int_or_none(order.order_progress)
    if progress is not None and progress >= 600:
        return True
    status = _text(order.order_status).lower()
    return any(
        token in status
        for token in (
            "complete",
            "completed",
            "closed",
            "final",
            "delivered",
            "shipped",
            "cancel",
            "refund",
            "return",
            "完了",
            "完成",
            "キャンセル",
        )
    )


def _completed_order_recheck_due(
    order: SalesOrderModel,
    now: datetime,
) -> bool:
    known_updates = [
        value
        for value in (
            order.last_synced_at,
            order.updated_at_remote,
        )
        if value is not None
    ]
    if not known_updates:
        return True
    return max(known_updates) <= now - COMPLETED_RECHECK_INTERVAL


def _dedupe_order_numbers(
    *groups: Iterable[Any],
) -> list[str]:
    result: list[str] = []
    seen: set[str] = set()
    for group in groups:
        for value in group:
            order_number = _text(value)
            if not order_number or order_number in seen:
                continue
            seen.add(order_number)
            result.append(order_number)
    return result


def _acquire_sync_lease(
    session: Session,
    *,
    owner_username: str,
    store_id: int,
    lease_status: str,
    now: datetime,
) -> bool:
    result = session.execute(
        _lease_acquisition_statement(
            owner_username,
            store_id,
            lease_status,
            now,
        )
    )
    return result.rowcount == 1


def _heartbeat_lease(
    session: Session,
    *,
    owner_username: str,
    store_id: int,
    lease_status: str,
    now: datetime,
    progress_current: int | None = None,
    progress_total: int | None = None,
) -> bool:
    values: dict[str, Any] = {"updated_at": now}
    if progress_current is not None:
        values["progress_current"] = progress_current
    if progress_total is not None:
        values["progress_total"] = progress_total
    result = session.execute(
        update(SalesSyncStateModel)
        .where(
            SalesSyncStateModel.store_id == store_id,
            SalesSyncStateModel.owner_username == owner_username,
            SalesSyncStateModel.sync_status == lease_status,
        )
        .values(**values)
    )
    return result.rowcount == 1


def _require_lease_heartbeat(
    session: Session,
    **kwargs: Any,
) -> None:
    if not _heartbeat_lease(session, **kwargs):
        raise SalesSyncLeaseLostError("销量同步租约已失效。")


def _heartbeat_lease_in_new_transaction(
    owner_username: str,
    store_id: int,
    lease_status: str,
    *,
    progress_current: int | None = None,
    progress_total: int | None = None,
) -> None:
    with session_scope() as session:
        _require_lease_heartbeat(
            session,
            owner_username=owner_username,
            store_id=store_id,
            lease_status=lease_status,
            now=_now(),
            progress_current=progress_current,
            progress_total=progress_total,
        )


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
        product_key = _daily_product_key(item)
        key = (
            item.ordered_at.date(),
            product_key,
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
) -> ReconcileOutcome:
    order_number = _first_text(payload, ("orderNumber",))
    if not order_number:
        raise IncompleteSnapshotError("订单号缺失。")

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
    if (
        order is not None
        and order.updated_at_remote is not None
        and remote_updated_at is None
    ):
        raise IncompleteSnapshotError("订单远端更新时间缺失。")
    if (
        order is not None
        and order.updated_at_remote is not None
        and remote_updated_at is not None
        and remote_updated_at < order.updated_at_remote
    ):
        order.last_synced_at = synced_at
        return ReconcileOutcome(
            affected_dates=set(),
            remote_updated_at=order.updated_at_remote,
            disposition="stale",
        )
    if (
        order is not None
        and order.updated_at_remote is not None
        and remote_updated_at == order.updated_at_remote
    ):
        order.last_synced_at = synced_at
        return ReconcileOutcome(
            affected_dates=set(),
            remote_updated_at=order.updated_at_remote,
            disposition="duplicate",
        )

    item_pairs = _validated_order_item_pairs(payload)
    order_canceled = _order_is_canceled(payload)
    full_refund = _order_is_full_refund(payload)
    full_return = _order_is_full_return(payload)
    if not item_pairs and not (
        order_canceled or full_refund or full_return
    ):
        raise IncompleteSnapshotError(
            "无明确取消、全额退款或全部退货的空商品快照不允许删除已有商品。"
        )

    ordered_at = _first_datetime(
        payload,
        (
            "orderDatetime",
            "orderDateTime",
            "orderDate",
            "orderTimestamp",
        ),
    ) or (order.ordered_at if order is not None else synced_at)
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
    order.is_canceled = order_canceled
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
    prepared_items: list[PreparedItemSnapshot] = []
    for position, (normalized_item, raw_item) in enumerate(
        item_pairs,
        start=1,
    ):
        item_detail_id = _snapshot_item_detail_id(
            order_number,
            normalized_item,
            position,
        )
        current_units = _non_negative_int(normalized_item.get("units"))
        item = existing_items.get(item_detail_id)
        ordered_units = (
            current_units if item is None else item.ordered_units
        )
        refund_units = _item_refund_units(raw_item)
        return_units = _item_return_units(raw_item)
        if full_refund:
            refund_units = max(refund_units, ordered_units)
        if full_return:
            return_units = max(return_units, ordered_units)
        derivation = derive_adjustments(
            ordered_units=ordered_units,
            latest_units=current_units,
            order_canceled=order.is_canceled,
            delete_item=(
                bool(normalized_item.get("deleteItemFlag"))
                and not (full_refund or full_return)
            ),
            canceled_units=_item_canceled_units(raw_item),
            refund_units=refund_units,
            return_units=return_units,
            return_refund=(
                _item_is_return_refund(raw_item)
                or (full_refund and return_units > 0)
                or (full_return and refund_units > 0)
            ),
        )
        prepared_items.append(
            PreparedItemSnapshot(
                position=position,
                normalized_item=normalized_item,
                raw_item=raw_item,
                item_detail_id=item_detail_id,
                current_units=current_units,
                ordered_units=ordered_units,
                unit_price=_first_decimal(
                    raw_item,
                    ("price", "priceTaxIncl"),
                ),
                derivation=derivation,
            )
        )

    attributed_refund_units = sum(
        prepared.derivation.refunded_units
        for prepared in prepared_items
    )
    attributed_return_units = sum(
        prepared.derivation.returned_units
        for prepared in prepared_items
    )
    attributed_refund_amount = sum(
        (
            _confirmed_attributed_amount(
                prepared.raw_item,
                reported_units=_item_refund_units(prepared.raw_item),
                confirmed_units=prepared.derivation.refunded_units,
                amount_keys=("refundedAmount", "refundAmount"),
                unit_price=prepared.unit_price,
            )
            for prepared in prepared_items
        ),
        Decimal("0"),
    )
    attributed_return_amount = sum(
        (
            _confirmed_attributed_amount(
                prepared.raw_item,
                reported_units=_item_return_units(prepared.raw_item),
                confirmed_units=prepared.derivation.returned_units,
                amount_keys=("returnedAmount", "returnAmount"),
                unit_price=prepared.unit_price,
            )
            for prepared in prepared_items
        ),
        Decimal("0"),
    )
    residual_refund_units = max(
        0,
        _order_refund_units(payload) - attributed_refund_units,
    )
    residual_return_units = max(
        0,
        _order_return_units(payload) - attributed_return_units,
    )
    residual_refund_amount = max(
        Decimal("0"),
        _order_refund_amount(payload) - attributed_refund_amount,
    )
    residual_return_amount = max(
        Decimal("0"),
        _order_return_amount(payload) - attributed_return_amount,
    )
    if full_refund:
        residual_refund_units = 0
        residual_refund_amount = Decimal("0")
    if full_return:
        residual_return_units = 0
        residual_return_amount = Decimal("0")
    has_unresolved_refund = _first_bool(
        payload,
        (
            "partialRefund",
            "hasPartialRefund",
            "unresolvedRefund",
        ),
    )
    has_unresolved_return = _first_bool(
        payload,
        (
            "partialReturn",
            "hasPartialReturn",
            "unresolvedReturn",
        ),
    )
    unresolved_refund = bool(
        residual_refund_units
        or residual_refund_amount
        or has_unresolved_refund
    )
    unresolved_return = bool(
        residual_return_units
        or residual_return_amount
        or has_unresolved_return
    )
    unresolved = unresolved_refund or unresolved_return
    seen_item_ids: set[str] = set()
    unresolved_trace_item = None
    if not prepared_items and unresolved and existing_items:
        unresolved_trace_item = min(
            existing_items.values(),
            key=lambda item: (
                item.id if item.id is not None else 0,
                item.item_detail_id,
            ),
        )

    for prepared in prepared_items:
        position = prepared.position
        normalized_item = prepared.normalized_item
        raw_item = prepared.raw_item
        item_detail_id = prepared.item_detail_id
        seen_item_ids.add(item_detail_id)

        current_units = prepared.current_units
        item = existing_items.get(item_detail_id)
        ordered_units = prepared.ordered_units
        item_unresolved_refund_units = (
            residual_refund_units if position == 1 else 0
        )
        item_unresolved_return_units = (
            residual_return_units if position == 1 else 0
        )
        item_unresolved_refund_amount = (
            residual_refund_amount if position == 1 else Decimal("0")
        )
        item_unresolved_return_amount = (
            residual_return_amount if position == 1 else Decimal("0")
        )
        derivation = replace(
            prepared.derivation,
            unresolved_refund_units=item_unresolved_refund_units,
            unresolved_return_units=item_unresolved_return_units,
            has_unresolved_refund=(
                unresolved_refund and position == 1
            ),
            has_unresolved_return=(
                unresolved_return and position == 1
            ),
        )
        sku_models = normalized_item.get("SkuModelList")
        sku_json = _json(sku_models if isinstance(sku_models, list) else [])
        sku_key = _sku_key(sku_json)
        unit_price = prepared.unit_price

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
            unresolved_refund_amount=item_unresolved_refund_amount,
            unresolved_return_amount=item_unresolved_return_amount,
        )

    for item_detail_id, item in existing_items.items():
        if item_detail_id in seen_item_ids:
            continue
        missing_refund_units = item.ordered_units if full_refund else 0
        missing_return_units = item.ordered_units if full_return else 0
        derivation = derive_adjustments(
            ordered_units=item.ordered_units,
            latest_units=0,
            order_canceled=order.is_canceled,
            delete_item=not (full_refund or full_return),
            refund_units=missing_refund_units,
            return_units=missing_return_units,
            return_refund=full_refund and full_return,
        )
        unresolved_refund_amount = Decimal("0")
        unresolved_return_amount = Decimal("0")
        raw_payload: dict[str, Any] = {
            "missingFromLatestSnapshot": True,
        }
        if item is unresolved_trace_item:
            derivation = replace(
                derivation,
                unresolved_refund_units=residual_refund_units,
                unresolved_return_units=residual_return_units,
                has_unresolved_refund=unresolved_refund,
                has_unresolved_return=unresolved_return,
            )
            unresolved_refund_amount = residual_refund_amount
            unresolved_return_amount = residual_return_amount
            raw_payload["orderLevelUnresolvedTrace"] = {
                key: payload.get(key)
                for key in (
                    "partialRefund",
                    "hasPartialRefund",
                    "unresolvedRefund",
                    "partialReturn",
                    "hasPartialReturn",
                    "unresolvedReturn",
                )
                if key in payload
            }
        item.latest_units = 0
        item.canceled_units = derivation.canceled_units
        item.refunded_units = derivation.refunded_units
        item.returned_units = derivation.returned_units
        item.effective_units = derivation.effective_units
        item.effective_amount = item.unit_price * derivation.effective_units
        item.delete_item_flag = True
        _reconcile_item_adjustments(
            session,
            item,
            derivation,
            raw_payload=raw_payload,
            remote_updated_at=remote_updated_at,
            unresolved_refund_amount=unresolved_refund_amount,
            unresolved_return_amount=unresolved_return_amount,
        )

    order.has_unresolved_adjustment = unresolved
    return ReconcileOutcome(
        affected_dates=affected_dates,
        remote_updated_at=remote_updated_at,
        disposition="applied",
    )


def _reconcile_item_adjustments(
    session: Session,
    item: SalesOrderItemModel,
    derivation: AdjustmentDerivation,
    *,
    raw_payload: dict[str, Any],
    remote_updated_at: datetime | None,
    unresolved_refund_amount: Decimal,
    unresolved_return_amount: Decimal,
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
    if (
        derivation.has_unresolved_refund
        or derivation.unresolved_refund_units
        or unresolved_refund_amount
    ):
        expected[f"{SNAPSHOT_ADJUSTMENT_SOURCE_PREFIX}refund_unresolved"] = {
            "adjustment_type": "refund",
            "units": derivation.unresolved_refund_units,
            "amount": unresolved_refund_amount,
            "status": "unresolved",
            "reason": "部分退款无法定位到具体商品",
        }
    if (
        derivation.has_unresolved_return
        or derivation.unresolved_return_units
        or unresolved_return_amount
    ):
        expected[f"{SNAPSHOT_ADJUSTMENT_SOURCE_PREFIX}return_unresolved"] = {
            "adjustment_type": "return",
            "units": derivation.unresolved_return_units,
            "amount": unresolved_return_amount,
            "status": "unresolved",
            "reason": "部分退货无法定位到具体商品",
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
    public_status = _public_sync_status(state.sync_status)
    payload = {
        "storeId": state.store_id,
        "ownerUsername": state.owner_username,
        "status": public_status,
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
            "status": public_status,
            "progressCurrent": state.progress_current,
            "progressTotal": state.progress_total,
        }
    return payload


def _public_sync_status(sync_status: str) -> str:
    if _text(sync_status).startswith("running"):
        return "running"
    return _text(sync_status)


def _validated_order_item_pairs(
    order: dict[str, Any],
) -> list[tuple[dict[str, Any], dict[str, Any]]]:
    if "PackageModelList" not in order:
        raise IncompleteSnapshotError("订单包裹列表缺失。")
    packages = order.get("PackageModelList")
    if not isinstance(packages, list):
        raise IncompleteSnapshotError("订单包裹列表格式无效。")

    raw_items: list[dict[str, Any]] = []
    for package in packages:
        if not isinstance(package, dict):
            raise IncompleteSnapshotError("订单包裹格式无效。")
        if "ItemModelList" not in package:
            raise IncompleteSnapshotError("订单商品列表缺失。")
        items = package.get("ItemModelList")
        if not isinstance(items, list):
            raise IncompleteSnapshotError("订单商品列表格式无效。")
        for item in items:
            if not isinstance(item, dict):
                raise IncompleteSnapshotError("订单商品格式无效。")
            if not _has_valid_item_units(item):
                raise IncompleteSnapshotError(
                    "订单商品数量缺失或格式无效。"
                )
            raw_items.append(item)

    normalized_items = list(rakuten_order_service.iter_order_items(order))
    if len(normalized_items) != len(raw_items):
        raise IncompleteSnapshotError("订单商品归一化结果不完整。")
    item_pairs = list(zip(normalized_items, raw_items))
    for normalized_item, raw_item in item_pairs:
        if not _has_item_identity(normalized_item):
            raise IncompleteSnapshotError("订单商品标识缺失。")
    return item_pairs


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


def _order_is_full_return(payload: dict[str, Any]) -> bool:
    if _first_bool(payload, ("isFullReturn", "fullReturn")):
        return True
    status = _first_text(
        payload,
        ("returnStatus", "orderStatus", "orderProgressName"),
    ).lower()
    if status in {"full", "fully_returned", "returned"}:
        return True
    return any(
        token in status
        for token in ("full return", "全部退货", "全返品")
    )


def _order_refund_units(payload: dict[str, Any]) -> int:
    return _first_int(
        payload,
        (
            "totalRefundUnits",
            "refundedUnits",
            "refundUnits",
            "unresolvedRefundUnits",
            "partialRefundUnits",
        ),
    )


def _order_refund_amount(payload: dict[str, Any]) -> Decimal:
    return _first_decimal(
        payload,
        (
            "totalRefundAmount",
            "refundedAmount",
            "refundAmount",
            "unresolvedRefundAmount",
            "partialRefundAmount",
        ),
    )


def _order_return_units(payload: dict[str, Any]) -> int:
    return _first_int(
        payload,
        (
            "totalReturnUnits",
            "returnedUnits",
            "returnUnits",
            "unresolvedReturnUnits",
            "partialReturnUnits",
        ),
    )


def _order_return_amount(payload: dict[str, Any]) -> Decimal:
    return _first_decimal(
        payload,
        (
            "totalReturnAmount",
            "returnedAmount",
            "returnAmount",
            "unresolvedReturnAmount",
            "partialReturnAmount",
        ),
    )


def _item_canceled_units(payload: dict[str, Any]) -> int:
    return _first_int(payload, ("canceledUnits", "cancelledUnits", "cancelUnits"))


def _item_refund_units(payload: dict[str, Any]) -> int:
    return _first_int(payload, ("refundedUnits", "refundUnits"))


def _item_return_units(payload: dict[str, Any]) -> int:
    return _first_int(payload, ("returnedUnits", "returnUnits"))


def _confirmed_attributed_amount(
    payload: dict[str, Any],
    *,
    reported_units: int,
    confirmed_units: int,
    amount_keys: Iterable[str],
    unit_price: Decimal,
) -> Decimal:
    if confirmed_units <= 0:
        return Decimal("0")
    explicit_amount = _first_decimal(payload, amount_keys)
    if explicit_amount > 0:
        if reported_units > confirmed_units:
            return (
                explicit_amount
                * Decimal(confirmed_units)
                / Decimal(reported_units)
            )
        return explicit_amount
    return unit_price * confirmed_units


def _item_is_return_refund(payload: dict[str, Any]) -> bool:
    return _first_bool(
        payload,
        (
            "returnRefund",
            "returnRefundFlag",
            "isReturnRefund",
        ),
    )


def _has_item_identity(payload: dict[str, Any]) -> bool:
    return any(
        _text(payload.get(key))
        for key in (
            "itemDetailId",
            "itemId",
            "itemNumber",
            "manageNumber",
        )
    )


def _has_valid_item_units(payload: dict[str, Any]) -> bool:
    if "units" not in payload or isinstance(payload.get("units"), bool):
        return False
    value = payload.get("units")
    try:
        parsed_units = int(value)
        exact_units = Decimal(str(value))
    except (InvalidOperation, OverflowError, TypeError, ValueError):
        return False
    return (
        exact_units.is_finite()
        and parsed_units >= 0
        and exact_units == Decimal(parsed_units)
    )


def _sku_key(sku_json: str) -> str:
    if sku_json == "[]":
        return ""
    digest = hashlib.sha256(sku_json.encode("utf-8")).hexdigest()
    return f"v1:{digest}"


def _daily_product_key(item: SalesOrderItemModel) -> str:
    manage_number = _text(item.manage_number)
    if manage_number:
        return manage_number
    item_number = _text(item.item_number)
    if item_number:
        return _bounded_fallback_product_key(
            "item-number",
            item_number,
        )
    item_id = _text(item.item_id)
    if item_id:
        return _bounded_fallback_product_key("item-id", item_id)
    return _bounded_fallback_product_key(
        "item-detail",
        _text(item.item_detail_id),
    )


def _bounded_fallback_product_key(prefix: str, value: str) -> str:
    readable = f"{prefix}:{value}"
    if len(readable) <= DAILY_PRODUCT_KEY_MAX_LENGTH:
        return readable
    digest = hashlib.sha256(value.encode("utf-8")).hexdigest()
    return f"v1:{prefix}:{digest}"


def _snapshot_item_detail_id(
    order_number: str,
    normalized_item: dict[str, Any],
    position: int,
) -> str:
    item_detail_id = _text(normalized_item.get("itemDetailId"))
    if item_detail_id:
        return item_detail_id
    line_fingerprint = _text(normalized_item.get("lineFingerprint"))
    if line_fingerprint:
        return line_fingerprint
    return _fallback_line_id(
        order_number,
        normalized_item,
        position,
    )


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
        raw_value = payload.get(key)
        if raw_value is None or isinstance(
            raw_value,
            (bool, bytes, bytearray, dict, list, set, tuple),
        ):
            continue
        normalized = _text(raw_value)
        if normalized:
            return normalized
    return ""


def _first_int(payload: dict[str, Any], keys: Iterable[str]) -> int:
    for key in keys:
        if key not in payload:
            continue
        parsed = _non_negative_int_or_none(payload.get(key))
        if parsed is not None:
            return parsed
    return 0


def _first_decimal(
    payload: dict[str, Any],
    keys: Iterable[str],
) -> Decimal:
    for key in keys:
        if key not in payload:
            continue
        value = payload.get(key)
        if (
            value is None
            or isinstance(value, bool)
            or (isinstance(value, str) and not value.strip())
        ):
            continue
        try:
            parsed = Decimal(str(value))
        except (InvalidOperation, OverflowError, TypeError, ValueError):
            continue
        if parsed.is_finite() and parsed >= 0:
            return parsed
    return Decimal("0")


def _first_bool(payload: dict[str, Any], keys: Iterable[str]) -> bool:
    for key in keys:
        if key not in payload:
            continue
        parsed = _bool_or_none(payload.get(key))
        if parsed is not None:
            return parsed
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
        return _utc_naive(value)
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
            return _utc_naive(parsed)
        except ValueError:
            continue
    return None


def _utc_naive(value: datetime) -> datetime:
    if value.tzinfo is not None and value.utcoffset() is not None:
        return value.astimezone(timezone.utc).replace(tzinfo=None)
    return value.replace(tzinfo=None)


def _non_negative_int_or_none(value: Any) -> int | None:
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, str) and not value.strip():
        return None
    try:
        parsed = int(value)
        exact = Decimal(str(value))
    except (InvalidOperation, OverflowError, TypeError, ValueError):
        return None
    if (
        not exact.is_finite()
        or parsed < 0
        or exact != Decimal(parsed)
    ):
        return None
    return parsed


def _non_negative_int(value: Any) -> int:
    try:
        return max(0, int(value))
    except (OverflowError, TypeError, ValueError):
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


def _bool_or_none(value: Any) -> bool | None:
    if isinstance(value, bool):
        return value
    if value is None:
        return None
    text = _text(value).lower()
    if text in {"1", "true", "yes", "on"}:
        return True
    if text in {"0", "false", "no", "off"}:
        return False
    return None


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
