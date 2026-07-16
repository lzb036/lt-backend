from __future__ import annotations

from contextlib import contextmanager
from datetime import date, datetime, timedelta
from decimal import Decimal
from threading import Event
import time as time_module
from unittest.mock import Mock

import pytest
from sqlalchemy import create_engine, event, func, select
from sqlalchemy.dialects import mysql
from sqlalchemy.orm import Session, sessionmaker

from app.db.database import Base
from app.db.models import (
    ProductSalesDailyModel,
    SalesItemAdjustmentModel,
    SalesOrderItemModel,
    SalesOrderModel,
    SalesSyncStateModel,
    StoreModel,
    UserAccountModel,
)
from app.services import sales_sync_service


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


def seed_store(
    session: Session,
    *,
    owner_username: str = "alice",
    store_code: str = "alice-shop",
) -> StoreModel:
    user = session.get(UserAccountModel, owner_username)
    if user is None:
        user = UserAccountModel(
            username=owner_username,
            display_name=owner_username.title(),
            password_salt_b64="salt",
            password_hash_b64="hash",
        )
        session.add(user)
        session.flush()
    store = StoreModel(
        owner_username=owner_username,
        store_code=store_code,
        store_name=f"{owner_username.title()} Shop",
        rakuten_service_secret_encrypted="encrypted-secret",
        rakuten_license_key_encrypted="encrypted-key",
    )
    session.add(store)
    session.flush()
    return store


def order_snapshot(
    *,
    order_number: str = "ORDER-1",
    units: int = 5,
    item_overrides: dict | None = None,
    order_overrides: dict | None = None,
) -> dict:
    item = {
        "itemDetailId": "detail-1",
        "itemId": "item-1",
        "manageNumber": "MN-1",
        "itemNumber": "ITEM-1",
        "itemName": "Demo Item",
        "SkuModelList": [{"variantId": "sku-blue"}],
        "units": units,
        "price": 100,
        "priceTaxIncl": 110,
        "deleteItemFlag": False,
        "restoreInventoryFlag": False,
    }
    item.update(item_overrides or {})
    order = {
        "orderNumber": order_number,
        "orderProgress": 300,
        "orderStatus": "normal",
        "orderDatetime": "2026-07-15T10:00:00",
        "updateDatetime": "2026-07-15T10:05:00",
        "totalPrice": units * 100,
        "currencyCode": "JPY",
        "PackageModelList": [{"ItemModelList": [item]}],
    }
    order.update(order_overrides or {})
    return order


def patch_local_sync_dependencies(
    monkeypatch,
    session_factory,
    snapshots: list[dict],
):
    @contextmanager
    def local_session_scope():
        session = session_factory()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    monkeypatch.setattr(sales_sync_service, "session_scope", local_session_scope)
    monkeypatch.setattr(sales_sync_service, "decrypt_text", lambda value: value)
    monkeypatch.setattr(
        sales_sync_service.rakuten_order_service,
        "search_order_numbers",
        lambda *_args, **_kwargs: [
            str(snapshot["orderNumber"]) for snapshot in snapshots
        ],
    )
    monkeypatch.setattr(
        sales_sync_service.rakuten_order_service,
        "get_orders",
        lambda *_args, **_kwargs: snapshots,
    )


@pytest.mark.parametrize(
    (
        "kwargs",
        "expected",
    ),
    [
        (
            {"ordered_units": 5},
            (0, 0, 0, 5),
        ),
        (
            {"ordered_units": 5, "order_canceled": True},
            (5, 0, 0, 0),
        ),
        (
            {"ordered_units": 5, "delete_item": True},
            (5, 0, 0, 0),
        ),
        (
            {"ordered_units": 5, "latest_units": 3},
            (2, 0, 0, 3),
        ),
        (
            {"ordered_units": 5, "refund_units": 2},
            (0, 2, 0, 3),
        ),
        (
            {"ordered_units": 5, "return_units": 2},
            (0, 0, 2, 3),
        ),
    ],
)
def test_derive_adjustments_calculates_confirmed_deductions(kwargs, expected):
    result = sales_sync_service.derive_adjustments(**kwargs)

    assert (
        result.canceled_units,
        result.refunded_units,
        result.returned_units,
        result.effective_units,
    ) == expected


def test_return_refund_is_deducted_once():
    result = sales_sync_service.derive_adjustments(
        ordered_units=5,
        refund_units=2,
        return_units=2,
        return_refund=True,
    )

    assert result.refunded_units == 0
    assert result.returned_units == 2
    assert result.effective_units == 3


def test_unresolved_partial_refund_does_not_reduce_effective_units():
    result = sales_sync_service.derive_adjustments(
        ordered_units=5,
        unresolved_refund_units=2,
    )

    assert result.refunded_units == 0
    assert result.effective_units == 5
    assert result.unresolved_refund_units == 2


def test_attributed_refund_explains_quantity_reduction_without_double_deduction():
    result = sales_sync_service.derive_adjustments(
        ordered_units=5,
        latest_units=3,
        refund_units=2,
    )

    assert result.canceled_units == 0
    assert result.refunded_units == 2
    assert result.effective_units == 3


def test_calculate_effective_units_clamps_at_zero():
    assert sales_sync_service.calculate_effective_units(2, 1, 1, 1) == 0


def test_sync_owned_store_enforces_owner_and_store_lookup(
    monkeypatch,
    session_factory,
):
    with session_factory() as session:
        store = seed_store(session, owner_username="alice")
        store_id = store.id
        session.commit()

    called = False

    @contextmanager
    def local_session_scope():
        session = session_factory()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def unexpected_search(*_args, **_kwargs):
        nonlocal called
        called = True
        return []

    monkeypatch.setattr(sales_sync_service, "session_scope", local_session_scope)
    monkeypatch.setattr(
        sales_sync_service.rakuten_order_service,
        "search_order_numbers",
        unexpected_search,
    )

    with pytest.raises(LookupError, match="店铺不存在或无权访问"):
        sales_sync_service.sync_owned_store("bob", store_id)

    assert not called


def test_sync_owned_store_returns_running_state_without_duplicate_api_call(
    monkeypatch,
    session_factory,
):
    now = datetime(2026, 7, 16, 12, 0, 0)
    with session_factory() as session:
        store = seed_store(session)
        state = SalesSyncStateModel(
            owner_username="alice",
            store_id=store.id,
            sync_status="running",
            progress_current=3,
            progress_total=10,
        )
        session.add(state)
        session.flush()
        state.updated_at = now
        store_id = store.id
        session.commit()

    called = False

    @contextmanager
    def local_session_scope():
        session = session_factory()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def unexpected_search(*_args, **_kwargs):
        nonlocal called
        called = True
        return []

    monkeypatch.setattr(sales_sync_service, "session_scope", local_session_scope)
    monkeypatch.setattr(sales_sync_service, "_now", lambda: now)
    monkeypatch.setattr(
        sales_sync_service.rakuten_order_service,
        "search_order_numbers",
        unexpected_search,
    )

    result = sales_sync_service.sync_owned_store("alice", store_id)

    assert result["status"] == "running"
    assert result["alreadyRunning"] is True
    assert result["progressCurrent"] == 3
    assert result["progressTotal"] == 10
    assert not called


def test_lease_acquisition_uses_atomic_conditional_update_for_mysql():
    now = datetime(2026, 7, 16, 12, 0, 0)
    statement = sales_sync_service._lease_acquisition_statement(
        "alice",
        7,
        "running:0123456789abcdef0123",
        now,
    )

    compiled = str(
        statement.compile(
            dialect=mysql.dialect(),
            compile_kwargs={"literal_binds": True},
        )
    ).replace("%%", "%")

    assert compiled.startswith("UPDATE lt_sales_sync_states SET")
    assert "sync_status='running:0123456789abcdef0123'" in compiled
    assert "owner_username = 'alice'" in compiled
    assert "store_id = 7" in compiled
    assert "sync_status NOT LIKE 'running%'" in compiled
    assert "updated_at < '2026-07-16 11:50:00'" in compiled


@pytest.mark.parametrize(
    ("rowcount", "expected"),
    [
        (1, True),
        (0, False),
        (-1, False),
    ],
)
def test_lease_acquisition_requires_exactly_one_updated_row(
    rowcount,
    expected,
):
    session = Mock(spec=Session)
    session.execute.return_value.rowcount = rowcount

    acquired = sales_sync_service._acquire_sync_lease(
        session,
        owner_username="alice",
        store_id=7,
        lease_status="running:0123456789abcdef0123",
        now=datetime(2026, 7, 16, 12, 0, 0),
    )

    assert acquired is expected
    session.execute.assert_called_once()


def test_active_sync_state_read_uses_mysql_current_read():
    statement = sales_sync_service._active_sync_state_statement(
        "alice",
        7,
    )

    compiled = str(
        statement.compile(
            dialect=mysql.dialect(),
            compile_kwargs={"literal_binds": True},
        )
    )

    assert "owner_username = 'alice'" in compiled
    assert "store_id = 7" in compiled
    assert compiled.endswith("FOR UPDATE")


def test_failed_acquisition_reads_active_state_in_fresh_transaction(
    monkeypatch,
    session_factory,
):
    now = datetime(2026, 7, 16, 12, 0, 0)
    with session_factory() as session:
        store = seed_store(session)
        session.add(
            SalesSyncStateModel(
                owner_username="alice",
                store_id=store.id,
                sync_status="running:active-owner-token",
                progress_current=2,
                progress_total=8,
                updated_at=now,
            )
        )
        store_id = store.id
        session.commit()

    scope_sessions: list[Session] = []

    @contextmanager
    def local_session_scope():
        session = session_factory()
        scope_sessions.append(session)
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    monkeypatch.setattr(sales_sync_service, "session_scope", local_session_scope)
    monkeypatch.setattr(sales_sync_service, "_now", lambda: now)
    monkeypatch.setattr(
        sales_sync_service,
        "_acquire_sync_lease",
        lambda *_args, **_kwargs: False,
    )
    monkeypatch.setattr(
        sales_sync_service.rakuten_order_service,
        "search_order_numbers",
        lambda *_args, **_kwargs: pytest.fail("API call must not run"),
    )

    result = sales_sync_service.sync_owned_store("alice", store_id)

    assert result["status"] == "running"
    assert result["alreadyRunning"] is True
    assert result["progressCurrent"] == 2
    assert result["progressTotal"] == 8
    assert len(scope_sessions) == 3
    assert len({id(session) for session in scope_sessions}) == 3


def test_periodic_lease_heartbeat_starts_ticks_and_stops(monkeypatch):
    calls: list[tuple[int | None, int | None]] = []
    ticked = Event()

    def fake_heartbeat(
        _owner_username,
        _store_id,
        _lease_status,
        *,
        progress_current=None,
        progress_total=None,
    ):
        calls.append((progress_current, progress_total))
        if len(calls) >= 2:
            ticked.set()

    monkeypatch.setattr(
        sales_sync_service,
        "_heartbeat_lease_in_new_transaction",
        fake_heartbeat,
    )

    heartbeat = sales_sync_service._PeriodicLeaseHeartbeat(
        "alice",
        7,
        "running:owner-token",
        interval_seconds=0.01,
    )
    with heartbeat:
        heartbeat.set_progress(1, 5)
        assert ticked.wait(1)
        heartbeat.raise_if_failed()

    stopped_count = len(calls)
    time_module.sleep(0.04)

    assert stopped_count >= 2
    assert len(calls) == stopped_count
    assert calls[-1] == (1, 5)


def test_periodic_lease_heartbeat_propagates_token_loss(monkeypatch):
    token_lost = Event()
    call_count = 0

    def fake_heartbeat(*_args, **_kwargs):
        nonlocal call_count
        call_count += 1
        if call_count >= 2:
            token_lost.set()
            raise sales_sync_service.SalesSyncLeaseLostError(
                "销量同步租约已失效。"
            )

    monkeypatch.setattr(
        sales_sync_service,
        "_heartbeat_lease_in_new_transaction",
        fake_heartbeat,
    )

    heartbeat = sales_sync_service._PeriodicLeaseHeartbeat(
        "alice",
        7,
        "running:owner-token",
        interval_seconds=0.01,
    )

    with pytest.raises(
        sales_sync_service.SalesSyncLeaseLostError,
        match="销量同步租约已失效",
    ):
        with heartbeat:
            assert token_lost.wait(1)
            heartbeat.raise_if_failed()

    stopped_count = call_count
    time_module.sleep(0.04)
    assert call_count == stopped_count


def test_periodic_heartbeat_covers_search_get_and_reconciliation(
    monkeypatch,
    session_factory,
):
    with session_factory() as session:
        store = seed_store(session)
        store_id = store.id
        session.commit()

    heartbeat_count = 0
    heartbeat_changed = Event()

    def fake_heartbeat(*_args, **_kwargs):
        nonlocal heartbeat_count
        heartbeat_count += 1
        heartbeat_changed.set()

    def wait_for_next_heartbeat():
        baseline = heartbeat_count
        deadline = time_module.monotonic() + 1
        while heartbeat_count <= baseline:
            heartbeat_changed.clear()
            remaining = deadline - time_module.monotonic()
            assert remaining > 0
            assert heartbeat_changed.wait(remaining)

    @contextmanager
    def local_session_scope():
        session = session_factory()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    snapshot = order_snapshot()
    original_reconcile = sales_sync_service._reconcile_order_snapshot

    def slow_search(*_args, **_kwargs):
        wait_for_next_heartbeat()
        return ["ORDER-1"]

    def slow_get(*_args, **_kwargs):
        wait_for_next_heartbeat()
        return [snapshot]

    def slow_reconcile(*args, **kwargs):
        wait_for_next_heartbeat()
        return original_reconcile(*args, **kwargs)

    monkeypatch.setattr(sales_sync_service, "session_scope", local_session_scope)
    monkeypatch.setattr(sales_sync_service, "decrypt_text", lambda value: value)
    monkeypatch.setattr(
        sales_sync_service,
        "SALES_SYNC_HEARTBEAT_INTERVAL_SECONDS",
        0.01,
    )
    monkeypatch.setattr(
        sales_sync_service,
        "_heartbeat_lease_in_new_transaction",
        fake_heartbeat,
    )
    monkeypatch.setattr(
        sales_sync_service.rakuten_order_service,
        "search_order_numbers",
        slow_search,
    )
    monkeypatch.setattr(
        sales_sync_service.rakuten_order_service,
        "get_orders",
        slow_get,
    )
    monkeypatch.setattr(
        sales_sync_service,
        "_reconcile_order_snapshot",
        slow_reconcile,
    )

    result = sales_sync_service.sync_owned_store("alice", store_id)

    assert result["status"] == "completed"
    assert heartbeat_count >= 4


def test_periodic_token_loss_aborts_sync_before_get_order(
    monkeypatch,
    session_factory,
):
    with session_factory() as session:
        store = seed_store(session)
        store_id = store.id
        session.commit()

    heartbeat_count = 0
    token_lost = Event()
    get_called = False

    def fake_heartbeat(*_args, **_kwargs):
        nonlocal heartbeat_count
        heartbeat_count += 1
        if heartbeat_count >= 2:
            token_lost.set()
            raise sales_sync_service.SalesSyncLeaseLostError(
                "销量同步租约已失效。"
            )

    @contextmanager
    def local_session_scope():
        session = session_factory()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def slow_search(*_args, **_kwargs):
        assert token_lost.wait(1)
        return ["ORDER-1"]

    def unexpected_get(*_args, **_kwargs):
        nonlocal get_called
        get_called = True
        return []

    monkeypatch.setattr(sales_sync_service, "session_scope", local_session_scope)
    monkeypatch.setattr(sales_sync_service, "decrypt_text", lambda value: value)
    monkeypatch.setattr(
        sales_sync_service,
        "SALES_SYNC_HEARTBEAT_INTERVAL_SECONDS",
        0.01,
    )
    monkeypatch.setattr(
        sales_sync_service,
        "_heartbeat_lease_in_new_transaction",
        fake_heartbeat,
    )
    monkeypatch.setattr(
        sales_sync_service.rakuten_order_service,
        "search_order_numbers",
        slow_search,
    )
    monkeypatch.setattr(
        sales_sync_service.rakuten_order_service,
        "get_orders",
        unexpected_get,
    )

    with pytest.raises(
        sales_sync_service.SalesSyncLeaseLostError,
        match="销量同步租约已失效",
    ):
        sales_sync_service.sync_owned_store("alice", store_id)

    assert get_called is False


def test_lease_heartbeat_requires_current_token(session_factory):
    now = datetime(2026, 7, 16, 12, 0, 0)
    with session_factory() as session:
        store = seed_store(session)
        state = SalesSyncStateModel(
            owner_username="alice",
            store_id=store.id,
            sync_status="running:new-owner-token",
            progress_current=2,
            progress_total=5,
        )
        session.add(state)
        session.commit()

        updated = sales_sync_service._heartbeat_lease(
            session,
            owner_username="alice",
            store_id=store.id,
            lease_status="running:old-owner-token",
            now=now,
            progress_current=4,
            progress_total=5,
        )
        session.commit()
        session.refresh(state)

    assert updated is False
    assert state.sync_status == "running:new-owner-token"
    assert state.progress_current == 2


def test_lease_heartbeat_updates_current_token(session_factory):
    now = datetime(2026, 7, 16, 12, 0, 0)
    with session_factory() as session:
        store = seed_store(session)
        state = SalesSyncStateModel(
            owner_username="alice",
            store_id=store.id,
            sync_status="running:current-owner-token",
            progress_current=1,
            progress_total=5,
        )
        session.add(state)
        session.commit()

        updated = sales_sync_service._heartbeat_lease(
            session,
            owner_username="alice",
            store_id=store.id,
            lease_status="running:current-owner-token",
            now=now,
            progress_current=3,
            progress_total=7,
        )
        session.commit()
        session.refresh(state)

    assert updated is True
    assert state.updated_at == now
    assert state.progress_current == 3
    assert state.progress_total == 7


def test_heartbeat_pulses_use_fresh_short_lived_sessions(
    monkeypatch,
    session_factory,
):
    with session_factory() as session:
        store = seed_store(session)
        session.add(
            SalesSyncStateModel(
                owner_username="alice",
                store_id=store.id,
                sync_status="running:owner-token",
            )
        )
        store_id = store.id
        session.commit()

    opened_sessions: list[Session] = []

    @contextmanager
    def local_session_scope():
        session = session_factory()
        opened_sessions.append(session)
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    monkeypatch.setattr(sales_sync_service, "session_scope", local_session_scope)

    sales_sync_service._heartbeat_lease_in_new_transaction(
        "alice",
        store_id,
        "running:owner-token",
    )
    sales_sync_service._heartbeat_lease_in_new_transaction(
        "alice",
        store_id,
        "running:owner-token",
    )

    assert len(opened_sessions) == 2
    assert opened_sessions[0] is not opened_sessions[1]


def test_stale_running_lease_is_reclaimed(
    monkeypatch,
    session_factory,
):
    now = datetime(2026, 7, 16, 12, 0, 0)
    with session_factory() as session:
        store = seed_store(session)
        state = SalesSyncStateModel(
            owner_username="alice",
            store_id=store.id,
            sync_status="running:stale-owner-token",
            progress_current=1,
            progress_total=10,
        )
        session.add(state)
        session.flush()
        state.updated_at = (
            now
            - sales_sync_service.SALES_SYNC_LEASE_TIMEOUT
            - timedelta(seconds=1)
        )
        store_id = store.id
        session.commit()

    patch_local_sync_dependencies(monkeypatch, session_factory, [])
    monkeypatch.setattr(sales_sync_service, "_now", lambda: now)

    result = sales_sync_service.sync_owned_store("alice", store_id)

    assert result["status"] == "completed"
    assert result["alreadyRunning"] is False


def test_lost_lease_error_path_does_not_overwrite_new_owner(
    monkeypatch,
    session_factory,
):
    now = datetime(2026, 7, 16, 12, 0, 0)
    with session_factory() as session:
        store = seed_store(session)
        store_id = store.id
        session.commit()

    @contextmanager
    def local_session_scope():
        session = session_factory()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def steal_lease_then_fail(*_args, **_kwargs):
        with session_factory() as session:
            state = session.get(SalesSyncStateModel, store_id)
            assert state is not None
            state.sync_status = "running:new-owner-token"
            state.updated_at = now
            session.commit()
        raise RuntimeError("remote failure")

    monkeypatch.setattr(sales_sync_service, "session_scope", local_session_scope)
    monkeypatch.setattr(sales_sync_service, "decrypt_text", lambda value: value)
    monkeypatch.setattr(sales_sync_service, "_now", lambda: now)
    monkeypatch.setattr(
        sales_sync_service.rakuten_order_service,
        "search_order_numbers",
        steal_lease_then_fail,
    )

    with pytest.raises(RuntimeError, match="remote failure"):
        sales_sync_service.sync_owned_store("alice", store_id)

    with session_factory() as session:
        state = session.get(SalesSyncStateModel, store_id)

    assert state is not None
    assert state.sync_status == "running:new-owner-token"
    assert state.last_error is None


def test_credential_decryption_failure_marks_current_lease_error(
    monkeypatch,
    session_factory,
):
    now = datetime(2026, 7, 16, 12, 0, 0)
    with session_factory() as session:
        store = seed_store(session)
        store_id = store.id
        session.commit()

    @contextmanager
    def local_session_scope():
        session = session_factory()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    monkeypatch.setattr(sales_sync_service, "session_scope", local_session_scope)
    monkeypatch.setattr(sales_sync_service, "_now", lambda: now)
    monkeypatch.setattr(
        sales_sync_service,
        "decrypt_text",
        lambda _value: (_ for _ in ()).throw(RuntimeError("decrypt failed")),
    )

    with pytest.raises(RuntimeError, match="decrypt failed"):
        sales_sync_service.sync_owned_store("alice", store_id)

    with session_factory() as session:
        state = session.get(SalesSyncStateModel, store_id)

    assert state is not None
    assert state.sync_status == "error"
    assert state.last_error == "销量同步失败，请稍后重试。"


def test_initial_sync_searches_default_90_days_without_local_rechecks(
    monkeypatch,
    session_factory,
):
    now = datetime(2026, 7, 16, 12, 0, 0)
    with session_factory() as session:
        store = seed_store(session)
        session.add(
            SalesOrderModel(
                owner_username="alice",
                store_id=store.id,
                order_number="LOCAL-INCOMPLETE-20",
                order_progress="300",
                order_status="normal",
                ordered_at=now - timedelta(days=20),
                updated_at_remote=now - timedelta(days=20),
                raw_order_json="{}",
                last_synced_at=now - timedelta(hours=1),
            )
        )
        store_id = store.id
        session.commit()

    search_call: dict[str, object] = {}
    requested_order_numbers: list[str] = []

    @contextmanager
    def local_session_scope():
        session = session_factory()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def fake_search(
        _secret,
        _key,
        start_at,
        end_at,
        statuses,
    ):
        search_call.update(
            {
                "startAt": start_at,
                "endAt": end_at,
                "statuses": statuses,
            }
        )
        return ["FIRST-REMOTE"]

    def fake_get(_secret, _key, order_numbers):
        requested_order_numbers.extend(order_numbers)
        return [
            order_snapshot(order_number=order_number)
            for order_number in order_numbers
        ]

    monkeypatch.setattr(sales_sync_service, "session_scope", local_session_scope)
    monkeypatch.setattr(sales_sync_service, "decrypt_text", lambda value: value)
    monkeypatch.setattr(sales_sync_service, "_now", lambda: now)
    monkeypatch.setattr(
        sales_sync_service.rakuten_order_service,
        "search_order_numbers",
        fake_search,
    )
    monkeypatch.setattr(
        sales_sync_service.rakuten_order_service,
        "get_orders",
        fake_get,
    )

    result = sales_sync_service.sync_owned_store("alice", store_id)

    with session_factory() as session:
        state = session.get(SalesSyncStateModel, store_id)

    assert result["status"] == "completed"
    assert search_call["startAt"] == now - timedelta(days=90)
    assert search_call["endAt"] == now
    assert search_call["statuses"] == sales_sync_service.RAKUTEN_ORDER_STATUSES
    assert requested_order_numbers == ["FIRST-REMOTE"]
    assert state is not None
    assert state.initial_sync_completed is True


def test_initial_sync_missing_order_detail_preserves_full_retry_window(
    monkeypatch,
    session_factory,
):
    now = datetime(2026, 7, 16, 12, 0, 0)
    with session_factory() as session:
        store = seed_store(session)
        store_id = store.id
        session.commit()

    search_starts: list[datetime] = []
    get_calls = 0

    @contextmanager
    def local_session_scope():
        session = session_factory()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def fake_search(
        _secret,
        _key,
        start_at,
        _end_at,
        _statuses,
    ):
        search_starts.append(start_at)
        return ["DAY-45-ORDER"]

    def fake_get(_secret, _key, _order_numbers):
        nonlocal get_calls
        get_calls += 1
        if get_calls == 1:
            return []
        return [order_snapshot(order_number="DAY-45-ORDER")]

    monkeypatch.setattr(sales_sync_service, "session_scope", local_session_scope)
    monkeypatch.setattr(sales_sync_service, "decrypt_text", lambda value: value)
    monkeypatch.setattr(sales_sync_service, "_now", lambda: now)
    monkeypatch.setattr(
        sales_sync_service.rakuten_order_service,
        "search_order_numbers",
        fake_search,
    )
    monkeypatch.setattr(
        sales_sync_service.rakuten_order_service,
        "get_orders",
        fake_get,
    )

    with pytest.raises(RuntimeError, match="订单详情不完整"):
        sales_sync_service.sync_owned_store("alice", store_id)

    with session_factory() as session:
        failed_state = session.get(SalesSyncStateModel, store_id)
    assert failed_state is not None
    assert failed_state.initial_sync_completed is False
    assert failed_state.sync_status == "error"

    result = sales_sync_service.sync_owned_store("alice", store_id)

    with session_factory() as session:
        completed_state = session.get(SalesSyncStateModel, store_id)
    assert result["status"] == "completed"
    assert completed_state is not None
    assert completed_state.initial_sync_completed is True
    assert search_starts == [
        now - timedelta(days=90),
        now - timedelta(days=90),
    ]


@pytest.mark.parametrize(
    "returned_orders",
    [
        [
            {
                "orderDatetime": "2026-07-15T10:00:00",
                "updateDatetime": "2026-07-15T10:05:00",
                "PackageModelList": [],
            }
        ],
        [order_snapshot(order_number="UNEXPECTED-ORDER")],
        [
            order_snapshot(
                order_number="EXPECTED-ORDER",
                order_overrides={
                    "PackageModelList": [
                        {"ItemModelList": ["malformed-item"]}
                    ]
                },
            )
        ],
    ],
)
def test_initial_sync_malformed_order_detail_does_not_mark_complete(
    monkeypatch,
    session_factory,
    returned_orders,
):
    with session_factory() as session:
        store = seed_store(session)
        store_id = store.id
        session.commit()

    @contextmanager
    def local_session_scope():
        session = session_factory()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    monkeypatch.setattr(sales_sync_service, "session_scope", local_session_scope)
    monkeypatch.setattr(sales_sync_service, "decrypt_text", lambda value: value)
    monkeypatch.setattr(
        sales_sync_service.rakuten_order_service,
        "search_order_numbers",
        lambda *_args, **_kwargs: ["EXPECTED-ORDER"],
    )
    monkeypatch.setattr(
        sales_sync_service.rakuten_order_service,
        "get_orders",
        lambda *_args, **_kwargs: returned_orders,
    )

    with pytest.raises(RuntimeError, match="订单详情不完整"):
        sales_sync_service.sync_owned_store("alice", store_id)

    with session_factory() as session:
        state = session.get(SalesSyncStateModel, store_id)
        orders = session.scalars(
            select(SalesOrderModel).where(
                SalesOrderModel.store_id == store_id
            )
        ).all()
    assert state is not None
    assert state.initial_sync_completed is False
    assert state.sync_status == "error"
    assert orders == []


def test_later_sync_searches_7_days_and_adds_local_recheck_candidates(
    monkeypatch,
    session_factory,
):
    now = datetime(2026, 7, 16, 12, 0, 0)

    def add_order(
        session,
        store,
        order_number,
        *,
        age_days,
        completed,
        last_synced_age,
        unresolved=False,
    ):
        order = SalesOrderModel(
            owner_username="alice",
            store_id=store.id,
            order_number=order_number,
            order_progress="700" if completed else "300",
            order_status="completed" if completed else "normal",
            ordered_at=now - timedelta(days=age_days),
            updated_at_remote=now - timedelta(days=age_days),
            has_unresolved_adjustment=unresolved,
            raw_order_json="{}",
            last_synced_at=now - last_synced_age,
        )
        session.add(order)
        session.flush()
        return order

    with session_factory() as session:
        store = seed_store(session)
        session.add(
            SalesSyncStateModel(
                owner_username="alice",
                store_id=store.id,
                initial_sync_completed=True,
                sync_status="idle",
            )
        )
        add_order(
            session,
            store,
            "RECENT-DUP",
            age_days=5,
            completed=False,
            last_synced_age=timedelta(hours=1),
        )
        add_order(
            session,
            store,
            "INCOMPLETE-20",
            age_days=20,
            completed=False,
            last_synced_age=timedelta(hours=1),
        )
        adjusted = add_order(
            session,
            store,
            "ADJUSTED-25",
            age_days=25,
            completed=True,
            last_synced_age=timedelta(hours=1),
        )
        adjusted_item = SalesOrderItemModel.from_service_payload(
            owner_username="alice",
            store_id=store.id,
            sales_order_id=adjusted.id,
            order_number=adjusted.order_number,
            item_detail_id="adjusted-detail",
            manage_number="MN-ADJUSTED",
            item_number="ITEM-ADJUSTED",
            item_id="item-adjusted",
            ordered_units=1,
            ordered_at=adjusted.ordered_at,
        )
        session.add(adjusted_item)
        session.flush()
        session.add(
            SalesItemAdjustmentModel(
                owner_username="alice",
                store_id=store.id,
                sales_order_item_id=adjusted_item.id,
                adjustment_type="refund",
                units=1,
                amount=Decimal("100"),
                source="seed:adjusted",
                status="confirmed",
                reason="seed",
                raw_payload_json="{}",
            )
        )
        add_order(
            session,
            store,
            "COMPLETED-DUE-80",
            age_days=80,
            completed=True,
            last_synced_age=timedelta(days=2),
        )
        add_order(
            session,
            store,
            "COMPLETED-FRESH-80",
            age_days=80,
            completed=True,
            last_synced_age=timedelta(hours=12),
        )
        add_order(
            session,
            store,
            "INCOMPLETE-31",
            age_days=31,
            completed=False,
            last_synced_age=timedelta(days=2),
        )
        add_order(
            session,
            store,
            "COMPLETED-DUE-91",
            age_days=91,
            completed=True,
            last_synced_age=timedelta(days=2),
        )
        store_id = store.id
        session.commit()

    search_call: dict[str, object] = {}
    requested_order_numbers: list[str] = []

    @contextmanager
    def local_session_scope():
        session = session_factory()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def fake_search(
        _secret,
        _key,
        start_at,
        end_at,
        statuses,
    ):
        search_call.update(
            {
                "startAt": start_at,
                "endAt": end_at,
                "statuses": statuses,
            }
        )
        return [
            "RECENT-DUP",
            "RECENT-ONLY",
            "RECENT-DUP",
            "",
        ]

    def fake_get(_secret, _key, order_numbers):
        requested_order_numbers.extend(order_numbers)
        return [
            order_snapshot(order_number=order_number)
            for order_number in order_numbers
        ]

    monkeypatch.setattr(sales_sync_service, "session_scope", local_session_scope)
    monkeypatch.setattr(sales_sync_service, "decrypt_text", lambda value: value)
    monkeypatch.setattr(sales_sync_service, "_now", lambda: now)
    monkeypatch.setattr(
        sales_sync_service.rakuten_order_service,
        "search_order_numbers",
        fake_search,
    )
    monkeypatch.setattr(
        sales_sync_service.rakuten_order_service,
        "get_orders",
        fake_get,
    )

    result = sales_sync_service.sync_owned_store(
        "alice",
        store_id,
        initial_days=1,
    )

    assert result["status"] == "completed"
    assert search_call["startAt"] == now - timedelta(days=7)
    assert search_call["endAt"] == now
    assert search_call["statuses"] == sales_sync_service.RAKUTEN_ORDER_STATUSES
    assert requested_order_numbers == [
        "RECENT-DUP",
        "RECENT-ONLY",
        "INCOMPLETE-20",
        "ADJUSTED-25",
        "COMPLETED-DUE-80",
    ]


def test_syncing_same_snapshot_twice_is_idempotent(
    monkeypatch,
    session_factory,
):
    with session_factory() as session:
        store = seed_store(session)
        store_id = store.id
        session.commit()

    snapshot = order_snapshot(item_overrides={"refundUnits": 2})
    patch_local_sync_dependencies(
        monkeypatch,
        session_factory,
        [snapshot],
    )

    first = sales_sync_service.sync_owned_store("alice", store_id)
    second = sales_sync_service.sync_owned_store("alice", store_id)

    with session_factory() as session:
        assert session.scalar(select(func.count(SalesOrderModel.id))) == 1
        assert session.scalar(select(func.count(SalesOrderItemModel.id))) == 1
        assert (
            session.scalar(select(func.count(SalesItemAdjustmentModel.id)))
            == 1
        )
        daily = session.scalars(select(ProductSalesDailyModel)).one()
        item = session.scalars(select(SalesOrderItemModel)).one()

    assert first["status"] == "completed"
    assert second["status"] == "completed"
    assert item.ordered_units == 5
    assert item.latest_units == 5
    assert item.product_key == sales_sync_service._daily_product_key(item)
    assert item.product_key == "MN-1"
    assert daily.ordered_units == 5
    assert item.refunded_units == 2
    assert item.effective_units == 3
    assert daily.refunded_units == 2
    assert daily.effective_units == 3
    assert daily.order_count == 1


def test_fallback_line_ids_survive_reordering_of_different_lines(
    monkeypatch,
    session_factory,
):
    with session_factory() as session:
        store = seed_store(session)
        store_id = store.id
        session.commit()

    line_a = {
        "itemDetailId": "",
        "itemId": "item-a",
        "manageNumber": "MN-A",
        "itemNumber": "ITEM-A",
        "itemName": "Item A",
        "SkuModelList": [{"variantId": "blue"}],
        "units": 1,
        "price": 100,
    }
    line_b = {
        "itemDetailId": "",
        "itemId": "item-b",
        "manageNumber": "MN-B",
        "itemNumber": "ITEM-B",
        "itemName": "Item B",
        "SkuModelList": [{"variantId": "red"}],
        "units": 1,
        "price": 200,
    }
    snapshots = [
        order_snapshot(
            order_number="REORDER-1",
            order_overrides={
                "updateDatetime": "2026-07-15T10:05:00",
                "PackageModelList": [
                    {"ItemModelList": [line_a, line_b]}
                ],
            },
        )
    ]
    patch_local_sync_dependencies(monkeypatch, session_factory, snapshots)

    sales_sync_service.sync_owned_store("alice", store_id)
    with session_factory() as session:
        first_ids = {
            item.item_number: item.item_detail_id
            for item in session.scalars(
                select(SalesOrderItemModel).where(
                    SalesOrderItemModel.store_id == store_id
                )
            ).all()
        }

    snapshots[0] = order_snapshot(
        order_number="REORDER-1",
        order_overrides={
            "updateDatetime": "2026-07-15T10:06:00",
            "PackageModelList": [
                {"ItemModelList": [line_b, line_a]}
            ],
        },
    )
    sales_sync_service.sync_owned_store("alice", store_id)

    with session_factory() as session:
        items = session.scalars(
            select(SalesOrderItemModel).where(
                SalesOrderItemModel.store_id == store_id
            )
        ).all()
        second_ids = {
            item.item_number: item.item_detail_id
            for item in items
        }

    assert len(items) == 2
    assert second_ids == first_ids


def test_aware_rakuten_timestamp_uses_shanghai_sales_day(
    monkeypatch,
    session_factory,
):
    now = datetime(2026, 7, 16, 12, 0, 0)
    with session_factory() as session:
        store = seed_store(session)
        store_id = store.id
        session.commit()

    snapshot = order_snapshot(
        order_number="TIMEZONE-1",
        order_overrides={
            "orderDatetime": "2026-07-16T00:30:00+09:00",
            "updateDatetime": "2026-07-16T00:35:00+09:00",
        },
    )
    patch_local_sync_dependencies(
        monkeypatch,
        session_factory,
        [snapshot],
    )
    monkeypatch.setattr(sales_sync_service, "_now", lambda: now)

    result = sales_sync_service.sync_owned_store("alice", store_id)

    with session_factory() as session:
        order = session.scalars(select(SalesOrderModel)).one()
        item = session.scalars(select(SalesOrderItemModel)).one()
        daily = session.scalars(select(ProductSalesDailyModel)).one()

    assert order.ordered_at == datetime(2026, 7, 15, 23, 30, 0)
    assert item.ordered_at == datetime(2026, 7, 15, 23, 30, 0)
    assert daily.sales_date == date(2026, 7, 15)
    assert result["lastRemoteUpdatedAt"] == (
        "2026-07-15T23:35:00+08:00"
    )
    assert result["lastSuccessfulSyncAt"] == (
        "2026-07-16T12:00:00+08:00"
    )


def test_newer_snapshot_is_not_overwritten_by_older_snapshot(
    monkeypatch,
    session_factory,
):
    with session_factory() as session:
        store = seed_store(session)
        store_id = store.id
        session.commit()

    snapshots = [
        order_snapshot(
            item_overrides={"refundUnits": 2},
            order_overrides={"updateDatetime": "2026-07-15T10:10:00"},
        )
    ]
    patch_local_sync_dependencies(monkeypatch, session_factory, snapshots)
    sales_sync_service.sync_owned_store("alice", store_id)

    snapshots[0] = order_snapshot(
        order_overrides={"updateDatetime": "2026-07-15T10:05:00"},
    )
    result = sales_sync_service.sync_owned_store("alice", store_id)

    with session_factory() as session:
        order = session.scalars(select(SalesOrderModel)).one()
        item = session.scalars(select(SalesOrderItemModel)).one()
        adjustments = session.scalars(
            select(SalesItemAdjustmentModel)
        ).all()

    assert result["staleOrderCount"] == 1
    assert order.updated_at_remote == datetime(2026, 7, 15, 10, 10, 0)
    assert item.refunded_units == 2
    assert item.effective_units == 3
    assert len(adjustments) == 1
    assert adjustments[0].status == "confirmed"


def test_newer_snapshot_wins_when_same_batch_contains_older_duplicate(
    monkeypatch,
    session_factory,
):
    with session_factory() as session:
        store = seed_store(session)
        store_id = store.id
        session.commit()

    snapshots = [
        order_snapshot(
            item_overrides={"refundUnits": 2},
            order_overrides={"updateDatetime": "2026-07-15T10:10:00"},
        ),
        order_snapshot(
            order_overrides={"updateDatetime": "2026-07-15T10:05:00"},
        ),
    ]
    patch_local_sync_dependencies(monkeypatch, session_factory, snapshots)

    result = sales_sync_service.sync_owned_store("alice", store_id)

    with session_factory() as session:
        order = session.scalars(select(SalesOrderModel)).one()
        item = session.scalars(select(SalesOrderItemModel)).one()
        adjustments = session.scalars(
            select(SalesItemAdjustmentModel)
        ).all()

    assert result["staleOrderCount"] == 1
    assert order.updated_at_remote == datetime(2026, 7, 15, 10, 10, 0)
    assert item.refunded_units == 2
    assert item.effective_units == 3
    assert len(adjustments) == 1
    assert adjustments[0].status == "confirmed"


def test_equivalent_offset_timestamp_keeps_first_accepted_snapshot(
    monkeypatch,
    session_factory,
):
    with session_factory() as session:
        store = seed_store(session)
        store_id = store.id
        session.commit()

    snapshots = [
        order_snapshot(
            item_overrides={"refundUnits": 2},
            order_overrides={
                "updateDatetime": "2026-07-15T10:10:00+09:00"
            },
        ),
        order_snapshot(
            order_overrides={"updateDatetime": "2026-07-15T01:10:00Z"},
        ),
    ]
    patch_local_sync_dependencies(monkeypatch, session_factory, snapshots)

    sales_sync_service.sync_owned_store("alice", store_id)

    with session_factory() as session:
        order = session.scalars(select(SalesOrderModel)).one()
        item = session.scalars(select(SalesOrderItemModel)).one()
        adjustment = session.scalars(select(SalesItemAdjustmentModel)).one()

    assert order.updated_at_remote == datetime(2026, 7, 15, 9, 10, 0)
    assert item.refunded_units == 2
    assert item.effective_units == 3
    assert adjustment.status == "confirmed"


def test_equal_timestamp_conflict_cannot_reverse_accepted_snapshot(
    monkeypatch,
    session_factory,
):
    with session_factory() as session:
        store = seed_store(session)
        store_id = store.id
        session.commit()

    snapshots = [
        order_snapshot(
            item_overrides={"refundUnits": 2},
            order_overrides={"updateDatetime": "2026-07-15T10:10:00"},
        )
    ]
    patch_local_sync_dependencies(monkeypatch, session_factory, snapshots)
    sales_sync_service.sync_owned_store("alice", store_id)

    snapshots[0] = order_snapshot(
        order_overrides={"updateDatetime": "2026-07-15T10:10:00"},
    )
    sales_sync_service.sync_owned_store("alice", store_id)

    with session_factory() as session:
        item = session.scalars(select(SalesOrderItemModel)).one()
        adjustment = session.scalars(select(SalesItemAdjustmentModel)).one()

    assert item.refunded_units == 2
    assert item.effective_units == 3
    assert adjustment.status == "confirmed"


def test_snapshot_without_remote_version_does_not_overwrite_versioned_order(
    monkeypatch,
    session_factory,
):
    with session_factory() as session:
        store = seed_store(session)
        store_id = store.id
        session.commit()

    snapshots = [
        order_snapshot(
            item_overrides={"refundUnits": 2},
            order_overrides={"updateDatetime": "2026-07-15T10:10:00"},
        )
    ]
    patch_local_sync_dependencies(monkeypatch, session_factory, snapshots)
    sales_sync_service.sync_owned_store("alice", store_id)

    snapshots[0] = order_snapshot()
    snapshots[0].pop("updateDatetime")
    with pytest.raises(
        sales_sync_service.SalesSyncIncompleteError,
        match="订单详情不完整",
    ):
        sales_sync_service.sync_owned_store("alice", store_id)

    with session_factory() as session:
        order = session.scalars(select(SalesOrderModel)).one()
        item = session.scalars(select(SalesOrderItemModel)).one()
        state = session.get(SalesSyncStateModel, store_id)

    assert order.updated_at_remote == datetime(2026, 7, 15, 10, 10, 0)
    assert item.refunded_units == 2
    assert item.effective_units == 3
    assert state is not None
    assert state.initial_sync_completed is True
    assert state.sync_status == "error"


def test_newer_quantity_reduction_preserves_first_ordered_units(
    monkeypatch,
    session_factory,
):
    with session_factory() as session:
        store = seed_store(session)
        store_id = store.id
        session.commit()

    snapshots = [
        order_snapshot(
            units=5,
            order_overrides={"updateDatetime": "2026-07-15T10:05:00"},
        )
    ]
    patch_local_sync_dependencies(monkeypatch, session_factory, snapshots)
    sales_sync_service.sync_owned_store("alice", store_id)

    snapshots[0] = order_snapshot(
        units=3,
        order_overrides={"updateDatetime": "2026-07-15T10:10:00"},
    )
    sales_sync_service.sync_owned_store("alice", store_id)

    with session_factory() as session:
        item = session.scalars(select(SalesOrderItemModel)).one()
        daily = session.scalars(select(ProductSalesDailyModel)).one()

    assert item.ordered_units == 5
    assert item.latest_units == 3
    assert item.canceled_units == 2
    assert item.effective_units == 3
    assert daily.ordered_units == 5
    assert daily.canceled_units == 2
    assert daily.effective_units == 3


def test_reconciliation_preserves_first_units_and_reverts_removed_adjustment(
    monkeypatch,
    session_factory,
):
    with session_factory() as session:
        store = seed_store(session)
        store_id = store.id
        session.commit()

    snapshots = [order_snapshot(item_overrides={"refundUnits": 2})]
    patch_local_sync_dependencies(monkeypatch, session_factory, snapshots)
    sales_sync_service.sync_owned_store("alice", store_id)

    snapshots[0] = order_snapshot(
        units=5,
        order_overrides={"updateDatetime": "2026-07-15T10:10:00"},
    )
    sales_sync_service.sync_owned_store("alice", store_id)

    with session_factory() as session:
        item = session.scalars(select(SalesOrderItemModel)).one()
        adjustment = session.scalars(select(SalesItemAdjustmentModel)).one()
        daily = session.scalars(select(ProductSalesDailyModel)).one()

    assert item.ordered_units == 5
    assert item.latest_units == 5
    assert item.refunded_units == 0
    assert item.effective_units == 5
    assert adjustment.adjustment_type == "refund"
    assert adjustment.status == "reverted"
    assert daily.effective_units == 5


def test_canceled_order_with_well_formed_empty_items_cancels_existing_lines(
    monkeypatch,
    session_factory,
):
    with session_factory() as session:
        store = seed_store(session)
        store_id = store.id
        session.commit()

    snapshots = [order_snapshot()]
    patch_local_sync_dependencies(monkeypatch, session_factory, snapshots)
    sales_sync_service.sync_owned_store("alice", store_id)

    snapshots[0] = order_snapshot(
        order_overrides={
            "isCanceled": True,
            "orderStatus": "canceled",
            "updateDatetime": "2026-07-15T10:10:00",
        }
    )
    snapshots[0]["PackageModelList"] = []
    sales_sync_service.sync_owned_store("alice", store_id)

    with session_factory() as session:
        item = session.scalars(select(SalesOrderItemModel)).one()
        adjustment = session.scalars(select(SalesItemAdjustmentModel)).one()

    assert item.ordered_units == 5
    assert item.latest_units == 0
    assert item.canceled_units == 5
    assert item.effective_units == 0
    assert adjustment.adjustment_type == "cancel"
    assert adjustment.status == "confirmed"


@pytest.mark.parametrize(
    "malformed_packages",
    [
        None,
        [{"ItemModelList": {"not": "a list"}}],
        [{"missingItemModelList": True}],
    ],
)
def test_incomplete_snapshot_fails_without_canceling_existing_lines(
    monkeypatch,
    session_factory,
    malformed_packages,
):
    with session_factory() as session:
        store = seed_store(session)
        store_id = store.id
        session.commit()

    snapshots = [order_snapshot()]
    patch_local_sync_dependencies(monkeypatch, session_factory, snapshots)
    sales_sync_service.sync_owned_store("alice", store_id)

    snapshots[0] = order_snapshot(
        order_overrides={"updateDatetime": "2026-07-15T10:10:00"},
    )
    if malformed_packages is None:
        snapshots[0].pop("PackageModelList")
    else:
        snapshots[0]["PackageModelList"] = malformed_packages
    with pytest.raises(
        sales_sync_service.SalesSyncIncompleteError,
        match="订单详情不完整",
    ):
        sales_sync_service.sync_owned_store("alice", store_id)

    with session_factory() as session:
        item = session.scalars(select(SalesOrderItemModel)).one()

    assert item.latest_units == 5
    assert item.canceled_units == 0
    assert item.effective_units == 5


def test_nonterminal_empty_snapshot_fails_without_canceling_lines(
    monkeypatch,
    session_factory,
):
    with session_factory() as session:
        store = seed_store(session)
        store_id = store.id
        session.commit()

    snapshots = [order_snapshot()]
    patch_local_sync_dependencies(monkeypatch, session_factory, snapshots)
    sales_sync_service.sync_owned_store("alice", store_id)

    snapshots[0] = order_snapshot(
        order_overrides={
            "orderStatus": "normal",
            "orderProgress": 300,
            "updateDatetime": "2026-07-15T10:10:00",
        }
    )
    snapshots[0]["PackageModelList"] = []
    with pytest.raises(
        sales_sync_service.SalesSyncIncompleteError,
        match="订单详情不完整",
    ):
        sales_sync_service.sync_owned_store("alice", store_id)

    with session_factory() as session:
        item = session.scalars(select(SalesOrderItemModel)).one()

    assert item.latest_units == 5
    assert item.canceled_units == 0


def test_completed_empty_snapshot_fails_without_canceling_lines(
    monkeypatch,
    session_factory,
):
    with session_factory() as session:
        store = seed_store(session)
        store_id = store.id
        session.commit()

    snapshots = [order_snapshot()]
    patch_local_sync_dependencies(monkeypatch, session_factory, snapshots)
    sales_sync_service.sync_owned_store("alice", store_id)

    snapshots[0] = order_snapshot(
        order_overrides={
            "orderStatus": "completed",
            "orderProgress": 700,
            "updateDatetime": "2026-07-15T10:10:00",
        }
    )
    snapshots[0]["PackageModelList"] = []
    with pytest.raises(
        sales_sync_service.SalesSyncIncompleteError,
        match="订单详情不完整",
    ):
        sales_sync_service.sync_owned_store("alice", store_id)

    with session_factory() as session:
        item = session.scalars(select(SalesOrderItemModel)).one()

    assert item.latest_units == 5
    assert item.canceled_units == 0
    assert item.effective_units == 5


@pytest.mark.parametrize(
    "malformed_item",
    [
        {
            "itemDetailId": "detail-1",
            "itemId": "item-1",
            "manageNumber": "MN-1",
        },
        {
            "itemDetailId": "detail-1",
            "itemId": "item-1",
            "manageNumber": "MN-1",
            "units": "not-a-number",
        },
        {
            "itemDetailId": "detail-1",
            "itemId": "item-1",
            "manageNumber": "MN-1",
            "units": "5.0",
        },
        {
            "itemDetailId": "detail-1",
            "itemId": "item-1",
            "manageNumber": "MN-1",
            "units": float("inf"),
        },
        {
            "units": 5,
        },
    ],
)
def test_incomplete_item_record_fails_without_canceling_lines(
    monkeypatch,
    session_factory,
    malformed_item,
):
    with session_factory() as session:
        store = seed_store(session)
        store_id = store.id
        session.commit()

    snapshots = [order_snapshot()]
    patch_local_sync_dependencies(monkeypatch, session_factory, snapshots)
    sales_sync_service.sync_owned_store("alice", store_id)

    snapshots[0] = order_snapshot(
        order_overrides={"updateDatetime": "2026-07-15T10:10:00"},
    )
    snapshots[0]["PackageModelList"] = [
        {"ItemModelList": [malformed_item]}
    ]
    with pytest.raises(
        sales_sync_service.SalesSyncIncompleteError,
        match="订单详情不完整",
    ):
        sales_sync_service.sync_owned_store("alice", store_id)

    with session_factory() as session:
        items = session.scalars(
            select(SalesOrderItemModel).order_by(
                SalesOrderItemModel.id.asc()
            )
        ).all()

    assert len(items) == 1
    assert items[0].item_detail_id == "detail-1"
    assert items[0].latest_units == 5
    assert items[0].canceled_units == 0
    assert items[0].effective_units == 5


def test_unattributed_partial_refund_is_unresolved_and_not_deducted(
    monkeypatch,
    session_factory,
):
    with session_factory() as session:
        store = seed_store(session)
        store_id = store.id
        session.commit()

    snapshot = order_snapshot(
        order_overrides={
            "partialRefund": True,
            "refundAmount": 200,
        },
    )
    patch_local_sync_dependencies(
        monkeypatch,
        session_factory,
        [snapshot],
    )

    sales_sync_service.sync_owned_store("alice", store_id)

    with session_factory() as session:
        order = session.scalars(select(SalesOrderModel)).one()
        item = session.scalars(select(SalesOrderItemModel)).one()
        adjustment = session.scalars(select(SalesItemAdjustmentModel)).one()

    assert order.has_unresolved_adjustment is True
    assert item.effective_units == 5
    assert adjustment.adjustment_type == "refund"
    assert adjustment.status == "unresolved"
    assert adjustment.amount == Decimal("200")


def test_partial_item_attribution_creates_unresolved_residual(
    monkeypatch,
    session_factory,
):
    with session_factory() as session:
        store = seed_store(session)
        store_id = store.id
        session.commit()

    snapshot = order_snapshot(
        item_overrides={
            "refundUnits": 2,
            "refundAmount": 200,
        },
        order_overrides={
            "partialRefund": True,
            "refundUnits": 3,
            "refundAmount": 300,
        },
    )
    patch_local_sync_dependencies(
        monkeypatch,
        session_factory,
        [snapshot],
    )

    sales_sync_service.sync_owned_store("alice", store_id)

    with session_factory() as session:
        order = session.scalars(select(SalesOrderModel)).one()
        item = session.scalars(select(SalesOrderItemModel)).one()
        adjustments = session.scalars(
            select(SalesItemAdjustmentModel).order_by(
                SalesItemAdjustmentModel.status.asc()
            )
        ).all()

    assert order.has_unresolved_adjustment is True
    assert item.refunded_units == 2
    assert item.effective_units == 3
    assert {
        (row.status, row.units, row.amount)
        for row in adjustments
    } == {
        ("confirmed", 2, Decimal("200")),
        ("unresolved", 1, Decimal("100")),
    }


def test_inferred_refund_amount_is_subtracted_from_order_residual(
    monkeypatch,
    session_factory,
):
    with session_factory() as session:
        store = seed_store(session)
        store_id = store.id
        session.commit()

    snapshot = order_snapshot(
        item_overrides={"refundUnits": 2},
        order_overrides={
            "partialRefund": True,
            "refundUnits": 3,
            "refundAmount": 300,
        },
    )
    patch_local_sync_dependencies(
        monkeypatch,
        session_factory,
        [snapshot],
    )

    sales_sync_service.sync_owned_store("alice", store_id)

    with session_factory() as session:
        adjustments = session.scalars(
            select(SalesItemAdjustmentModel).order_by(
                SalesItemAdjustmentModel.status.asc()
            )
        ).all()

    assert {
        (row.status, row.units, row.amount)
        for row in adjustments
    } == {
        ("confirmed", 2, Decimal("200")),
        ("unresolved", 1, Decimal("100")),
    }


def test_residual_amount_uses_only_clamped_confirmed_item_units(
    monkeypatch,
    session_factory,
):
    with session_factory() as session:
        store = seed_store(session)
        store_id = store.id
        session.commit()

    snapshot = order_snapshot(
        item_overrides={"refundUnits": 10},
        order_overrides={
            "partialRefund": True,
            "refundUnits": 7,
            "refundAmount": 700,
        },
    )
    patch_local_sync_dependencies(
        monkeypatch,
        session_factory,
        [snapshot],
    )

    sales_sync_service.sync_owned_store("alice", store_id)

    with session_factory() as session:
        item = session.scalars(select(SalesOrderItemModel)).one()
        adjustments = session.scalars(
            select(SalesItemAdjustmentModel).order_by(
                SalesItemAdjustmentModel.status.asc()
            )
        ).all()

    assert item.refunded_units == 5
    assert {
        (row.status, row.units, row.amount)
        for row in adjustments
    } == {
        ("confirmed", 5, Decimal("500")),
        ("unresolved", 2, Decimal("200")),
    }


def test_partial_return_attribution_creates_unresolved_residual(
    monkeypatch,
    session_factory,
):
    with session_factory() as session:
        store = seed_store(session)
        store_id = store.id
        session.commit()

    snapshot = order_snapshot(
        item_overrides={
            "returnUnits": 2,
            "returnAmount": 200,
        },
        order_overrides={
            "partialReturn": True,
            "returnUnits": 3,
            "returnAmount": 300,
        },
    )
    patch_local_sync_dependencies(
        monkeypatch,
        session_factory,
        [snapshot],
    )

    sales_sync_service.sync_owned_store("alice", store_id)

    with session_factory() as session:
        order = session.scalars(select(SalesOrderModel)).one()
        item = session.scalars(select(SalesOrderItemModel)).one()
        adjustments = session.scalars(
            select(SalesItemAdjustmentModel).order_by(
                SalesItemAdjustmentModel.status.asc()
            )
        ).all()

    assert order.has_unresolved_adjustment is True
    assert item.returned_units == 2
    assert item.effective_units == 3
    assert {
        (
            row.adjustment_type,
            row.status,
            row.units,
            row.amount,
        )
        for row in adjustments
    } == {
        ("return", "confirmed", 2, Decimal("200")),
        ("return", "unresolved", 1, Decimal("100")),
    }


def test_inferred_return_amount_is_subtracted_from_order_residual(
    monkeypatch,
    session_factory,
):
    with session_factory() as session:
        store = seed_store(session)
        store_id = store.id
        session.commit()

    snapshot = order_snapshot(
        item_overrides={"returnUnits": 2},
        order_overrides={
            "partialReturn": True,
            "returnUnits": 3,
            "returnAmount": 300,
        },
    )
    patch_local_sync_dependencies(
        monkeypatch,
        session_factory,
        [snapshot],
    )

    sales_sync_service.sync_owned_store("alice", store_id)

    with session_factory() as session:
        adjustments = session.scalars(
            select(SalesItemAdjustmentModel).order_by(
                SalesItemAdjustmentModel.status.asc()
            )
        ).all()

    assert {
        (
            row.adjustment_type,
            row.status,
            row.units,
            row.amount,
        )
        for row in adjustments
    } == {
        ("return", "confirmed", 2, Decimal("200")),
        ("return", "unresolved", 1, Decimal("100")),
    }


@pytest.mark.parametrize(
    ("flag_name", "adjustment_type"),
    [
        ("partialRefund", "refund"),
        ("hasPartialRefund", "refund"),
        ("unresolvedRefund", "refund"),
        ("partialReturn", "return"),
        ("hasPartialReturn", "return"),
        ("unresolvedReturn", "return"),
    ],
)
def test_partial_flags_create_zero_value_unresolved_marker(
    monkeypatch,
    session_factory,
    flag_name,
    adjustment_type,
):
    with session_factory() as session:
        store = seed_store(session)
        store_id = store.id
        session.commit()

    snapshot = order_snapshot(order_overrides={flag_name: True})
    patch_local_sync_dependencies(
        monkeypatch,
        session_factory,
        [snapshot],
    )

    sales_sync_service.sync_owned_store("alice", store_id)

    with session_factory() as session:
        order = session.scalars(select(SalesOrderModel)).one()
        adjustment = session.scalars(select(SalesItemAdjustmentModel)).one()

    assert order.has_unresolved_adjustment is True
    assert adjustment.adjustment_type == adjustment_type
    assert adjustment.status == "unresolved"
    assert adjustment.units == 0
    assert adjustment.amount == Decimal("0")


def test_alias_parsers_skip_null_and_malformed_values_end_to_end(
    monkeypatch,
    session_factory,
):
    with session_factory() as session:
        store = seed_store(session)
        store_id = store.id
        session.commit()

    snapshot = order_snapshot(
        item_overrides={
            "refundedUnits": None,
            "refundUnits": 2,
            "refundedAmount": "invalid",
            "refundAmount": 200,
        },
        order_overrides={
            "partialRefund": None,
            "hasPartialRefund": True,
            "orderStatus": {"malformed": True},
            "status": "normal",
            "currencyCode": ["malformed"],
            "currency": "USD",
            "totalRefundUnits": None,
            "refundedUnits": "invalid",
            "refundUnits": 3,
            "totalRefundAmount": None,
            "refundedAmount": "invalid",
            "refundAmount": 300,
        },
    )
    patch_local_sync_dependencies(
        monkeypatch,
        session_factory,
        [snapshot],
    )

    sales_sync_service.sync_owned_store("alice", store_id)

    with session_factory() as session:
        order = session.scalars(select(SalesOrderModel)).one()
        item = session.scalars(select(SalesOrderItemModel)).one()
        adjustments = session.scalars(
            select(SalesItemAdjustmentModel).order_by(
                SalesItemAdjustmentModel.status.asc()
            )
        ).all()

    assert order.order_status == "normal"
    assert order.currency == "USD"
    assert item.refunded_units == 2
    assert {
        (row.status, row.units, row.amount)
        for row in adjustments
    } == {
        ("confirmed", 2, Decimal("200")),
        ("unresolved", 1, Decimal("100")),
    }


def test_boolean_alias_parser_skips_null_full_refund_flag(
    monkeypatch,
    session_factory,
):
    with session_factory() as session:
        store = seed_store(session)
        store_id = store.id
        session.commit()

    snapshot = order_snapshot(
        order_overrides={
            "isFullRefund": None,
            "fullRefund": True,
        },
    )
    patch_local_sync_dependencies(
        monkeypatch,
        session_factory,
        [snapshot],
    )

    sales_sync_service.sync_owned_store("alice", store_id)

    with session_factory() as session:
        item = session.scalars(select(SalesOrderItemModel)).one()

    assert item.refunded_units == 5
    assert item.effective_units == 0


def test_full_refund_fallback_is_not_suppressed_by_partial_item_attribution(
    monkeypatch,
    session_factory,
):
    with session_factory() as session:
        store = seed_store(session)
        store_id = store.id
        session.commit()

    snapshot = order_snapshot(
        item_overrides={
            "refundUnits": 2,
            "refundAmount": 200,
        },
        order_overrides={
            "isFullRefund": True,
            "refundUnits": 5,
            "refundAmount": 500,
        },
    )
    patch_local_sync_dependencies(
        monkeypatch,
        session_factory,
        [snapshot],
    )

    sales_sync_service.sync_owned_store("alice", store_id)

    with session_factory() as session:
        order = session.scalars(select(SalesOrderModel)).one()
        item = session.scalars(select(SalesOrderItemModel)).one()
        adjustments = session.scalars(
            select(SalesItemAdjustmentModel)
        ).all()

    assert order.has_unresolved_adjustment is False
    assert item.refunded_units == 5
    assert item.effective_units == 0
    assert len(adjustments) == 1
    assert adjustments[0].status == "confirmed"
    assert adjustments[0].units == 5


def test_terminal_empty_full_refund_uses_refund_fallback_for_existing_lines(
    monkeypatch,
    session_factory,
):
    with session_factory() as session:
        store = seed_store(session)
        store_id = store.id
        session.commit()

    snapshots = [order_snapshot()]
    patch_local_sync_dependencies(monkeypatch, session_factory, snapshots)
    sales_sync_service.sync_owned_store("alice", store_id)

    snapshots[0] = order_snapshot(
        order_overrides={
            "isFullRefund": True,
            "orderStatus": "refunded",
            "refundUnits": 5,
            "refundAmount": 500,
            "updateDatetime": "2026-07-15T10:10:00",
        }
    )
    snapshots[0]["PackageModelList"] = []
    sales_sync_service.sync_owned_store("alice", store_id)

    with session_factory() as session:
        item = session.scalars(select(SalesOrderItemModel)).one()
        adjustment = session.scalars(select(SalesItemAdjustmentModel)).one()

    assert item.canceled_units == 0
    assert item.refunded_units == 5
    assert item.effective_units == 0
    assert adjustment.adjustment_type == "refund"
    assert adjustment.status == "confirmed"


@pytest.mark.parametrize(
    ("terminal_overrides", "adjustment_type"),
    [
        (
            {
                "isFullRefund": True,
                "partialRefund": True,
                "orderStatus": "refunded",
            },
            "refund",
        ),
        (
            {
                "isFullReturn": True,
                "unresolvedReturn": True,
                "orderStatus": "returned",
            },
            "return",
        ),
    ],
)
def test_empty_terminal_partial_flags_persist_item_unresolved_trace(
    monkeypatch,
    session_factory,
    terminal_overrides,
    adjustment_type,
):
    with session_factory() as session:
        store = seed_store(session)
        store_id = store.id
        session.commit()

    snapshots = [order_snapshot()]
    patch_local_sync_dependencies(monkeypatch, session_factory, snapshots)
    sales_sync_service.sync_owned_store("alice", store_id)

    snapshots[0] = order_snapshot(
        order_overrides={
            **terminal_overrides,
            "updateDatetime": "2026-07-15T10:10:00",
        }
    )
    snapshots[0]["PackageModelList"] = []
    sales_sync_service.sync_owned_store("alice", store_id)

    with session_factory() as session:
        order = session.scalars(select(SalesOrderModel)).one()
        item = session.scalars(select(SalesOrderItemModel)).one()
        unresolved_rows = session.scalars(
            select(SalesItemAdjustmentModel).where(
                SalesItemAdjustmentModel.status == "unresolved"
            )
        ).all()

    assert order.has_unresolved_adjustment is True
    assert len(unresolved_rows) == 1
    assert unresolved_rows[0].sales_order_item_id == item.id
    assert unresolved_rows[0].adjustment_type == adjustment_type
    assert unresolved_rows[0].units == 0
    assert unresolved_rows[0].amount == Decimal("0")


def test_sync_rebuilds_multi_order_multi_sku_daily_aggregates(
    monkeypatch,
    session_factory,
):
    with session_factory() as session:
        store = seed_store(session)
        store_id = store.id
        session.commit()

    snapshots = [
        order_snapshot(
            order_number="ORDER-BLUE-1",
            units=2,
            item_overrides={
                "itemDetailId": "blue-1",
                "SkuModelList": [{"variantId": "blue"}],
            },
        ),
        order_snapshot(
            order_number="ORDER-BLUE-2",
            units=1,
            item_overrides={
                "itemDetailId": "blue-2",
                "SkuModelList": [{"variantId": "blue"}],
            },
        ),
        order_snapshot(
            order_number="ORDER-RED-1",
            units=4,
            item_overrides={
                "itemDetailId": "red-1",
                "SkuModelList": [{"variantId": "red"}],
            },
        ),
    ]
    patch_local_sync_dependencies(
        monkeypatch,
        session_factory,
        snapshots,
    )

    sales_sync_service.sync_owned_store("alice", store_id)

    with session_factory() as session:
        rows = session.scalars(
            select(ProductSalesDailyModel).order_by(
                ProductSalesDailyModel.ordered_units.asc()
            )
        ).all()

    assert len(rows) == 2
    assert {
        (row.order_count, row.ordered_units, row.effective_units)
        for row in rows
    } == {
        (2, 3, 3),
        (1, 4, 4),
    }


def test_daily_aggregation_separates_blank_manage_number_products(
    monkeypatch,
    session_factory,
):
    with session_factory() as session:
        store = seed_store(session)
        store_id = store.id
        session.commit()

    snapshots = [
        order_snapshot(
            order_number="ORDER-A",
            units=2,
            item_overrides={
                "itemDetailId": "detail-a",
                "manageNumber": "",
                "itemNumber": "ITEM-A",
                "itemId": "item-a",
                "SkuModelList": [{"variantId": "same-sku"}],
            },
        ),
        order_snapshot(
            order_number="ORDER-B",
            units=3,
            item_overrides={
                "itemDetailId": "detail-b",
                "manageNumber": "",
                "itemNumber": "ITEM-B",
                "itemId": "item-b",
                "SkuModelList": [{"variantId": "same-sku"}],
            },
        ),
        order_snapshot(
            order_number="ORDER-C",
            units=4,
            item_overrides={
                "itemDetailId": "detail-c",
                "manageNumber": "",
                "itemNumber": "",
                "itemId": "item-c",
                "SkuModelList": [{"variantId": "same-sku"}],
            },
        ),
    ]
    patch_local_sync_dependencies(
        monkeypatch,
        session_factory,
        snapshots,
    )

    sales_sync_service.sync_owned_store("alice", store_id)

    with session_factory() as session:
        rows = session.scalars(
            select(ProductSalesDailyModel).order_by(
                ProductSalesDailyModel.manage_number.asc()
            )
        ).all()

    assert len(rows) == 3
    assert {
        (row.manage_number, row.ordered_units, row.order_count)
        for row in rows
    } == {
        ("item-id:item-c", 4, 1),
        ("item-number:ITEM-A", 2, 1),
        ("item-number:ITEM-B", 3, 1),
    }


def test_daily_fallback_keys_bound_and_distinguish_255_char_identifiers(
    monkeypatch,
    session_factory,
):
    with session_factory() as session:
        store = seed_store(session)
        store_id = store.id
        session.commit()

    item_number_a = "A" * 255
    item_number_b = ("A" * 254) + "B"
    item_id_c = "C" * 255
    snapshots = [
        order_snapshot(
            order_number="ORDER-LONG-A",
            item_overrides={
                "itemDetailId": "detail-long-a",
                "manageNumber": "",
                "itemNumber": item_number_a,
                "itemId": "item-a",
                "SkuModelList": [{"variantId": "same-sku"}],
            },
        ),
        order_snapshot(
            order_number="ORDER-LONG-B",
            item_overrides={
                "itemDetailId": "detail-long-b",
                "manageNumber": "",
                "itemNumber": item_number_b,
                "itemId": "item-b",
                "SkuModelList": [{"variantId": "same-sku"}],
            },
        ),
        order_snapshot(
            order_number="ORDER-LONG-C",
            item_overrides={
                "itemDetailId": "detail-long-c",
                "manageNumber": "",
                "itemNumber": "",
                "itemId": item_id_c,
                "SkuModelList": [{"variantId": "same-sku"}],
            },
        ),
    ]
    patch_local_sync_dependencies(
        monkeypatch,
        session_factory,
        snapshots,
    )

    sales_sync_service.sync_owned_store("alice", store_id)

    with session_factory() as session:
        rows = session.scalars(select(ProductSalesDailyModel)).all()
        items = session.scalars(
            select(SalesOrderItemModel).order_by(
                SalesOrderItemModel.order_number.asc()
            )
        ).all()

    product_keys = {row.manage_number for row in rows}
    assert len(rows) == 3
    assert len(product_keys) == 3
    assert all(len(product_key) <= 255 for product_key in product_keys)
    assert sum(
        product_key.startswith("v1:item-number:")
        for product_key in product_keys
    ) == 2
    assert sum(
        product_key.startswith("v1:item-id:")
        for product_key in product_keys
    ) == 1
    assert [item.item_number for item in items] == [
        item_number_a,
        item_number_b,
        "",
    ]
    assert items[2].item_id == item_id_c


def test_numeric_aliases_skip_overflow_and_non_finite_values_end_to_end(
    monkeypatch,
    session_factory,
):
    with session_factory() as session:
        store = seed_store(session)
        store_id = store.id
        session.commit()

    snapshot = order_snapshot(
        item_overrides={
            "refundedUnits": float("inf"),
            "refundUnits": 2,
            "refundedAmount": Decimal("NaN"),
            "refundAmount": 200,
        },
        order_overrides={
            "partialRefund": True,
            "totalRefundUnits": Decimal("Infinity"),
            "refundedUnits": float("nan"),
            "refundUnits": 3,
            "totalRefundAmount": Decimal("Infinity"),
            "refundedAmount": Decimal("NaN"),
            "refundAmount": 300,
        },
    )
    patch_local_sync_dependencies(
        monkeypatch,
        session_factory,
        [snapshot],
    )

    sales_sync_service.sync_owned_store("alice", store_id)

    with session_factory() as session:
        item = session.scalars(select(SalesOrderItemModel)).one()
        adjustments = session.scalars(
            select(SalesItemAdjustmentModel).order_by(
                SalesItemAdjustmentModel.status.asc()
            )
        ).all()

    assert item.refunded_units == 2
    assert {
        (row.status, row.units, row.amount)
        for row in adjustments
    } == {
        ("confirmed", 2, Decimal("200")),
        ("unresolved", 1, Decimal("100")),
    }


def test_numeric_aliases_catch_overflow_error_and_try_later_alias():
    class OverflowingNumber:
        def __int__(self):
            raise OverflowError

        def __str__(self):
            raise OverflowError

    payload = {
        "overflow": OverflowingNumber(),
        "valid": 125,
    }

    assert sales_sync_service._first_int(
        payload,
        ("overflow", "valid"),
    ) == 125
    assert sales_sync_service._first_decimal(
        payload,
        ("overflow", "valid"),
    ) == Decimal("125")


def test_rebuild_daily_sales_replaces_range_without_committing(
    session_factory,
):
    with session_factory() as session:
        store = seed_store(session)
        order = SalesOrderModel(
            owner_username="alice",
            store_id=store.id,
            order_number="ORDER-DAILY",
            ordered_at=datetime(2026, 7, 15, 10, 0, 0),
            raw_order_json="{}",
            last_synced_at=datetime(2026, 7, 16, 9, 0, 0),
        )
        session.add(order)
        session.flush()
        session.add(
            SalesOrderItemModel.from_service_payload(
                owner_username="alice",
                store_id=store.id,
                sales_order_id=order.id,
                order_number=order.order_number,
                item_detail_id="daily-detail",
                manage_number="MN-DAILY",
                item_number="ITEM-DAILY",
                sku_key="sku",
                item_name="Daily Item",
                unit_price=Decimal("100"),
                ordered_units=3,
                refunded_units=1,
                ordered_at=order.ordered_at,
            )
        )
        session.add(
            ProductSalesDailyModel(
                owner_username="alice",
                store_id=store.id,
                sales_date=date(2026, 7, 15),
                manage_number="STALE",
                sku_key="",
                item_name_snapshot="Stale",
                order_count=99,
            )
        )
        store_id = store.id
        session.commit()

    with session_factory() as session:
        sales_sync_service.rebuild_daily_sales(
            session,
            store_id,
            date(2026, 7, 15),
            date(2026, 7, 15),
        )
        session.flush()
        rebuilt = session.scalars(select(ProductSalesDailyModel)).one()
        assert rebuilt.manage_number == "MN-DAILY"
        assert rebuilt.ordered_units == 3
        assert rebuilt.refunded_units == 1
        assert rebuilt.effective_units == 2
        assert rebuilt.gross_sales_amount == Decimal("300")
        assert rebuilt.effective_sales_amount == Decimal("200")
        session.rollback()

    with session_factory() as session:
        persisted = session.scalars(
            select(ProductSalesDailyModel).where(
                ProductSalesDailyModel.store_id == store_id
            )
        ).one()

    assert persisted.manage_number == "STALE"
    assert persisted.order_count == 99
