from __future__ import annotations

from contextlib import contextmanager
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Any

import pytest
from pydantic import ValidationError
from sqlalchemy import create_engine, event, select
from sqlalchemy.orm import Session, sessionmaker

from app.db.database import Base
from app.db.models import (
    ProductModel,
    ProductSalesDailyModel,
    SalesItemAdjustmentModel,
    SalesOrderItemModel,
    SalesOrderModel,
    SalesSyncStateModel,
    StoreModel,
    UserAccountModel,
)
from app.services import sales_analysis_service, sales_sync_service


TOOL_NAMES = [
    "list_owned_stores",
    "get_store_sales_overview",
    "get_product_sales_ranking",
    "get_product_sales_trend",
    "compare_product_sales",
    "get_sku_sales_breakdown",
    "get_slow_moving_products",
    "get_sales_adjustment_summary",
]


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


def _add_user_and_store(
    session: Session,
    owner_username: str,
    store_code: str,
) -> StoreModel:
    session.add(
        UserAccountModel(
            username=owner_username,
            display_name=owner_username.title(),
            password_salt_b64="salt",
            password_hash_b64="hash",
            created_at=datetime(2026, 7, 16, 4, 0, 0),
            updated_at=datetime(2026, 7, 16, 4, 0, 0),
        )
    )
    session.flush()
    store = StoreModel(
        owner_username=owner_username,
        store_code=store_code,
        store_name=f"{owner_username.title()} Shop",
        created_at=datetime(2026, 7, 16, 5, 0, 0),
        updated_at=datetime(2026, 7, 16, 5, 0, 0),
    )
    session.add(store)
    session.flush()
    return store


def _add_product(
    session: Session,
    *,
    owner_username: str,
    store_id: int,
    manage_number: str,
    title: str,
    listed_at: datetime,
    rakuten_listing_status: str = "listed",
) -> None:
    product = ProductModel(
            owner_username=owner_username,
            store_id=store_id,
            title=title,
            source_url=f"https://example.test/{owner_username}/{manage_number}",
            source_url_hash=f"{owner_username}-{manage_number}",
            rakuten_manage_number=manage_number,
            item_number=f"ITEM-{manage_number}",
            review_status="listed",
            rakuten_listing_status=rakuten_listing_status,
            listed_at=listed_at,
            created_at=datetime(2026, 7, 16, 5, 0, 0),
            updated_at=datetime(2026, 7, 16, 5, 10, 0),
    )
    session.add(product)
    session.flush()


def _add_daily(
    session: Session,
    *,
    owner_username: str,
    store_id: int,
    sales_date: date,
    manage_number: str,
    sku_key: str,
    item_name: str,
    order_count: int,
    ordered_units: int,
    canceled_units: int = 0,
    refunded_units: int = 0,
    returned_units: int = 0,
    effective_units: int,
    gross_amount: str,
    effective_amount: str,
    item_number: str | None = None,
) -> None:
    daily = ProductSalesDailyModel(
            owner_username=owner_username,
            store_id=store_id,
            sales_date=sales_date,
            manage_number=manage_number,
            item_number=item_number or f"ITEM-{manage_number}",
            sku_key=sku_key,
            item_name_snapshot=item_name,
            order_count=order_count,
            ordered_units=ordered_units,
            canceled_units=canceled_units,
            refunded_units=refunded_units,
            returned_units=returned_units,
            effective_units=effective_units,
            gross_sales_amount=Decimal(gross_amount),
            effective_sales_amount=Decimal(effective_amount),
            created_at=datetime(2026, 7, 16, 6, 0, 0),
            updated_at=datetime(2026, 7, 16, 6, 30, 0),
    )
    session.add(daily)
    session.flush()


def _add_order_with_adjustments(
    session: Session,
    *,
    owner_username: str,
    store_id: int,
    order_number: str,
) -> None:
    order = SalesOrderModel(
        owner_username=owner_username,
        store_id=store_id,
        order_number=order_number,
        order_progress="300",
        order_status="normal",
        ordered_at=datetime(2026, 7, 15, 10, 0, 0),
        total_amount=Decimal("300"),
        raw_order_json="{}",
        last_synced_at=datetime(2026, 7, 16, 6, 0, 0),
        created_at=datetime(2026, 7, 16, 5, 20, 0),
        updated_at=datetime(2026, 7, 16, 5, 20, 0),
    )
    session.add(order)
    session.flush()
    item = SalesOrderItemModel.from_service_payload(
        owner_username=owner_username,
        store_id=store_id,
        sales_order_id=order.id,
        order_number=order_number,
        item_detail_id=f"{order_number}-item",
        manage_number="MN-A",
        item_number="ITEM-MN-A",
        item_name="Alpha",
        unit_price=Decimal("100"),
        ordered_units=3,
        ordered_at=order.ordered_at,
    )
    item.created_at = datetime(2026, 7, 16, 5, 30, 0)
    item.updated_at = datetime(2026, 7, 16, 5, 30, 0)
    session.add(item)
    session.flush()
    session.add_all(
        [
            SalesItemAdjustmentModel(
                owner_username=owner_username,
                store_id=store_id,
                sales_order_item_id=item.id,
                adjustment_type="return",
                units=1,
                amount=Decimal("100"),
                source="test",
                status="confirmed",
                reason="returned",
                raw_payload_json="{}",
                created_at=datetime(2026, 7, 16, 6, 0, 0),
                updated_at=datetime(2026, 7, 16, 6, 20, 0),
            ),
            SalesItemAdjustmentModel(
                owner_username=owner_username,
                store_id=store_id,
                sales_order_item_id=item.id,
                adjustment_type="refund",
                units=1,
                amount=Decimal("25"),
                source="test",
                status="unresolved",
                reason="unattributed partial refund",
                raw_payload_json="{}",
                created_at=datetime(2026, 7, 16, 6, 0, 0),
                updated_at=datetime(2026, 7, 16, 6, 25, 0),
            ),
        ]
    )


def _add_order(
    session: Session,
    *,
    owner_username: str,
    store_id: int,
    order_number: str,
    ordered_at: datetime,
    has_unresolved_adjustment: bool = False,
    updated_at: datetime | None = None,
    updated_at_remote: datetime | None = None,
    last_synced_at: datetime | None = None,
) -> SalesOrderModel:
    source_updated_at = updated_at or datetime(2026, 7, 16, 5, 20, 0)
    order = SalesOrderModel(
        owner_username=owner_username,
        store_id=store_id,
        order_number=order_number,
        order_progress="300",
        order_status="normal",
        ordered_at=ordered_at,
        updated_at_remote=updated_at_remote,
        total_amount=Decimal("300"),
        has_unresolved_adjustment=has_unresolved_adjustment,
        raw_order_json="{}",
        last_synced_at=last_synced_at or source_updated_at,
        created_at=source_updated_at,
        updated_at=source_updated_at,
    )
    session.add(order)
    session.flush()
    return order


def _add_order_item(
    session: Session,
    *,
    order: SalesOrderModel,
    item_detail_id: str,
    manage_number: str,
    item_number: str,
    sku_key: str,
    item_name: str,
    ordered_units: int = 1,
    updated_at: datetime | None = None,
) -> SalesOrderItemModel:
    source_updated_at = updated_at or datetime(2026, 7, 16, 5, 30, 0)
    item = SalesOrderItemModel.from_service_payload(
        owner_username=order.owner_username,
        store_id=order.store_id,
        sales_order_id=order.id,
        order_number=order.order_number,
        item_detail_id=item_detail_id,
        manage_number=manage_number,
        item_number=item_number,
        sku_key=sku_key,
        item_name=item_name,
        unit_price=Decimal("100"),
        ordered_units=ordered_units,
        ordered_at=order.ordered_at,
    )
    item.created_at = source_updated_at
    item.updated_at = source_updated_at
    session.add(item)
    session.flush()
    return item


@pytest.fixture()
def seeded_sales(session_factory):
    with session_factory() as session:
        alice_store = _add_user_and_store(session, "alice", "alice-shop")
        bob_store = _add_user_and_store(session, "bob", "bob-shop")

        _add_product(
            session,
            owner_username="alice",
            store_id=alice_store.id,
            manage_number="MN-A",
            title="Alpha",
            listed_at=datetime(2026, 1, 1, 0, 0, 0),
        )
        _add_product(
            session,
            owner_username="alice",
            store_id=alice_store.id,
            manage_number="MN-B",
            title="Beta",
            listed_at=datetime(2026, 1, 2, 0, 0, 0),
        )
        _add_product(
            session,
            owner_username="alice",
            store_id=alice_store.id,
            manage_number="MN-ZERO",
            title="Zero",
            listed_at=datetime(2026, 1, 3, 0, 0, 0),
        )
        _add_product(
            session,
            owner_username="alice",
            store_id=alice_store.id,
            manage_number="MN-UNLISTED",
            title="Unlisted",
            listed_at=datetime(2026, 1, 4, 0, 0, 0),
            rakuten_listing_status="unlisted",
        )
        _add_product(
            session,
            owner_username="bob",
            store_id=bob_store.id,
            manage_number="MN-BOB",
            title="Bob Product",
            listed_at=datetime(2026, 1, 1, 0, 0, 0),
        )

        _add_daily(
            session,
            owner_username="alice",
            store_id=alice_store.id,
            sales_date=date(2026, 7, 14),
            manage_number="MN-A",
            sku_key="blue",
            item_name="Alpha",
            order_count=2,
            ordered_units=3,
            refunded_units=1,
            effective_units=2,
            gross_amount="300",
            effective_amount="200",
        )
        _add_daily(
            session,
            owner_username="alice",
            store_id=alice_store.id,
            sales_date=date(2026, 7, 15),
            manage_number="MN-A",
            sku_key="red",
            item_name="Alpha",
            order_count=1,
            ordered_units=4,
            returned_units=1,
            effective_units=3,
            gross_amount="400",
            effective_amount="300",
        )
        _add_daily(
            session,
            owner_username="alice",
            store_id=alice_store.id,
            sales_date=date(2026, 7, 15),
            manage_number="MN-B",
            sku_key="default",
            item_name="Beta",
            order_count=1,
            ordered_units=1,
            effective_units=1,
            gross_amount="50",
            effective_amount="50",
        )
        _add_daily(
            session,
            owner_username="bob",
            store_id=bob_store.id,
            sales_date=date(2026, 7, 15),
            manage_number="MN-BOB",
            sku_key="secret",
            item_name="Bob Product",
            order_count=99,
            ordered_units=99,
            effective_units=99,
            gross_amount="9999",
            effective_amount="9999",
        )
        _add_order_with_adjustments(
            session,
            owner_username="alice",
            store_id=alice_store.id,
            order_number="ALICE-ORDER-1",
        )
        _add_order_with_adjustments(
            session,
            owner_username="bob",
            store_id=bob_store.id,
            order_number="BOB-ORDER-1",
        )
        session.add_all(
            [
                SalesOrderModel(
                    owner_username="alice",
                    store_id=alice_store.id,
                    order_number="ALICE-ORDER-2",
                    order_progress="300",
                    order_status="normal",
                    ordered_at=datetime(2026, 7, 14, 11, 0, 0),
                    total_amount=Decimal("200"),
                    raw_order_json="{}",
                    last_synced_at=datetime(2026, 7, 16, 6, 0, 0),
                    created_at=datetime(2026, 7, 16, 5, 20, 0),
                    updated_at=datetime(2026, 7, 16, 5, 20, 0),
                ),
                SalesOrderModel(
                    owner_username="alice",
                    store_id=alice_store.id,
                    order_number="ALICE-ORDER-3",
                    order_progress="300",
                    order_status="normal",
                    ordered_at=datetime(2026, 7, 15, 12, 0, 0),
                    total_amount=Decimal("50"),
                    raw_order_json="{}",
                    last_synced_at=datetime(2026, 7, 16, 6, 0, 0),
                    created_at=datetime(2026, 7, 16, 5, 20, 0),
                    updated_at=datetime(2026, 7, 16, 5, 20, 0),
                ),
            ]
        )
        session.commit()
        return {
            "alice_store_id": alice_store.id,
            "bob_store_id": bob_store.id,
        }


@pytest.fixture(autouse=True)
def local_session_scope(monkeypatch, session_factory):
    @contextmanager
    def _session_scope():
        with session_factory() as session:
            yield session

    monkeypatch.setattr(sales_analysis_service, "session_scope", _session_scope)


def execute(tool_name: str, arguments: dict[str, Any]) -> dict[str, Any]:
    return sales_analysis_service.execute_sales_tool("alice", tool_name, arguments)


def assert_common_metadata(
    result: dict[str, Any],
    *,
    store_id: int,
    metric: str,
) -> None:
    assert result["store"] == {"id": store_id, "name": "Alice Shop"}
    assert result["range"] == {"start": "2026-07-14", "end": "2026-07-15"}
    assert result["metric"] == metric
    assert result["dataUpdatedAt"] == "2026-07-16T06:30:00+08:00"
    assert result["unresolvedAdjustmentCount"] == 1


def test_registers_exactly_eight_strict_read_only_tools():
    assert [tool["function"]["name"] for tool in sales_analysis_service.SALES_ANALYSIS_TOOLS] == TOOL_NAMES
    for tool in sales_analysis_service.SALES_ANALYSIS_TOOLS:
        assert tool["type"] == "function"
        parameters = tool["function"]["parameters"]
        assert parameters["additionalProperties"] is False
    sku_tool = next(
        tool
        for tool in sales_analysis_service.SALES_ANALYSIS_TOOLS
        if tool["function"]["name"] == "get_sku_sales_breakdown"
    )
    limit_schema = sku_tool["function"]["parameters"]["properties"]["limit"]
    assert limit_schema["default"] == 100
    assert limit_schema["minimum"] == 1
    assert limit_schema["maximum"] == 100


def test_list_owned_stores_only_returns_current_owner(seeded_sales):
    result = execute("list_owned_stores", {})

    assert result["rows"] == [
        {
            "id": seeded_sales["alice_store_id"],
            "name": "Alice Shop",
            "code": "alice-shop",
            "enabled": True,
        }
    ]
    assert "bob" not in repr(result).lower()


@pytest.mark.parametrize(
    ("tool_name", "arguments"),
    [
        (
            "get_store_sales_overview",
            {"startDate": "2026-07-14", "endDate": "2026-07-15"},
        ),
        (
            "get_product_sales_ranking",
            {
                "startDate": "2026-07-14",
                "endDate": "2026-07-15",
                "metric": "effectiveUnits",
                "limit": 10,
                "includeSku": False,
            },
        ),
        (
            "get_product_sales_trend",
            {
                "manageNumber": "MN-A",
                "startDate": "2026-07-14",
                "endDate": "2026-07-15",
                "grain": "day",
            },
        ),
        (
            "compare_product_sales",
            {
                "manageNumbers": ["MN-A", "MN-B"],
                "startDate": "2026-07-14",
                "endDate": "2026-07-15",
                "grain": "day",
            },
        ),
        (
            "get_sku_sales_breakdown",
            {
                "manageNumber": "MN-A",
                "startDate": "2026-07-14",
                "endDate": "2026-07-15",
            },
        ),
        (
            "get_slow_moving_products",
            {
                "startDate": "2026-07-14",
                "endDate": "2026-07-15",
                "minListedDays": 30,
                "maxEffectiveUnits": 1,
                "limit": 10,
            },
        ),
        (
            "get_sales_adjustment_summary",
            {"startDate": "2026-07-14", "endDate": "2026-07-15"},
        ),
    ],
)
def test_every_store_scoped_tool_rejects_another_owners_store(
    seeded_sales,
    tool_name,
    arguments,
):
    with pytest.raises(LookupError, match="店铺不存在或无权访问"):
        execute(
            tool_name,
            {"storeId": seeded_sales["bob_store_id"], **arguments},
        )


@pytest.mark.parametrize(
    ("tool_name", "arguments"),
    [
        (
            "get_store_sales_overview",
            {
                "storeId": 1,
                "startDate": "最近30天",
                "endDate": "2026-07-15",
            },
        ),
        (
            "get_store_sales_overview",
            {
                "storeId": 1,
                "startDate": "2026-07-16",
                "endDate": "2026-07-15",
            },
        ),
        (
            "get_store_sales_overview",
            {
                "storeId": 1,
                "startDate": "2025-07-14",
                "endDate": "2026-07-15",
            },
        ),
        (
            "get_product_sales_ranking",
            {
                "storeId": 1,
                "startDate": "2026-07-14",
                "endDate": "2026-07-15",
                "limit": 101,
            },
        ),
        (
            "get_sku_sales_breakdown",
            {
                "storeId": 1,
                "manageNumber": "MN-A",
                "startDate": "2026-07-14",
                "endDate": "2026-07-15",
                "limit": 0,
            },
        ),
        (
            "get_sku_sales_breakdown",
            {
                "storeId": 1,
                "manageNumber": "MN-A",
                "startDate": "2026-07-14",
                "endDate": "2026-07-15",
                "limit": 101,
            },
        ),
        (
            "get_store_sales_overview",
            {
                "storeId": 1,
                "startDate": "9999-12-31",
                "endDate": "9999-12-31",
            },
        ),
        (
            "get_store_sales_overview",
            {
                "storeId": 1,
                "startDate": "2026-07-14",
                "endDate": "2026-07-15",
                "compareStartDate": "9999-12-31",
                "compareEndDate": "9999-12-31",
            },
        ),
        (
            "get_slow_moving_products",
            {
                "storeId": 1,
                "startDate": "0001-01-01",
                "endDate": "0001-01-01",
                "minListedDays": 2,
            },
        ),
        (
            "get_product_sales_trend",
            {
                "storeId": 1,
                "manageNumber": "MN-A",
                "startDate": "2026-07-14",
                "endDate": "2026-07-15",
                "grain": "hour",
            },
        ),
        (
            "compare_product_sales",
            {
                "storeId": 1,
                "manageNumbers": ["MN-A"],
                "startDate": "2026-07-14",
                "endDate": "2026-07-15",
            },
        ),
        (
            "compare_product_sales",
            {
                "storeId": 1,
                "manageNumbers": [f"MN-{index}" for index in range(21)],
                "startDate": "2026-07-14",
                "endDate": "2026-07-15",
            },
        ),
        (
            "get_sales_adjustment_summary",
            {
                "storeId": 1,
                "startDate": "2026-07-14",
                "endDate": "2026-07-15",
                "sql": "SELECT * FROM lt_sales_orders",
            },
        ),
        (
            "get_store_sales_overview",
            {
                "store_id": 1,
                "start_date": "2026-07-14",
                "end_date": "2026-07-15",
            },
        ),
    ],
)
def test_tool_arguments_are_strict_and_bounded(tool_name, arguments):
    with pytest.raises(ValidationError):
        execute(tool_name, arguments)


def test_store_overview_returns_effective_totals_and_comparison_metadata(seeded_sales):
    result = execute(
        "get_store_sales_overview",
        {
            "storeId": seeded_sales["alice_store_id"],
            "startDate": "2026-07-14",
            "endDate": "2026-07-15",
            "compareStartDate": "2026-07-12",
            "compareEndDate": "2026-07-13",
        },
    )

    assert_common_metadata(
        result,
        store_id=seeded_sales["alice_store_id"],
        metric="effectiveUnits",
    )
    assert result["rows"] == [
        {
            "orderCount": 3,
            "orderedUnits": 8,
            "effectiveUnits": 6,
            "grossSalesAmount": 750.0,
            "effectiveSalesAmount": 550.0,
            "canceledUnits": 0,
            "refundedUnits": 1,
            "returnedUnits": 1,
        }
    ]
    assert result["comparison"]["range"] == {
        "start": "2026-07-12",
        "end": "2026-07-13",
    }
    assert result["comparison"]["effectiveUnits"] == 0


def test_product_ranking_is_chart_ready_and_does_not_leak_other_tenants(seeded_sales):
    result = execute(
        "get_product_sales_ranking",
        {
            "storeId": seeded_sales["alice_store_id"],
            "startDate": "2026-07-14",
            "endDate": "2026-07-15",
            "metric": "effectiveUnits",
            "limit": 10,
            "includeSku": False,
        },
    )

    assert_common_metadata(
        result,
        store_id=seeded_sales["alice_store_id"],
        metric="effectiveUnits",
    )
    assert [(row["manageNumber"], row["metricValue"]) for row in result["rows"]] == [
        ("MN-A", 5),
        ("MN-B", 1),
    ]
    assert "Bob Product" not in repr(result)


def test_product_ranking_uses_manage_number_and_distinct_fact_orders(session_factory):
    with session_factory() as session:
        store = _add_user_and_store(session, "alice", "ranking-facts")
        _add_daily(
            session,
            owner_username="alice",
            store_id=store.id,
            sales_date=date(2026, 7, 14),
            manage_number="MN-STABLE",
            item_number="ITEM-OLD",
            sku_key="blue",
            item_name="Stable Product",
            order_count=1,
            ordered_units=2,
            effective_units=2,
            gross_amount="200",
            effective_amount="200",
        )
        _add_daily(
            session,
            owner_username="alice",
            store_id=store.id,
            sales_date=date(2026, 7, 15),
            manage_number="MN-STABLE",
            item_number="ITEM-NEW",
            sku_key="blue",
            item_name="Stable Product",
            order_count=1,
            ordered_units=1,
            effective_units=1,
            gross_amount="100",
            effective_amount="100",
        )
        _add_daily(
            session,
            owner_username="alice",
            store_id=store.id,
            sales_date=date(2026, 7, 15),
            manage_number="MN-STABLE",
            item_number="ITEM-NEW",
            sku_key="red",
            item_name="Stable Product",
            order_count=1,
            ordered_units=4,
            effective_units=4,
            gross_amount="400",
            effective_amount="400",
        )
        first_order = _add_order(
            session,
            owner_username="alice",
            store_id=store.id,
            order_number="FACT-ORDER-1",
            ordered_at=datetime(2026, 7, 14, 10, 0, 0),
        )
        _add_order_item(
            session,
            order=first_order,
            item_detail_id="fact-1-blue",
            manage_number="MN-STABLE",
            item_number="ITEM-OLD",
            sku_key="blue",
            item_name="Stable Product",
            ordered_units=2,
        )
        second_order = _add_order(
            session,
            owner_username="alice",
            store_id=store.id,
            order_number="FACT-ORDER-2",
            ordered_at=datetime(2026, 7, 15, 10, 0, 0),
        )
        _add_order_item(
            session,
            order=second_order,
            item_detail_id="fact-2-blue",
            manage_number="MN-STABLE",
            item_number="ITEM-NEW",
            sku_key="blue",
            item_name="Stable Product",
        )
        _add_order_item(
            session,
            order=second_order,
            item_detail_id="fact-2-red",
            manage_number="MN-STABLE",
            item_number="ITEM-NEW",
            sku_key="red",
            item_name="Stable Product",
            ordered_units=4,
        )
        session.commit()
        store_id = store.id

    product_result = execute(
        "get_product_sales_ranking",
        {
            "storeId": store_id,
            "startDate": "2026-07-14",
            "endDate": "2026-07-15",
            "metric": "orderCount",
            "limit": 10,
            "includeSku": False,
        },
    )
    sku_result = execute(
        "get_product_sales_ranking",
        {
            "storeId": store_id,
            "startDate": "2026-07-14",
            "endDate": "2026-07-15",
            "metric": "orderCount",
            "limit": 10,
            "includeSku": True,
        },
    )

    assert len(product_result["rows"]) == 1
    assert product_result["rows"][0]["manageNumber"] == "MN-STABLE"
    assert product_result["rows"][0]["orderCount"] == 2
    assert product_result["rows"][0]["metricValue"] == 2
    assert {
        (row["skuKey"], row["orderCount"])
        for row in sku_result["rows"]
    } == {("blue", 2), ("red", 1)}


def test_product_ranking_reuses_daily_fallback_key_for_blank_manage_number(
    session_factory,
):
    long_item_number = "I" * 250
    with session_factory() as session:
        store = _add_user_and_store(session, "alice", "ranking-fallback")
        first_order = _add_order(
            session,
            owner_username="alice",
            store_id=store.id,
            order_number="FALLBACK-ORDER-1",
            ordered_at=datetime(2026, 7, 14, 10, 0, 0),
        )
        first_item = _add_order_item(
            session,
            order=first_order,
            item_detail_id="fallback-1-blue",
            manage_number="",
            item_number=long_item_number,
            sku_key="blue",
            item_name="Fallback Product",
        )
        _add_order_item(
            session,
            order=first_order,
            item_detail_id="fallback-1-blue-duplicate",
            manage_number="",
            item_number=long_item_number,
            sku_key="blue",
            item_name="Fallback Product",
        )
        second_order = _add_order(
            session,
            owner_username="alice",
            store_id=store.id,
            order_number="FALLBACK-ORDER-2",
            ordered_at=datetime(2026, 7, 15, 10, 0, 0),
        )
        _add_order_item(
            session,
            order=second_order,
            item_detail_id="fallback-2-red",
            manage_number="",
            item_number=long_item_number,
            sku_key="red",
            item_name="Fallback Product",
        )
        fallback_key = sales_sync_service._daily_product_key(first_item)
        assert fallback_key.startswith("v1:item-number:")
        assert len(fallback_key) <= 255
        _add_daily(
            session,
            owner_username="alice",
            store_id=store.id,
            sales_date=date(2026, 7, 14),
            manage_number=fallback_key,
            item_number=long_item_number,
            sku_key="blue",
            item_name="Fallback Product",
            order_count=1,
            ordered_units=2,
            effective_units=2,
            gross_amount="200",
            effective_amount="200",
        )
        _add_daily(
            session,
            owner_username="alice",
            store_id=store.id,
            sales_date=date(2026, 7, 15),
            manage_number=fallback_key,
            item_number=long_item_number,
            sku_key="red",
            item_name="Fallback Product",
            order_count=1,
            ordered_units=1,
            effective_units=1,
            gross_amount="100",
            effective_amount="100",
        )
        session.commit()
        store_id = store.id

    product_result = execute(
        "get_product_sales_ranking",
        {
            "storeId": store_id,
            "startDate": "2026-07-14",
            "endDate": "2026-07-15",
            "metric": "orderCount",
            "limit": 10,
            "includeSku": False,
        },
    )
    sku_result = execute(
        "get_product_sales_ranking",
        {
            "storeId": store_id,
            "startDate": "2026-07-14",
            "endDate": "2026-07-15",
            "metric": "orderCount",
            "limit": 10,
            "includeSku": True,
        },
    )

    assert product_result["rows"][0]["manageNumber"] == fallback_key
    assert product_result["rows"][0]["orderCount"] == 2
    assert {
        (row["skuKey"], row["orderCount"])
        for row in sku_result["rows"]
    } == {("blue", 1), ("red", 1)}


def test_product_trend_supports_day_week_and_month_grains(seeded_sales):
    daily = execute(
        "get_product_sales_trend",
        {
            "storeId": seeded_sales["alice_store_id"],
            "manageNumber": "MN-A",
            "startDate": "2026-07-14",
            "endDate": "2026-07-15",
            "grain": "day",
        },
    )
    weekly = execute(
        "get_product_sales_trend",
        {
            "storeId": seeded_sales["alice_store_id"],
            "manageNumber": "MN-A",
            "startDate": "2026-07-14",
            "endDate": "2026-07-15",
            "grain": "week",
        },
    )
    monthly = execute(
        "get_product_sales_trend",
        {
            "storeId": seeded_sales["alice_store_id"],
            "manageNumber": "MN-A",
            "startDate": "2026-07-14",
            "endDate": "2026-07-15",
            "grain": "month",
        },
    )

    assert [row["period"] for row in daily["rows"]] == [
        "2026-07-14",
        "2026-07-15",
    ]
    assert weekly["rows"] == [
        {
            "period": "2026-07-13",
            "orderedUnits": 7,
            "effectiveUnits": 5,
            "effectiveSalesAmount": 500.0,
        }
    ]
    assert monthly["rows"][0]["period"] == "2026-07"


def test_product_comparison_returns_summary_and_long_form_series(seeded_sales):
    result = execute(
        "compare_product_sales",
        {
            "storeId": seeded_sales["alice_store_id"],
            "manageNumbers": ["MN-A", "MN-B"],
            "startDate": "2026-07-14",
            "endDate": "2026-07-15",
            "grain": "day",
        },
    )

    assert [row["manageNumber"] for row in result["rows"]] == ["MN-A", "MN-B"]
    assert result["rows"][0]["effectiveUnits"] == 5
    assert result["rows"][0]["adjustmentRate"] == pytest.approx(2 / 7)
    assert {
        (row["period"], row["manageNumber"], row["effectiveUnits"])
        for row in result["series"]
    } == {
        ("2026-07-14", "MN-A", 2),
        ("2026-07-15", "MN-A", 3),
        ("2026-07-15", "MN-B", 1),
    }


def test_trend_and_comparison_aggregate_high_sku_rows_in_sql(session_factory):
    with session_factory() as session:
        store = _add_user_and_store(session, "alice", "high-sku")
        session.add_all(
            [
                ProductSalesDailyModel(
                    owner_username="alice",
                    store_id=store.id,
                    sales_date=date(2026, 7, 14),
                    manage_number=manage_number,
                    item_number=f"ITEM-{manage_number}",
                    sku_key=f"{manage_number}-sku-{index:03d}",
                    item_name_snapshot=manage_number,
                    order_count=1,
                    ordered_units=units,
                    canceled_units=0,
                    refunded_units=0,
                    returned_units=0,
                    effective_units=units,
                    gross_sales_amount=Decimal(100 * units),
                    effective_sales_amount=Decimal(100 * units),
                    created_at=datetime(2026, 7, 16, 6, 0, 0),
                    updated_at=datetime(2026, 7, 16, 6, 30, 0),
                )
                for manage_number, units in (
                    ("MN-HIGH", 1),
                    ("MN-OTHER", 2),
                )
                for index in range(120)
            ]
        )
        session.commit()
        store_id = store.id

    statements: list[str] = []
    engine = session_factory.kw["bind"]

    def _capture_statement(_, __, statement, ___, ____, _____):
        statements.append(statement)

    event.listen(engine, "before_cursor_execute", _capture_statement)
    try:
        trend = execute(
            "get_product_sales_trend",
            {
                "storeId": store_id,
                "manageNumber": "MN-HIGH",
                "startDate": "2026-07-14",
                "endDate": "2026-07-15",
                "grain": "week",
            },
        )
        comparison = execute(
            "compare_product_sales",
            {
                "storeId": store_id,
                "manageNumbers": ["MN-HIGH", "MN-OTHER"],
                "startDate": "2026-07-14",
                "endDate": "2026-07-15",
                "grain": "month",
            },
        )
    finally:
        event.remove(engine, "before_cursor_execute", _capture_statement)

    assert trend["rows"] == [
        {
            "period": "2026-07-13",
            "orderedUnits": 120,
            "effectiveUnits": 120,
            "effectiveSalesAmount": 12000.0,
        }
    ]
    assert comparison["rows"][0]["effectiveUnits"] == 120
    assert comparison["rows"][1]["effectiveUnits"] == 240
    assert {
        (row["period"], row["manageNumber"], row["effectiveUnits"])
        for row in comparison["series"]
    } == {
        ("2026-07", "MN-HIGH", 120),
        ("2026-07", "MN-OTHER", 240),
    }
    daily_statements = [
        statement.lower()
        for statement in statements
        if "from lt_product_sales_daily" in statement.lower()
    ]
    assert sum(
        "group by" in statement and "sum(" in statement
        for statement in daily_statements
    ) >= 3
    assert all(
        "lt_product_sales_daily.sku_key" not in statement
        and "lt_product_sales_daily.id" not in statement
        for statement in daily_statements
    )


def test_sku_breakdown_returns_units_amounts_and_shares(seeded_sales):
    result = execute(
        "get_sku_sales_breakdown",
        {
            "storeId": seeded_sales["alice_store_id"],
            "manageNumber": "MN-A",
            "startDate": "2026-07-14",
            "endDate": "2026-07-15",
        },
    )

    assert [(row["skuKey"], row["effectiveUnits"]) for row in result["rows"]] == [
        ("red", 3),
        ("blue", 2),
    ]
    assert result["rows"][0]["unitShare"] == pytest.approx(0.6)
    assert result["rows"][0]["salesShare"] == pytest.approx(0.6)


def test_sku_breakdown_applies_requested_limit_in_sql(
    seeded_sales,
    session_factory,
):
    statements: list[str] = []
    engine = session_factory.kw["bind"]

    def _capture_statement(_, __, statement, ___, ____, _____):
        statements.append(statement)

    event.listen(engine, "before_cursor_execute", _capture_statement)
    try:
        result = execute(
            "get_sku_sales_breakdown",
            {
                "storeId": seeded_sales["alice_store_id"],
                "manageNumber": "MN-A",
                "startDate": "2026-07-14",
                "endDate": "2026-07-15",
                "limit": 1,
            },
        )
    finally:
        event.remove(engine, "before_cursor_execute", _capture_statement)

    assert len(result["rows"]) == 1
    assert any(
        "group by" in statement.lower()
        and "limit" in statement.lower()
        and "lt_product_sales_daily" in statement.lower()
        for statement in statements
    )


def test_slow_movers_include_zero_sales_listed_products(seeded_sales):
    result = execute(
        "get_slow_moving_products",
        {
            "storeId": seeded_sales["alice_store_id"],
            "startDate": "2026-07-14",
            "endDate": "2026-07-15",
            "minListedDays": 30,
            "maxEffectiveUnits": 1,
            "limit": 10,
        },
    )

    assert [(row["manageNumber"], row["effectiveUnits"]) for row in result["rows"]] == [
        ("MN-ZERO", 0),
        ("MN-B", 1),
    ]
    assert all(row["listedDays"] >= 30 for row in result["rows"])
    assert "MN-UNLISTED" not in {
        row["manageNumber"] for row in result["rows"]
    }


def test_adjustment_summary_reports_confirmed_and_unresolved_rows(seeded_sales):
    result = execute(
        "get_sales_adjustment_summary",
        {
            "storeId": seeded_sales["alice_store_id"],
            "startDate": "2026-07-14",
            "endDate": "2026-07-15",
        },
    )

    assert_common_metadata(
        result,
        store_id=seeded_sales["alice_store_id"],
        metric="adjustmentUnits",
    )
    assert result["rows"] == [
        {
            "adjustmentType": "refund",
            "status": "unresolved",
            "adjustmentCount": 1,
            "units": 1,
            "amount": 25.0,
        },
        {
            "adjustmentType": "return",
            "status": "confirmed",
            "adjustmentCount": 1,
            "units": 1,
            "amount": 100.0,
        },
    ]


def test_unresolved_adjustments_are_counted_once_per_order_and_include_flags(
    session_factory,
):
    with session_factory() as session:
        store = _add_user_and_store(session, "alice", "unresolved-orders")
        item_backed_order = _add_order(
            session,
            owner_username="alice",
            store_id=store.id,
            order_number="UNRESOLVED-WITH-ITEMS",
            ordered_at=datetime(2026, 7, 15, 10, 0, 0),
            has_unresolved_adjustment=True,
        )
        first_item = _add_order_item(
            session,
            order=item_backed_order,
            item_detail_id="unresolved-item-1",
            manage_number="MN-A",
            item_number="ITEM-A",
            sku_key="blue",
            item_name="Alpha",
        )
        second_item = _add_order_item(
            session,
            order=item_backed_order,
            item_detail_id="unresolved-item-2",
            manage_number="MN-A",
            item_number="ITEM-A",
            sku_key="red",
            item_name="Alpha",
        )
        session.add_all(
            [
                SalesItemAdjustmentModel(
                    owner_username="alice",
                    store_id=store.id,
                    sales_order_item_id=first_item.id,
                    adjustment_type="refund",
                    units=1,
                    amount=Decimal("25"),
                    source="test:first",
                    status="unresolved",
                    reason="partial refund",
                    raw_payload_json="{}",
                    created_at=datetime(2026, 7, 16, 6, 0, 0),
                    updated_at=datetime(2026, 7, 16, 6, 10, 0),
                ),
                SalesItemAdjustmentModel(
                    owner_username="alice",
                    store_id=store.id,
                    sales_order_item_id=first_item.id,
                    adjustment_type="refund",
                    units=0,
                    amount=Decimal("15"),
                    source="test:second",
                    status="unresolved",
                    reason="unattributed amount",
                    raw_payload_json="{}",
                    created_at=datetime(2026, 7, 16, 6, 0, 0),
                    updated_at=datetime(2026, 7, 16, 6, 15, 0),
                ),
                SalesItemAdjustmentModel(
                    owner_username="alice",
                    store_id=store.id,
                    sales_order_item_id=second_item.id,
                    adjustment_type="refund",
                    units=1,
                    amount=Decimal("60"),
                    source="test:third",
                    status="unresolved",
                    reason="partial refund",
                    raw_payload_json="{}",
                    created_at=datetime(2026, 7, 16, 6, 0, 0),
                    updated_at=datetime(2026, 7, 16, 6, 20, 0),
                ),
            ]
        )
        _add_order(
            session,
            owner_username="alice",
            store_id=store.id,
            order_number="UNRESOLVED-FLAG-ONLY",
            ordered_at=datetime(2026, 7, 15, 11, 0, 0),
            has_unresolved_adjustment=True,
        )
        bob_store = _add_user_and_store(session, "bob", "bob-unresolved")
        _add_order(
            session,
            owner_username="bob",
            store_id=bob_store.id,
            order_number="BOB-UNRESOLVED",
            ordered_at=datetime(2026, 7, 15, 11, 0, 0),
            has_unresolved_adjustment=True,
        )
        session.commit()
        store_id = store.id

    result = execute(
        "get_sales_adjustment_summary",
        {
            "storeId": store_id,
            "startDate": "2026-07-14",
            "endDate": "2026-07-15",
        },
    )

    assert result["unresolvedAdjustmentCount"] == 2
    assert result["rows"] == [
        {
            "adjustmentType": "refund",
            "status": "unresolved",
            "adjustmentCount": 1,
            "units": 2,
            "amount": 100.0,
        },
        {
            "adjustmentType": "unattributed",
            "status": "unresolved",
            "adjustmentCount": 1,
            "units": 0,
            "amount": 0.0,
        },
    ]
    assert "BOB-UNRESOLVED" not in repr(result)


def test_data_updated_at_uses_local_naive_fallback_and_authoritative_sync(
    session_factory,
):
    with session_factory() as session:
        store = _add_user_and_store(session, "alice", "updated-sources")
        order = _add_order(
            session,
            owner_username="alice",
            store_id=store.id,
            order_number="LOCAL-FRESHNESS",
            ordered_at=datetime(2026, 7, 15, 10, 0, 0),
            updated_at=datetime(2026, 7, 16, 21, 30, 0),
            updated_at_remote=datetime(2026, 7, 16, 23, 45, 0),
            last_synced_at=datetime(2026, 7, 16, 21, 15, 0),
        )
        session.commit()
        store_id = store.id
        order_id = order.id

    arguments = {
        "storeId": store_id,
        "startDate": "2026-07-14",
        "endDate": "2026-07-15",
    }
    order_only = execute("get_store_sales_overview", arguments)
    assert order_only["rows"][0]["orderCount"] == 1
    assert order_only["dataUpdatedAt"] == "2026-07-16T21:30:00+08:00"

    with session_factory() as session:
        session.add(
            SalesSyncStateModel(
                store_id=store_id,
                owner_username="alice",
                initial_sync_completed=True,
                last_successful_sync_at=datetime(2026, 7, 16, 20, 0, 0),
                last_remote_updated_at=datetime(2026, 7, 16, 23, 59, 0),
                sync_status="idle",
                created_at=datetime(2026, 7, 16, 22, 0, 0),
                updated_at=datetime(2026, 7, 16, 22, 0, 0),
            )
        )
        session.commit()
    assert execute("get_store_sales_overview", arguments)["dataUpdatedAt"] == (
        "2026-07-16T20:00:00+08:00"
    )
    assert execute("list_owned_stores", {})["dataUpdatedAt"] == (
        "2026-07-16T20:00:00+08:00"
    )
    assert order_id > 0


def test_iso_updated_at_converts_aware_values_to_shanghai():
    assert sales_analysis_service._iso_updated_at(
        datetime(2026, 7, 16, 13, 30, 0, tzinfo=timezone.utc)
    ) == "2026-07-16T21:30:00+08:00"


def test_unknown_tool_is_rejected_without_database_fallback():
    with pytest.raises(ValueError, match="未知的销量分析工具"):
        execute("run_sql", {"sql": "SELECT 1"})
