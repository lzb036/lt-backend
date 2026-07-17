from __future__ import annotations

from contextlib import contextmanager
from types import SimpleNamespace

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from app.db.database import Base
from app.db.models import UserAccountModel
from app.services import sales_analysis_settings_service


@pytest.fixture()
def settings_session_factory(monkeypatch: pytest.MonkeyPatch):
    engine = create_engine(
        "sqlite+pysqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
        future=True,
    )
    Base.metadata.create_all(engine)
    factory = sessionmaker(
        bind=engine,
        autoflush=False,
        expire_on_commit=False,
        future=True,
    )

    with factory.begin() as session:
        session.add_all(
            [
                UserAccountModel(
                    username="alice",
                    display_name="Alice",
                    password_salt_b64="salt",
                    password_hash_b64="hash",
                ),
                UserAccountModel(
                    username="bob",
                    display_name="Bob",
                    password_salt_b64="salt",
                    password_hash_b64="hash",
                ),
            ]
        )

    @contextmanager
    def _session_scope():
        with factory() as session:
            try:
                yield session
                session.commit()
            except Exception:
                session.rollback()
                raise

    monkeypatch.setattr(
        sales_analysis_settings_service,
        "session_scope",
        _session_scope,
    )
    try:
        yield factory
    finally:
        engine.dispose()


def _payload(**overrides):
    values = {
        "defaultPeriodDays": 30,
        "defaultRankingLimit": 10,
        "defaultMetric": "effectiveUnits",
        "defaultGrain": "day",
        "answerDetailLevel": "standard",
        "prioritizeAdjustmentRisk": True,
        "showDataUpdatedAt": True,
        "showMetricDefinition": True,
        "customBusinessInstructions": "",
    }
    values.update(overrides)
    return SimpleNamespace(**values)


def test_get_settings_creates_camel_case_defaults(
    settings_session_factory,
) -> None:
    assert sales_analysis_settings_service.get_settings("alice") == {
        "defaultPeriodDays": 30,
        "defaultRankingLimit": 10,
        "defaultMetric": "effectiveUnits",
        "defaultGrain": "day",
        "answerDetailLevel": "standard",
        "prioritizeAdjustmentRisk": True,
        "showDataUpdatedAt": True,
        "showMetricDefinition": True,
        "customBusinessInstructions": "",
    }


def test_user_sales_analysis_settings_are_owner_scoped(
    settings_session_factory,
) -> None:
    updated = sales_analysis_settings_service.update_settings(
        "alice",
        _payload(defaultRankingLimit=20),
    )

    assert updated["defaultRankingLimit"] == 20
    assert (
        sales_analysis_settings_service.get_settings("alice")[
            "defaultRankingLimit"
        ]
        == 20
    )
    assert (
        sales_analysis_settings_service.get_settings("bob")[
            "defaultRankingLimit"
        ]
        == 10
    )


@pytest.mark.parametrize("period", [7, 30, 60, 90])
def test_update_settings_accepts_supported_periods(
    settings_session_factory,
    period: int,
) -> None:
    result = sales_analysis_settings_service.update_settings(
        "alice",
        _payload(defaultPeriodDays=period),
    )

    assert result["defaultPeriodDays"] == period


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("defaultPeriodDays", 14),
        ("defaultRankingLimit", 4),
        ("defaultRankingLimit", 101),
        ("defaultMetric", "profit"),
        ("defaultGrain", "quarter"),
        ("answerDetailLevel", "verbose"),
        ("customBusinessInstructions", "x" * 4001),
    ],
)
def test_update_settings_rejects_unsupported_values(
    settings_session_factory,
    field: str,
    value,
) -> None:
    with pytest.raises(ValueError):
        sales_analysis_settings_service.update_settings(
            "alice",
            _payload(**{field: value}),
        )


def test_update_settings_normalizes_enum_and_boolean_values(
    settings_session_factory,
) -> None:
    result = sales_analysis_settings_service.update_settings(
        "alice",
        _payload(
            defaultMetric=" orderCount ",
            defaultGrain=" WEEK ",
            answerDetailLevel=" detailed ",
            prioritizeAdjustmentRisk=1,
            showDataUpdatedAt=0,
            showMetricDefinition=True,
            customBusinessInstructions="  先列出风险  ",
        ),
    )

    assert result == {
        "defaultPeriodDays": 30,
        "defaultRankingLimit": 10,
        "defaultMetric": "orderCount",
        "defaultGrain": "week",
        "answerDetailLevel": "detailed",
        "prioritizeAdjustmentRisk": True,
        "showDataUpdatedAt": False,
        "showMetricDefinition": True,
        "customBusinessInstructions": "先列出风险",
    }


def test_default_metric_matches_existing_sales_tool_metric_contract(
    settings_session_factory,
) -> None:
    result = sales_analysis_settings_service.update_settings(
        "alice",
        _payload(defaultMetric="effectiveSalesAmount"),
    )

    assert result["defaultMetric"] == "effectiveSalesAmount"


def test_capability_and_constraint_catalogs_are_fresh_read_only_data() -> None:
    capabilities = sales_analysis_settings_service.capability_catalog()
    constraints = sales_analysis_settings_service.constraint_catalog()

    assert len(capabilities) == 9
    assert {"title", "description", "example", "metrics"} <= capabilities[0].keys()
    assert [section["key"] for section in constraints] == [
        "dataPermissions",
        "aiAndSecrets",
        "analysisScope",
        "metricDefinitions",
        "errorHandling",
    ]
    assert all(section["items"] for section in constraints)

    capabilities[0]["title"] = "changed"
    constraints[0]["items"].append("changed")

    assert (
        sales_analysis_settings_service.capability_catalog()[0]["title"]
        != "changed"
    )
    assert (
        "changed"
        not in sales_analysis_settings_service.constraint_catalog()[0]["items"]
    )
