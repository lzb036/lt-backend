from __future__ import annotations

import json
from contextlib import contextmanager
from datetime import datetime, timedelta
from unittest.mock import patch

import pytest
from fastapi.routing import APIRoute
from pydantic import ValidationError
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.api import crawler as crawler_api
from app.db.database import Base
from app.db.models import ProductModel, UserAccountModel
from app.services import crawler_service


@pytest.fixture()
def local_database(monkeypatch):
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    Base.metadata.create_all(engine)
    factory = sessionmaker(bind=engine, expire_on_commit=False, future=True)

    @contextmanager
    def local_session_scope():
        session = factory()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    with local_session_scope() as session:
        session.add_all([
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
        ])

    monkeypatch.setattr(crawler_service, "session_scope", local_session_scope)
    yield local_session_scope
    engine.dispose()


def add_product(
    session_scope,
    *,
    title: str,
    owner: str = "alice",
    source: str = "manual",
    status: str = "pending",
    genre_id: str = "100001",
    created_at: datetime | None = None,
    replacement: bool = False,
) -> int:
    raw_payload = {
        "title": title,
        "genreId": genre_id,
    }
    if replacement:
        raw_payload["_replacement"] = {
            "taskId": f"replacement-{title}",
            "targetProductId": 999,
        }
    with session_scope() as session:
        row = ProductModel(
            owner_username=owner,
            title=title,
            source_url=f"https://example.com/{owner}/{title}",
            source_url_hash=f"{owner}-{title}",
            collection_source=source,
            review_status=status,
            genre_id=genre_id,
            raw_payload_json=json.dumps(raw_payload),
            created_at=created_at or datetime.now(),
        )
        session.add(row)
        session.flush()
        return row.id


def statuses(session_scope) -> dict[str, str]:
    with session_scope() as session:
        return {
            row.title: row.review_status
            for row in session.query(ProductModel).order_by(ProductModel.id).all()
        }


def test_count_mode_approves_exact_eligible_count_in_default_order(local_database, monkeypatch) -> None:
    now = datetime.now()
    newest_invalid = add_product(
        local_database,
        title="newest-invalid",
        genre_id="",
        created_at=now,
    )
    newest_valid = add_product(
        local_database,
        title="newest-valid",
        created_at=now - timedelta(seconds=1),
    )
    replacement = add_product(
        local_database,
        title="replacement",
        replacement=True,
        created_at=now - timedelta(seconds=2),
    )
    older_valid = add_product(
        local_database,
        title="older-valid",
        created_at=now - timedelta(seconds=3),
    )
    add_product(
        local_database,
        title="oldest-valid",
        created_at=now - timedelta(seconds=4),
    )

    monkeypatch.setattr(
        crawler_service,
        "rakuten_genre_path",
        lambda genre_id: "有效品类" if genre_id == "100001" else "",
    )
    with patch("app.services.ai_title_service.cleanup_title_versions_for_approved_product") as cleanup:
        result = crawler_service.approve_pending_products_by_count(
            "alice",
            collection_source="manual",
            count=2,
        )

    assert result["approvedProductIds"] == [newest_valid, older_valid]
    assert result["approvedCount"] == 2
    assert result["skippedCount"] == 2
    assert {item["productId"] for item in result["skipped"]} == {newest_invalid, replacement}
    assert cleanup.call_count == 2
    assert statuses(local_database) == {
        "newest-invalid": "pending",
        "newest-valid": "approved",
        "replacement": "pending",
        "older-valid": "approved",
        "oldest-valid": "pending",
    }


def test_all_mode_keeps_source_owner_and_status_isolation(local_database, monkeypatch) -> None:
    manual_id = add_product(local_database, title="alice-manual")
    scheduled_id = add_product(local_database, title="alice-scheduled", source="scheduled")
    approved_id = add_product(local_database, title="already-approved", status="approved")
    other_owner_id = add_product(local_database, title="bob-manual", owner="bob")

    monkeypatch.setattr(crawler_service, "rakuten_genre_path", lambda _genre_id: "有效品类")
    with patch("app.services.ai_title_service.cleanup_title_versions_for_approved_product"):
        result = crawler_service.approve_pending_products_by_count(
            "alice",
            collection_source="scheduled",
            count=None,
        )

    assert result["approvedProductIds"] == [scheduled_id]
    assert result["approvedCount"] == 1
    current_statuses = statuses(local_database)
    assert current_statuses["alice-manual"] == "pending"
    assert current_statuses["alice-scheduled"] == "approved"
    assert current_statuses["already-approved"] == "approved"
    assert current_statuses["bob-manual"] == "pending"
    assert {manual_id, approved_id, other_owner_id}.isdisjoint(result["approvedProductIds"])


def test_count_mode_reports_when_fewer_eligible_products_exist(local_database, monkeypatch) -> None:
    add_product(local_database, title="valid")
    add_product(local_database, title="invalid", genre_id="")
    monkeypatch.setattr(
        crawler_service,
        "rakuten_genre_path",
        lambda genre_id: "有效品类" if genre_id else "",
    )
    with patch("app.services.ai_title_service.cleanup_title_versions_for_approved_product"):
        result = crawler_service.approve_pending_products_by_count(
            "alice",
            collection_source="manual",
            count=5,
        )

    assert result["approvedCount"] == 1
    assert result["skippedCount"] == 1


def test_service_rejects_invalid_source_and_count(local_database) -> None:
    with pytest.raises(RuntimeError, match="采集来源"):
        crawler_service.approve_pending_products_by_count(
            "alice",
            collection_source="other",
            count=1,
        )
    with pytest.raises(RuntimeError, match="大于 0"):
        crawler_service.approve_pending_products_by_count(
            "alice",
            collection_source="manual",
            count=0,
        )


def test_payload_requires_count_for_count_mode() -> None:
    with pytest.raises(ValidationError, match="必须填写通过数量"):
        crawler_api.ProductApproveByCountPayload(
            collectionSource="manual",
            mode="count",
        )
    payload = crawler_api.ProductApproveByCountPayload(
        collectionSource="scheduled",
        mode="all",
    )
    assert payload.count is None


def test_route_uses_products_manage_permission() -> None:
    route = next(
        route
        for route in crawler_api.router.routes
        if isinstance(route, APIRoute)
        and route.path == "/crawler/products/approve-by-count"
        and "POST" in route.methods
    )
    assert route.dependencies == []
    assert route.dependant.dependencies[0].call is crawler_api.require_products_permission
