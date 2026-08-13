from __future__ import annotations

import json
from contextlib import contextmanager
from unittest.mock import patch

import pytest
from fastapi.routing import APIRoute
from sqlalchemy import create_engine, event
from sqlalchemy.orm import sessionmaker

from app.api import users as users_api
from app.db.database import Base
from app.db.models import (
    CrawlLogModel,
    ProductModel,
    StoreModel,
    SystemMaintenanceSettingModel,
    SystemSettingModel,
    SystemTaskControlModel,
    UserAccountModel,
    UserSecretProfileModel,
)
from app.services import user_service


@pytest.fixture()
def local_database(monkeypatch):
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)

    @event.listens_for(engine, "connect")
    def _enable_foreign_keys(dbapi_connection, _):
        cursor = dbapi_connection.cursor()
        cursor.execute("PRAGMA foreign_keys=ON")
        cursor.close()

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

    monkeypatch.setattr(user_service, "session_scope", local_session_scope)
    yield local_session_scope
    engine.dispose()


def add_user_data(session_scope, username: str = "operator") -> int:
    with session_scope() as session:
        session.add(
            UserAccountModel(
                username=username,
                display_name="Operator",
                role="operator",
                password_salt_b64="salt",
                password_hash_b64="hash",
            )
        )
        session.flush()
        session.add(UserSecretProfileModel(owner_username=username))
        store = StoreModel(
            owner_username=username,
            store_code="store-code",
            store_name="Store",
        )
        session.add(store)
        session.flush()
        product = ProductModel(
            owner_username=username,
            store_id=store.id,
            title="Product",
            source_url="https://example.com/product",
            source_url_hash="product",
            raw_payload_json=json.dumps({"title": "Product"}),
        )
        session.add(product)
        session.add(
            CrawlLogModel(
                owner_username=username,
                task_id=None,
                level="info",
                message="log",
            )
        )
        session.add(
            SystemSettingModel(
                key=f"deletedProductImageCleanup:{username}",
                value_json="{}",
            )
        )
        session.add(
            SystemMaintenanceSettingModel(
                id=1,
                updated_by=username,
            )
        )
        session.add(
            SystemTaskControlModel(
                id=1,
                stopped_by=username,
                resumed_by=username,
            )
        )
        session.flush()
        return product.id


def test_delete_user_removes_related_data_and_cleans_product_images(local_database) -> None:
    product_id = add_user_data(local_database)

    with patch("app.services.crawler_service.cleanup_product_image_ids") as cleanup:
        result = user_service.delete_user(
            "operator",
            confirmation_text="删除用户 operator",
        )

    assert result == {
        "deleted": True,
        "username": "operator",
        "deletedProductCount": 1,
    }
    cleanup.assert_called_once_with([product_id])
    with local_database() as session:
        assert session.get(UserAccountModel, "operator") is None
        assert session.query(StoreModel).count() == 0
        assert session.query(ProductModel).count() == 0
        assert session.query(UserSecretProfileModel).count() == 0
        assert session.query(CrawlLogModel).count() == 0
        assert session.query(SystemSettingModel).count() == 0
        assert session.get(SystemMaintenanceSettingModel, 1).updated_by == ""
        task_control = session.get(SystemTaskControlModel, 1)
        assert task_control.stopped_by == ""
        assert task_control.resumed_by == ""


def test_delete_user_requires_exact_confirmation(local_database) -> None:
    add_user_data(local_database)

    with pytest.raises(RuntimeError, match="删除用户 operator"):
        user_service.delete_user(
            "operator",
            confirmation_text="operator",
        )

    with local_database() as session:
        assert session.get(UserAccountModel, "operator") is not None


def test_delete_user_rejects_superadmin(local_database) -> None:
    with local_database() as session:
        session.add(
            UserAccountModel(
                username="superadmin",
                display_name="Superadmin",
                role="superadmin",
                password_salt_b64="salt",
                password_hash_b64="hash",
            )
        )
        session.flush()

    with pytest.raises(RuntimeError, match="不能删除超级管理员"):
        user_service.delete_user(
            "superadmin",
            confirmation_text="删除用户 superadmin",
        )


def test_delete_user_route_requires_superadmin() -> None:
    route = next(
        route
        for route in users_api.router.routes
        if isinstance(route, APIRoute)
        and route.path == "/users/{username}"
        and "DELETE" in route.methods
    )
    assert route.dependant.dependencies[0].call is users_api.require_superadmin
