from __future__ import annotations

from contextlib import contextmanager
from unittest.mock import patch

import pytest
from fastapi.routing import APIRoute
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.api import crawler as crawler_api
from app.db.database import Base
from app.db.models import UserAccountModel
from app.services import crawler_service


@pytest.fixture()
def session_factory():
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
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


@pytest.fixture()
def local_session_scope(monkeypatch, session_factory):
    @contextmanager
    def _session_scope():
        session = session_factory()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    monkeypatch.setattr(crawler_service, "session_scope", _session_scope)
    return _session_scope


def add_user(session_factory, username: str) -> None:
    with session_factory() as session:
        session.add(
            UserAccountModel(
                username=username,
                display_name=username,
                password_salt_b64="salt",
                password_hash_b64="hash",
            )
        )
        session.commit()


def test_collection_genre_defaults_allow_all(
    session_factory,
    local_session_scope,
) -> None:
    add_user(session_factory, "alice")

    config = crawler_service.user_collection_genre_config("alice")

    assert config == {
        "defaultPolicy": "allow",
        "unknownGenrePolicy": "allow",
        "ruleCount": 0,
    }


def test_collection_genre_rules_are_isolated_and_deepest_rule_wins(
    session_factory,
    local_session_scope,
) -> None:
    add_user(session_factory, "alice")
    add_user(session_factory, "bob")
    parent_path = "キッズ・ベビー・マタニティ>キッズファッション"
    child_path = f"{parent_path}>親子ペアルック"

    parent_rule = crawler_service.save_user_collection_genre_rule(
        "alice",
        genre_path=parent_path,
        policy="deny",
    )
    child_rule = crawler_service.save_user_collection_genre_rule(
        "alice",
        genre_path=child_path,
        genre_id="566733",
        policy="allow",
    )

    with session_factory() as session:
        alice_snapshot = crawler_service.collection_genre_policy_snapshot(session, "alice")
        bob_snapshot = crawler_service.collection_genre_policy_snapshot(session, "bob")

    assert parent_rule["explicitPolicy"] == "deny"
    assert child_rule["explicitPolicy"] == "allow"
    assert crawler_service.resolve_collection_genre_policy(
        alice_snapshot,
        genre_id="566733",
    )["policy"] == "allow"
    assert crawler_service.resolve_collection_genre_policy(
        alice_snapshot,
        genre_path=f"{parent_path}>その他",
    )["policy"] == "deny"
    assert crawler_service.resolve_collection_genre_policy(
        bob_snapshot,
        genre_id="566733",
    )["policy"] == "allow"
    assert crawler_service.delete_user_collection_genre_rule(
        "bob",
        parent_rule["ruleId"],
    ) is False
    with session_factory() as session:
        refreshed_alice_snapshot = crawler_service.collection_genre_policy_snapshot(session, "alice")
    assert crawler_service.resolve_collection_genre_policy(
        refreshed_alice_snapshot,
        genre_path=f"{parent_path}>その他",
    )["policy"] == "deny"


def test_unknown_genre_uses_current_users_setting(
    session_factory,
    local_session_scope,
) -> None:
    add_user(session_factory, "alice")
    crawler_service.save_user_collection_genre_config(
        "alice",
        default_policy="allow",
        unknown_genre_policy="deny",
    )

    with session_factory() as session:
        snapshot = crawler_service.collection_genre_policy_snapshot(session, "alice")

    decision = crawler_service.resolve_collection_genre_policy(snapshot, genre_id="")
    assert decision["allowed"] is False
    assert decision["sourceType"] == "unknown"


def test_denied_collection_genre_is_skipped_before_database_write() -> None:
    snapshot = crawler_service.CollectionGenrePolicySnapshot(
        default_policy="allow",
        unknown_genre_policy="allow",
        rules_by_path={
            "キッズ・ベビー・マタニティ>キッズファッション": "deny",
        },
    )
    item = {
        "title": "亲子装",
        "source_url": "https://item.rakuten.co.jp/example/item/",
        "raw": {"genreId": "566733"},
    }

    with patch.object(crawler_service, "session_scope") as session_scope:
        result = crawler_service.save_collected_item(
            "alice",
            "task-id",
            item,
            collection_genre_policy=snapshot,
        )

    session_scope.assert_not_called()
    assert result["saved"] is False
    assert result["skipped"] is True
    assert "禁止采集" in result["error"]


def test_collection_genre_routes_use_crawler_permission() -> None:
    expected_routes = {
        ("GET", "/crawler/settings/collection-genres/config"),
        ("PUT", "/crawler/settings/collection-genres/config"),
        ("GET", "/crawler/settings/collection-genres/children"),
        ("GET", "/crawler/settings/collection-genres/search"),
        ("PUT", "/crawler/settings/collection-genres/rules"),
        ("DELETE", "/crawler/settings/collection-genres/rules/{rule_id}"),
        ("GET", "/crawler/settings/collection-genres/pending-impact"),
    }
    actual_routes = {
        (method, route.path)
        for route in crawler_api.router.routes
        if isinstance(route, APIRoute)
        for method in route.methods
        if route.path.startswith("/crawler/settings/collection-genres")
    }

    assert actual_routes == expected_routes
    for method, path in expected_routes:
        route = next(
            route
            for route in crawler_api.router.routes
            if isinstance(route, APIRoute) and route.path == path and method in route.methods
        )
        dependency_calls = [dependency.call for dependency in route.dependant.dependencies]
        assert crawler_api.require_crawler_permission in dependency_calls
