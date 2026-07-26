from __future__ import annotations

from contextlib import contextmanager

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.db.database import Base
from app.db.models import UserAccountModel
from app.services import user_service


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

    monkeypatch.setattr(user_service, "session_scope", _session_scope)
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


def test_pagination_preferences_are_isolated_by_user_and_list(
    session_factory,
    local_session_scope,
):
    add_user(session_factory, "alice")
    add_user(session_factory, "bob")

    assert user_service.update_pagination_preference("alice", "stores", 300) == {
        "stores": 300,
    }
    assert user_service.update_pagination_preference("alice", "sync-tasks", 90) == {
        "stores": 300,
        "sync-tasks": 90,
    }
    assert user_service.update_pagination_preference("bob", "stores", 60) == {
        "stores": 60,
    }

    with session_factory() as session:
        alice_row = session.get(UserAccountModel, "alice")
        bob_row = session.get(UserAccountModel, "bob")
        alice = user_service.account_to_public(
            alice_row,
            include_pagination_preferences=True,
        )
        bob = user_service.account_to_public(
            bob_row,
            include_pagination_preferences=True,
        )
        listed_alice = user_service.account_to_public(alice_row)

    assert alice["paginationPreferences"] == {"stores": 300, "sync-tasks": 90}
    assert bob["paginationPreferences"] == {"stores": 60}
    assert "paginationPreferences" not in listed_alice


@pytest.mark.parametrize(
    ("list_key", "page_size", "message"),
    [
        ("../stores", 30, "分页列表标识无效"),
        ("stores", 0, "分页数量必须在"),
        ("stores", 501, "分页数量必须在"),
    ],
)
def test_pagination_preference_rejects_invalid_values(
    session_factory,
    local_session_scope,
    list_key,
    page_size,
    message,
):
    add_user(session_factory, "alice")

    with pytest.raises(RuntimeError, match=message):
        user_service.update_pagination_preference("alice", list_key, page_size)
