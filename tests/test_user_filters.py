from __future__ import annotations

from contextlib import contextmanager

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.db.database import Base
from app.db.models import UserAccountModel
from app.services import user_service


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
                display_name="Alice Smith",
                password_salt_b64="salt",
                password_hash_b64="hash",
            ),
            UserAccountModel(
                username="bob",
                display_name="Bob Jones",
                password_salt_b64="salt",
                password_hash_b64="hash",
            ),
            UserAccountModel(
                username="alicia",
                display_name="Alice Brown",
                password_salt_b64="salt",
                password_hash_b64="hash",
            ),
        ])

    monkeypatch.setattr(user_service, "session_scope", local_session_scope)
    monkeypatch.setattr(user_service, "ensure_initial_superadmin", lambda: None)
    yield local_session_scope
    engine.dispose()


def test_user_list_filters_by_username(local_database) -> None:
    result = user_service.list_users(page=1, page_size=20, username="ali")

    assert result["total"] == 2
    assert [item["username"] for item in result["users"]] == ["alice", "alicia"]


def test_user_list_filters_by_display_name(local_database) -> None:
    result = user_service.list_users(page=1, page_size=20, display_name="Jones")

    assert result["total"] == 1
    assert [item["username"] for item in result["users"]] == ["bob"]


def test_user_list_combines_username_and_display_name_filters(local_database) -> None:
    result = user_service.list_users(
        page=1,
        page_size=20,
        username="ali",
        display_name="Brown",
    )

    assert result["total"] == 1
    assert [item["username"] for item in result["users"]] == ["alicia"]


def test_user_list_filter_is_applied_before_pagination(local_database) -> None:
    result = user_service.list_users(page=2, page_size=1, username="ali")

    assert result["total"] == 2
    assert [item["username"] for item in result["users"]] == ["alicia"]
