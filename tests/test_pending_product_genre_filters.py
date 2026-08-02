from __future__ import annotations

from contextlib import contextmanager

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.db.database import Base
from app.db.models import ProductModel, UserAccountModel
from app.services import crawler_service


def test_pending_products_filter_by_genre_status_and_tree_path(monkeypatch) -> None:
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

    genres = {
        "100001": {"genrePath": "服饰>女装>连衣裙"},
        "100002": {"genrePath": "服饰>女装>半身裙"},
        "200001": {"genrePath": "鞋靴>女鞋>凉鞋"},
    }
    monkeypatch.setattr(crawler_service, "session_scope", local_session_scope)
    monkeypatch.setattr(
        crawler_service,
        "load_rakuten_attribute_rules",
        lambda: {"genres": genres},
    )

    with local_session_scope() as session:
        session.add(
            UserAccountModel(
                username="alice",
                display_name="Alice",
                password_salt_b64="salt",
                password_hash_b64="hash",
            )
        )
        session.add_all(
            [
                ProductModel(
                    owner_username="alice",
                    title="连衣裙",
                    source_url="https://example.com/dress",
                    source_url_hash="dress",
                    review_status="pending",
                    genre_id="100001",
                ),
                ProductModel(
                    owner_username="alice",
                    title="半身裙",
                    source_url="https://example.com/skirt",
                    source_url_hash="skirt",
                    review_status="pending",
                    genre_id="100002",
                ),
                ProductModel(
                    owner_username="alice",
                    title="凉鞋",
                    source_url="https://example.com/shoes",
                    source_url_hash="shoes",
                    review_status="pending",
                    genre_id="200001",
                ),
                ProductModel(
                    owner_username="alice",
                    title="没有品类",
                    source_url="https://example.com/missing",
                    source_url_hash="missing",
                    review_status="pending",
                    genre_id="",
                ),
                ProductModel(
                    owner_username="alice",
                    title="无效品类",
                    source_url="https://example.com/invalid",
                    source_url_hash="invalid",
                    review_status="pending",
                    genre_id="999999",
                ),
            ]
        )

    present = crawler_service.list_products(
        "alice",
        status="pending",
        genre_status="present",
        page=1,
        page_size=20,
    )
    missing = crawler_service.list_products(
        "alice",
        status="pending",
        genre_status="missing",
        page=1,
        page_size=20,
    )
    women_clothing = crawler_service.list_products(
        "alice",
        status="pending",
        genre_path="服饰>女装",
        page=1,
        page_size=20,
    )

    assert {item["title"] for item in present["products"]} == {"连衣裙", "半身裙", "凉鞋"}
    assert {item["title"] for item in missing["products"]} == {"没有品类", "无效品类"}
    assert {item["title"] for item in women_clothing["products"]} == {"连衣裙", "半身裙"}

    with pytest.raises(ValueError, match="仅支持待审核"):
        crawler_service.list_products(
            "alice",
            status="approved",
            genre_status="present",
        )

    engine.dispose()
