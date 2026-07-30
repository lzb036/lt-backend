from __future__ import annotations

from contextlib import contextmanager

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.db.database import Base
from app.db.models import ProductModel, UserAccountModel
from app.services import crawler_service


def test_pending_products_are_split_by_collection_source(monkeypatch) -> None:
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
        session.add_all([
            ProductModel(
                owner_username="alice",
                title="Alice manual",
                source_url="https://example.com/alice-manual",
                source_url_hash="alice-manual",
                review_status="pending",
            ),
            ProductModel(
                owner_username="alice",
                title="Alice scheduled",
                source_url="https://example.com/alice-scheduled",
                source_url_hash="alice-scheduled",
                scheduled_crawl_id=101,
                collection_source="scheduled",
                review_status="pending",
            ),
            ProductModel(
                owner_username="alice",
                title="Alice approved",
                source_url="https://example.com/alice-approved",
                source_url_hash="alice-approved",
                review_status="approved",
            ),
            ProductModel(
                owner_username="bob",
                title="Bob scheduled",
                source_url="https://example.com/bob-scheduled",
                source_url_hash="bob-scheduled",
                scheduled_crawl_id=102,
                collection_source="scheduled",
                review_status="pending",
            ),
        ])

    monkeypatch.setattr(crawler_service, "session_scope", local_session_scope)

    manual = crawler_service.list_products(
        "alice",
        status="pending",
        collection_source="manual",
        page=1,
        page_size=20,
    )
    scheduled = crawler_service.list_products(
        "alice",
        status="pending",
        collection_source="scheduled",
        page=1,
        page_size=20,
    )

    assert [item["title"] for item in manual["products"]] == ["Alice manual"]
    assert [item["title"] for item in scheduled["products"]] == ["Alice scheduled"]

    with local_session_scope() as session:
        scheduled_product = session.query(ProductModel).filter_by(
            owner_username="alice",
            title="Alice scheduled",
        ).one()
        scheduled_product.scheduled_crawl_id = None

    scheduled_after_source_deletion = crawler_service.list_products(
        "alice",
        status="pending",
        collection_source="scheduled",
        page=1,
        page_size=20,
    )
    assert [
        item["title"]
        for item in scheduled_after_source_deletion["products"]
    ] == ["Alice scheduled"]

    with pytest.raises(ValueError, match="仅支持待审核商品"):
        crawler_service.list_products(
            "alice",
            status="approved",
            collection_source="manual",
        )

    engine.dispose()
