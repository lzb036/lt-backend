from __future__ import annotations

from contextlib import contextmanager

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.db.database import Base
from app.db.models import ProductModel, UserAccountModel
from app.services import crawler_service


def test_manual_pending_products_support_review_filters_and_sorting(monkeypatch) -> None:
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
                    title="Unknown reviews",
                    source_url="https://example.com/unknown",
                    source_url_hash="unknown",
                    collection_source="manual",
                    review_status="pending",
                    price=50,
                    review_count=None,
                ),
                ProductModel(
                    owner_username="alice",
                    title="No reviews",
                    source_url="https://example.com/none",
                    source_url_hash="none",
                    collection_source="manual",
                    review_status="pending",
                    price=200,
                    review_count=0,
                ),
                ProductModel(
                    owner_username="alice",
                    title="Some reviews",
                    source_url="https://example.com/some",
                    source_url_hash="some",
                    collection_source="manual",
                    review_status="pending",
                    price=100,
                    review_count=3,
                ),
                ProductModel(
                    owner_username="alice",
                    title="Many reviews",
                    source_url="https://example.com/many",
                    source_url_hash="many",
                    collection_source="manual",
                    review_status="pending",
                    price=None,
                    review_count=20,
                ),
            ]
        )

    monkeypatch.setattr(crawler_service, "session_scope", local_session_scope)

    has_reviews = crawler_service.list_products(
        "alice",
        status="pending",
        collection_source="manual",
        review_filter="has",
    )
    no_reviews = crawler_service.list_products(
        "alice",
        status="pending",
        collection_source="manual",
        review_filter="none",
    )
    price_ascending = crawler_service.list_products(
        "alice",
        status="pending",
        collection_source="manual",
        sort="price_asc",
    )
    price_descending = crawler_service.list_products(
        "alice",
        status="pending",
        collection_source="manual",
        sort="price_desc",
    )
    reviews_descending = crawler_service.list_products(
        "alice",
        status="pending",
        collection_source="manual",
        sort="review_count_desc",
    )

    assert {item["title"] for item in has_reviews} == {"Some reviews", "Many reviews"}
    assert {item["title"] for item in no_reviews} == {"No reviews", "Unknown reviews"}
    assert [item["reviewCount"] for item in reviews_descending] == [20, 3, 0, 0]
    assert [item["title"] for item in price_ascending] == [
        "Unknown reviews",
        "Some reviews",
        "No reviews",
        "Many reviews",
    ]
    assert [item["title"] for item in price_descending] == [
        "No reviews",
        "Some reviews",
        "Unknown reviews",
        "Many reviews",
    ]

    engine.dispose()


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        (
            {"status": "pending", "collection_source": "scheduled", "review_filter": "has"},
            "仅支持手动采集待审核商品",
        ),
        (
            {"status": "approved", "collection_source": "manual", "sort": "price_asc"},
            "仅支持手动采集待审核商品",
        ),
    ],
)
def test_review_filters_and_sorting_are_scoped_to_manual_pending_products(
    kwargs,
    message,
) -> None:
    with pytest.raises(ValueError, match=message):
        crawler_service.list_products("alice", **kwargs)


def test_unknown_review_filter_is_rejected() -> None:
    with pytest.raises(ValueError, match="评论状态筛选条件无效"):
        crawler_service.list_products(
            "alice",
            status="pending",
            collection_source="manual",
            review_filter="unknown",
        )


@pytest.mark.parametrize(
    ("raw_payload", "expected"),
    [
        ({"reviewCount": 0}, 0),
        ({"reviewCount": "1,234"}, 1234),
        ({"reviewCount": -2}, 0),
        ({"reviewCount": "invalid"}, None),
        ({}, None),
    ],
)
def test_product_review_count_normalization(raw_payload, expected) -> None:
    assert crawler_service.product_review_count(raw_payload) == expected
