from __future__ import annotations

from contextlib import contextmanager

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.db.database import Base
from app.db.models import ProductModel, UserAccountModel
from app.services import crawler_service


def test_approved_list_hides_listing_products_and_restores_only_failed_products(monkeypatch) -> None:
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
                    title="普通已审核商品",
                    source_url="https://example.com/approved",
                    source_url_hash="approved",
                    review_status="approved",
                ),
                ProductModel(
                    owner_username="alice",
                    title="正在上架商品",
                    source_url="https://example.com/listing",
                    source_url_hash="listing",
                    review_status="approved",
                    listing_task_id="listing-task",
                ),
            ]
        )

    monkeypatch.setattr(crawler_service, "session_scope", local_session_scope)

    while_listing = crawler_service.list_products(
        "alice",
        status="approved",
        page=1,
        page_size=20,
    )
    assert while_listing["total"] == 1
    assert [item["title"] for item in while_listing["products"]] == ["普通已审核商品"]

    with local_session_scope() as session:
        listing_product = session.query(ProductModel).filter_by(
            owner_username="alice",
            title="正在上架商品",
        ).one()
        listing_product.listing_task_id = None

    after_failure = crawler_service.list_products(
        "alice",
        status="approved",
        page=1,
        page_size=20,
    )
    assert after_failure["total"] == 2
    assert {item["title"] for item in after_failure["products"]} == {
        "普通已审核商品",
        "正在上架商品",
    }

    with local_session_scope() as session:
        listing_product = session.query(ProductModel).filter_by(
            owner_username="alice",
            title="正在上架商品",
        ).one()
        listing_product.review_status = "listed_master"

    after_success = crawler_service.list_products(
        "alice",
        status="approved",
        page=1,
        page_size=20,
    )
    assert after_success["total"] == 1
    assert [item["title"] for item in after_success["products"]] == ["普通已审核商品"]

    engine.dispose()
