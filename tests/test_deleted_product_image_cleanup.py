from contextlib import contextmanager
from datetime import datetime
import json
from pathlib import Path

import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

from app.db.database import Base
from app.db.models import (
    DeletedProductImageCleanupModel,
    ProductModel,
    StoreModel,
    SyncTaskModel,
    UserAccountModel,
)
from app.services import crawler_service


@pytest.fixture()
def session_factory():
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    Base.metadata.create_all(engine)
    factory = sessionmaker(bind=engine, expire_on_commit=False, future=True)
    try:
        yield factory
    finally:
        engine.dispose()


def install_session_scope(monkeypatch, session_factory):
    @contextmanager
    def local_session_scope():
        session = session_factory()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    monkeypatch.setattr(crawler_service, "session_scope", local_session_scope)


def seed_owner_store(session_factory):
    with session_factory() as session:
        session.add(UserAccountModel(
            username="alice",
            display_name="Alice",
            password_salt_b64="salt",
            password_hash_b64="hash",
        ))
        session.flush()
        store = StoreModel(
            owner_username="alice",
            store_code="shop",
            store_name="Shop",
            enabled=True,
            rakuten_service_secret_encrypted="secret",
            rakuten_license_key_encrypted="key",
        )
        session.add(store)
        session.commit()
        return int(store.id)


def test_deleted_image_cleanup_tasks_chunk_and_remove_successful_records(monkeypatch, session_factory):
    install_session_scope(monkeypatch, session_factory)
    store_id = seed_owner_store(session_factory)
    with session_factory() as session:
        session.add_all([
            DeletedProductImageCleanupModel(
                owner_username="alice",
                store_id=store_id,
                store_name="Shop",
                original_product_id=1000 + index,
                product_code=f"p-{index}",
                cabinet_targets_json='[{"filePath":"p.jpg","fileName":"p.jpg"}]',
                local_image_urls_json="[]",
            )
            for index in range(51)
        ])
        session.commit()

    with session_factory() as session:
        task_refs, product_count = crawler_service.create_deleted_product_image_cleanup_tasks(
            session,
            datetime(2026, 7, 25, 9, 0, 0),
        )
        session.commit()
        tasks = session.scalars(
            select(SyncTaskModel).where(SyncTaskModel.task_type == "deleted_product_image_cleanup")
        ).all()
        assert product_count == 51
        assert len(task_refs) == 2
        assert sorted(task.total_count for task in tasks) == [1, 50]

    monkeypatch.setattr(crawler_service, "decrypt_text", lambda value: value)
    monkeypatch.setattr(crawler_service, "delete_cabinet_targets", lambda *args: (1, []))
    monkeypatch.setattr(crawler_service, "cleanup_queued_local_image_urls", lambda *args: None)
    with session_factory() as session:
        first_task = session.scalars(
            select(SyncTaskModel)
            .where(SyncTaskModel.task_type == "deleted_product_image_cleanup")
            .order_by(SyncTaskModel.total_count.asc())
        ).first()
        cleanup_ids = crawler_service.sync_task_payload(first_task)["cleanupRecordIds"]

    result = crawler_service.perform_deleted_product_image_cleanup(
        "alice",
        store_id,
        cleanup_ids,
    )
    assert result["successCount"] == 1
    with session_factory() as session:
        assert session.get(DeletedProductImageCleanupModel, cleanup_ids[0]) is None


def test_store_product_delete_queues_images_without_deleting_them(monkeypatch, session_factory):
    store_id = seed_owner_store(session_factory)
    monkeypatch.setattr(crawler_service, "decrypt_text", lambda value: value)
    monkeypatch.setattr(crawler_service, "delete_rakuten_item", lambda *args: None)
    monkeypatch.setattr(
        crawler_service,
        "delete_product_cabinet_images",
        lambda *args: (_ for _ in ()).throw(AssertionError("images must not be deleted immediately")),
    )
    with session_factory() as session:
        product = ProductModel(
            owner_username="alice",
            store_id=store_id,
            title="Product",
            source_url="https://example.com/product",
            source_url_hash="hash",
            rakuten_manage_number="manage-1",
            item_number="item-1",
            review_status="listed",
            raw_payload_json=json.dumps({
                "images": ["https://image.rakuten.co.jp/shop/cabinet/products/p1.jpg"],
            }),
        )
        session.add(product)
        session.flush()
        product.image_url = f"/api/static/product-images/{product.id}/main.jpg"
        store = session.get(StoreModel, store_id)
        crawler_service.delete_store_product_from_rakuten(session, product, {})
        cleanup = session.scalar(select(DeletedProductImageCleanupModel))
        assert cleanup is not None
        assert cleanup.original_product_id == product.id
        assert len(json.loads(cleanup.cabinet_targets_json)) == 1
        assert json.loads(cleanup.local_image_urls_json) == [product.image_url]


def test_deleted_image_cleanup_endpoints_are_superadmin_only():
    source = Path("app/api/crawler.py").read_text(encoding="utf-8")
    assert '@router.get("/settings/time/deleted-product-images")' in source
    assert '@router.post("/settings/time/deleted-product-images/run")' in source
    endpoint_source = source[source.index('@router.get("/settings/time/deleted-product-images")'):source.index('@router.get("/settings/resources/proxy-usage")')]
    assert endpoint_source.count("Depends(require_superadmin)") == 2
