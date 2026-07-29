from __future__ import annotations

from contextlib import contextmanager
import json
import threading
import time
from types import SimpleNamespace

import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

from app.db.database import Base
from app.db.models import ListingTaskModel, ProductModel, StoreModel, UserAccountModel
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


def listing_product_and_store():
    product = SimpleNamespace(
        id=123,
        title="Listing product",
        image_url="https://example.com/source.jpg",
        raw_payload_json="{}",
        review_status="approved",
        rakuten_manage_number=None,
    )
    store = SimpleNamespace(
        id=7,
        store_code="shop-code",
        store_name="Store",
        alias_name="Store 7",
    )
    return product, store


def install_direct_listing_mocks(monkeypatch, *, inventory_error: Exception | None = None):
    calls: list[tuple[str, object]] = []
    uploaded_main = [{"location": "/cabinet/main.jpg", "fileId": "main-1"}]
    uploaded_description = [{"location": "/cabinet/description.jpg", "fileId": "desc-1"}]

    monkeypatch.setattr(crawler_service, "product_raw_payload", lambda _product: {"variants": {}})
    monkeypatch.setattr(crawler_service, "generate_listing_manage_number", lambda *_args: "manage-1")
    monkeypatch.setattr(
        crawler_service,
        "upload_product_images_to_rakuten",
        lambda *_args, **_kwargs: uploaded_main,
    )
    monkeypatch.setattr(
        crawler_service,
        "upload_product_description_images_to_rakuten",
        lambda *_args, **_kwargs: {
            "rawPayload": {"variants": {}},
            "uploadedImages": uploaded_description,
        },
    )

    def build_payload(_product, _raw, images, *, manage_number, hide_item):
        calls.append(("build", {"manageNumber": manage_number, "hideItem": hide_item}))
        return {
            "itemNumber": manage_number,
            "hideItem": hide_item,
            "images": images,
            "variants": {"sku-1": {"standardPrice": 1000}},
        }

    monkeypatch.setattr(crawler_service, "build_rakuten_item_upsert_payload", build_payload)

    def put_item(_secret, _key, manage_number, payload):
        calls.append(("put", {"manageNumber": manage_number, "hideItem": payload["hideItem"]}))
        return payload

    monkeypatch.setattr(crawler_service, "put_rakuten_item_with_attribute_retry", put_item)
    monkeypatch.setattr(
        crawler_service,
        "build_rakuten_inventory_upsert_payloads",
        lambda *_args: [{"sku": "sku-1", "quantity": 1000}],
    )

    def upsert_inventory(_secret, _key, payloads):
        calls.append(("inventory", payloads))
        if inventory_error is not None:
            raise inventory_error

    monkeypatch.setattr(crawler_service, "bulk_upsert_rakuten_inventories", upsert_inventory)
    monkeypatch.setattr(
        crawler_service,
        "patch_rakuten_item_visibility",
        lambda *_args, **_kwargs: calls.append(("visibility", None)),
    )
    monkeypatch.setattr(
        crawler_service,
        "delete_rakuten_item",
        lambda *_args, **_kwargs: calls.append(("delete", None)),
    )
    monkeypatch.setattr(
        crawler_service,
        "rollback_uploaded_listing_images",
        lambda _secret, _key, images: calls.append(("rollback", images)) or "",
    )
    monkeypatch.setattr(crawler_service, "price_from_rakuten_item", lambda _payload: 1000)
    monkeypatch.setattr(
        crawler_service,
        "build_rakuten_cabinet_image_url",
        lambda _store_code, location: f"https://example.com{location}",
    )
    return calls


def test_listing_creates_complete_visible_item_with_one_item_write(monkeypatch):
    product, store = listing_product_and_store()
    calls = install_direct_listing_mocks(monkeypatch)

    result = crawler_service.create_store_product_on_rakuten(
        "secret",
        "key",
        store,
        product,
    )

    assert result["manageNumber"] == "manage-1"
    assert [name for name, _payload in calls].count("put") == 1
    assert [name for name, _payload in calls].count("inventory") == 1
    assert "visibility" not in [name for name, _payload in calls]
    assert ("build", {"manageNumber": "manage-1", "hideItem": False}) in calls


def test_listing_inventory_failure_deletes_visible_item_and_rolls_back_images(monkeypatch):
    product, store = listing_product_and_store()
    calls = install_direct_listing_mocks(
        monkeypatch,
        inventory_error=RuntimeError("inventory failed"),
    )

    with pytest.raises(RuntimeError, match="inventory failed"):
        crawler_service.create_store_product_on_rakuten(
            "secret",
            "key",
            store,
            product,
        )

    names = [name for name, _payload in calls]
    assert names.count("put") == 1
    assert names.count("delete") == 1
    assert names.count("rollback") == 1
    assert "visibility" not in names


def test_listing_image_preparation_preserves_order_and_skips_unavailable(monkeypatch):
    monkeypatch.setattr(crawler_service.settings, "listing_image_prepare_workers", 4)
    monkeypatch.setattr(
        crawler_service,
        "prepare_rakuten_listing_image",
        lambda image_url: None if image_url == "missing" else {
            "sourceUrl": image_url,
            "suffix": ".jpg",
        },
    )

    prepared = crawler_service.prepare_rakuten_listing_images(
        ["first", "missing", "second"],
    )

    assert [row["sourceUrl"] for row in prepared] == ["first", "second"]


def test_listing_task_limit_remains_fifty():
    assert crawler_service.BATCH_TASK_PRODUCT_LIMIT == 50
    assert [len(chunk) for chunk in crawler_service.chunk_product_ids(list(range(1, 102)))] == [50, 50, 1]


def test_listing_dispatch_uses_global_user_and_store_capacity(
    monkeypatch,
    session_factory,
):
    install_session_scope(monkeypatch, session_factory)
    monkeypatch.setattr(crawler_service, "finalize_stale_cancel_requested_tasks", lambda *_args, **_kwargs: 0)
    monkeypatch.setattr(crawler_service, "reconcile_interrupted_running_tasks", lambda *_args, **_kwargs: 0)
    monkeypatch.setattr(crawler_service, "should_use_redis_task_queue", lambda: False)
    monkeypatch.setattr(crawler_service.settings, "max_running_listing_tasks_global", 5)
    monkeypatch.setattr(crawler_service.settings, "max_running_listing_tasks_per_user", 2)
    monkeypatch.setattr(crawler_service.settings, "max_running_listing_tasks_per_store", 1)
    dispatched: list[tuple[str, str]] = []
    monkeypatch.setattr(
        crawler_service,
        "dispatch_listing_task",
        lambda owner, task_id: dispatched.append((owner, task_id)),
    )

    with session_factory() as session:
        session.add_all([
            UserAccountModel(
                username=username,
                display_name=username,
                password_salt_b64="salt",
                password_hash_b64="hash",
            )
            for username in ("alice", "bob")
        ])
        alice_store = StoreModel(
            owner_username="alice",
            store_code="alice-store-1",
            store_name="Alice Store 1",
        )
        alice_store_two = StoreModel(
            owner_username="alice",
            store_code="alice-store-2",
            store_name="Alice Store 2",
        )
        bob_store = StoreModel(
            owner_username="bob",
            store_code="bob-store",
            store_name="Bob Store",
        )
        session.add_all([alice_store, alice_store_two, bob_store])
        session.flush()
        task_specs = [
            ("a1", "alice", alice_store.id),
            ("a2", "alice", alice_store_two.id),
            ("a3", "alice", alice_store.id),
            ("b1", "bob", bob_store.id),
        ]
        session.add_all([
            ListingTaskModel(
                id=task_id,
                owner_username=owner,
                store_id=store_id,
                task_name=task_id,
                status="queued",
                product_ids_json=json.dumps({
                    "productIds": [index],
                    "storeIds": [store_id],
                }),
            )
            for index, (task_id, owner, store_id) in enumerate(task_specs, start=1)
        ])
        session.commit()

    crawler_service.dispatch_next_listing_task()

    assert dispatched == [
        ("alice", "a1"),
        ("alice", "a2"),
        ("bob", "b1"),
    ]
    with session_factory() as session:
        delayed = session.get(ListingTaskModel, "a3")
        assert delayed is not None
        assert "并发额度" in delayed.message


def test_listing_task_processes_six_products_concurrently(
    monkeypatch,
    session_factory,
):
    install_session_scope(monkeypatch, session_factory)
    monkeypatch.setattr(crawler_service.settings, "listing_product_workers", 6)
    monkeypatch.setattr(crawler_service, "listing_task_start_wait_reason", lambda *_args: "")
    monkeypatch.setattr(crawler_service, "decrypt_text", lambda value: value)
    monkeypatch.setattr(crawler_service, "fetch_rakuten_cabinet_usage", lambda *_args: {})
    monkeypatch.setattr(crawler_service, "apply_store_cabinet_usage", lambda *_args: None)
    monkeypatch.setattr(crawler_service, "raise_if_task_cancelled", lambda *_args: None)
    monkeypatch.setattr(crawler_service, "listing_task_cancel_requested", lambda *_args: False)
    monkeypatch.setattr(crawler_service, "update_task_progress", lambda *_args, **_kwargs: None)

    active = 0
    max_active = 0
    active_lock = threading.Lock()

    def fake_attempt(
        _owner,
        _task_id,
        _store_id,
        product_id,
        _secret,
        _key,
        _cabinet_usage,
    ):
        nonlocal active, max_active
        with active_lock:
            active += 1
            max_active = max(max_active, active)
        time.sleep(0.05)
        with active_lock:
            active -= 1
        return crawler_service.ListingProductAttemptResult(
            product_id=product_id,
            success=True,
        )

    monkeypatch.setattr(crawler_service, "run_listing_product_attempt", fake_attempt)

    with session_factory() as session:
        session.add(
            UserAccountModel(
                username="alice",
                display_name="alice",
                password_salt_b64="salt",
                password_hash_b64="hash",
            )
        )
        store = StoreModel(
            owner_username="alice",
            store_code="alice-store",
            store_name="Alice Store",
            enabled=True,
            rakuten_service_secret_encrypted="secret",
            rakuten_license_key_encrypted="key",
        )
        session.add(store)
        session.flush()
        products = [
            ProductModel(
                owner_username="alice",
                title=f"Product {index}",
                source_url=f"https://example.com/{index}",
                source_url_hash=f"hash-{index}",
                review_status="approved",
                listing_task_id="listing-task",
            )
            for index in range(6)
        ]
        session.add_all(products)
        session.flush()
        product_ids = [int(product.id) for product in products]
        session.add(
            ListingTaskModel(
                id="listing-task",
                owner_username="alice",
                store_id=store.id,
                task_name="Listing task",
                status="queued",
                total_count=6,
                product_ids_json=json.dumps({
                    "productIds": product_ids,
                    "successIds": [],
                    "failedIds": [],
                    "storeIds": [store.id],
                }),
            )
        )
        session.commit()

    crawler_service._run_listing_task("alice", "listing-task")

    assert max_active == 6
    with session_factory() as session:
        task = session.get(ListingTaskModel, "listing-task")
        assert task is not None
        assert task.status == "success"
        assert task.success_count == 6
        assert task.failed_count == 0
