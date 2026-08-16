from __future__ import annotations

from contextlib import contextmanager
import json
import threading
import time
from io import BytesIO
from types import SimpleNamespace

import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

from app.db.database import Base
from app.db.models import (
    ListingTaskModel,
    ProductListingPreparationModel,
    ProductModel,
    StoreModel,
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


def listing_product_and_store():
    product = SimpleNamespace(
        id=123,
        title="Listing product",
        source_url="https://item.rakuten.co.jp/source-shop/item-1/",
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


def install_direct_listing_mocks(
    monkeypatch,
    *,
    inventory_error: Exception | None = None,
    visibility_error: Exception | None = None,
):
    calls: list[tuple[str, object]] = []
    uploaded_main = [{"location": "/cabinet/main.jpg", "fileId": "main-1"}]
    uploaded_description = [{"location": "/cabinet/description.jpg", "fileId": "desc-1"}]

    monkeypatch.setattr(crawler_service, "product_raw_payload", lambda _product: {"variants": {}})
    monkeypatch.setattr(crawler_service, "generate_listing_manage_number", lambda *_args: "manage-1")
    def upload_main(*_args, **kwargs):
        calls.append(("main_images", list(kwargs.get("source_images") or [])))
        calls.append(("main_require_complete", kwargs.get("require_complete")))
        return uploaded_main

    monkeypatch.setattr(crawler_service, "upload_product_images_to_rakuten", upload_main)
    def upload_description(*_args, **kwargs):
        calls.append(("description_images", None))
        calls.append(("description_require_complete", kwargs.get("require_complete")))
        return {
            "rawPayload": {"variants": {}},
            "uploadedImages": uploaded_description,
        }

    monkeypatch.setattr(
        crawler_service,
        "upload_product_description_images_to_rakuten",
        upload_description,
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
    def patch_visibility(*_args, **kwargs):
        calls.append(("visibility", kwargs.get("hide_item")))
        if visibility_error is not None:
            raise visibility_error

    monkeypatch.setattr(crawler_service, "patch_rakuten_item_visibility", patch_visibility)
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


def test_listing_creates_hidden_complete_item_then_publishes(monkeypatch):
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
    assert ("build", {"manageNumber": "manage-1", "hideItem": True}) in calls
    assert ("main_images", ["https://example.com/source.jpg"]) in calls
    assert ("main_require_complete", True) in calls
    assert ("description_images", None) in calls
    assert ("description_require_complete", True) in calls
    assert ("visibility", False) in calls
    assert result["payload"]["hideItem"] is False
    assert result["payload"]["listingImageCompletion"]["status"] == "success"


def test_listing_creation_uploads_all_main_and_description_images(monkeypatch):
    product, store = listing_product_and_store()
    calls = install_direct_listing_mocks(monkeypatch)
    source_images = [
        "https://example.com/1.jpg",
        "https://example.com/2.jpg",
        "https://example.com/3.jpg",
    ]
    monkeypatch.setattr(crawler_service, "product_images_for_edit", lambda _product: source_images)
    monkeypatch.setattr(
        crawler_service,
        "product_raw_payload",
        lambda _product: {
            "variants": {},
            "descriptions": [
                {"label": "商品説明", "value": '<img src="https://example.com/detail.jpg">'},
            ],
        },
    )
    result = crawler_service.create_store_product_on_rakuten(
        "secret",
        "key",
        store,
        product,
    )

    assert ("main_images", source_images) in calls
    assert ("description_images", None) in calls
    assert result["payload"]["listingImageCompletion"]["remainingMainImageCount"] == 0


def test_listing_inventory_failure_deletes_hidden_item_and_rolls_back_all_images(monkeypatch):
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
    rollback_payload = next(payload for name, payload in calls if name == "rollback")
    assert len(rollback_payload) == 2


def test_listing_publish_failure_deletes_hidden_item_and_rolls_back_all_images(monkeypatch):
    product, store = listing_product_and_store()
    calls = install_direct_listing_mocks(
        monkeypatch,
        visibility_error=RuntimeError("publish failed"),
    )

    with pytest.raises(RuntimeError, match="publish failed"):
        crawler_service.create_store_product_on_rakuten(
            "secret",
            "key",
            store,
            product,
        )

    names = [name for name, _payload in calls]
    assert names.count("put") == 1
    assert names.count("inventory") == 1
    assert names.count("visibility") == 1
    assert names.count("delete") == 1
    assert names.count("rollback") == 1
    rollback_payload = next(payload for name, payload in calls if name == "rollback")
    assert len(rollback_payload) == 2


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


def test_listing_image_preparation_reports_read_failures_as_missing(monkeypatch):
    monkeypatch.setattr(
        crawler_service,
        "load_product_image_bytes",
        lambda image_url, **_kwargs: (
            (_ for _ in ()).throw(RuntimeError("读取 OSS 商品图片失败。"))
            if image_url == "temporary-failure"
            else {
                "content": b"image",
                "suffix": ".jpg",
                "contentType": "image/jpeg",
            }
        ),
    )
    monkeypatch.setattr(
        crawler_service,
        "prepare_rakuten_cabinet_image",
        lambda image_data: image_data,
    )

    prepared = crawler_service.prepare_rakuten_listing_images(
        ["first", "temporary-failure", "second"],
    )

    assert [row["sourceUrl"] for row in prepared] == ["first", "second"]


def test_listing_image_preparation_skips_unsupported_suffix(monkeypatch):
    monkeypatch.setattr(
        crawler_service,
        "load_product_image_bytes",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            RuntimeError("图片格式只支持 jpg、jpeg、png、gif。")
        ),
    )

    assert crawler_service.prepare_rakuten_listing_images(["unsupported-image"]) == []


def test_listing_image_preparation_reuses_prepared_cache(monkeypatch):
    loaded_urls = []
    monkeypatch.setattr(
        crawler_service,
        "load_product_image_bytes",
        lambda image_url, **_kwargs: loaded_urls.append(image_url) or {
            "content": b"prepared-image",
            "suffix": ".jpg",
            "contentType": "image/jpeg",
        },
    )
    monkeypatch.setattr(
        crawler_service,
        "prepare_rakuten_cabinet_image",
        lambda _image_data: (_ for _ in ()).throw(AssertionError("cached image must not be recompressed")),
    )

    prepared = crawler_service.prepare_rakuten_listing_images(
        ["source-image"],
        prepared_image_map={"source-image": "prepared-image"},
    )

    assert loaded_urls == ["prepared-image"]
    assert prepared == [
        {
            "content": b"prepared-image",
            "suffix": ".jpg",
            "contentType": "image/jpeg",
            "sourceUrl": "source-image",
        }
    ]


def test_cabinet_preparation_reuses_compliant_source_image():
    from PIL import Image

    output = BytesIO()
    Image.new("RGB", (100, 100), (255, 255, 255)).save(output, format="JPEG")
    content = output.getvalue()

    prepared = crawler_service.prepare_rakuten_cabinet_image(
        {
            "content": content,
            "suffix": ".jpg",
            "contentType": "image/jpeg",
        }
    )

    assert prepared["content"] == content
    assert prepared["sourceReusable"] is True


def test_prepared_cache_reuses_local_source_url(monkeypatch):
    source_url = crawler_service.local_product_image_url(123, "source.jpg")
    monkeypatch.setattr(
        crawler_service,
        "store_product_image_content",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("compliant local image must not be duplicated")
        ),
    )

    prepared_url = crawler_service.store_prepared_rakuten_listing_image(
        123,
        {
            "sourceUrl": source_url,
            "content": b"image",
            "suffix": ".jpg",
            "contentType": "image/jpeg",
            "sourceReusable": True,
        },
    )

    assert prepared_url == source_url


def test_listing_preparation_cache_invalidates_after_product_change():
    product = SimpleNamespace(
        id=123,
        title="Original title",
        genre_id="123456",
        price=1000,
        image_url="source-image",
        raw_payload_json="{}",
        source_url="https://example.com/item",
        item_number="",
    )
    fingerprint = crawler_service.listing_preparation_source_fingerprint(product, {})
    product.raw_payload_json = json.dumps(
        {
            crawler_service.LISTING_PREPARATION_CACHE_KEY: {
                "version": crawler_service.LISTING_PREPARATION_CACHE_VERSION,
                "sourceFingerprint": fingerprint,
                "preflight": {"status": "passed"},
            }
        }
    )

    assert crawler_service.listing_preparation_cache(product)["preflight"]["status"] == "passed"

    product.title = "Changed title"

    assert crawler_service.listing_preparation_cache(product) == {}


def test_listing_preflight_blocks_when_preparation_cache_is_missing(monkeypatch):
    product, store = listing_product_and_store()
    monkeypatch.setattr(crawler_service, "listing_preparation_cache", lambda *_args, **_kwargs: {})
    monkeypatch.setattr(
        crawler_service,
        "_listing_preflight_product_check_uncached",
        lambda _product, _store: {
            "productId": product.id,
            "productCode": "product-123",
            "productTitle": product.title,
            "status": "passed",
            "issueCount": 0,
            "blockerCount": 0,
            "warningCount": 0,
            "issues": [],
            "preview": {},
        },
    )
    monkeypatch.setattr(crawler_service, "listing_preparation_ready", lambda *_args, **_kwargs: False)

    result = crawler_service.listing_preflight_product_check(product, store)

    assert result["status"] == "blocked"
    assert result["blockerCount"] == 1
    assert result["issues"][0]["code"] == "listing_preparation_required"


def test_collected_product_preparation_persists_preflight_and_image_cache(monkeypatch):
    product = SimpleNamespace(
        id=123,
        owner_username="owner",
        review_status="pending",
        title="Listing product",
        genre_id="123456",
        price=1000,
        image_url="source-image",
        source_url="https://example.com/item",
        item_number="",
        raw_payload_json=json.dumps({"descriptions": []}),
    )
    preparation = SimpleNamespace(source_fingerprint="", cache_json="{}")

    @contextmanager
    def local_session_scope():
        yield SimpleNamespace(
            get=lambda model, _product_id: (
                preparation
                if model is crawler_service.ProductListingPreparationModel
                else product
            ),
            flush=lambda: None,
        )

    monkeypatch.setattr(crawler_service, "session_scope", local_session_scope)
    monkeypatch.setattr(crawler_service, "product_images_for_edit", lambda _product: ["source-image"])
    monkeypatch.setattr(crawler_service, "product_descriptions", lambda _payload: [])
    monkeypatch.setattr(
        crawler_service,
        "_listing_preflight_product_check_uncached",
        lambda _product, _store: {"status": "passed", "issues": []},
    )
    monkeypatch.setattr(
        crawler_service,
        "prepare_rakuten_listing_images",
        lambda _images: [
            {
                "sourceUrl": "source-image",
                "content": b"prepared-image",
                "suffix": ".jpg",
                "contentType": "image/jpeg",
            }
        ],
    )
    monkeypatch.setattr(
        crawler_service,
        "store_prepared_rakuten_listing_image",
        lambda _product_id, _image: "prepared-image",
    )
    monkeypatch.setattr(
        crawler_service,
        "collect_local_product_image_urls",
        lambda _payload: ["source-image", "prepared-image"],
    )
    cleanup_calls = []
    monkeypatch.setattr(
        crawler_service,
        "remove_unused_local_product_images",
        lambda product_id, urls: cleanup_calls.append((product_id, urls)),
    )

    result = crawler_service.prepare_collected_product_for_listing(None, 123)
    saved_cache = json.loads(preparation.cache_json)

    assert result["preflight"]["status"] == "passed"
    assert saved_cache["images"] == [
        {"sourceUrl": "source-image", "preparedUrl": "prepared-image"}
    ]
    assert cleanup_calls == [(123, ["source-image", "prepared-image"])]


def test_collected_product_preparation_removes_unreadable_images(monkeypatch):
    first_image = "https://example.com/first.jpg"
    broken_image = "https://example.com/broken.jpg"
    product = SimpleNamespace(
        id=123,
        owner_username="owner",
        review_status="pending",
        title="Listing product",
        genre_id="123456",
        price=1000,
        image_url=first_image,
        source_url="https://example.com/item",
        item_number="",
        raw_payload_json=json.dumps({"images": [first_image, broken_image]}),
    )
    preparation = SimpleNamespace(source_fingerprint="", cache_json="{}")

    @contextmanager
    def local_session_scope():
        yield SimpleNamespace(
            get=lambda model, _product_id: (
                preparation
                if model is crawler_service.ProductListingPreparationModel
                else product
            ),
            flush=lambda: None,
        )

    monkeypatch.setattr(crawler_service, "session_scope", local_session_scope)
    monkeypatch.setattr(crawler_service, "product_images_for_edit", lambda _product: [first_image, broken_image])
    monkeypatch.setattr(crawler_service, "product_descriptions", lambda _payload: [])
    monkeypatch.setattr(
        crawler_service,
        "_listing_preflight_product_check_uncached",
        lambda _product, _store: {
            "status": "passed",
            "issueCount": 0,
            "blockerCount": 0,
            "warningCount": 0,
            "issues": [],
        },
    )
    monkeypatch.setattr(
        crawler_service,
        "prepare_rakuten_listing_images",
        lambda _images: [
            {
                "sourceUrl": first_image,
                "content": b"prepared-image",
                "suffix": ".jpg",
                "contentType": "image/jpeg",
            }
        ],
    )
    monkeypatch.setattr(
        crawler_service,
        "store_prepared_rakuten_listing_image",
        lambda _product_id, _image: "prepared-image",
    )
    monkeypatch.setattr(crawler_service, "collect_local_product_image_urls", lambda _payload: [])
    monkeypatch.setattr(crawler_service, "remove_unused_local_product_images", lambda *_args: None)

    result = crawler_service.prepare_collected_product_for_listing(None, 123)
    saved_payload = json.loads(product.raw_payload_json)

    assert result["removedImageCount"] == 1
    assert result["missingImageCount"] == 0
    assert saved_payload["images"] == [first_image]
    assert saved_payload["ltListingPreparationRemovedImages"] == [broken_image]


def test_listing_main_image_upload_rejects_incomplete_preparation(monkeypatch):
    product, store = listing_product_and_store()
    monkeypatch.setattr(
        crawler_service,
        "recover_missing_local_product_images",
        lambda _product, images: images,
    )
    monkeypatch.setattr(
        crawler_service,
        "prepare_rakuten_listing_images",
        lambda _images, **_kwargs: [
            {
                "sourceUrl": "https://example.com/first.jpg",
                "content": b"image",
                "suffix": ".jpg",
                "contentType": "image/jpeg",
            }
        ],
    )
    rollback_calls = []
    monkeypatch.setattr(
        crawler_service,
        "rollback_uploaded_listing_images",
        lambda _secret, _key, images: rollback_calls.append(images) or "",
    )

    with pytest.raises(RuntimeError, match=r"商品主图未能完整读取（缺少 1 张）"):
        crawler_service.upload_product_images_to_rakuten(
            "secret",
            "key",
            store,
            product,
            "manage-1",
            source_images=[
                "https://example.com/first.jpg",
                "https://example.com/missing.jpg",
            ],
            require_complete=True,
        )

    assert rollback_calls == [[]]


def test_listing_description_image_upload_rejects_incomplete_preparation(monkeypatch):
    product, store = listing_product_and_store()
    raw_payload = {
        "descriptions": [
            {
                "label": "PC用 商品説明文",
                "value": (
                    '<img src="https://example.com/first.jpg">'
                    '<img src="https://example.com/missing.jpg">'
                ),
            }
        ]
    }
    monkeypatch.setattr(
        crawler_service,
        "prepare_rakuten_listing_images",
        lambda _images, **_kwargs: [
            {
                "sourceUrl": "https://example.com/first.jpg",
                "content": b"image",
                "suffix": ".jpg",
                "contentType": "image/jpeg",
            }
        ],
    )
    rollback_calls = []
    monkeypatch.setattr(
        crawler_service,
        "rollback_uploaded_listing_images",
        lambda _secret, _key, images: rollback_calls.append(images) or "",
    )

    with pytest.raises(RuntimeError, match=r"商品说明图未能完整读取（缺少 1 张）"):
        crawler_service.upload_product_description_images_to_rakuten(
            "secret",
            "key",
            store,
            product,
            "manage-1",
            raw_payload,
            require_complete=True,
        )

    assert rollback_calls == [[]]


def test_preparation_ready_trusts_valid_cache_without_storage_checks(monkeypatch):
    product = SimpleNamespace(
        id=123,
        title="Listing product",
        genre_id="123456",
        price=1000,
        image_url="source-image",
        raw_payload_json="{}",
        source_url="https://example.com/item",
        item_number="",
    )
    base_payload = {"images": ["source-image"]}
    fingerprint = crawler_service.listing_preparation_source_fingerprint(product, base_payload)
    payload = {
        **base_payload,
        crawler_service.LISTING_PREPARATION_CACHE_KEY: {
            "version": crawler_service.LISTING_PREPARATION_CACHE_VERSION,
            "sourceFingerprint": fingerprint,
            "missingImageCount": 0,
            "images": [
                {"sourceUrl": "source-image", "preparedUrl": "prepared-image"}
            ],
        },
    }

    def fail_if_storage_checked(*_args, **_kwargs):
        raise AssertionError("valid cache must not trigger per-image storage checks")

    monkeypatch.setattr(crawler_service, "listing_prepared_image_map", fail_if_storage_checked)
    monkeypatch.setattr(
        crawler_service,
        "listing_preparation_expected_image_urls",
        fail_if_storage_checked,
    )

    assert crawler_service.listing_preparation_ready(product, payload) is True


def test_preparation_ready_rejects_cache_with_missing_images(monkeypatch):
    product = SimpleNamespace(
        id=123,
        title="Listing product",
        genre_id="123456",
        price=1000,
        image_url="source-image",
        raw_payload_json="{}",
        source_url="https://example.com/item",
        item_number="",
    )
    base_payload = {"images": ["source-image"]}
    fingerprint = crawler_service.listing_preparation_source_fingerprint(product, base_payload)
    payload = {
        **base_payload,
        crawler_service.LISTING_PREPARATION_CACHE_KEY: {
            "version": crawler_service.LISTING_PREPARATION_CACHE_VERSION,
            "sourceFingerprint": fingerprint,
            "missingImageCount": 2,
            "images": [],
        },
    }

    monkeypatch.setattr(
        crawler_service,
        "listing_prepared_image_map",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("storage must not be checked when cache reports missing images")
        ),
    )

    assert crawler_service.listing_preparation_ready(product, payload) is False


def test_preparation_ready_falls_back_to_full_check_without_image_list(monkeypatch):
    product = SimpleNamespace(
        id=123,
        title="Listing product",
        genre_id="123456",
        price=1000,
        image_url="source-image",
        raw_payload_json="{}",
        source_url="https://example.com/item",
        item_number="",
    )
    base_payload = {"images": ["source-image"]}
    fingerprint = crawler_service.listing_preparation_source_fingerprint(product, base_payload)
    payload = {
        **base_payload,
        crawler_service.LISTING_PREPARATION_CACHE_KEY: {
            "version": crawler_service.LISTING_PREPARATION_CACHE_VERSION,
            "sourceFingerprint": fingerprint,
            "missingImageCount": 0,
        },
    }

    monkeypatch.setattr(
        crawler_service,
        "listing_preparation_expected_image_urls",
        lambda *_args, **_kwargs: ["source-image"],
    )
    monkeypatch.setattr(
        crawler_service,
        "listing_prepared_image_map",
        lambda *_args, **_kwargs: {"source-image": "prepared-image"},
    )

    assert crawler_service.listing_preparation_ready(product, payload) is True


def test_listing_preflight_passes_fetched_cache_into_ready(monkeypatch):
    product = SimpleNamespace(
        id=123,
        title="Listing product",
        source_url="https://item.rakuten.co.jp/source-shop/item-1/",
        image_url="https://example.com/source.jpg",
        raw_payload_json="{}",
        review_status="approved",
        rakuten_manage_number=None,
        item_number="",
        genre_id="123456",
        price=1000,
    )
    store = SimpleNamespace(id=7, store_code="shop-code", store_name="Store", alias_name="Store 7")
    base_payload: dict = {}
    fingerprint = crawler_service.listing_preparation_source_fingerprint(product, base_payload)
    cached = {
        "version": crawler_service.LISTING_PREPARATION_CACHE_VERSION,
        "sourceFingerprint": fingerprint,
        "preflight": {
            "status": "passed",
            "issues": [],
            "issueCount": 0,
            "blockerCount": 0,
            "warningCount": 0,
        },
    }
    product.raw_payload_json = json.dumps(
        {crawler_service.LISTING_PREPARATION_CACHE_KEY: cached}
    )
    seen: dict[str, object] = {}

    def fake_ready(_product, _raw_payload, prepared_cache=None):
        seen["prepared_cache"] = prepared_cache
        return True

    monkeypatch.setattr(crawler_service, "listing_preparation_ready", fake_ready)

    result = crawler_service.listing_preflight_product_check(product, store)

    assert result["status"] == "passed"
    assert seen["prepared_cache"] == cached


def test_auto_listing_candidates_preload_preparation_caches(
    monkeypatch,
    session_factory,
):
    install_session_scope(monkeypatch, session_factory)
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
            store_code="shop-a",
            store_name="店铺 A",
            enabled=True,
        )
        session.add(store)
        session.flush()
        products = [
            ProductModel(
                owner_username="alice",
                title=f"商品 {index}",
                source_url=f"https://example.com/{index}",
                source_url_hash=f"hash-{index}",
                review_status="approved",
                raw_payload_json="{}",
            )
            for index in range(1, 4)
        ]
        session.add_all(products)
        session.flush()
        products[0].raw_payload_json = json.dumps(
            {"listedStores": [{"storeId": store.id}]}
        )
        session.add(
            ProductListingPreparationModel(
                product_id=products[1].id,
                source_fingerprint="fp",
                cache_json=json.dumps(
                    {"version": 1, "preflight": {"status": "passed"}}
                ),
            )
        )
        session.commit()
        store_id = store.id
        product_ids = [int(product.id) for product in products]

    seen_checks: list[tuple[int, object]] = []

    def fake_check(product, _store):
        seen_checks.append(
            (int(product.id), getattr(product, "_listing_preparation_cache_payload", None))
        )
        return {"productId": product.id, "issues": []}

    monkeypatch.setattr(crawler_service, "listing_preflight_product_check", fake_check)

    selected, preflight_by_id = crawler_service.auto_listing_candidate_product_ids(
        "alice",
        store_id,
        2,
    )

    assert selected == [product_ids[1], product_ids[2]]
    assert set(preflight_by_id) == set(selected)
    assert [product_id for product_id, _cache in seen_checks] == [
        product_ids[1],
        product_ids[2],
    ]
    assert seen_checks[0][1] == {"version": 1, "preflight": {"status": "passed"}}
    assert seen_checks[1][1] == {}


def test_auto_listing_candidates_paginate_in_created_order(
    monkeypatch,
    session_factory,
):
    install_session_scope(monkeypatch, session_factory)
    monkeypatch.setattr(crawler_service, "AUTO_LISTING_CANDIDATE_PAGE_SIZE", 2)
    from datetime import datetime as dt

    with session_factory() as session:
        session.add(
            UserAccountModel(
                username="alice",
                display_name="alice",
                password_salt_b64="salt",
                password_hash_b64="hash",
            )
        )
        store = StoreModel(owner_username="alice", store_code="shop-a", store_name="店铺 A", enabled=True)
        session.add(store)
        session.flush()
        created_bases = [dt(2026, 1, index + 1, 8, 0, 0) for index in range(6)]
        products = [
            ProductModel(
                owner_username="alice",
                title=f"商品 {index}",
                source_url=f"https://example.com/{index}",
                source_url_hash=f"hash-{index}",
                review_status="approved",
                raw_payload_json="{}",
                created_at=created_bases[index],
            )
            for index in range(6)
        ]
        session.add_all(products)
        session.flush()
        # 第 3 个商品正在上架中,应当被跳过
        products[2].listing_task_id = "busy-task"
        session.commit()
        store_id = store.id
        expected_ids = [int(products[0].id), int(products[1].id), int(products[3].id), int(products[4].id)]

    monkeypatch.setattr(
        crawler_service,
        "listing_preflight_product_check",
        lambda product, _store: {"productId": product.id, "issues": []},
    )

    selected, preflight_by_id = crawler_service.auto_listing_candidate_product_ids(
        "alice",
        store_id,
        4,
    )

    assert selected == expected_ids
    assert set(preflight_by_id) == set(selected)


def _seed_listing_products_and_store(session):
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
        store_code="shop-a",
        store_name="店铺 A",
        alias_name="A 店",
        enabled=True,
        rakuten_service_secret_encrypted="secret",
        rakuten_license_key_encrypted="key",
    )
    session.add(store)
    session.flush()
    products = [
        ProductModel(
            owner_username="alice",
            title=f"商品 {index}",
            source_url=f"https://example.com/{index}",
            source_url_hash=f"hash-{index}",
            review_status="approved",
            raw_payload_json="{}",
        )
        for index in range(1, 4)
    ]
    session.add_all(products)
    session.flush()
    return store, products


def test_create_listing_task_skips_redundant_checks_when_prechecked(
    monkeypatch,
    session_factory,
):
    install_session_scope(monkeypatch, session_factory)
    with session_factory() as session:
        store, products = _seed_listing_products_and_store(session)
        product_ids = [int(product.id) for product in products]
        store_id = store.id
        session.commit()

    monkeypatch.setattr(
        crawler_service,
        "ensure_system_task_dispatch_allowed",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(crawler_service, "decrypt_text", lambda value: value or "")
    monkeypatch.setattr(crawler_service, "dispatch_next_listing_task", lambda: None)
    monkeypatch.setattr(
        crawler_service,
        "partition_product_ids_by_active_task_conflicts",
        lambda _session, _owner, _store_ids, ids: (list(ids), []),
    )
    monkeypatch.setattr(
        crawler_service,
        "listing_task_to_public",
        lambda row: {"id": row.id, "status": row.status},
    )
    monkeypatch.setattr(
        crawler_service,
        "listing_preflight_product_check",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("preflight must be skipped when prechecked")
        ),
    )
    monkeypatch.setattr(
        crawler_service,
        "product_raw_payload",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("payload must not be parsed when prechecked")
        ),
    )

    result = crawler_service.create_listing_task(
        "alice",
        SimpleNamespace(
            productIds=product_ids,
            storeIds=[store_id],
            taskName="批量上架",
        ),
        preflight_by_id={
            product_id: {"productId": product_id, "issues": []}
            for product_id in product_ids
        },
    )

    assert result["summary"]["total"] == 3
    assert result["summary"]["taskCount"] == 1
    assert result["listingTask"]["status"] == "queued"
    with session_factory() as session:
        tasks = session.scalars(select(ListingTaskModel)).all()
        assert len(tasks) == 1
        assert tasks[0].total_count == 3


def test_create_listing_task_still_preflights_without_prechecked(
    monkeypatch,
    session_factory,
):
    install_session_scope(monkeypatch, session_factory)
    with session_factory() as session:
        store, products = _seed_listing_products_and_store(session)
        product_ids = [int(product.id) for product in products]
        store_id = store.id
        session.commit()

    monkeypatch.setattr(
        crawler_service,
        "ensure_system_task_dispatch_allowed",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(crawler_service, "decrypt_text", lambda value: value or "")
    monkeypatch.setattr(crawler_service, "dispatch_next_listing_task", lambda: None)
    monkeypatch.setattr(
        crawler_service,
        "partition_product_ids_by_active_task_conflicts",
        lambda _session, _owner, _store_ids, ids: (list(ids), []),
    )
    monkeypatch.setattr(
        crawler_service,
        "listing_task_to_public",
        lambda row: {"id": row.id, "status": row.status},
    )
    checked: list[int] = []
    monkeypatch.setattr(
        crawler_service,
        "listing_preflight_product_check",
        lambda product, _store: checked.append(int(product.id))
        or {"productId": product.id, "issues": []},
    )

    result = crawler_service.create_listing_task(
        "alice",
        SimpleNamespace(
            productIds=product_ids,
            storeIds=[store_id],
            taskName="批量上架",
        ),
    )

    assert result["summary"]["taskCount"] == 1
    assert checked == sorted(product_ids)


def test_listing_task_limit_remains_fifty():
    assert crawler_service.BATCH_TASK_PRODUCT_LIMIT == 50
    assert [len(chunk) for chunk in crawler_service.chunk_product_ids(list(range(1, 102)))] == [50, 50, 1]


def test_listing_retry_uses_lower_product_concurrency(monkeypatch):
    monkeypatch.setattr(crawler_service.settings, "listing_product_workers", 2)
    monkeypatch.setattr(crawler_service.settings, "listing_retry_product_workers", 1)

    assert crawler_service.listing_task_product_worker_count(50, retry=False) == 2
    assert crawler_service.listing_task_product_worker_count(50, retry=True) == 1
    assert crawler_service.listing_task_product_worker_count(1, retry=True) == 1


def test_rakuten_cabinet_request_interval_uses_configured_cooldown(monkeypatch):
    monkeypatch.setattr(
        crawler_service.settings,
        "rakuten_cabinet_request_min_interval_seconds",
        0.8,
    )
    monkeypatch.setattr(
        crawler_service.settings,
        "rakuten_cabinet_qps_cooldown_interval_seconds",
        1.5,
    )
    monkeypatch.setattr(crawler_service, "should_use_redis_task_queue", lambda: True)

    class FakeRedis:
        @staticmethod
        def exists(_key):
            return 1

    monkeypatch.setattr(crawler_service, "redis_connection", lambda: FakeRedis())

    assert crawler_service.rakuten_cabinet_request_interval("shop") == 1.5


def test_listing_dispatch_uses_global_user_and_store_capacity(
    monkeypatch,
    session_factory,
):
    install_session_scope(monkeypatch, session_factory)
    monkeypatch.setattr(crawler_service, "finalize_stale_cancel_requested_tasks", lambda *_args, **_kwargs: 0)
    monkeypatch.setattr(crawler_service, "reconcile_interrupted_running_tasks", lambda *_args, **_kwargs: 0)
    monkeypatch.setattr(crawler_service, "should_use_redis_task_queue", lambda: False)
    monkeypatch.setattr(crawler_service.settings, "max_running_listing_tasks_global", 8)
    monkeypatch.setattr(crawler_service.settings, "max_running_listing_tasks_per_user", 1)
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
        ("bob", "b1"),
    ]
    with session_factory() as session:
        for task_id in ("a2", "a3"):
            delayed = session.get(ListingTaskModel, task_id)
            assert delayed is not None
            assert "并发额度" in delayed.message


def test_listing_dispatch_gives_eight_users_one_slot_each(
    monkeypatch,
    session_factory,
):
    install_session_scope(monkeypatch, session_factory)
    monkeypatch.setattr(
        crawler_service,
        "finalize_stale_cancel_requested_tasks",
        lambda *_args, **_kwargs: 0,
    )
    monkeypatch.setattr(
        crawler_service,
        "reconcile_interrupted_running_tasks",
        lambda *_args, **_kwargs: 0,
    )
    monkeypatch.setattr(
        crawler_service,
        "should_use_redis_task_queue",
        lambda: False,
    )
    monkeypatch.setattr(
        crawler_service.settings,
        "max_running_listing_tasks_global",
        8,
    )
    monkeypatch.setattr(
        crawler_service.settings,
        "max_running_listing_tasks_per_user",
        1,
    )
    monkeypatch.setattr(
        crawler_service.settings,
        "max_running_listing_tasks_per_store",
        1,
    )
    dispatched: list[tuple[str, str]] = []
    monkeypatch.setattr(
        crawler_service,
        "dispatch_listing_task",
        lambda owner, task_id: dispatched.append((owner, task_id)),
    )

    with session_factory() as session:
        for index in range(1, 11):
            username = f"user-{index}"
            session.add(
                UserAccountModel(
                    username=username,
                    display_name=username,
                    password_salt_b64="salt",
                    password_hash_b64="hash",
                )
            )
            store = StoreModel(
                owner_username=username,
                store_code=f"store-{index}",
                store_name=f"Store {index}",
            )
            session.add(store)
            session.flush()
            session.add(
                ListingTaskModel(
                    id=f"task-{index}",
                    owner_username=username,
                    store_id=store.id,
                    task_name=f"Task {index}",
                    status="queued",
                    product_ids_json=json.dumps(
                        {
                            "productIds": [index],
                            "storeIds": [store.id],
                        }
                    ),
                )
            )
        session.commit()

    crawler_service.dispatch_next_listing_task()

    assert len(dispatched) == 8
    assert len({owner for owner, _task_id in dispatched}) == 8
    assert all(
        task_id == f"task-{owner.removeprefix('user-')}"
        for owner, task_id in dispatched
    )


def test_listing_task_processes_two_products_concurrently(
    monkeypatch,
    session_factory,
):
    install_session_scope(monkeypatch, session_factory)
    monkeypatch.setattr(crawler_service.settings, "listing_product_workers", 2)
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
    all_workers_started = threading.Barrier(2)

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
        all_workers_started.wait(timeout=2)
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
            for index in range(2)
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
                total_count=2,
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

    assert max_active == 2
    with session_factory() as session:
        task = session.get(ListingTaskModel, "listing-task")
        assert task is not None
        assert task.status == "success"
        assert task.success_count == 2
        assert task.failed_count == 0
